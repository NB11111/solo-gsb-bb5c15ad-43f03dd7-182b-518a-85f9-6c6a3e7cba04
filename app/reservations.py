"""会议预约核心逻辑：幂等创建、改期、取消与单事务冲突检测。

- 参与者 ID 去重并按字典序排序后，作为规范化请求内容参与幂等比较；
- 区间一律按半开 ``[start, end)`` 处理：首尾相接（相邻）不算冲突；
- 冲突检查与写入在同一个 ``BEGIN IMMEDIATE`` 事务内完成：
  SQLite 在事务开始时即取写锁，并发请求被串行化；
  任一参与者与已有预约重叠都会整体回滚，不会留下部分写入。
- 每条预约带单调递增的 ``version``（创建为 1）；改期/取消采用乐观并发控制：
  请求须携带 ``expected_version``，匹配才执行，成功后 ``version`` 加 1。
- 取消是软删除：``status`` 置为 ``canceled`` 并记录 ``canceled_at``，
  同事务删除全部参与者占用行使时段立即可再预约；预约行保留，
  使相同 ``expected_version`` 的取消重试能幂等返回同一结果，
  且取消后的改期请求被拒绝（不会复活）。
- 每次**成功**的创建/改期/取消都在同一事务内向 ``reservation_events``
  追加哈希链审计事件（:mod:`app.audit`）；失败请求与幂等重放不产生事件。
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

from . import audit
from . import db

# 定宽 UTC 文本格式：字典序即时间序，可直接用于 SQL 范围比较；
# 审计事件的时间戳与快照共用同一格式（定义在 audit 模块）
_CANON_FMT = audit.CANON_FMT


class ReservationConflictError(Exception):
    """至少一位参与者与已有预约或有效临时保留存在半开区间重叠。"""

    def __init__(self, conflicts: list[dict[str, object]]) -> None:
        # 冲突项形如 {"reservation_id": ..., "participant_id": ...}
        # 或       {"hold_id": ..., "participant_id": ...}
        self.conflicts = conflicts

        def _describe(c: dict[str, object]) -> str:
            if "hold_id" in c:
                return (f"participant {c['participant_id']!r} overlaps "
                        f"reservation hold #{c['hold_id']}")
            return (f"participant {c['participant_id']!r} overlaps "
                    f"reservation #{c['reservation_id']}")

        pretty = ", ".join(_describe(c) for c in conflicts)
        super().__init__(f"reservation conflict: {pretty}")


class RequestIdConflictError(Exception):
    """request_id 已被内容不同的请求占用。"""

    def __init__(self, existing_reservation_id: int) -> None:
        self.existing_reservation_id = existing_reservation_id
        super().__init__(
            f"request_id is already used by reservation #{existing_reservation_id} "
            "with different content"
        )


class ReservationNotFoundError(Exception):
    """指定 ID 的预约不存在。"""

    def __init__(self, reservation_id: int) -> None:
        self.reservation_id = reservation_id
        super().__init__(f"reservation #{reservation_id} not found")


class VersionConflictError(Exception):
    """expected_version 与预约当前版本不一致（乐观锁）。"""

    def __init__(self, reservation_id: int, current_version: int) -> None:
        self.reservation_id = reservation_id
        self.current_version = current_version
        super().__init__(
            f"expected_version does not match current version of reservation "
            f"#{reservation_id}"
        )


class ReservationCanceledError(Exception):
    """预约已取消：不可再改期（取消是终态，不会复活）。"""

    def __init__(self, reservation_id: int, current_version: int) -> None:
        self.reservation_id = reservation_id
        self.current_version = current_version
        super().__init__(f"reservation #{reservation_id} has been canceled")


@dataclass(frozen=True)
class Reservation:
    id: int
    request_id: str
    start_utc: datetime
    end_utc: datetime
    participants: tuple[str, ...]
    created_at: datetime
    version: int = 1
    status: str = "active"  # active / canceled
    canceled_at: datetime | None = None


_RESERVATION_COLUMNS = (
    "id, request_id, start_utc, end_utc, participants_key, version,"
    " status, canceled_at, created_at"
)


def _canon(dt: datetime) -> str:
    """统一成定宽 UTC 字符串，保证字典序与时间序一致。"""
    return dt.astimezone(timezone.utc).strftime(_CANON_FMT)


def _parse(text: str) -> datetime:
    return datetime.strptime(text, _CANON_FMT).replace(tzinfo=timezone.utc)


def normalize_participants(participant_ids: Iterable[str]) -> tuple[str, ...]:
    """去重 + 字典序排序，得到规范化参与者列表。"""
    return tuple(sorted(set(participant_ids)))


def _row_to_reservation(row: sqlite3.Row) -> Reservation:
    canceled_raw = row["canceled_at"]
    return Reservation(
        id=row["id"],
        request_id=row["request_id"],
        start_utc=_parse(row["start_utc"]),
        end_utc=_parse(row["end_utc"]),
        participants=tuple(json.loads(row["participants_key"])),
        created_at=_parse(row["created_at"]),
        version=row["version"],
        status=row["status"],
        canceled_at=_parse(canceled_raw) if canceled_raw else None,
    )


def load_reservation(
    conn: sqlite3.Connection, reservation_id: int
) -> Reservation | None:
    """在调用方已打开的连接/事务内按 ID 读取预约，不存在返回 ``None``。"""
    row = conn.execute(
        f"SELECT {_RESERVATION_COLUMNS} FROM reservations WHERE id = ?",
        (reservation_id,),
    ).fetchone()
    return _row_to_reservation(row) if row is not None else None


def _expire_due_holds(conn: sqlite3.Connection, now_s: str) -> None:
    """事务内惰性过期：把所有已到期的 pending 保留标记为 ``expired``。

    在每个 ``BEGIN IMMEDIATE`` 事务入口调用：状态翻转与随后的冲突检查
    在同一事务完成，过期保留不再占用时段。惰性导入避免与 holds 模块循环导入。
    """
    from . import holds
    holds.expire_due_holds(conn, now_s)


def collect_reservation_conflicts(
    conn: sqlite3.Connection,
    participants: tuple[str, ...],
    start_s: str,
    end_s: str,
    *,
    exclude_reservation_id: int | None = None,
) -> list[dict[str, object]]:
    """收集与**进行中预约**的半开区间冲突（相邻区间因等号不成立而不命中）。

    ``exclude_reservation_id`` 用于改期路径排除自身旧占用。
    """
    conflicts: list[dict[str, object]] = []
    sql = (
        "SELECT reservation_id, participant_id"
        " FROM reservation_participants"
        " WHERE participant_id = ? AND start_utc < ? AND ? < end_utc"
    )
    params: list[object] = []
    if exclude_reservation_id is not None:
        sql += " AND reservation_id != ?"
        params.append(exclude_reservation_id)
    sql += " ORDER BY reservation_id"
    for pid in participants:
        for hit in conn.execute(sql, (pid, end_s, start_s, *params)):
            conflicts.append({
                "reservation_id": hit["reservation_id"],
                "participant_id": hit["participant_id"],
            })
    return conflicts


def _collect_hold_conflicts(
    conn: sqlite3.Connection,
    participants: tuple[str, ...],
    start_s: str,
    end_s: str,
    now_s: str,
) -> list[dict[str, object]]:
    """收集与**未过期 pending 临时保留**的半开区间冲突（惰性导入避免循环）。"""
    from . import holds
    return holds.collect_hold_conflicts(
        conn, participants, start_s, end_s, now_s)


def insert_reservation(
    conn: sqlite3.Connection,
    *,
    request_id: str,
    start_s: str,
    end_s: str,
    participants: tuple[str, ...],
    now_s: str,
) -> int:
    """在调用方事务内写入预约 + 参与者占用 + ``created`` 审计事件，返回新预约 ID。

    供创建接口与临时保留确认接口共用：不自行提交，与调用方业务写入同生共死。
    """
    participants_key = json.dumps(list(participants), ensure_ascii=False)
    cursor = conn.execute(
        "INSERT INTO reservations"
        " (request_id, start_utc, end_utc, participants_key, version, created_at)"
        " VALUES (?, ?, ?, ?, 1, ?)",
        (request_id, start_s, end_s, participants_key, now_s),
    )
    reservation_id = int(cursor.lastrowid)
    conn.executemany(
        "INSERT INTO reservation_participants"
        " (reservation_id, participant_id, start_utc, end_utc)"
        " VALUES (?, ?, ?, ?)",
        [(reservation_id, pid, start_s, end_s) for pid in participants],
    )
    # 审计事件与业务写入同一事务：COMMIT 前任一失败则整体回滚，
    # 不会出现“有预约无事件”或“有事件无预约”
    snapshot = audit.build_snapshot(
        reservation_id=reservation_id,
        request_id=request_id,
        start_utc=start_s,
        end_utc=end_s,
        participants=participants,
        version=1,
        status="active",
        canceled_at=None,
        created_at=now_s,
    )
    audit.append_event(
        conn,
        reservation_id=reservation_id,
        action="created",
        timestamp=now_s,
        snapshot=snapshot,
    )
    return reservation_id


def create_reservation(
    request_id: str,
    start_utc: datetime,
    end_utc: datetime,
    participant_ids: Iterable[str],
    db_path: str | None = None,
) -> tuple[Reservation, bool]:
    """创建预约，返回 ``(预约, 是否新建)``。

    - ``request_id`` 已存在且规范化内容（起止时刻 + 去重排序后的参与者）相同
      → 返回原预约，``created=False``，不新增记录；
    - ``request_id`` 已存在但内容不同 → :class:`RequestIdConflictError`；
    - 任一参与者与已有预约半开区间重叠 → :class:`ReservationConflictError`，
      整体回滚，不留下任何部分写入。
    """
    participants = normalize_participants(participant_ids)
    start_s = _canon(start_utc)
    end_s = _canon(end_utc)
    participants_key = json.dumps(list(participants), ensure_ascii=False)

    path = db.ensure_initialized(db_path)
    conn = db.connect(path)
    try:
        # BEGIN IMMEDIATE：事务开始即持有数据库写锁，
        # 使“检查 + 写入”对并发请求原子（并发写者被串行化）。
        conn.execute("BEGIN IMMEDIATE")
        try:
            now_s = _canon(datetime.now(timezone.utc))
            # 事务内先惰性过期到期保留，随后冲突检查只面对有效占用
            _expire_due_holds(conn, now_s)

            row = conn.execute(
                f"SELECT {_RESERVATION_COLUMNS}"
                " FROM reservations WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if row is not None:
                if (row["start_utc"] == start_s
                        and row["end_utc"] == end_s
                        and row["participants_key"] == participants_key):
                    conn.execute("COMMIT")
                    return _row_to_reservation(row), False
                raise RequestIdConflictError(row["id"])

            # request_id 命名空间与候补队列共用：pending 候补若已占用该键，
            # 内容不同 → 409（同内容的预订由候补晋升路径接管，见 waitlist.join）。
            from . import waitlist as waitlist_core
            wl_row = conn.execute(
                "SELECT id, start_utc, end_utc, participants_key, status"
                " FROM reservation_waitlist WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if wl_row is not None and wl_row["status"] == "pending":
                if not (wl_row["start_utc"] == start_s
                        and wl_row["end_utc"] == end_s
                        and wl_row["participants_key"] == participants_key):
                    raise waitlist_core.WaitlistContentConflictError(wl_row["id"])

            conflicts = collect_reservation_conflicts(
                conn, participants, start_s, end_s)
            conflicts.extend(_collect_hold_conflicts(
                conn, participants, start_s, end_s, now_s))
            if conflicts:
                raise ReservationConflictError(conflicts)

            reservation_id = insert_reservation(
                conn,
                request_id=request_id,
                start_s=start_s,
                end_s=end_s,
                participants=participants,
                now_s=now_s,
            )
            # 同 request_id 的 pending 候补由本次直订兑现：原子回填为 promoted
            if wl_row is not None:
                waitlist_core.mark_promoted(
                    conn, waitlist_id=wl_row["id"],
                    reservation_id=reservation_id, now_s=now_s)
            conn.execute("COMMIT")
            return Reservation(
                id=reservation_id,
                request_id=request_id,
                start_utc=_parse(start_s),
                end_utc=_parse(end_s),
                participants=participants,
                created_at=_parse(now_s),
                version=1,
            ), True
        except BaseException:
            # 无活动事务时 rollback 为 no-op；任何失败都保证整体回滚
            conn.rollback()
            raise
    finally:
        conn.close()


def reschedule_reservation(
    reservation_id: int,
    expected_version: int,
    start_utc: datetime,
    end_utc: datetime,
    participant_ids: Iterable[str],
    db_path: str | None = None,
) -> Reservation:
    """改期：原子替换预约时间与参与者占用，成功后 ``version`` 加 1。

    整个流程在单个 ``BEGIN IMMEDIATE`` 事务内完成，任何一步失败都回滚，
    预约行与参与者占用均保持原状：

    1. 预约不存在 → :class:`ReservationNotFoundError`（API 层映射为 404）；
    2. 预约已取消 → :class:`ReservationCanceledError`（409）：
       取消是终态，改期不会使其复活；
    3. ``expected_version`` 与当前版本不一致 → :class:`VersionConflictError`
       （409，携带 ``current_version``）；
    4. 半开区间冲突检查：沿用创建时的重叠判定，但通过
       ``reservation_id != ?`` **排除自身**（自身旧占用不算冲突）；
       有冲突 → :class:`ReservationConflictError`（409 + 冲突明细）。

    成功时更新 ``reservations`` 行（时间、规范化参与者、version+1）并整体
    替换 ``reservation_participants`` 占用；``request_id`` 与 ``created_at``
    保持不变。
    """
    participants = normalize_participants(participant_ids)
    start_s = _canon(start_utc)
    end_s = _canon(end_utc)
    participants_key = json.dumps(list(participants), ensure_ascii=False)

    path = db.ensure_initialized(db_path)
    conn = db.connect(path)
    try:
        # BEGIN IMMEDIATE：与创建路径一致，事务开始即持写锁，
        # “存在性 + 版本 + 冲突检查 + 替换” 对并发请求原子。
        conn.execute("BEGIN IMMEDIATE")
        try:
            now_s = _canon(datetime.now(timezone.utc))
            _expire_due_holds(conn, now_s)

            row = conn.execute(
                f"SELECT {_RESERVATION_COLUMNS} FROM reservations WHERE id = ?",
                (reservation_id,),
            ).fetchone()
            if row is None:
                raise ReservationNotFoundError(reservation_id)

            current_version = row["version"]
            if row["status"] == "canceled":
                # 取消是终态：任何改期都拒绝，预约不会被复活
                raise ReservationCanceledError(reservation_id, current_version)
            if expected_version != current_version:
                raise VersionConflictError(reservation_id, current_version)

            # 半开区间重叠（同创建路径），reservation_id != ? 排除自身：
            # 改期前自己在该参与者上的旧占用不应被视为冲突。
            conflicts = collect_reservation_conflicts(
                conn, participants, start_s, end_s,
                exclude_reservation_id=reservation_id)
            # 未过期的临时保留同样占用时段（保留之间不排除自身）
            conflicts.extend(_collect_hold_conflicts(
                conn, participants, start_s, end_s, now_s))
            if conflicts:
                raise ReservationConflictError(conflicts)

            conn.execute(
                "UPDATE reservations"
                " SET start_utc = ?, end_utc = ?, participants_key = ?,"
                "     version = version + 1"
                " WHERE id = ?",
                (start_s, end_s, participants_key, reservation_id),
            )
            # 参与者集合可能变化：先删除自身全部旧占用（子表行），再插入新占用。
            # 子表删除不会反向影响 reservations 父表行；
            # 两步与 UPDATE 同事务提交，失败即整体回滚。
            conn.execute(
                "DELETE FROM reservation_participants WHERE reservation_id = ?",
                (reservation_id,),
            )
            conn.executemany(
                "INSERT INTO reservation_participants"
                " (reservation_id, participant_id, start_utc, end_utc)"
                " VALUES (?, ?, ?, ?)",
                [(reservation_id, pid, start_s, end_s) for pid in participants],
            )
            # 同事务追加 rescheduled 审计事件：快照为改期后的新状态，
            # request_id / created_at 沿用原值；now_s 取事务入口时刻
            snapshot = audit.build_snapshot(
                reservation_id=reservation_id,
                request_id=row["request_id"],
                start_utc=start_s,
                end_utc=end_s,
                participants=participants,
                version=current_version + 1,
                status="active",
                canceled_at=None,
                created_at=row["created_at"],
            )
            audit.append_event(
                conn,
                reservation_id=reservation_id,
                action="rescheduled",
                timestamp=now_s,
                snapshot=snapshot,
            )
            conn.execute("COMMIT")
            return Reservation(
                id=reservation_id,
                request_id=row["request_id"],
                start_utc=_parse(start_s),
                end_utc=_parse(end_s),
                participants=participants,
                created_at=_parse(row["created_at"]),
                version=current_version + 1,
            )
        except BaseException:
            # 无活动事务时 rollback 为 no-op；任何失败都保证整体回滚
            conn.rollback()
            raise
    finally:
        conn.close()


def cancel_reservation(
    reservation_id: int,
    expected_version: int,
    db_path: str | None = None,
) -> tuple[Reservation, bool]:
    """取消预约（软删除），返回 ``(取消后的预约, 是否本次执行取消)``。

    整个流程在单个 ``BEGIN IMMEDIATE`` 事务内完成，任何一步失败都回滚：

    1. 预约不存在 → :class:`ReservationNotFoundError`（API 层映射为 404）；
    2. 预约已取消（终态）：

       - ``expected_version`` 等于取消时客户端所持的原版本（即
         ``current_version - 1``，取消恰好把版本推进 1）→ 判定为同一取消
         请求的幂等重试：返回数据库里保存的同一取消结果（``canceled_at``
         与版本均不变），``canceled=False``；
       - 其他版本 → :class:`VersionConflictError`（409 + ``current_version``）；

    3. 进行中但 ``expected_version`` 与当前版本不一致
       → :class:`VersionConflictError`（409 + ``current_version``）；
    4. 否则执行取消：``status`` 置为 ``canceled``、记录 ``canceled_at``、
       ``version`` 加 1，并**删除全部参与者占用行**——时段在同一事务
       提交后立即可被他人预约；预约行本身保留（软删除），此后改期
       请求一律 409，不会复活。
    5. 释放占用后，在**同一事务**内按 FIFO 扫描受释放时段影响的候补队列
       （``reservation_waitlist``）：跳过仍与其他进行中预约或未过期保留
       冲突的候选项，把首个可满足项原子转为正式预约（复用正式预约写入，
       并追加现有 ``created`` 哈希链审计事件）。无可满足候补则不晋升。
    """
    path = db.ensure_initialized(db_path)
    conn = db.connect(path)
    try:
        # BEGIN IMMEDIATE：与创建/改期路径一致，事务开始即持写锁，
        # “存在性 + 状态 + 版本校验 + 取消 + 释放占用 + 候补晋升”
        # 对并发请求原子。
        conn.execute("BEGIN IMMEDIATE")
        try:
            now_s = _canon(datetime.now(timezone.utc))
            # 事务入口惰性过期到期保留：随后的候补晋升冲突检查只面对
            # 有效占用，到期保留腾出的时段可被晋升立即利用
            _expire_due_holds(conn, now_s)

            row = conn.execute(
                f"SELECT {_RESERVATION_COLUMNS} FROM reservations WHERE id = ?",
                (reservation_id,),
            ).fetchone()
            if row is None:
                raise ReservationNotFoundError(reservation_id)

            current_version = row["version"]
            if row["status"] == "canceled":
                # 取消恰好把版本 +1：expected_version 等于取消前版本
                # 即为同一取消请求的重试 → 幂等返回已存储的同一结果
                if expected_version == current_version - 1:
                    conn.execute("COMMIT")
                    return _row_to_reservation(row), False
                raise VersionConflictError(reservation_id, current_version)

            if expected_version != current_version:
                raise VersionConflictError(reservation_id, current_version)

            canceled_participants = tuple(
                json.loads(row["participants_key"]))
            canceled_start_s = row["start_utc"]
            canceled_end_s = row["end_utc"]
            conn.execute(
                "UPDATE reservations"
                " SET status = 'canceled', canceled_at = ?, version = version + 1"
                " WHERE id = ?",
                (now_s, reservation_id),
            )
            # 释放全部参与者占用：与 UPDATE 同事务提交，
            # 提交后该时段立即可再次预约；失败则整体回滚。
            conn.execute(
                "DELETE FROM reservation_participants WHERE reservation_id = ?",
                (reservation_id,),
            )
            # 同事务追加 canceled 审计事件：快照记录取消后的终态
            # （status=canceled、version+1、canceled_at）
            snapshot = audit.build_snapshot(
                reservation_id=reservation_id,
                request_id=row["request_id"],
                start_utc=row["start_utc"],
                end_utc=row["end_utc"],
                participants=json.loads(row["participants_key"]),
                version=current_version + 1,
                status="canceled",
                canceled_at=now_s,
                created_at=row["created_at"],
            )
            audit.append_event(
                conn,
                reservation_id=reservation_id,
                action="canceled",
                timestamp=now_s,
                snapshot=snapshot,
            )
            # 取消与释放占用已在本事务完成（占用行已删除，其他事务在本事务
            # 提交前无法看到中间状态）：同事务按 FIFO 扫描受影响候补，
            # 跳过仍冲突项，把首个可满足项原子转为正式预约（其 created
            # 审计事件与取消事件同事务提交，同生共死）。
            from . import waitlist as waitlist_core
            waitlist_core.promote_from_waitlist(
                conn,
                participants=canceled_participants,
                start_s=canceled_start_s,
                end_s=canceled_end_s,
                now_s=now_s,
            )
            conn.execute("COMMIT")
            return Reservation(
                id=reservation_id,
                request_id=row["request_id"],
                start_utc=_parse(row["start_utc"]),
                end_utc=_parse(row["end_utc"]),
                participants=tuple(json.loads(row["participants_key"])),
                created_at=_parse(row["created_at"]),
                version=current_version + 1,
                status="canceled",
                canceled_at=_parse(now_s),
            ), True
        except BaseException:
            # 无活动事务时 rollback 为 no-op；任何失败都保证整体回滚
            conn.rollback()
            raise
    finally:
        conn.close()


# ---------------------------------------------------------------- 变更历史


def get_reservation_history(
    reservation_id: int,
    db_path: str | None = None,
) -> list[dict[str, object]]:
    """返回预约的变更历史，**按序重放并重验整条哈希链**后才返回。

    - 预约不存在 → :class:`ReservationNotFoundError`（API 层映射为 404）；
    - 链上任一事件的序号、``prev_hash`` 或哈希重算不一致
      → :class:`audit.ChainVerificationError`（API 层映射为 409，
      且不返回任何历史内容）；
    - 校验通过 → 按 ``seq`` 升序返回事件（含动作、序号、时间、快照、
      前后哈希与 canonical_event）。
    """
    path = db.ensure_initialized(db_path)
    conn = db.connect(path)
    try:
        exists = conn.execute(
            "SELECT 1 FROM reservations WHERE id = ?",
            (reservation_id,),
        ).fetchone()
        if exists is None:
            raise ReservationNotFoundError(reservation_id)
        rows = conn.execute(
            "SELECT reservation_id, seq, action, timestamp, snapshot,"
            "       prev_hash, hash, canonical_event"
            " FROM reservation_events WHERE reservation_id = ?"
            " ORDER BY seq ASC",
            (reservation_id,),
        ).fetchall()
        # 404 语义优先；存在预约但无事件/链损坏统一视为验链异常（409）
        return audit.verify_events(rows)
    finally:
        conn.close()
