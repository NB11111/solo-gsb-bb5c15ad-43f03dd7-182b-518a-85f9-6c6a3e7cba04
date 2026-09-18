"""预约候补队列（waitlist）核心逻辑：入队/直订、幂等与取消后的 FIFO 晋升。

候补是“时段当前被占用也愿意排队等待”的请求：

- ``POST /api/v1/reservation-waitlist`` 在单个 ``BEGIN IMMEDIATE`` 事务内：
  若请求时段当前可预约（与**进行中预约**及**未过期 pending 保留**均无半开
  区间重叠），则直接创建一条正式预约（复用 :func:`insert_reservation`，
  同事务写入并追加 ``created`` 哈希链审计事件）；否则把请求按**创建时间**
  记入 pending 候补队列，不占用任何时段——pending 候补**不会**阻止他人的
  正式预约/保留/改期。
- 相同 ``request_id`` 幂等：同键 + 相同规范化内容（起止、去重排序参与者）
  的重试返回候补当前状态（pending 或已 promoted 及关联预约）；
  同键不同内容 → :class:`WaitlistContentConflictError`（409）。
  候补与正式预约共用同一 ``request_id`` 命名空间：直接走
  ``POST /reservations``（或保留确认）已成功的请求，再以相同内容加入候补
  会返回其关联预约（必要时把候补行回填为 promoted）；内容不同一律 409。
- 取消正式预约时（见 :mod:`app.reservations`），在**同一取消事务**内
  :func:`promote_from_waitlist` 按 ``(created_at, id)`` 的 FIFO 顺序扫描
  与被取消预约“参与者相交且时段重叠”的候补，**跳过仍冲突项**（与其他进行中
  预约或未过期保留重叠），把**首个**可满足的候选项原子转为正式预约
  （同事务写入并追加现有 ``created`` 哈希链审计事件）；
  若没有任何可满足的候补则不发生晋升。
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

from . import db
from .reservations import (
    RequestIdConflictError,
    Reservation,
    _canon,
    _parse,
    collect_reservation_conflicts,
    insert_reservation,
    load_reservation,
    normalize_participants,
)

_WAITLIST_COLUMNS = (
    "id, request_id, start_utc, end_utc, participants_key, status,"
    " reservation_id, created_at, promoted_at"
)


class WaitlistNotFoundError(Exception):
    """``request_id`` 从未通过候补接口登记过。"""

    def __init__(self, request_id: str) -> None:
        self.request_id = request_id
        super().__init__(f"reservation waitlist request {request_id!r} not found")


class WaitlistContentConflictError(Exception):
    """``request_id`` 已被内容不同的候补请求占用。"""

    def __init__(self, waitlist_id: int) -> None:
        self.waitlist_id = waitlist_id
        super().__init__(
            f"request_id is already used by reservation waitlist entry "
            f"#{waitlist_id} with different content"
        )


class WaitlistReservationMissingError(Exception):
    """内部防御：promoted 候补关联的预约行缺失（数据损坏）。"""

    def __init__(self, reservation_id: int | None) -> None:
        self.reservation_id = reservation_id
        super().__init__(
            f"reservation #{reservation_id} referenced by waitlist entry not found"
        )


@dataclass(frozen=True)
class WaitlistEntry:
    id: int
    request_id: str
    start_utc: datetime
    end_utc: datetime
    participants: tuple[str, ...]
    created_at: datetime
    status: str = "pending"  # pending / promoted
    reservation_id: int | None = None
    promoted_at: datetime | None = None


@dataclass(frozen=True)
class WaitlistJoinResult:
    """候补请求处理结果。

    - ``entry``：候补行（pending 或已 promoted）；
    - ``reservation``：直订/回填/晋升得到的正式预约（pending 时为 ``None``）；
    - ``inserted``：本次是否实际写入新行（候补行或正式预约）。幂等重放、
      跨端点同内容返回既有预约时为 ``False``（API 层据此返回 200）。
    """

    entry: WaitlistEntry
    reservation: Reservation | None
    inserted: bool


def _row_to_entry(row: sqlite3.Row) -> WaitlistEntry:
    return WaitlistEntry(
        id=row["id"],
        request_id=row["request_id"],
        start_utc=_parse(row["start_utc"]),
        end_utc=_parse(row["end_utc"]),
        participants=tuple(json.loads(row["participants_key"])),
        created_at=_parse(row["created_at"]),
        status=row["status"],
        reservation_id=row["reservation_id"],
        promoted_at=_parse(row["promoted_at"]) if row["promoted_at"] else None,
    )


def _insert_waitlist(
    conn: sqlite3.Connection,
    *,
    request_id: str,
    start_s: str,
    end_s: str,
    participants: tuple[str, ...],
    now_s: str,
) -> int:
    """在调用方事务内写入候补行及其参与者行，返回新候补 ID（不提交）。"""
    participants_key = json.dumps(list(participants), ensure_ascii=False)
    cursor = conn.execute(
        "INSERT INTO reservation_waitlist"
        " (request_id, start_utc, end_utc, participants_key, status,"
        "  created_at, promoted_at)"
        " VALUES (?, ?, ?, ?, 'pending', ?, NULL)",
        (request_id, start_s, end_s, participants_key, now_s),
    )
    waitlist_id = int(cursor.lastrowid)
    conn.executemany(
        "INSERT INTO reservation_waitlist_participants"
        " (waitlist_id, participant_id, start_utc, end_utc)"
        " VALUES (?, ?, ?, ?)",
        [(waitlist_id, pid, start_s, end_s) for pid in participants],
    )
    return waitlist_id


def _same_content(
    row: sqlite3.Row,
    *,
    start_s: str,
    end_s: str,
    participants_key: str,
) -> bool:
    return (
        row["start_utc"] == start_s
        and row["end_utc"] == end_s
        and row["participants_key"] == participants_key
    )


def mark_promoted(
    conn: sqlite3.Connection,
    *,
    waitlist_id: int,
    reservation_id: int,
    now_s: str,
) -> None:
    """把候补行标记为 promoted 并关联正式预约（在调用方事务内，不提交）。

    幂等：已是 promoted 的行不再变更（``promoted_at`` 保留首次晋升时刻）。
    """
    conn.execute(
        "UPDATE reservation_waitlist"
        " SET status = 'promoted', reservation_id = ?, promoted_at = ?"
        " WHERE id = ? AND status = 'pending'",
        (reservation_id, now_s, waitlist_id),
    )


def join_waitlist(
    request_id: str,
    start_utc: datetime,
    end_utc: datetime,
    participant_ids: Iterable[str],
    db_path: str | None = None,
) -> WaitlistJoinResult:
    """加入候补：当前可预约则直接创建正式预约，否则按创建时间进入 pending 队列。

    单个 ``BEGIN IMMEDIATE`` 事务内：

    1. 事务入口先惰性过期到期保留，使冲突检查只面对有效占用；
    2. 同一 ``request_id`` 已有候补行：
       规范化内容不同 → :class:`WaitlistContentConflictError`（409）；
       内容相同 → 返回候补当前状态（pending 原样返回；已 promoted 时
       一并读取关联预约，含该预约后来被改期/取消后的最新状态）；
    3. 同一 ``request_id`` 已被正式预约占用（跨端点：直接创建或保留确认
       产生）：内容不同 → :class:`RequestIdConflictError`（409）；
       内容相同 → 幂等返回该预约，并把对应候补行回填为 promoted
       （该端点首次见到此 request_id 时）；
    4. 否则按半开区间检查与进行中预约、未过期 pending 保留的冲突
       ——**候补之间互不冲突，也不参与冲突检查**：

       - 无冲突 → 同事务插入正式预约（含 ``created`` 审计事件）+
         promoted 候补行；
       - 有冲突 → 写入 pending 候补行（``created_at`` 即 FIFO 排序键）。
    """
    participants = normalize_participants(participant_ids)
    start_s = _canon(start_utc)
    end_s = _canon(end_utc)
    participants_key = json.dumps(list(participants), ensure_ascii=False)

    path = db.ensure_initialized(db_path)
    conn = db.connect(path)
    try:
        # BEGIN IMMEDIATE：读改写对并发请求原子（并发写者被串行化），
        # 直订与入队的判定不会与并发的取消晋升相互穿插。
        conn.execute("BEGIN IMMEDIATE")
        try:
            now_s = _canon(datetime.now(timezone.utc))
            from . import holds as hold_core  # 惰性导入，避免模块初始化期循环依赖
            hold_core.expire_due_holds(conn, now_s)

            existing = conn.execute(
                f"SELECT {_WAITLIST_COLUMNS} FROM reservation_waitlist"
                " WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if existing is not None:
                if not _same_content(
                        existing, start_s=start_s, end_s=end_s,
                        participants_key=participants_key):
                    raise WaitlistContentConflictError(existing["id"])
                entry = _row_to_entry(existing)
                reservation = None
                if entry.status == "promoted":
                    reservation = load_reservation(conn, entry.reservation_id)
                    if reservation is None:
                        # 关联预约行缺失属于数据损坏
                        raise WaitlistReservationMissingError(entry.reservation_id)
                conn.execute("COMMIT")
                return WaitlistJoinResult(entry=entry, reservation=reservation,
                                          inserted=False)

            reservation_row = conn.execute(
                "SELECT id FROM reservations WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if reservation_row is not None:
                # request_id 跨端点共用：直接预约/保留确认已先占用该键。
                reservation = load_reservation(conn, reservation_row["id"])
                if (reservation.start_utc != _parse(start_s)
                        or reservation.end_utc != _parse(end_s)
                        or reservation.participants != participants):
                    raise RequestIdConflictError(reservation.id)
                waitlist_id = _insert_waitlist(
                    conn, request_id=request_id, start_s=start_s, end_s=end_s,
                    participants=participants, now_s=now_s)
                mark_promoted(conn, waitlist_id=waitlist_id,
                              reservation_id=reservation.id, now_s=now_s)
                entry = conn.execute(
                    f"SELECT {_WAITLIST_COLUMNS} FROM reservation_waitlist WHERE id = ?",
                    (waitlist_id,),
                ).fetchone()
                conn.execute("COMMIT")
                return WaitlistJoinResult(entry=_row_to_entry(entry),
                                          reservation=reservation, inserted=False)

            conflicts = collect_reservation_conflicts(
                conn, participants, start_s, end_s)
            conflicts.extend(
                hold_core.collect_hold_conflicts(
                    conn, participants, start_s, end_s, now_s))
            if conflicts:
                waitlist_id = _insert_waitlist(
                    conn, request_id=request_id, start_s=start_s, end_s=end_s,
                    participants=participants, now_s=now_s)
                conn.execute("COMMIT")
                entry = WaitlistEntry(
                    id=waitlist_id, request_id=request_id,
                    start_utc=_parse(start_s), end_utc=_parse(end_s),
                    participants=participants, created_at=_parse(now_s),
                    status="pending",
                )
                return WaitlistJoinResult(entry=entry, reservation=None,
                                          inserted=True)

            # 当前可预约：直接创建正式预约（同事务写入 + created 审计事件）
            reservation_id = insert_reservation(
                conn, request_id=request_id, start_s=start_s, end_s=end_s,
                participants=participants, now_s=now_s)
            waitlist_id = _insert_waitlist(
                conn, request_id=request_id, start_s=start_s, end_s=end_s,
                participants=participants, now_s=now_s)
            mark_promoted(conn, waitlist_id=waitlist_id,
                          reservation_id=reservation_id, now_s=now_s)
            entry = conn.execute(
                f"SELECT {_WAITLIST_COLUMNS} FROM reservation_waitlist WHERE id = ?",
                (waitlist_id,),
            ).fetchone()
            committed = Reservation(
                id=reservation_id, request_id=request_id,
                start_utc=_parse(start_s), end_utc=_parse(end_s),
                participants=participants, created_at=_parse(now_s), version=1)
            conn.execute("COMMIT")
            return WaitlistJoinResult(entry=_row_to_entry(entry),
                                      reservation=committed, inserted=True)
        except BaseException:
            conn.rollback()
            raise
    finally:
        conn.close()


def get_waitlist_entry(
    request_id: str,
    db_path: str | None = None,
) -> tuple[WaitlistEntry, Reservation | None]:
    """按 ``request_id`` 读取候补状态与关联预约。

    未通过候补接口登记过（候补行不存在）→ :class:`WaitlistNotFoundError`（404）。
    promoted 但关联预约行缺失 → :class:`ReservationNotFoundError`（404）。
    """
    path = db.ensure_initialized(db_path)
    conn = db.connect(path)
    try:
        row = conn.execute(
            f"SELECT {_WAITLIST_COLUMNS} FROM reservation_waitlist"
            " WHERE request_id = ?",
            (request_id,),
        ).fetchone()
        if row is None:
            raise WaitlistNotFoundError(request_id)
        entry = _row_to_entry(row)
        reservation = None
        if entry.status == "promoted":
            reservation = load_reservation(conn, entry.reservation_id)
            if reservation is None:
                from .reservations import ReservationNotFoundError
                raise ReservationNotFoundError(entry.reservation_id)
        return entry, reservation
    finally:
        conn.close()


def _affected_pending_entries(
    conn: sqlite3.Connection,
    *,
    participants: tuple[str, ...],
    start_s: str,
    end_s: str,
) -> list[sqlite3.Row]:
    """扫描“受影响”的 pending 候补（FIFO）：与给定参与者集合相交且时段重叠。

    半开区间重叠 + 至少一位共同参与者；按 ``(created_at, id)`` 升序，
    即严格的创建时间 FIFO。JOIN + ``GROUP BY`` 为同一候补的多位共同参与者
    去重，顺序由显式 ORDER BY 确定。
    """
    if not participants:
        return []
    placeholders = ",".join("?" for _ in participants)
    rows = conn.execute(
        f"SELECT w.id, w.request_id, w.start_utc, w.end_utc,"
        f"        w.participants_key, w.status, w.reservation_id,"
        f"        w.created_at, w.promoted_at"
        f" FROM reservation_waitlist w"
        f" JOIN reservation_waitlist_participants wp ON wp.waitlist_id = w.id"
        f" WHERE w.status = 'pending'"
        f"   AND wp.participant_id IN ({placeholders})"
        f"   AND wp.start_utc < ? AND ? < wp.end_utc"
        f" GROUP BY w.id"
        f" ORDER BY w.created_at ASC, w.id ASC",
        (*participants, end_s, start_s),
    ).fetchall()
    return rows


def promote_from_waitlist(
    conn: sqlite3.Connection,
    *,
    participants: tuple[str, ...],
    start_s: str,
    end_s: str,
    now_s: str,
) -> Reservation | None:
    """取消事务内的 FIFO 候补晋升（在调用方的写事务中执行，不自行提交）。

    1. 按 :func:`_affected_pending_entries` 以 ``(created_at, id)`` FIFO
       顺序枚举与刚释放时段（参与者相交 + 半开区间重叠）相关的 pending 候补；
    2. 依次检查每个候选项是否与**其他进行中预约**（自动排除无占用的已取消
       预约）或**未过期 pending 保留**仍冲突——**跳过仍冲突项**继续扫描；
    3. 把**首个**可满足项原子转为正式预约：复用
       :func:`insert_reservation`（写入预约、参与者占用与现有 ``created``
       哈希链审计事件），并把候补行置为 promoted、回填 reservation_id 与
       promoted_at；随后立即停止（一次取消至多晋升一条候补，避免连续
       抢占尚被占用的时段）；
    4. 全部候选仍冲突或无候选 → 返回 ``None``，取消照常提交。

    冲突检查只需面对取消后的占用状态：调用方须已删除被取消预约的参与者
    占用行、并在事务入口完成到期保留的惰性过期。
    """
    from . import holds as hold_core

    candidates = _affected_pending_entries(
        conn, participants=participants, start_s=start_s, end_s=end_s)
    for row in candidates:
        cand_participants = tuple(json.loads(row["participants_key"]))
        conflicts = collect_reservation_conflicts(
            conn, cand_participants, row["start_utc"], row["end_utc"])
        conflicts.extend(
            hold_core.collect_hold_conflicts(
                conn, cand_participants, row["start_utc"], row["end_utc"], now_s))
        if conflicts:
            # 仍被其他预约/保留占用：跳过该项，继续扫描更晚的候选
            continue

        reservation_id = insert_reservation(
            conn,
            request_id=row["request_id"],
            start_s=row["start_utc"],
            end_s=row["end_utc"],
            participants=cand_participants,
            now_s=now_s,
        )
        mark_promoted(conn, waitlist_id=row["id"],
                      reservation_id=reservation_id, now_s=now_s)
        promoted = load_reservation(conn, reservation_id)
        return promoted
    return None
