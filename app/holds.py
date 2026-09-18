"""预约临时保留（hold）核心逻辑：临时占位、惰性过期与确认转正式预约。

临时保留是正式预约前的短时占位：

- ``POST /api/v1/reservation-holds`` 在单个 ``BEGIN IMMEDIATE`` 事务内：
  先把所有已到期的 pending 保留**惰性标记为 expired**（“忽略已过期保留”），
  再与**进行中预约**（``reservation_participants``）和**其他未过期 pending
  保留**（``reservation_hold_participants``）按统一的半开区间
  ``[start, end)`` 规则检查冲突——首尾相接不算冲突；有冲突则整体回滚，
  不写入任何数据。
- 保留有效期 ``ttl_seconds``（1..900）在创建时固化为 ``expires_at``；
  过期仅在写事务内惰性翻转状态，不需要后台清理任务。
- ``POST .../{hold_id}/confirm`` 仅当保留仍为**未过期 pending** 时，
  在同一事务内转为正式预约（复用正式预约的写入与 ``created`` 审计事件），
  成功后把保留标记为 confirmed 并回填 ``reservation_id``；
  重复确认幂等返回同一预约；已过期 → :class:`HoldExpiredError`（410）。

保留同样会阻止并发的正式预约创建/改期（见 :mod:`app.reservations`），
因此“未过期 pending”在 TTL 内确认不会因他人抢占而失败。
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

from . import db
from .reservations import (
    RequestIdConflictError,
    Reservation,
    ReservationConflictError,
    ReservationNotFoundError,
    _canon,
    _parse,
    collect_reservation_conflicts,
    insert_reservation,
    load_reservation,
    normalize_participants,
)

MIN_TTL_SECONDS = 1
MAX_TTL_SECONDS = 900

_HOLD_COLUMNS = (
    "id, hold_key, start_utc, end_utc, participants_key, ttl_seconds,"
    " status, reservation_id, created_at, expires_at, confirmed_at, expired_at"
)


class HoldNotFoundError(Exception):
    """指定 ID 的临时保留不存在。"""

    def __init__(self, hold_id: int) -> None:
        self.hold_id = hold_id
        super().__init__(f"reservation hold #{hold_id} not found")


class HoldExpiredError(Exception):
    """临时保留已过期（或已失效），不能再确认。"""

    def __init__(self, hold_id: int, status: str) -> None:
        self.hold_id = hold_id
        self.status = status
        super().__init__(f"reservation hold #{hold_id} has {status}")


class HoldKeyConflictError(Exception):
    """``hold_key`` 已被另一条仍为 pending、但内容不同的保留占用。"""

    def __init__(self, hold_id: int) -> None:
        self.hold_id = hold_id
        super().__init__(
            f"hold_key is already used by pending hold #{hold_id} "
            "with different content"
        )


@dataclass(frozen=True)
class Hold:
    id: int
    hold_key: str
    start_utc: datetime
    end_utc: datetime
    participants: tuple[str, ...]
    ttl_seconds: int
    created_at: datetime
    expires_at: datetime
    status: str = "pending"  # pending / confirmed / expired
    reservation_id: int | None = None
    confirmed_at: datetime | None = None
    expired_at: datetime | None = None


def _row_to_hold(row: sqlite3.Row) -> Hold:
    return Hold(
        id=row["id"],
        hold_key=row["hold_key"],
        start_utc=_parse(row["start_utc"]),
        end_utc=_parse(row["end_utc"]),
        participants=tuple(json.loads(row["participants_key"])),
        ttl_seconds=row["ttl_seconds"],
        created_at=_parse(row["created_at"]),
        expires_at=_parse(row["expires_at"]),
        status=row["status"],
        reservation_id=row["reservation_id"],
        confirmed_at=_parse(row["confirmed_at"]) if row["confirmed_at"] else None,
        expired_at=_parse(row["expired_at"]) if row["expired_at"] else None,
    )


def expire_due_holds(conn: sqlite3.Connection, now_s: str) -> int:
    """把所有 ``expires_at <= now`` 的 pending 保留标记为 expired。

    在调用方已打开的写事务内执行，不自行提交；返回翻转的行数。
    参与者占用行保留在表中（状态过滤 + 时间过滤使其不再参与冲突检查）。
    """
    cursor = conn.execute(
        "UPDATE reservation_holds"
        " SET status = 'expired', expired_at = ?"
        " WHERE status = 'pending' AND expires_at <= ?",
        (now_s, now_s),
    )
    return cursor.rowcount


def collect_hold_conflicts(
    conn: sqlite3.Connection,
    participants: tuple[str, ...],
    start_s: str,
    end_s: str,
    now_s: str | None = None,
) -> list[dict[str, object]]:
    """收集与**未过期 pending 保留**的半开区间冲突。

    半开重叠：``existing.start < new.end AND new.start < existing.end``
    （相邻区间不冲突）。双重保险过滤 status 与 expires_at：即便惰性过期
    尚未翻转某行，``expires_at <= now`` 的保留也不会命中。``now_s``
    取调用方事务入口时刻；未提供时退化为当前时间。
    """
    if now_s is None:
        now_s = _canon(datetime.now(timezone.utc))
    conflicts: list[dict[str, object]] = []
    for pid in participants:
        for hit in conn.execute(
            "SELECT hp.hold_id, hp.participant_id"
            " FROM reservation_hold_participants hp"
            " JOIN reservation_holds h ON h.id = hp.hold_id"
            " WHERE hp.participant_id = ?"
            "   AND hp.start_utc < ? AND ? < hp.end_utc"
            "   AND h.status = 'pending' AND h.expires_at > ?"
            " ORDER BY hp.hold_id",
            (pid, end_s, start_s, now_s),
        ):
            conflicts.append({
                "hold_id": hit["hold_id"],
                "participant_id": hit["participant_id"],
            })
    return conflicts


def create_hold(
    hold_key: str,
    start_utc: datetime,
    end_utc: datetime,
    participant_ids: Iterable[str],
    ttl_seconds: int,
    db_path: str | None = None,
) -> tuple[Hold, bool]:
    """创建临时保留，返回 ``(保留, 是否新建)``。

    整个流程在单个 ``BEGIN IMMEDIATE`` 事务内完成，任何一步失败都回滚：

    1. 惰性过期：``expires_at <= now`` 的 pending 保留翻转为 expired；
    2. 同一 ``hold_key`` 已有 pending 保留：规范化内容（起止 + 去重排序后的
       参与者 + ttl）相同 → 幂等返回原保留（``created=False``），
       内容不同 → :class:`HoldKeyConflictError`（409）；
       confirmed/expired 的同名保留不阻止重新申请（部分唯一索引仅约束 pending）；
    3. 与进行中预约、其他未过期 pending 保留按半开区间检查冲突
       → :class:`ReservationConflictError`（409 + 冲突明细），不写入；
    4. 否则写入保留与其参与者占用，``expires_at = now + ttl_seconds``。
    """
    participants = normalize_participants(participant_ids)
    start_s = _canon(start_utc)
    end_s = _canon(end_utc)
    participants_key = json.dumps(list(participants), ensure_ascii=False)

    path = db.ensure_initialized(db_path)
    conn = db.connect(path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            now_dt = datetime.now(timezone.utc)
            now_s = _canon(now_dt)
            expire_due_holds(conn, now_s)

            row = conn.execute(
                f"SELECT {_HOLD_COLUMNS} FROM reservation_holds"
                " WHERE hold_key = ? AND status = 'pending'",
                (hold_key,),
            ).fetchone()
            if row is not None:
                if (row["start_utc"] == start_s
                        and row["end_utc"] == end_s
                        and row["participants_key"] == participants_key
                        and row["ttl_seconds"] == ttl_seconds):
                    conn.execute("COMMIT")
                    return _row_to_hold(row), False
                raise HoldKeyConflictError(row["id"])

            # 先查进行中预约，再查其他有效保留；任一冲突都整体回滚
            conflicts = collect_reservation_conflicts(
                conn, participants, start_s, end_s)
            conflicts.extend(
                collect_hold_conflicts(conn, participants, start_s, end_s, now_s))
            if conflicts:
                raise ReservationConflictError(conflicts)

            expires_s = _canon(now_dt + timedelta(seconds=ttl_seconds))
            cursor = conn.execute(
                "INSERT INTO reservation_holds"
                " (hold_key, start_utc, end_utc, participants_key, ttl_seconds,"
                "  status, created_at, expires_at)"
                " VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)",
                (hold_key, start_s, end_s, participants_key, ttl_seconds,
                 now_s, expires_s),
            )
            hold_id = int(cursor.lastrowid)
            conn.executemany(
                "INSERT INTO reservation_hold_participants"
                " (hold_id, participant_id, start_utc, end_utc)"
                " VALUES (?, ?, ?, ?)",
                [(hold_id, pid, start_s, end_s) for pid in participants],
            )
            conn.execute("COMMIT")
            return Hold(
                id=hold_id,
                hold_key=hold_key,
                start_utc=_parse(start_s),
                end_utc=_parse(end_s),
                participants=participants,
                ttl_seconds=ttl_seconds,
                created_at=_parse(now_s),
                expires_at=_parse(expires_s),
            ), True
        except BaseException:
            conn.rollback()
            raise
    finally:
        conn.close()


def confirm_hold(
    hold_id: int,
    request_id: str,
    db_path: str | None = None,
) -> tuple[Reservation, bool]:
    """确认保留：未过期 pending → 同事务转正式预约，返回 ``(预约, 是否本次确认)``。

    单个 ``BEGIN IMMEDIATE`` 事务内：

    1. 保留不存在 → :class:`HoldNotFoundError`（404）；
    2. 已 confirmed → 幂等返回其关联的同一预约（``confirmed=False``）；
       关联预约行缺失属于数据损坏，按 404 处理；
    3. 先惰性过期；保留非 pending（此时只可能是 expired）或已过 ``expires_at``
       → :class:`HoldExpiredError`（410）；
    4. ``request_id`` 已被其他预约占用 → :class:`RequestIdConflictError`（409），
       本次确认不落库（客户端换 request_id 后在 TTL 内可重试）；
    5. 同事务插入正式预约（含 ``created`` 审计事件），把保留置为 confirmed、
       回填 ``reservation_id`` / ``confirmed_at``，并删除保留的参与者占用行
       （占用语义已由正式预约接管）。重复确认（含不同 request_id 的重试）
       一律返回同一预约。
    """
    path = db.ensure_initialized(db_path)
    conn = db.connect(path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            now_s = _canon(datetime.now(timezone.utc))
            expire_due_holds(conn, now_s)

            row = conn.execute(
                f"SELECT {_HOLD_COLUMNS} FROM reservation_holds WHERE id = ?",
                (hold_id,),
            ).fetchone()
            if row is None:
                raise HoldNotFoundError(hold_id)

            if row["status"] == "confirmed":
                reservation = load_reservation(conn, row["reservation_id"])
                if reservation is None:
                    raise ReservationNotFoundError(row["reservation_id"])
                conn.execute("COMMIT")
                return reservation, False

            if row["status"] != "pending" or row["expires_at"] <= now_s:
                raise HoldExpiredError(hold_id, row["status"])

            existing = conn.execute(
                "SELECT id FROM reservations WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if existing is not None:
                raise RequestIdConflictError(existing["id"])

            # request_id 命名空间与候补队列共用：pending 候补若已占用该键，
            # 内容不同 → 409；内容相同则由本次确认兑现（下方回填为 promoted）。
            from . import waitlist as waitlist_core
            wl_row = conn.execute(
                "SELECT id, start_utc, end_utc, participants_key, status"
                " FROM reservation_waitlist WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if wl_row is not None and wl_row["status"] == "pending":
                if not (wl_row["start_utc"] == row["start_utc"]
                        and wl_row["end_utc"] == row["end_utc"]
                        and wl_row["participants_key"] == row["participants_key"]):
                    raise waitlist_core.WaitlistContentConflictError(wl_row["id"])

            participants = tuple(json.loads(row["participants_key"]))
            reservation_id = insert_reservation(
                conn,
                request_id=request_id,
                start_s=row["start_utc"],
                end_s=row["end_utc"],
                participants=participants,
                now_s=now_s,
            )
            # 同 request_id 的 pending 候补由本次确认兑现：原子回填为 promoted
            if wl_row is not None:
                waitlist_core.mark_promoted(
                    conn, waitlist_id=wl_row["id"],
                    reservation_id=reservation_id, now_s=now_s)
            conn.execute(
                "UPDATE reservation_holds"
                " SET status = 'confirmed', reservation_id = ?, confirmed_at = ?"
                " WHERE id = ?",
                (reservation_id, now_s, hold_id),
            )
            # 占用已由正式预约接管：删除保留参与者行，避免确认后重复占位
            conn.execute(
                "DELETE FROM reservation_hold_participants WHERE hold_id = ?",
                (hold_id,),
            )

            # 提交前在同一事务内组装返回对象（与 GET 路径读取到的内容一致）
            committed = Reservation(
                id=reservation_id,
                request_id=request_id,
                start_utc=_parse(row["start_utc"]),
                end_utc=_parse(row["end_utc"]),
                participants=participants,
                created_at=_parse(now_s),
                version=1,
            )
            conn.execute("COMMIT")
            return committed, True
        except BaseException:
            conn.rollback()
            raise
    finally:
        conn.close()
