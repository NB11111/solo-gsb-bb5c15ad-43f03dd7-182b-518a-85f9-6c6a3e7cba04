"""预约变更历史：哈希链审计事件。

每条成功的创建（``created``）、改期（``rescheduled``）、取消（``canceled``）
都在其业务变更的**同一数据库事务内**追加一个审计事件；升级前已存在的预约则在
初始化时补写一条 ``baseline`` 事件（仅记录“初始化时观测到的快照”，不伪造旧操作）。

事件字段（即哈希所覆盖的全部内容）：

- ``seq``：该预约内从 1 递增的序号；
- ``action``：``created`` / ``rescheduled`` / ``canceled`` / ``baseline``；
- ``timestamp``：事件时间（UTC 定宽文本，与业务写入同一时钟读数）；
- ``snapshot``：事件发生后预约的完整快照。

规范化与链规则：

- ``canonical_event`` 为**不含哈希字段**、键按字典序排序、非 ASCII 不转义、
  无任何多余空白的紧凑 JSON（``json.dumps(..., sort_keys=True,
  ensure_ascii=False, separators=(",", ":"))``）；
- 事件哈希链：``hash = SHA-256(prev_hash 的 ASCII 字节 +
  canonical_event 的 UTF-8 字节)``，十六进制摘要；
- 首条事件的 ``prev_hash`` 为 64 个 ``0``（:data:`GENESIS_PREV_HASH`）。
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from typing import Any, Iterable

# UTC 定宽文本格式：与 reservations 表一致，字典序即时间序
CANON_FMT = "%Y-%m-%dT%H:%M:%S.%fZ"

GENESIS_PREV_HASH = "0" * 64
_HEX64 = re.compile(r"^[0-9a-f]{64}$")

# 允许的事件动作；snapshot 必须使用的键集合（顺序仅为可读性，序列化时另行排序）
ACTIONS = ("created", "rescheduled", "canceled", "baseline")
_SNAPSHOT_KEYS = (
    "reservation_id", "request_id", "start_utc", "end_utc", "participants",
    "version", "status", "canceled_at", "created_at",
)


class ChainVerificationError(Exception):
    """历史哈希链重验失败：序号、prev_hash 或事件哈希不一致。"""


def canonical_json(payload: dict[str, Any]) -> str:
    """不含哈希字段、键排序、非 ASCII 不转义的无空白紧凑 JSON。

    调用方负责确保 ``payload`` 中不包含 ``prev_hash`` / ``hash`` 等哈希字段。
    """
    return json.dumps(payload, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"))


def chain_hash(prev_hash: str, canonical_event: str) -> str:
    """``SHA-256(prev_hash 的 ASCII 字节 + canonical_event 的 UTF-8 字节)``。"""
    return hashlib.sha256(
        prev_hash.encode("ascii") + canonical_event.encode("utf-8")
    ).hexdigest()


def build_snapshot(
    *,
    reservation_id: int,
    request_id: str,
    start_utc: str,
    end_utc: str,
    participants: Iterable[str],
    version: int,
    status: str,
    canceled_at: str | None,
    created_at: str,
) -> dict[str, Any]:
    """构造与预约输出同构、键固定的快照（参与者须为去重排序后的列表）。"""
    return {
        "reservation_id": reservation_id,
        "request_id": request_id,
        "start_utc": start_utc,
        "end_utc": end_utc,
        "participants": list(participants),
        "version": version,
        "status": status,
        "canceled_at": canceled_at,
        "created_at": created_at,
    }


def append_event(
    conn: sqlite3.Connection,
    *,
    reservation_id: int,
    action: str,
    timestamp: str,
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    """在**调用方已打开的事务内**追加一条审计事件并返回其完整记录。

    ``seq`` 取该预约当前最大序号 + 1（调用路径均在 ``BEGIN IMMEDIATE``
    写事务中，读取与写入对并发请求原子），首条为 1；``prev_hash`` 取上一条
    事件的 ``hash``，首条为 :data:`GENESIS_PREV_HASH`。

    本函数不自行提交事务：业务写入与事件写入同生共死，由调用方 COMMIT。
    """
    if action not in ACTIONS:
        raise ValueError(f"unknown audit action: {action!r}")

    last = conn.execute(
        "SELECT seq, hash FROM reservation_events"
        " WHERE reservation_id = ? ORDER BY seq DESC LIMIT 1",
        (reservation_id,),
    ).fetchone()
    if last is None:
        seq = 1
        prev_hash = GENESIS_PREV_HASH
    else:
        seq = int(last["seq"]) + 1
        prev_hash = last["hash"]

    canonical_event = canonical_json({
        "seq": seq,
        "action": action,
        "timestamp": timestamp,
        "snapshot": snapshot,
    })
    event_hash = chain_hash(prev_hash, canonical_event)

    conn.execute(
        "INSERT INTO reservation_events"
        " (reservation_id, seq, action, timestamp, snapshot,"
        "  prev_hash, hash, canonical_event)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            reservation_id, seq, action, timestamp,
            json.dumps(snapshot, ensure_ascii=False),
            prev_hash, event_hash, canonical_event,
        ),
    )
    return {
        "reservation_id": reservation_id,
        "seq": seq,
        "action": action,
        "timestamp": timestamp,
        "snapshot": snapshot,
        "prev_hash": prev_hash,
        "hash": event_hash,
        "canonical_event": canonical_event,
    }


def _verify_snapshot_shape(snapshot: Any) -> None:
    if not isinstance(snapshot, dict):
        raise ChainVerificationError("snapshot must be a JSON object")
    if set(snapshot.keys()) != set(_SNAPSHOT_KEYS):
        raise ChainVerificationError("snapshot keys mismatch")
    if not isinstance(snapshot["participants"], list) or not all(
            isinstance(p, str) for p in snapshot["participants"]):
        raise ChainVerificationError("snapshot.participants must be strings")
    if snapshot["status"] not in ("active", "canceled"):
        raise ChainVerificationError("snapshot.status invalid")


def verify_events(rows: Iterable[sqlite3.Row]) -> list[dict[str, Any]]:
    """按序重放并重验整条哈希链，返回有序事件记录列表。

    逐事件校验：

    1. ``seq`` 从 1 起严格递增（无缺号/重复/乱序）；
    2. ``prev_hash`` 等于上一条事件的 ``hash``（首条为 64 个 0），
       且两条哈希均为 64 位十六进制；
    3. 重新规范化后的 ``canonical_event`` 与存储值逐字节一致，
       重算哈希等于存储的 ``hash``；
    4. ``snapshot`` 独立列与 ``canonical_event`` 中的快照一致。

    任一不满足 → :class:`ChainVerificationError`。
    """
    events: list[dict[str, Any]] = []
    prev_hash = GENESIS_PREV_HASH
    for expected_seq, row in enumerate(rows, start=1):
        seq = int(row["seq"])
        if seq != expected_seq:
            raise ChainVerificationError(
                f"seq gap: expected {expected_seq}, got {seq}")

        stored_prev = row["prev_hash"]
        stored_hash = row["hash"]
        if not _HEX64.match(stored_prev) or stored_prev != prev_hash:
            raise ChainVerificationError(f"prev_hash mismatch at seq {seq}")
        if not _HEX64.match(stored_hash):
            raise ChainVerificationError(f"hash malformed at seq {seq}")

        action = row["action"]
        timestamp = row["timestamp"]
        try:
            embedded = json.loads(row["canonical_event"])
        except (ValueError, TypeError) as exc:
            raise ChainVerificationError(
                f"canonical_event unreadable at seq {seq}") from exc
        if not isinstance(embedded, dict) or set(embedded.keys()) != {
                "seq", "action", "timestamp", "snapshot"}:
            raise ChainVerificationError(
                f"canonical_event shape mismatch at seq {seq}")
        if (embedded["seq"], embedded["action"],
                embedded["timestamp"]) != (seq, action, timestamp):
            raise ChainVerificationError(
                f"canonical_event header mismatch at seq {seq}")

        snapshot = embedded["snapshot"]
        _verify_snapshot_shape(snapshot)

        recanonicalized = canonical_json({
            "seq": seq,
            "action": action,
            "timestamp": timestamp,
            "snapshot": snapshot,
        })
        if recanonicalized != row["canonical_event"]:
            raise ChainVerificationError(
                f"canonical_event is not canonical at seq {seq}")
        recomputed = chain_hash(prev_hash, recanonicalized)
        if recomputed != stored_hash:
            raise ChainVerificationError(f"hash mismatch at seq {seq}")

        # 独立存储的 action / timestamp / snapshot 也须与链上内容一致
        try:
            stored_snapshot = json.loads(row["snapshot"])
        except (ValueError, TypeError) as exc:
            raise ChainVerificationError(
                f"snapshot column unreadable at seq {seq}") from exc
        if stored_snapshot != snapshot:
            raise ChainVerificationError(
                f"snapshot column diverges from chain at seq {seq}")

        events.append({
            "reservation_id": int(row["reservation_id"]),
            "seq": seq,
            "action": action,
            "timestamp": timestamp,
            "snapshot": snapshot,
            "prev_hash": stored_prev,
            "hash": stored_hash,
            "canonical_event": row["canonical_event"],
        })
        prev_hash = stored_hash

    if not events:
        raise ChainVerificationError("reservation has no audit events")
    return events
