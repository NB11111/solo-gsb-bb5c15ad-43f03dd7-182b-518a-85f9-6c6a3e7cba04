"""预约候补队列（reservation waitlist）测试。

覆盖：可预约直订（promoted + created 哈希链事件）、冲突入 pending、
pending 不占位、request_id 幂等（同内容重放 200 / 异内容 409）、GET 状态、
取消事务内 FIFO 晋升（created_at 严格先后）、跳过仍冲突项（其他预约与未过期
保留）、仅扫描受影响候补、晋升原子性（同事务 created 事件）、幂等取消不重复
晋升、跨端点 request_id 一致性（直接预约 / 保留确认）、并发入队与并发取消、
旧库无损新建候补表且迁移幂等。
"""

from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app import db as db_module
from app import reservations as reservation_core
from app import waitlist as waitlist_core
from app.main import app


@pytest.fixture()
def db_path(tmp_path, monkeypatch) -> str:
    """每个测试用独立的 SQLite 文件。"""
    path = str(tmp_path / "reservations.db")
    monkeypatch.setenv("RESERVATIONS_DB_PATH", path)
    return path


@pytest.fixture()
def client(db_path) -> TestClient:
    return TestClient(app)


def _wl_payload(**overrides):
    payload = {
        "request_id": "wl-1",
        "start_utc": "2026-03-09T13:00:00Z",
        "end_utc": "2026-03-09T14:00:00Z",
        "participant_ids": ["alice", "bob"],
    }
    payload.update(overrides)
    return payload


def _resv_payload(**overrides):
    payload = {
        "request_id": "req-1",
        "start_utc": "2026-03-09T13:00:00Z",
        "end_utc": "2026-03-09T14:00:00Z",
        "participant_ids": ["alice", "bob"],
    }
    payload.update(overrides)
    return payload


def _hold_payload(**overrides):
    payload = {
        "hold_key": "hold-1",
        "start_utc": "2026-03-09T13:00:00Z",
        "end_utc": "2026-03-09T14:00:00Z",
        "participant_ids": ["alice"],
        "ttl_seconds": 300,
    }
    payload.update(overrides)
    return payload


def _utc(y, mo, d, h, mi=0):
    return datetime(y, mo, d, h, mi, tzinfo=timezone.utc)


def _cancel(client, reservation_id: int, expected_version: int = 1):
    return client.delete(
        f"/api/v1/reservations/{reservation_id}"
        f"?expected_version={expected_version}")


# ----------------------------------------------------------- 入队 / 直订

def test_join_free_slot_creates_reservation_directly_201(client):
    resp = client.post("/api/v1/reservation-waitlist",
                       json=_wl_payload(participant_ids=["bob", "alice", "bob"]))
    assert resp.status_code == 201, resp.text
    data = resp.json()
    assert data["waitlist_id"] == 1
    assert data["request_id"] == "wl-1"
    assert data["participants"] == ["alice", "bob"]  # 去重 + 字典序
    assert data["status"] == "promoted"
    assert data["promoted_at"] is not None
    reservation = data["reservation"]
    assert reservation["reservation_id"] == 1
    assert reservation["status"] == "active"
    assert reservation["version"] == 1
    assert reservation["participants"] == ["alice", "bob"]
    assert data["created_at"]


def test_join_conflicting_slot_enqueues_pending_201(client):
    assert client.post("/api/v1/reservations",
                       json=_resv_payload()).status_code == 201
    resp = client.post("/api/v1/reservation-waitlist",
                       json=_wl_payload(request_id="wl-2"))
    assert resp.status_code == 201, resp.text
    data = resp.json()
    assert data["status"] == "pending"
    assert data["reservation"] is None
    assert data["promoted_at"] is None
    assert data["created_at"]


def test_pending_waitlist_does_not_block_reservations_or_holds(client):
    # 已有预约占住 alice 13:00-14:00；候补排队
    assert client.post("/api/v1/reservations",
                       json=_resv_payload(request_id="blk")
                       ).status_code == 201
    assert client.post("/api/v1/reservation-waitlist",
                       json=_wl_payload(request_id="wl-p")
                       ).json()["status"] == "pending"
    # pending 候补不占位：不相交参与者可正常预约
    resp = client.post("/api/v1/reservations", json=_resv_payload(
        request_id="req-carol", participant_ids=["carol"]))
    assert resp.status_code == 201
    # 首尾相接（相邻）也可正常预约
    resp = client.post("/api/v1/reservations", json=_resv_payload(
        request_id="req-adj",
        start_utc="2026-03-09T14:00:00Z",
        end_utc="2026-03-09T15:00:00Z",
        participant_ids=["alice"]))
    assert resp.status_code == 201
    # pending 候补不阻止同参与者的保留（不同时段）
    resp = client.post("/api/v1/reservation-holds", json=_hold_payload(
        hold_key="h-late",
        start_utc="2026-03-09T15:00:00Z",
        end_utc="2026-03-09T15:30:00Z"))
    assert resp.status_code == 201
    # 候补仍为 pending
    assert client.get("/api/v1/reservation-waitlist/wl-p").json()["status"] == "pending"


def test_join_validation_reuses_reservation_rules(client):
    # end <= start
    resp = client.post("/api/v1/reservation-waitlist",
                       json=_wl_payload(end_utc="2026-03-09T13:00:00Z"))
    assert resp.status_code == 422
    # 缺时区偏移
    resp = client.post("/api/v1/reservation-waitlist",
                       json=_wl_payload(start_utc="2026-03-09T13:00:00"))
    assert resp.status_code == 422
    # 空参与者
    resp = client.post("/api/v1/reservation-waitlist",
                       json=_wl_payload(participant_ids=[]))
    assert resp.status_code == 422
    # 空 request_id
    resp = client.post("/api/v1/reservation-waitlist",
                       json=_wl_payload(request_id=""))
    assert resp.status_code == 422


# ------------------------------------------------------------------- 幂等

def test_pending_replay_same_content_returns_200(client):
    assert client.post("/api/v1/reservations",
                       json=_resv_payload()).status_code == 201
    first = client.post("/api/v1/reservation-waitlist",
                        json=_wl_payload(request_id="wl-2"))
    assert first.status_code == 201
    again = client.post("/api/v1/reservation-waitlist",
                        json=_wl_payload(request_id="wl-2",
                                         participant_ids=["bob", "alice"]))
    assert again.status_code == 200
    assert again.json()["waitlist_id"] == first.json()["waitlist_id"]
    assert again.json()["status"] == "pending"


def test_pending_replay_different_content_returns_409(client):
    assert client.post("/api/v1/reservations",
                       json=_resv_payload()).status_code == 201
    assert client.post("/api/v1/reservation-waitlist",
                       json=_wl_payload(request_id="wl-2")
                       ).status_code == 201
    resp = client.post("/api/v1/reservation-waitlist",
                       json=_wl_payload(request_id="wl-2",
                                        end_utc="2026-03-09T14:30:00Z"))
    assert resp.status_code == 409
    assert resp.json()["existing_waitlist_id"] == 1


def test_promoted_replay_returns_same_reservation_200(client, db_path):
    first = client.post("/api/v1/reservation-waitlist",
                        json=_wl_payload())
    assert first.status_code == 201
    rid = first.json()["reservation"]["reservation_id"]
    again = client.post("/api/v1/reservation-waitlist",
                        json=_wl_payload())
    assert again.status_code == 200
    data = again.json()
    assert data["status"] == "promoted"
    assert data["reservation"]["reservation_id"] == rid
    # 不重复写入预约与候补行
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM reservations").fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM reservation_waitlist").fetchone()[0] == 1


# ------------------------------------------------------------------- GET

def test_get_pending_and_promoted(client):
    assert client.post("/api/v1/reservations",
                       json=_resv_payload()).status_code == 201
    client.post("/api/v1/reservation-waitlist",
                json=_wl_payload(request_id="wl-2"))
    g = client.get("/api/v1/reservation-waitlist/wl-2")
    assert g.status_code == 200
    data = g.json()
    assert data["status"] == "pending"
    assert data["reservation"] is None
    assert data["start_utc"] == "2026-03-09T13:00:00Z"

    _cancel(client, 1)
    g = client.get("/api/v1/reservation-waitlist/wl-2")
    assert g.status_code == 200
    data = g.json()
    assert data["status"] == "promoted"
    assert data["reservation"]["reservation_id"] == 2
    assert data["promoted_at"] is not None


def test_get_unknown_request_id_returns_404(client):
    resp = client.get("/api/v1/reservation-waitlist/never-seen")
    assert resp.status_code == 404


def test_get_returns_latest_reservation_state_after_reschedule_and_cancel(client):
    client.post("/api/v1/reservation-waitlist", json=_wl_payload())
    rid = 1
    # 晋升得到的预约可正常改期：GET 候补应反映最新状态
    resp = client.put(f"/api/v1/reservations/{rid}", json={
        "expected_version": 1,
        "start_utc": "2026-03-09T15:00:00Z",
        "end_utc": "2026-03-09T16:00:00Z",
        "participant_ids": ["alice", "carol"],
    })
    assert resp.status_code == 200
    g = client.get("/api/v1/reservation-waitlist/wl-1").json()
    assert g["reservation"]["version"] == 2
    assert g["reservation"]["participants"] == ["alice", "carol"]
    assert g["reservation"]["start_utc"] == "2026-03-09T15:00:00Z"
    # 取消后关联仍保留，GET 显示 canceled 终态
    _cancel(client, rid, expected_version=2)
    g = client.get("/api/v1/reservation-waitlist/wl-1").json()
    assert g["status"] == "promoted"
    assert g["reservation"]["status"] == "canceled"


# ------------------------------------------------------- 取消触发 FIFO 晋升

def test_cancel_promotes_fifo_head_atomically(client):
    # 阻塞预约 #1（alice+bob 13-14）
    assert client.post("/api/v1/reservations",
                       json=_resv_payload(request_id="blk")
                       ).status_code == 201
    # 两条候补按创建时间先后入队
    w1 = client.post("/api/v1/reservation-waitlist", json=_wl_payload(
        request_id="wl-a",
        start_utc="2026-03-09T13:30:00Z",
        end_utc="2026-03-09T13:45:00Z",
        participant_ids=["alice"]))
    w2 = client.post("/api/v1/reservation-waitlist", json=_wl_payload(
        request_id="wl-b",
        start_utc="2026-03-09T13:30:00Z",
        end_utc="2026-03-09T13:45:00Z",
        participant_ids=["alice"]))
    assert w1.json()["status"] == "pending"
    assert w2.json()["status"] == "pending"

    # 取消 #1：同一事务内 FIFO 队首 wl-a 晋升；wl-b 仍 pending
    assert _cancel(client, 1).status_code == 200
    ga = client.get("/api/v1/reservation-waitlist/wl-a").json()
    gb = client.get("/api/v1/reservation-waitlist/wl-b").json()
    assert ga["status"] == "promoted"
    assert ga["reservation"]["participants"] == ["alice"]
    assert ga["reservation"]["start_utc"] == "2026-03-09T13:30:00Z"
    assert gb["status"] == "pending"

    # 晋升产生的正式预约与普通创建完全等价：created 哈希链可验
    promoted_id = ga["reservation"]["reservation_id"]
    history = client.get(
        f"/api/v1/reservations/{promoted_id}/history")
    assert history.status_code == 200
    actions = [e["action"] for e in history.json()["events"]]
    assert actions == ["created"]


def test_promotion_writes_created_event_in_same_transaction(client, db_path):
    assert client.post("/api/v1/reservations",
                       json=_resv_payload(request_id="blk")
                       ).status_code == 201
    client.post("/api/v1/reservation-waitlist", json=_wl_payload(
        request_id="wl-a", participant_ids=["alice"]))

    with sqlite3.connect(db_path) as conn:
        before_res = conn.execute("SELECT COUNT(*) FROM reservations").fetchone()[0]
        before_ev = conn.execute(
            "SELECT COUNT(*) FROM reservation_events").fetchone()[0]

    assert _cancel(client, 1).status_code == 200

    with sqlite3.connect(db_path) as conn:
        after_res = conn.execute("SELECT COUNT(*) FROM reservations").fetchone()[0]
        rows = conn.execute(
            "SELECT r.id, r.status, r.request_id,"
            " (SELECT COUNT(*) FROM reservation_events e"
            "   WHERE e.reservation_id = r.id) AS ev_count"
            " FROM reservations r ORDER BY r.id").fetchall()
    # 取消后新增一条正式预约；每条预约都恰好有与其生命周期匹配的事件
    assert after_res == before_res + 1
    # #1：created + canceled；晋升的新预约：created
    assert rows == [(1, "canceled", "blk", 2), (2, "active", "wl-a", 1)]
    assert before_ev + 2 == sum(r[3] for r in rows)


def test_fifo_order_uses_creation_time_then_id(client, db_path):
    # 创建时间先后决定 FIFO；created_at 相同时由自增 id 兜底，顺序即插入顺序。
    assert client.post("/api/v1/reservations",
                       json=_resv_payload(request_id="blk")
                       ).status_code == 201
    for rid in ("wl-1", "wl-2", "wl-3"):
        assert client.post("/api/v1/reservation-waitlist", json=_wl_payload(
            request_id=rid, participant_ids=["alice"])).status_code == 201

    # 取消 #1 → 最早上队的 wl-1 晋升（预约 #2，继续占住该时段）
    _cancel(client, 1)
    assert client.get("/api/v1/reservation-waitlist/wl-1").json()["status"] == "promoted"
    assert client.get("/api/v1/reservation-waitlist/wl-2").json()["status"] == "pending"
    assert client.get("/api/v1/reservation-waitlist/wl-3").json()["status"] == "pending"

    # 把剩余候补的 created_at 改成相同值：取消 #2 后应由 id 最小的 wl-2 晋升
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE reservation_waitlist SET created_at = '2026-03-01T00:00:00.000000Z'"
            " WHERE status = 'pending'")
        conn.commit()
    _cancel(client, 2)
    assert client.get("/api/v1/reservation-waitlist/wl-2").json()["status"] == "promoted"
    assert client.get("/api/v1/reservation-waitlist/wl-3").json()["status"] == "pending"

    # 再取消 #3（wl-2 的预约）→ wl-3 晋升
    _cancel(client, 3)
    assert client.get("/api/v1/reservation-waitlist/wl-3").json()["status"] == "promoted"


def test_promotion_skips_still_conflicting_candidate(client):
    # #1 = alice+bob 13-14（将取消）；#2 = carol 13:30-13:45（持续占位）
    assert client.post("/api/v1/reservations", json=_resv_payload(
        request_id="r1", participant_ids=["alice", "bob"])).status_code == 201
    assert client.post("/api/v1/reservations", json=_resv_payload(
        request_id="r2",
        start_utc="2026-03-09T13:30:00Z",
        end_utc="2026-03-09T13:45:00Z",
        participant_ids=["carol"])).status_code == 201
    # wa（FIFO 更早）：alice+carol 13:30-13:45 —— 取消 #1 后仍被 #2（carol）挡住
    assert client.post("/api/v1/reservation-waitlist", json=_wl_payload(
        request_id="wa",
        start_utc="2026-03-09T13:30:00Z",
        end_utc="2026-03-09T13:45:00Z",
        participant_ids=["alice", "carol"])).json()["status"] == "pending"
    # wb（更晚）：bob 13:30-13:45 —— 取消 #1 后即可满足
    assert client.post("/api/v1/reservation-waitlist", json=_wl_payload(
        request_id="wb",
        start_utc="2026-03-09T13:30:00Z",
        end_utc="2026-03-09T13:45:00Z",
        participant_ids=["bob"])).json()["status"] == "pending"

    _cancel(client, 1)
    assert client.get("/api/v1/reservation-waitlist/wa").json()["status"] == "pending"
    assert client.get("/api/v1/reservation-waitlist/wb").json()["status"] == "promoted"

    # 之后 #2 也取消：wa 失去所有挡路者，被该次取消事务晋升
    _cancel(client, 2)
    assert client.get("/api/v1/reservation-waitlist/wa").json()["status"] == "promoted"


def test_promotion_skips_candidate_blocked_by_unexpired_hold(client, db_path):
    assert client.post("/api/v1/reservations", json=_resv_payload(
        request_id="r1")).status_code == 201
    # 候补窗口比取消窗口更长：13:30-14:30（#1 占 13-14）
    assert client.post("/api/v1/reservation-waitlist", json=_wl_payload(
        request_id="wl-wide",
        start_utc="2026-03-09T13:30:00Z",
        end_utc="2026-03-09T14:30:00Z",
        participant_ids=["alice"])).json()["status"] == "pending"
    # 他人在 14:00-14:30 持有未过期保留（此时段空闲，可成功）
    assert client.post("/api/v1/reservation-holds", json=_hold_payload(
        hold_key="h-tail",
        start_utc="2026-03-09T14:00:00Z",
        end_utc="2026-03-09T14:30:00Z")).status_code == 201

    _cancel(client, 1)
    # 候补仍与未过期保留冲突 → 跳过（无其他候选），保持 pending
    assert client.get("/api/v1/reservation-waitlist/wl-wide").json()["status"] == "pending"

    # 保留到期（直接改写过期时间模拟）后，再由另一次取消触发扫描 → 晋升
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE reservation_holds SET expires_at = '2000-01-01T00:00:00.000000Z'"
            " WHERE hold_key = 'h-tail'")
        conn.commit()
    # 新的可取消预约需与 wl-wide 时段相交（14:15-14:20 在已过期保留腾出后可建）
    r3 = client.post("/api/v1/reservations", json=_resv_payload(
        request_id="r3",
        start_utc="2026-03-09T14:15:00Z",
        end_utc="2026-03-09T14:20:00Z",
        participant_ids=["alice"]))
    assert r3.status_code == 201  # 写事务入口已惰性过期保留
    _cancel(client, r3.json()["reservation_id"])
    assert client.get(
        "/api/v1/reservation-waitlist/wl-wide").json()["status"] == "promoted"


def test_promotion_only_scans_affected_candidates(client):
    # 不相关参与者 / 不相交时段的候补不被扫描
    assert client.post("/api/v1/reservations", json=_resv_payload(
        request_id="r1", participant_ids=["alice"])).status_code == 201
    # zoe 的候补（不相交参与者）当前即可直订
    assert client.post("/api/v1/reservation-waitlist", json=_wl_payload(
        request_id="wl-zoe", participant_ids=["zoe"])).json()["status"] == "promoted"
    # alice 候补排队
    assert client.post("/api/v1/reservation-waitlist", json=_wl_payload(
        request_id="wl-alice", participant_ids=["alice"])).json()["status"] == "pending"
    _cancel(client, 1)
    assert client.get(
        "/api/v1/reservation-waitlist/wl-alice").json()["status"] == "promoted"
    # zoe 的预约不受任何影响
    zoe = client.get("/api/v1/reservation-waitlist/wl-zoe").json()
    assert zoe["reservation"]["status"] == "active"


def test_adjacent_window_candidate_not_promoted_by_cancel(client):
    # 候选窗口与被取消窗口首尾相接（半开不重叠）→ 不受影响；其本身本就可直订
    assert client.post("/api/v1/reservations", json=_resv_payload(
        request_id="r1", participant_ids=["alice"])).status_code == 201
    resp = client.post("/api/v1/reservation-waitlist", json=_wl_payload(
        request_id="wl-adj",
        start_utc="2026-03-09T14:00:00Z",
        end_utc="2026-03-09T15:00:00Z",
        participant_ids=["alice"]))
    # 相邻区间本就无冲突 → 直接 promoted，无需等待取消
    assert resp.json()["status"] == "promoted"


def test_idempotent_cancel_replay_does_not_promote_again(client, db_path):
    assert client.post("/api/v1/reservations",
                       json=_resv_payload(request_id="blk")
                       ).status_code == 201
    for rid in ("wl-1", "wl-2", "wl-3"):
        client.post("/api/v1/reservation-waitlist",
                    json=_wl_payload(request_id=rid, participant_ids=["alice"]))

    assert _cancel(client, 1, expected_version=1).status_code == 200
    # 原 expected_version 的幂等重试：不再次晋升
    assert _cancel(client, 1, expected_version=1).status_code == 200
    with sqlite3.connect(db_path) as conn:
        promoted_n = conn.execute(
            "SELECT COUNT(*) FROM reservation_waitlist"
            " WHERE status = 'promoted'").fetchone()[0]
        pending_n = conn.execute(
            "SELECT COUNT(*) FROM reservation_waitlist"
            " WHERE status = 'pending'").fetchone()[0]
    assert promoted_n == 1 and pending_n == 2


def test_cancel_version_mismatch_rolls_back_promotion(client):
    # 版本不符的取消整体回滚：候补不晋升、占用不释放
    assert client.post("/api/v1/reservations",
                       json=_resv_payload(request_id="blk")
                       ).status_code == 201
    client.post("/api/v1/reservation-waitlist",
                json=_wl_payload(request_id="wl-1", participant_ids=["alice"]))
    resp = _cancel(client, 1, expected_version=99)
    assert resp.status_code == 409
    assert client.get(
        "/api/v1/reservation-waitlist/wl-1").json()["status"] == "pending"
    # 时段仍被占用
    blocked = client.post("/api/v1/reservations", json=_resv_payload(
        request_id="other", participant_ids=["alice"]))
    assert blocked.status_code == 409


def test_cancel_and_promotion_commit_atomically_on_promotion_failure(
        client, db_path, monkeypatch):
    # 晋升写入若失败，取消（含 canceled 事件、占用释放、候补状态）整体回滚
    from app import waitlist as waitlist_core

    assert client.post("/api/v1/reservations",
                       json=_resv_payload(request_id="blk")
                       ).status_code == 201
    client.post("/api/v1/reservation-waitlist",
                json=_wl_payload(request_id="wl-1", participant_ids=["alice"]))

    real_insert = reservation_core.insert_reservation
    calls = {"n": 0}

    def flaky_insert(conn, **kwargs):
        calls["n"] += 1
        if kwargs.get("request_id") == "wl-1":
            raise RuntimeError("injected promotion failure")
        return real_insert(conn, **kwargs)

    monkeypatch.setattr(reservation_core, "insert_reservation", flaky_insert)
    # waitlist 模块以 from .reservations import insert_reservation 绑定了同一对象
    monkeypatch.setattr(waitlist_core, "insert_reservation", flaky_insert)

    with pytest.raises(RuntimeError, match="injected"):
        reservation_core.cancel_reservation(1, expected_version=1, db_path=db_path)

    with sqlite3.connect(db_path) as conn:
        status, version = conn.execute(
            "SELECT status, version FROM reservations WHERE id = 1").fetchone()
        canceled_events = conn.execute(
            "SELECT COUNT(*) FROM reservation_events"
            " WHERE reservation_id = 1 AND action = 'canceled'").fetchone()[0]
        occupancy = conn.execute(
            "SELECT COUNT(*) FROM reservation_participants"
            " WHERE reservation_id = 1").fetchone()[0]
        wl_status = conn.execute(
            "SELECT status FROM reservation_waitlist WHERE request_id='wl-1'"
        ).fetchone()[0]
        promoted_reservations = conn.execute(
            "SELECT COUNT(*) FROM reservations WHERE request_id='wl-1'").fetchone()[0]
    # 全部回到取消前状态（阻塞预约含 alice/bob 两条占用行）
    assert (status, version) == ("active", 1)
    assert canceled_events == 0
    assert occupancy == 2
    assert wl_status == "pending"
    assert promoted_reservations == 0
    assert calls["n"] == 1

    # 移除故障后取消恢复正常：候补成功晋升
    monkeypatch.undo()
    reservation_core.cancel_reservation(1, expected_version=1, db_path=db_path)
    assert waitlist_core.get_waitlist_entry(
        "wl-1", db_path=db_path)[0].status == "promoted"


# ------------------------------------------------- 跨端点 request_id 一致性

def test_direct_reservation_then_waitlist_same_id_same_content_200(client, db_path):
    assert client.post("/api/v1/reservations", json=_resv_payload(
        request_id="shared")).status_code == 201
    resp = client.post("/api/v1/reservation-waitlist",
                       json=_wl_payload(request_id="shared"))
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "promoted"
    assert data["reservation"]["reservation_id"] == 1
    # 不新建正式预约
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM reservations WHERE request_id='shared'"
        ).fetchone()[0] == 1


def test_direct_reservation_then_waitlist_same_id_diff_content_409(client):
    assert client.post("/api/v1/reservations", json=_resv_payload(
        request_id="shared")).status_code == 201
    resp = client.post("/api/v1/reservation-waitlist", json=_wl_payload(
        request_id="shared", end_utc="2026-03-09T15:00:00Z"))
    assert resp.status_code == 409
    assert resp.json()["existing_reservation_id"] == 1


def test_pending_waitlist_blocks_direct_reservation_same_id_diff_content(client):
    assert client.post("/api/v1/reservations",
                       json=_resv_payload(request_id="blk")
                       ).status_code == 201
    assert client.post("/api/v1/reservation-waitlist", json=_wl_payload(
        request_id="wl-1", participant_ids=["alice"])).json()["status"] == "pending"
    # 同 request_id 不同内容直接预约 → 409（候补占用了该幂等键）
    resp = client.post("/api/v1/reservations", json=_resv_payload(
        request_id="wl-1", end_utc="2026-03-09T14:30:00Z",
        participant_ids=["alice"]))
    assert resp.status_code == 409
    assert resp.json()["existing_waitlist_id"]
    assert client.get(
        "/api/v1/reservation-waitlist/wl-1").json()["status"] == "pending"


def test_cancel_promotion_then_direct_replay_is_idempotent(client):
    assert client.post("/api/v1/reservations",
                       json=_resv_payload(request_id="blk")
                       ).status_code == 201
    client.post("/api/v1/reservation-waitlist",
                json=_wl_payload(request_id="wl-1", participant_ids=["alice"]))
    _cancel(client, 1)
    promoted_id = client.get(
        "/api/v1/reservation-waitlist/wl-1").json()["reservation"]["reservation_id"]
    # 取消已把 wl-1 晋升为正式预约；同内容 POST /reservations 是幂等重放 → 200
    resp = client.post("/api/v1/reservations",
                       json=_resv_payload(request_id="wl-1",
                                          participant_ids=["alice"]))
    assert resp.status_code == 200
    assert resp.json()["reservation_id"] == promoted_id


def test_direct_reservation_fulfils_pending_waitlist_when_slot_freed_by_hold_expiry(
        client, db_path):
    # 候补仅因未过期保留而 pending；保留到期（非取消）后，同 request_id 同内容
    # 的直接预约兑现该候补（回填 promoted），不产生重复行。
    assert client.post("/api/v1/reservation-holds", json=_hold_payload(
        hold_key="h1")).status_code == 201
    assert client.post("/api/v1/reservation-waitlist", json=_wl_payload(
        request_id="wl-1")).json()["status"] == "pending"

    # 直接把保留改为已过期，模拟 TTL 到期
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE reservation_holds SET expires_at = '2000-01-01T00:00:00.000000Z'"
            " WHERE id = 1")
        conn.commit()

    resp = client.post("/api/v1/reservations", json=_resv_payload(
        request_id="wl-1"))
    assert resp.status_code == 201, resp.text  # 写事务入口惰性过期保留后时段空闲
    rid = resp.json()["reservation_id"]
    g = client.get("/api/v1/reservation-waitlist/wl-1").json()
    assert g["status"] == "promoted"
    assert g["reservation"]["reservation_id"] == rid


def test_hold_confirm_same_id_diff_content_409(client):
    assert client.post("/api/v1/reservations", json=_resv_payload(
        request_id="blk")).status_code == 201
    # 候补窗口 13:30-14:30（pending）
    assert client.post("/api/v1/reservation-waitlist", json=_wl_payload(
        request_id="wl-wide",
        start_utc="2026-03-09T13:30:00Z",
        end_utc="2026-03-09T14:30:00Z")).json()["status"] == "pending"
    # 保留 14:00-14:30（空闲时段）
    assert client.post("/api/v1/reservation-holds", json=_hold_payload(
        hold_key="h-tail",
        start_utc="2026-03-09T14:00:00Z",
        end_utc="2026-03-09T14:30:00Z")).status_code == 201
    # 用候补的 request_id 确认保留：窗口内容不一致 → 409，保留仍 pending
    resp = client.post("/api/v1/reservation-holds/1/confirm",
                       json={"request_id": "wl-wide"})
    assert resp.status_code == 409
    assert resp.json()["existing_waitlist_id"]
    # 换 request_id 可正常确认
    assert client.post("/api/v1/reservation-holds/1/confirm",
                       json={"request_id": "req-other"}).status_code == 201


def test_hold_confirm_same_id_same_content_fulfils_pending_waitlist(client, db_path):
    # 保留先占住时段 → 候补入 pending；用候补的 request_id 同内容确认保留
    assert client.post("/api/v1/reservation-holds", json=_hold_payload(
        hold_key="h1",
        start_utc="2026-04-01T09:00:00Z",
        end_utc="2026-04-01T10:00:00Z")).status_code == 201
    assert client.post("/api/v1/reservation-waitlist", json=_wl_payload(
        request_id="wh1",
        start_utc="2026-04-01T09:00:00Z",
        end_utc="2026-04-01T10:00:00Z",
        participant_ids=["alice"])).json()["status"] == "pending"

    resp = client.post("/api/v1/reservation-holds/1/confirm",
                       json={"request_id": "wh1"})
    assert resp.status_code == 201, resp.text
    rid = resp.json()["reservation_id"]

    g = client.get("/api/v1/reservation-waitlist/wh1").json()
    assert g["status"] == "promoted"
    assert g["reservation"]["reservation_id"] == rid

    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM reservations WHERE request_id='wh1'"
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM reservation_events e"
            " JOIN reservations r ON r.id = e.reservation_id"
            " WHERE r.request_id = 'wh1' AND e.action = 'created'"
        ).fetchone()[0] == 1


# ---------------------------------------------------------------- 并发

def test_concurrent_waits_then_cancel_promotes_exactly_one(db_path):
    blocker, _ = reservation_core.create_reservation(
        "blk", _utc(2026, 3, 9, 13), _utc(2026, 3, 9, 14),
        ["alice"], db_path=db_path)
    barrier = threading.Barrier(12)

    def join(i: int):
        barrier.wait()
        return waitlist_core.join_waitlist(
            f"wl-{i:02d}", _utc(2026, 3, 9, 13, 30), _utc(2026, 3, 9, 13, 45),
            ["alice"], db_path=db_path)

    with ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(join, range(12)))
    assert all(res.reservation is None for res in results)

    reservation_core.cancel_reservation(
        blocker.id, expected_version=1, db_path=db_path)

    with sqlite3.connect(db_path) as conn:
        promoted = conn.execute(
            "SELECT request_id FROM reservation_waitlist"
            " WHERE status='promoted'").fetchall()
        pending_n = conn.execute(
            "SELECT COUNT(*) FROM reservation_waitlist"
            " WHERE status='pending'").fetchone()[0]
        created_events = conn.execute(
            "SELECT COUNT(*) FROM reservation_events WHERE action='created'"
        ).fetchone()[0]
    assert len(promoted) == 1
    assert pending_n == 11
    # 阻塞预约 created + 晋升预约 created
    assert created_events == 2


def test_concurrent_cancels_promote_at_most_one(db_path):
    blocker, _ = reservation_core.create_reservation(
        "blk", _utc(2026, 3, 9, 13), _utc(2026, 3, 9, 14),
        ["alice"], db_path=db_path)
    for i in range(6):
        waitlist_core.join_waitlist(
            f"wl-{i}", _utc(2026, 3, 9, 13, 30), _utc(2026, 3, 9, 13, 45),
            ["alice"], db_path=db_path)

    barrier = threading.Barrier(8)

    def cancel():
        barrier.wait()
        return reservation_core.cancel_reservation(
            blocker.id, expected_version=1, db_path=db_path)

    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(lambda _: cancel(), range(8)))
    assert sum(1 for _, did in outcomes if did) == 1

    with sqlite3.connect(db_path) as conn:
        promoted_n = conn.execute(
            "SELECT COUNT(*) FROM reservation_waitlist"
            " WHERE status='promoted'").fetchone()[0]
    assert promoted_n == 1


# ------------------------------------------------------- 旧库无损迁移

def test_init_creates_waitlist_tables_losslessly(tmp_path, monkeypatch):
    path = str(tmp_path / "legacy.db")
    conn = sqlite3.connect(path)
    try:
        conn.executescript("""\
CREATE TABLE reservations (
    id INTEGER PRIMARY KEY AUTOINCREMENT, request_id TEXT NOT NULL UNIQUE,
    start_utc TEXT NOT NULL, end_utc TEXT NOT NULL,
    participants_key TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE reservation_participants (
    reservation_id INTEGER NOT NULL REFERENCES reservations(id) ON DELETE CASCADE,
    participant_id TEXT NOT NULL, start_utc TEXT NOT NULL, end_utc TEXT NOT NULL,
    PRIMARY KEY (reservation_id, participant_id));
""")
        conn.execute(
            "INSERT INTO reservations"
            " (request_id, start_utc, end_utc, participants_key, created_at)"
            " VALUES ('old', '2026-03-09T13:00:00.000000Z',"
            " '2026-03-09T14:00:00.000000Z', '[\"alice\"]',"
            " '2026-03-01T00:00:00.000000Z')")
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setenv("RESERVATIONS_DB_PATH", path)
    db_module.init_db(path)
    db_module.init_db(path)  # 幂等重复执行

    conn = sqlite3.connect(path)
    try:
        names = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        assert {"reservation_waitlist",
                "reservation_waitlist_participants"} <= names
        # 既有预约与占用无损
        assert conn.execute(
            "SELECT request_id, participants_key FROM reservations"
        ).fetchone() == ("old", "[\"alice\"]")
    finally:
        conn.close()

    # 新表立即可用：候补直订 + 取消触发晋升
    result = waitlist_core.join_waitlist(
        "wl-new", _utc(2026, 5, 1, 13), _utc(2026, 5, 1, 14),
        ["alice"], db_path=path)
    assert result.inserted is True
    assert result.entry.status == "promoted"
    assert result.reservation is not None

    blocker, _ = reservation_core.create_reservation(
        "blk2", _utc(2026, 6, 1, 13), _utc(2026, 6, 1, 14),
        ["bob"], db_path=path)
    pending = waitlist_core.join_waitlist(
        "wl-p", _utc(2026, 6, 1, 13, 30), _utc(2026, 6, 1, 13, 45),
        ["bob"], db_path=path)
    assert pending.entry.status == "pending"
    reservation_core.cancel_reservation(
        blocker.id, expected_version=1, db_path=path)
    entry, promoted_res = waitlist_core.get_waitlist_entry(
        "wl-p", db_path=path)
    assert entry.status == "promoted" and promoted_res is not None


# ---------------------------------------------------------------- 核心层

def test_core_get_waitlist_entry_not_found(db_path):
    with pytest.raises(waitlist_core.WaitlistNotFoundError):
        waitlist_core.get_waitlist_entry("nope", db_path=db_path)


def test_core_join_and_get_roundtrip(db_path):
    result = waitlist_core.join_waitlist(
        "wl-x", _utc(2026, 3, 9, 13), _utc(2026, 3, 9, 14),
        ["bob", "alice"], db_path=db_path)
    assert result.inserted is True
    assert result.entry.participants == ("alice", "bob")
    assert result.reservation is not None

    entry, reservation = waitlist_core.get_waitlist_entry(
        "wl-x", db_path=db_path)
    assert entry.status == "promoted"
    assert reservation is not None and reservation.request_id == "wl-x"

    replay = waitlist_core.join_waitlist(
        "wl-x", _utc(2026, 3, 9, 13), _utc(2026, 3, 9, 14),
        ["alice", "bob"], db_path=db_path)
    assert replay.inserted is False
    assert replay.entry.id == result.entry.id
