"""预约时段原子交换接口测试：

- 成功交换：双方 start_utc/end_utc 互换、version 各 +1、
  request_id/参与者/created_at 不变，占用随新时段平移；
- 交换前校验：任一不存在 404、任一已取消 409、任一版本不符 409；
- 冲突检查：排除交换双方后，新时段与其他进行中预约/未过期保留重叠 409，
  相邻（半开）不冲突，交换双方互为占用不算冲突；
- 事务原子性：任一失败整体回滚（双方预约、占用、审计链均不变）；
- 审计：成功交换为双方各追加一条 rescheduled 哈希链事件，失败不产生事件；
- 并发：同一对预约的并发交换恰一个赢家。
"""

from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app import reservations as core
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


def _resv_payload(**overrides):
    payload = {
        "request_id": "req-1",
        "start_utc": "2026-03-09T13:00:00Z",
        "end_utc": "2026-03-09T14:00:00Z",
        "participant_ids": ["alice", "bob"],
    }
    payload.update(overrides)
    return payload


def _utc(y, mo, d, h, mi=0):
    return datetime(y, mo, d, h, mi, tzinfo=timezone.utc)


def _create_pair(client):
    """创建两条互不冲突的预约：#A alice 13:00–14:00、#B carol 16:00–17:00。"""
    first = client.post("/api/v1/reservations", json=_resv_payload(
        request_id="req-A",
        start_utc="2026-03-09T13:00:00Z",
        end_utc="2026-03-09T14:00:00Z",
        participant_ids=["alice"],
    )).json()
    second = client.post("/api/v1/reservations", json=_resv_payload(
        request_id="req-B",
        start_utc="2026-03-09T16:00:00Z",
        end_utc="2026-03-09T17:00:00Z",
        participant_ids=["carol"],
    )).json()
    return first, second


def _swap(client, first_id, second_id, first_version=1, second_version=1):
    return client.post("/api/v1/reservations/swap", json={
        "first": {"reservation_id": first_id, "expected_version": first_version},
        "second": {"reservation_id": second_id, "expected_version": second_version},
    })


def _force_hold_expired(path: str, hold_id: int) -> None:
    """直接把保留的 expires_at 改到过去（不翻转 status），模拟 TTL 到期。"""
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "UPDATE reservation_holds SET expires_at = ? WHERE id = ?",
            ("2000-01-01T00:00:00.000000Z", hold_id),
        )
        conn.commit()
    finally:
        conn.close()


# ------------------------------------------------------------------ 成功交换

def test_swap_success_exchanges_windows_and_bumps_versions(client):
    first, second = _create_pair(client)

    resp = _swap(client, first["reservation_id"], second["reservation_id"])
    assert resp.status_code == 200, resp.text
    data = resp.json()

    out_a, out_b = data["first"], data["second"]
    # 时段互换
    assert out_a["start_utc"] == "2026-03-09T16:00:00Z"
    assert out_a["end_utc"] == "2026-03-09T17:00:00Z"
    assert out_b["start_utc"] == "2026-03-09T13:00:00Z"
    assert out_b["end_utc"] == "2026-03-09T14:00:00Z"
    # 版本各 +1
    assert out_a["version"] == first["version"] + 1
    assert out_b["version"] == second["version"] + 1
    # request_id / 参与者 / created_at / 状态保持不变
    assert out_a["request_id"] == first["request_id"]
    assert out_b["request_id"] == second["request_id"]
    assert out_a["participants"] == first["participants"]
    assert out_b["participants"] == second["participants"]
    assert out_a["created_at"] == first["created_at"]
    assert out_b["created_at"] == second["created_at"]
    assert out_a["status"] == out_b["status"] == "active"
    assert out_a["canceled_at"] is None and out_b["canceled_at"] is None


def test_swap_moves_occupancy_to_new_windows(client):
    first, second = _create_pair(client)
    assert _swap(client, first["reservation_id"],
                 second["reservation_id"]).status_code == 200

    # alice 的旧窗口 13:00–14:00 已释放：他人可预约
    resp = client.post("/api/v1/reservations", json=_resv_payload(
        request_id="req-alice-old-free",
        start_utc="2026-03-09T13:00:00Z",
        end_utc="2026-03-09T14:00:00Z",
        participant_ids=["alice"],
    ))
    assert resp.status_code == 201, resp.text
    # alice 的新窗口 16:00–17:00 被占用
    resp = client.post("/api/v1/reservations", json=_resv_payload(
        request_id="req-alice-new-busy",
        start_utc="2026-03-09T16:30:00Z",
        end_utc="2026-03-09T16:45:00Z",
        participant_ids=["alice"],
    ))
    assert resp.status_code == 409
    # carol 的新窗口 13:00–14:00 被占用（与上面 alice 的预约参与者不相交）
    resp = client.post("/api/v1/reservations", json=_resv_payload(
        request_id="req-carol-new-busy",
        start_utc="2026-03-09T13:30:00Z",
        end_utc="2026-03-09T13:45:00Z",
        participant_ids=["carol"],
    ))
    assert resp.status_code == 409
    # carol 的旧窗口 16:00–17:00 已释放
    resp = client.post("/api/v1/reservations", json=_resv_payload(
        request_id="req-carol-old-free",
        start_utc="2026-03-09T16:00:00Z",
        end_utc="2026-03-09T17:00:00Z",
        participant_ids=["carol"],
    ))
    assert resp.status_code == 201, resp.text


def test_swap_after_reschedule_uses_current_versions(client):
    first, second = _create_pair(client)
    # 先把第一条改期到 15:00–16:00（version → 2），再与第二条交换
    assert client.put(
        f"/api/v1/reservations/{first['reservation_id']}", json={
            "expected_version": 1,
            "start_utc": "2026-03-09T15:00:00Z",
            "end_utc": "2026-03-09T16:00:00Z",
            "participant_ids": ["alice"],
        }).status_code == 200

    resp = _swap(client, first["reservation_id"], second["reservation_id"],
                 first_version=2, second_version=1)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["first"]["start_utc"] == "2026-03-09T16:00:00Z"
    assert data["first"]["version"] == 3
    assert data["second"]["start_utc"] == "2026-03-09T15:00:00Z"
    assert data["second"]["version"] == 2


def test_swap_same_participant_in_both_legs_is_allowed(client):
    """交换双方共享参与者：双方原时段本就不重叠，交换后占用集合不变。"""
    first = client.post("/api/v1/reservations", json=_resv_payload(
        request_id="req-A",
        start_utc="2026-03-09T13:00:00Z",
        end_utc="2026-03-09T14:00:00Z",
        participant_ids=["alice"],
    )).json()
    # 紧邻其后的同一参与者（半开区间相邻不冲突）
    second = client.post("/api/v1/reservations", json=_resv_payload(
        request_id="req-B",
        start_utc="2026-03-09T14:00:00Z",
        end_utc="2026-03-09T15:00:00Z",
        participant_ids=["alice"],
    )).json()

    # 不排除交换双方时，对方在自己新时段上的旧占用会被误判为冲突
    resp = _swap(client, first["reservation_id"], second["reservation_id"])
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["first"]["start_utc"] == "2026-03-09T14:00:00Z"
    assert data["second"]["start_utc"] == "2026-03-09T13:00:00Z"


def test_swap_adjacent_third_party_window_does_not_conflict(client):
    """第三方预约与交换后的新时段相邻（半开区间首尾相接）不算冲突。"""
    first, second = _create_pair(client)
    # dave 的预约与 first 的新时段 16:00–17:00 相邻
    assert client.post("/api/v1/reservations", json=_resv_payload(
        request_id="req-adjacent",
        start_utc="2026-03-09T17:00:00Z",
        end_utc="2026-03-09T18:00:00Z",
        participant_ids=["alice"],
    )).status_code == 201

    resp = _swap(client, first["reservation_id"], second["reservation_id"])
    assert resp.status_code == 200, resp.text


# ------------------------------------------------------------------ 前置校验

def test_swap_unknown_id_returns_404(client):
    first, _ = _create_pair(client)
    # 第二条不存在
    resp = _swap(client, first["reservation_id"], 999)
    assert resp.status_code == 404
    assert "999" in resp.json()["detail"]
    # 第一条不存在
    resp = _swap(client, 998, first["reservation_id"])
    assert resp.status_code == 404
    assert "998" in resp.json()["detail"]


def test_swap_canceled_reservation_returns_409(client):
    first, second = _create_pair(client)
    assert client.delete(
        f"/api/v1/reservations/{second['reservation_id']}",
        params={"expected_version": 1},
    ).status_code == 200

    resp = _swap(client, first["reservation_id"], second["reservation_id"],
                 second_version=2)
    assert resp.status_code == 409
    assert "canceled" in resp.json()["detail"]
    assert resp.json()["current_version"] == 2

    # 第一条已取消同样 409
    third = client.post("/api/v1/reservations", json=_resv_payload(
        request_id="req-C",
        start_utc="2026-03-09T18:00:00Z",
        end_utc="2026-03-09T19:00:00Z",
        participant_ids=["dave"],
    )).json()
    assert client.delete(
        f"/api/v1/reservations/{first['reservation_id']}",
        params={"expected_version": 1},
    ).status_code == 200
    resp = _swap(client, first["reservation_id"], third["reservation_id"])
    assert resp.status_code == 409
    assert "canceled" in resp.json()["detail"]


def test_swap_stale_version_returns_409_with_current_version(client):
    first, second = _create_pair(client)

    # 第一条版本不符
    resp = _swap(client, first["reservation_id"], second["reservation_id"],
                 first_version=5)
    assert resp.status_code == 409
    assert resp.json()["current_version"] == 1
    # 第二条版本不符
    resp = _swap(client, first["reservation_id"], second["reservation_id"],
                 second_version=7)
    assert resp.status_code == 409
    assert resp.json()["current_version"] == 1

    # 双方均未变化：带上正确版本交换成功
    resp = _swap(client, first["reservation_id"], second["reservation_id"])
    assert resp.status_code == 200, resp.text
    # 交换后旧版本立即过期
    resp = _swap(client, first["reservation_id"], second["reservation_id"],
                 first_version=1, second_version=1)
    assert resp.status_code == 409
    assert resp.json()["current_version"] == 2


def test_swap_same_reservation_id_returns_422(client):
    first, _ = _create_pair(client)
    rid = first["reservation_id"]
    resp = _swap(client, rid, rid)
    assert resp.status_code == 422


def test_swap_validation_errors_return_422(client):
    first, second = _create_pair(client)
    a, b = first["reservation_id"], second["reservation_id"]
    # expected_version < 1
    assert _swap(client, a, b, first_version=0).status_code == 422
    assert _swap(client, a, b, second_version=0).status_code == 422
    # reservation_id < 1
    assert _swap(client, 0, b).status_code == 422
    # 缺字段
    assert client.post("/api/v1/reservations/swap", json={
        "first": {"reservation_id": a},
        "second": {"reservation_id": b, "expected_version": 1},
    }).status_code == 422
    assert client.post("/api/v1/reservations/swap", json={}).status_code == 422


def test_swap_core_rejects_same_id(db_path):
    with pytest.raises(ValueError):
        core.swap_reservations(
            first_id=1, first_expected_version=1,
            second_id=1, second_expected_version=1,
            db_path=db_path,
        )


# ------------------------------------------------------------------ 冲突检测

def test_swap_conflict_with_other_reservation_returns_409(client):
    first, second = _create_pair(client)
    # dave 在 16:30–17:30 有预约，与 first 的新时段 16:00–17:00 重叠
    third = client.post("/api/v1/reservations", json=_resv_payload(
        request_id="req-C",
        start_utc="2026-03-09T16:30:00Z",
        end_utc="2026-03-09T17:30:00Z",
        participant_ids=["alice"],
    )).json()

    resp = _swap(client, first["reservation_id"], second["reservation_id"])
    assert resp.status_code == 409
    assert resp.json()["conflicts"] == [
        {"reservation_id": third["reservation_id"], "participant_id": "alice"}
    ]


def test_swap_conflict_with_unexpired_hold_returns_409(client):
    first, second = _create_pair(client)
    # carol 在 13:30–14:30 有未过期保留，与 second 的新时段 13:00–14:00 重叠
    hold = client.post("/api/v1/reservation-holds", json={
        "hold_key": "hold-block",
        "start_utc": "2026-03-09T13:30:00Z",
        "end_utc": "2026-03-09T14:30:00Z",
        "participant_ids": ["carol"],
        "ttl_seconds": 300,
    })
    assert hold.status_code == 201

    resp = _swap(client, first["reservation_id"], second["reservation_id"])
    assert resp.status_code == 409
    assert resp.json()["conflicts"] == [
        {"hold_id": hold.json()["hold_id"], "participant_id": "carol"}
    ]


def test_swap_ignores_expired_hold(client, db_path):
    first, second = _create_pair(client)
    hold = client.post("/api/v1/reservation-holds", json={
        "hold_key": "hold-expired",
        "start_utc": "2026-03-09T13:30:00Z",
        "end_utc": "2026-03-09T14:30:00Z",
        "participant_ids": ["carol"],
        "ttl_seconds": 300,
    }).json()
    _force_hold_expired(db_path, hold["hold_id"])

    resp = _swap(client, first["reservation_id"], second["reservation_id"])
    assert resp.status_code == 200, resp.text


# ------------------------------------------------------------------ 原子回滚

def test_failed_swap_rolls_back_everything(client):
    first, second = _create_pair(client)
    # 制造冲突：alice 在 first 的新时段上有另一场预约
    client.post("/api/v1/reservations", json=_resv_payload(
        request_id="req-blocker",
        start_utc="2026-03-09T16:30:00Z",
        end_utc="2026-03-09T17:30:00Z",
        participant_ids=["alice"],
    ))

    resp = _swap(client, first["reservation_id"], second["reservation_id"])
    assert resp.status_code == 409

    # 双方预约保持原状：幂等重放创建请求读到的仍是原窗口与原版本
    replay_a = client.post("/api/v1/reservations", json=_resv_payload(
        request_id="req-A",
        start_utc="2026-03-09T13:00:00Z",
        end_utc="2026-03-09T14:00:00Z",
        participant_ids=["alice"],
    ))
    assert replay_a.status_code == 200
    assert replay_a.json()["version"] == 1
    replay_b = client.post("/api/v1/reservations", json=_resv_payload(
        request_id="req-B",
        start_utc="2026-03-09T16:00:00Z",
        end_utc="2026-03-09T17:00:00Z",
        participant_ids=["carol"],
    ))
    assert replay_b.status_code == 200
    assert replay_b.json()["version"] == 1

    # 占用未平移：carol 在 13:00–14:00 仍空闲，在 16:00–17:00 仍被占用
    resp = client.post("/api/v1/reservations", json=_resv_payload(
        request_id="req-carol-still-free",
        start_utc="2026-03-09T13:30:00Z",
        end_utc="2026-03-09T13:45:00Z",
        participant_ids=["carol"],
    ))
    assert resp.status_code == 201, resp.text
    resp = client.post("/api/v1/reservations", json=_resv_payload(
        request_id="req-carol-still-busy",
        start_utc="2026-03-09T16:15:00Z",
        end_utc="2026-03-09T16:45:00Z",
        participant_ids=["carol"],
    ))
    assert resp.status_code == 409

    # 审计链未追加事件：双方历史仍只有 created 一条
    for rid in (first["reservation_id"], second["reservation_id"]):
        history = client.get(f"/api/v1/reservations/{rid}/history")
        assert history.status_code == 200
        assert [e["action"] for e in history.json()["events"]] == ["created"]


def test_version_conflict_swap_leaves_no_events(client):
    first, second = _create_pair(client)
    resp = _swap(client, first["reservation_id"], second["reservation_id"],
                 first_version=9)
    assert resp.status_code == 409
    for rid in (first["reservation_id"], second["reservation_id"]):
        history = client.get(f"/api/v1/reservations/{rid}/history")
        assert [e["action"] for e in history.json()["events"]] == ["created"]


# ------------------------------------------------------------------ 审计事件

def test_swap_appends_rescheduled_event_to_both_chains(client):
    first, second = _create_pair(client)
    assert _swap(client, first["reservation_id"],
                 second["reservation_id"]).status_code == 200

    history_a = client.get(
        f"/api/v1/reservations/{first['reservation_id']}/history").json()
    history_b = client.get(
        f"/api/v1/reservations/{second['reservation_id']}/history").json()

    for history, rid, start, end, version in (
        (history_a, first["reservation_id"],
         "2026-03-09T16:00:00Z", "2026-03-09T17:00:00Z", 2),
        (history_b, second["reservation_id"],
         "2026-03-09T13:00:00Z", "2026-03-09T14:00:00Z", 2),
    ):
        events = history["events"]
        assert [e["action"] for e in events] == ["created", "rescheduled"]
        assert [e["seq"] for e in events] == [1, 2]
        # rescheduled 快照为交换后的新状态
        snap = events[1]["snapshot"]
        assert snap["reservation_id"] == rid
        assert snap["start_utc"] == start
        assert snap["end_utc"] == end
        assert snap["version"] == version
        assert snap["status"] == "active"
        # 哈希链衔接：第二条 prev_hash 为第一条 hash
        assert events[1]["prev_hash"] == events[0]["hash"]
        assert events[0]["prev_hash"] == "0" * 64


# ---------------------------------------------------------------------- 并发

def test_concurrent_swaps_single_winner(db_path):
    """8 个并发交换（同一对预约、同一版本）：恰一个成功，其余 409 版本冲突。"""
    first, _ = core.create_reservation(
        request_id="req-A",
        start_utc=_utc(2026, 3, 9, 13),
        end_utc=_utc(2026, 3, 9, 14),
        participant_ids=["alice"],
        db_path=db_path,
    )
    second, _ = core.create_reservation(
        request_id="req-B",
        start_utc=_utc(2026, 3, 9, 16),
        end_utc=_utc(2026, 3, 9, 17),
        participant_ids=["carol"],
        db_path=db_path,
    )
    barrier = threading.Barrier(8)

    def worker():
        barrier.wait()
        return core.swap_reservations(
            first_id=first.id, first_expected_version=1,
            second_id=second.id, second_expected_version=1,
            db_path=db_path,
        )

    oks, stale = [], []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(lambda: worker()) for _ in range(8)]
        for f in futures:
            try:
                oks.append(f.result())
            except core.VersionConflictError as exc:
                stale.append(exc)

    assert len(oks) == 1
    assert len(stale) == 7
    swapped_a, swapped_b = oks[0]
    # 恰一次交换：双方版本各 +1，时段互换
    assert swapped_a.version == 2 and swapped_b.version == 2
    assert (swapped_a.start_utc, swapped_a.end_utc) == (
        _utc(2026, 3, 9, 16), _utc(2026, 3, 9, 17))
    assert (swapped_b.start_utc, swapped_b.end_utc) == (
        _utc(2026, 3, 9, 13), _utc(2026, 3, 9, 14))
    # 落败方看到的当前版本均为 2
    assert {e.current_version for e in stale} == {2}
