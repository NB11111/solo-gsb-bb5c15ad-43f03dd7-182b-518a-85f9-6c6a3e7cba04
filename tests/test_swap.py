"""两条 active 预约原子交换时段（POST /api/v1/reservations/swap）测试：

- 成功互换 start_utc/end_utc，参与者与 request_id 不变，两边版本各 +1；
- 双方互相排除：共享参与者可互换，相邻区间不冲突；
- 与第三方 active 预约 / 未过期 pending 保留冲突 → 409 且整体回滚
  （时间、占用、版本、审计事件全部保持原状）；
- 不存在 404、已取消 409、任一方版本过期 409（回传该方 current_version）；
- 同 ID / 非法字段 422；成功后两边各追加一条 rescheduled 哈希链事件；
- 交换不触发候补晋升；并发交换恰好一个赢家。
"""

from __future__ import annotations

import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app import db as db_module
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


def _utc(y, mo, d, h, mi=0):
    return datetime(y, mo, d, h, mi, tzinfo=timezone.utc)


def _create(client, request_id, start, end, participant_ids):
    resp = client.post("/api/v1/reservations", json={
        "request_id": request_id,
        "start_utc": start,
        "end_utc": end,
        "participant_ids": participant_ids,
    })
    assert resp.status_code == 201, resp.text
    return resp.json()


def _swap(client, id1, v1, id2, v2):
    return client.post("/api/v1/reservations/swap", json={
        "first": {"reservation_id": id1, "expected_version": v1},
        "second": {"reservation_id": id2, "expected_version": v2},
    })


# ------------------------------------------------------------------ 成功路径


def test_swap_success_swaps_windows_and_bumps_both_versions(client):
    first = _create(client, "req-1", "2026-03-09T13:00:00Z",
                    "2026-03-09T14:00:00Z", ["alice", "bob"])
    second = _create(client, "req-2", "2026-03-09T16:00:00Z",
                     "2026-03-09T17:00:00Z", ["carol"])

    resp = _swap(client, first["reservation_id"], 1, second["reservation_id"], 1)
    assert resp.status_code == 200, resp.text
    data = resp.json()

    a, b = data["first"], data["second"]
    assert a["reservation_id"] == first["reservation_id"]
    assert b["reservation_id"] == second["reservation_id"]
    # 时段互换
    assert a["start_utc"] == "2026-03-09T16:00:00Z"
    assert a["end_utc"] == "2026-03-09T17:00:00Z"
    assert b["start_utc"] == "2026-03-09T13:00:00Z"
    assert b["end_utc"] == "2026-03-09T14:00:00Z"
    # 两边版本各 +1
    assert a["version"] == 2 and b["version"] == 2


def test_swap_keeps_participants_request_id_and_created_at(client):
    first = _create(client, "req-1", "2026-03-09T13:00:00Z",
                    "2026-03-09T14:00:00Z", ["bob", "alice"])
    second = _create(client, "req-2", "2026-03-09T16:00:00Z",
                     "2026-03-09T17:00:00Z", ["carol"])

    data = _swap(client, first["reservation_id"], 1,
                 second["reservation_id"], 1).json()
    a, b = data["first"], data["second"]
    # 参与者集合不变（仍规范化排序）
    assert a["participants"] == ["alice", "bob"]
    assert b["participants"] == ["carol"]
    # request_id / created_at / 状态不变
    assert a["request_id"] == first["request_id"]
    assert b["request_id"] == second["request_id"]
    assert a["created_at"] == first["created_at"]
    assert b["created_at"] == second["created_at"]
    assert a["status"] == "active" and b["status"] == "active"
    assert a["canceled_at"] is None and b["canceled_at"] is None


def test_swap_response_order_matches_request_order(client):
    first = _create(client, "req-1", "2026-03-09T13:00:00Z",
                    "2026-03-09T14:00:00Z", ["alice"])
    second = _create(client, "req-2", "2026-03-09T16:00:00Z",
                     "2026-03-09T17:00:00Z", ["bob"])

    # 请求中调换双方位置：first=second 原预约
    data = _swap(client, second["reservation_id"], 1,
                 first["reservation_id"], 1).json()
    assert data["first"]["reservation_id"] == second["reservation_id"]
    assert data["second"]["reservation_id"] == first["reservation_id"]
    assert data["first"]["start_utc"] == "2026-03-09T13:00:00Z"
    assert data["second"]["start_utc"] == "2026-03-09T16:00:00Z"


def test_swap_shared_participants_allowed(client):
    """双方共享参与者：互换后彼此占用不算冲突（排除交换双方）。"""
    first = _create(client, "req-1", "2026-03-10T09:00:00Z",
                    "2026-03-10T10:00:00Z", ["dave", "erin"])
    second = _create(client, "req-2", "2026-03-10T11:00:00Z",
                     "2026-03-10T12:00:00Z", ["dave"])

    resp = _swap(client, first["reservation_id"], 1,
                 second["reservation_id"], 1)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["first"]["start_utc"] == "2026-03-10T11:00:00Z"
    assert data["second"]["start_utc"] == "2026-03-10T09:00:00Z"
    assert data["first"]["participants"] == ["dave", "erin"]
    assert data["second"]["participants"] == ["dave"]


def test_swap_adjacent_intervals_do_not_conflict(client):
    first = _create(client, "req-1", "2026-03-15T09:00:00Z",
                    "2026-03-15T10:00:00Z", ["mia"])
    second = _create(client, "req-2", "2026-03-15T10:00:00Z",
                     "2026-03-15T11:00:00Z", ["nick"])
    resp = _swap(client, first["reservation_id"], 1,
                 second["reservation_id"], 1)
    assert resp.status_code == 200, resp.text


def test_swap_moves_participant_occupancy_rows(client):
    """交换后旧窗口释放、新窗口按原参与者集合占位。"""
    first = _create(client, "req-1", "2026-03-09T13:00:00Z",
                    "2026-03-09T14:00:00Z", ["alice"])
    second = _create(client, "req-2", "2026-03-09T16:00:00Z",
                     "2026-03-09T17:00:00Z", ["carol"])
    assert _swap(client, first["reservation_id"], 1,
                 second["reservation_id"], 1).status_code == 200

    # alice 旧窗口已释放
    resp = client.post("/api/v1/reservations", json={
        "request_id": "req-alice-free",
        "start_utc": "2026-03-09T13:30:00Z",
        "end_utc": "2026-03-09T13:45:00Z",
        "participant_ids": ["alice"],
    })
    assert resp.status_code == 201, resp.text
    # alice 在新窗口（原 second 时段）被占用
    resp = client.post("/api/v1/reservations", json={
        "request_id": "req-alice-busy",
        "start_utc": "2026-03-09T16:30:00Z",
        "end_utc": "2026-03-09T16:45:00Z",
        "participant_ids": ["alice"],
    })
    assert resp.status_code == 409
    # carol 随 second 换到 13-14：该窗口 carol 被占用
    resp = client.post("/api/v1/reservations", json={
        "request_id": "req-carol-busy",
        "start_utc": "2026-03-09T13:30:00Z",
        "end_utc": "2026-03-09T13:45:00Z",
        "participant_ids": ["carol"],
    })
    assert resp.status_code == 409


# ------------------------------------------------------------------ 冲突回滚


def test_swap_conflict_with_third_party_returns_409_and_rolls_back(client):
    first = _create(client, "req-A", "2026-03-09T13:00:00Z",
                    "2026-03-09T14:00:00Z", ["alice"])
    second = _create(client, "req-B", "2026-03-09T16:00:00Z",
                     "2026-03-09T17:00:00Z", ["bob"])
    # 第三方在 second 的时段占用 alice：first 换过去会撞上
    third = _create(client, "req-C", "2026-03-09T16:30:00Z",
                    "2026-03-09T16:45:00Z", ["alice", "zoe"])

    resp = _swap(client, first["reservation_id"], 1,
                 second["reservation_id"], 1)
    assert resp.status_code == 409
    assert resp.json()["conflicts"] == [
        {"reservation_id": third["reservation_id"], "participant_id": "alice"}
    ]

    # 整体回滚：两边时间、版本、占用全部保持原状
    replay_first = client.post("/api/v1/reservations", json={
        "request_id": "req-A",
        "start_utc": "2026-03-09T13:00:00Z",
        "end_utc": "2026-03-09T14:00:00Z",
        "participant_ids": ["alice"],
    }).json()
    replay_second = client.post("/api/v1/reservations", json={
        "request_id": "req-B",
        "start_utc": "2026-03-09T16:00:00Z",
        "end_utc": "2026-03-09T17:00:00Z",
        "participant_ids": ["bob"],
    }).json()
    assert replay_first["version"] == 1
    assert replay_second["version"] == 1
    # alice 在旧窗口仍被占用、bob 在旧窗口仍被占用
    for req, pid in (("req-check-a", "alice"), ("req-check-b", "bob")):
        slot = ("13:30", "13:45") if pid == "alice" else ("16:30", "16:45")
        blocked = client.post("/api/v1/reservations", json={
            "request_id": req,
            "start_utc": f"2026-03-09T{slot[0]}:00Z",
            "end_utc": f"2026-03-09T{slot[1]}:00Z",
            "participant_ids": [pid],
        })
        assert blocked.status_code == 409
    # zoe 仅在 third 上有占用，未留下任何残留
    assert client.post("/api/v1/reservations", json={
        "request_id": "req-zoe-free",
        "start_utc": "2026-03-09T13:30:00Z",
        "end_utc": "2026-03-09T13:45:00Z",
        "participant_ids": ["zoe"],
    }).status_code == 201


def test_swap_conflict_with_unexpired_hold_returns_409(client):
    first = _create(client, "req-1", "2026-03-13T09:00:00Z",
                    "2026-03-13T10:00:00Z", ["heidi"])
    second = _create(client, "req-2", "2026-03-13T11:00:00Z",
                     "2026-03-13T12:00:00Z", ["ivan"])
    hold = client.post("/api/v1/reservation-holds", json={
        "hold_key": "hold-block",
        "start_utc": "2026-03-13T11:30:00Z",
        "end_utc": "2026-03-13T11:45:00Z",
        "participant_ids": ["heidi"],
        "ttl_seconds": 600,
    })
    assert hold.status_code == 201, hold.text

    resp = _swap(client, first["reservation_id"], 1,
                 second["reservation_id"], 1)
    assert resp.status_code == 409
    assert resp.json()["conflicts"] == [
        {"hold_id": hold.json()["hold_id"], "participant_id": "heidi"}
    ]
    # 回滚：版本未推进
    assert client.post("/api/v1/reservations", json={
        "request_id": "req-1",
        "start_utc": "2026-03-13T09:00:00Z",
        "end_utc": "2026-03-13T10:00:00Z",
        "participant_ids": ["heidi"],
    }).json()["version"] == 1


def test_swap_conflict_appends_no_audit_events(client):
    first = _create(client, "req-A", "2026-03-09T13:00:00Z",
                    "2026-03-09T14:00:00Z", ["alice"])
    second = _create(client, "req-B", "2026-03-09T16:00:00Z",
                     "2026-03-09T17:00:00Z", ["bob"])
    _create(client, "req-C", "2026-03-09T16:30:00Z",
            "2026-03-09T16:45:00Z", ["alice"])

    assert _swap(client, first["reservation_id"], 1,
                 second["reservation_id"], 1).status_code == 409

    for rid in (first["reservation_id"], second["reservation_id"]):
        history = client.get(f"/api/v1/reservations/{rid}/history")
        assert history.status_code == 200
        actions = [e["action"] for e in history.json()["events"]]
        assert actions == ["created"]  # 失败交换不产生事件


# ------------------------------------------------------------------ 前置校验


def test_swap_unknown_id_returns_404(client):
    first = _create(client, "req-1", "2026-03-09T13:00:00Z",
                    "2026-03-09T14:00:00Z", ["alice"])
    resp = _swap(client, first["reservation_id"], 1, 999, 1)
    assert resp.status_code == 404
    assert "999" in resp.json()["detail"]


def test_swap_canceled_reservation_returns_409(client):
    first = _create(client, "req-1", "2026-03-09T13:00:00Z",
                    "2026-03-09T14:00:00Z", ["alice"])
    second = _create(client, "req-2", "2026-03-09T16:00:00Z",
                     "2026-03-09T17:00:00Z", ["bob"])
    # 取消 second（version 1 → 2）
    canceled = client.delete(
        f"/api/v1/reservations/{second['reservation_id']}",
        params={"expected_version": 1},
    )
    assert canceled.status_code == 200

    resp = _swap(client, first["reservation_id"], 1,
                 second["reservation_id"], 2)
    assert resp.status_code == 409
    body = resp.json()
    assert "canceled" in body["detail"]
    assert body["current_version"] == 2

    # 已取消预约不会被复活；first 也保持原状
    assert client.post("/api/v1/reservations", json={
        "request_id": "req-1",
        "start_utc": "2026-03-09T13:00:00Z",
        "end_utc": "2026-03-09T14:00:00Z",
        "participant_ids": ["alice"],
    }).json()["version"] == 1


def test_swap_stale_version_returns_409_with_current_version(client):
    first = _create(client, "req-1", "2026-03-09T13:00:00Z",
                    "2026-03-09T14:00:00Z", ["alice"])
    second = _create(client, "req-2", "2026-03-09T16:00:00Z",
                     "2026-03-09T17:00:00Z", ["bob"])

    # 第一方版本过期
    resp = _swap(client, first["reservation_id"], 5,
                 second["reservation_id"], 1)
    assert resp.status_code == 409
    body = resp.json()
    assert body["current_version"] == 1
    assert str(first["reservation_id"]) in body["detail"]

    # 第二方版本过期
    resp = _swap(client, first["reservation_id"], 1,
                 second["reservation_id"], 5)
    assert resp.status_code == 409
    body = resp.json()
    assert body["current_version"] == 1
    assert str(second["reservation_id"]) in body["detail"]

    # 带上正确版本后成功
    resp = _swap(client, first["reservation_id"], 1,
                 second["reservation_id"], 1)
    assert resp.status_code == 200, resp.text


def test_swap_requires_distinct_reservation_ids(client):
    resp = client.post("/api/v1/reservations/swap", json={
        "first": {"reservation_id": 1, "expected_version": 1},
        "second": {"reservation_id": 1, "expected_version": 1},
    })
    assert resp.status_code == 422


def test_swap_validation_errors_return_422(client):
    # id < 1
    assert client.post("/api/v1/reservations/swap", json={
        "first": {"reservation_id": 0, "expected_version": 1},
        "second": {"reservation_id": 2, "expected_version": 1},
    }).status_code == 422
    # expected_version < 1
    assert client.post("/api/v1/reservations/swap", json={
        "first": {"reservation_id": 1, "expected_version": 0},
        "second": {"reservation_id": 2, "expected_version": 1},
    }).status_code == 422
    # 缺少一方
    assert client.post("/api/v1/reservations/swap", json={
        "first": {"reservation_id": 1, "expected_version": 1},
    }).status_code == 422
    # 空请求体
    assert client.post("/api/v1/reservations/swap", json={}).status_code == 422


# ------------------------------------------------------------------ 哈希链


def test_swap_appends_rescheduled_events_to_both_chains(client):
    first = _create(client, "req-1", "2026-03-09T13:00:00Z",
                    "2026-03-09T14:00:00Z", ["alice"])
    second = _create(client, "req-2", "2026-03-09T16:00:00Z",
                     "2026-03-09T17:00:00Z", ["bob"])
    assert _swap(client, first["reservation_id"], 1,
                 second["reservation_id"], 1).status_code == 200

    expected_starts = {
        first["reservation_id"]: "2026-03-09T16:00:00Z",
        second["reservation_id"]: "2026-03-09T13:00:00Z",
    }
    for rid, new_start in expected_starts.items():
        history = client.get(f"/api/v1/reservations/{rid}/history").json()
        events = history["events"]
        assert [e["seq"] for e in events] == [1, 2]
        assert [e["action"] for e in events] == ["created", "rescheduled"]
        ev = events[1]
        # 快照记录互换后的新状态与版本 2，request_id 不变
        assert ev["snapshot"]["start_utc"] == new_start
        assert ev["snapshot"]["version"] == 2
        assert ev["snapshot"]["request_id"] == events[0]["snapshot"]["request_id"]
        assert ev["prev_hash"] == events[0]["hash"]

        # 独立重算整条哈希链
        prev = "0" * 64
        for e in events:
            assert e["prev_hash"] == prev
            digest = hashlib.sha256(
                prev.encode("ascii") + e["canonical_event"].encode("utf-8")
            ).hexdigest()
            assert digest == e["hash"]
            prev = e["hash"]


# ------------------------------------------------------------------ 候补语义


def test_swap_does_not_trigger_waitlist_promotion(client):
    """交换不触发候补晋升（仅取消会触发）：pending 候补保持 pending。"""
    first = _create(client, "req-A", "2026-03-09T13:00:00Z",
                    "2026-03-09T14:00:00Z", ["alice"])
    second = _create(client, "req-B", "2026-03-09T15:00:00Z",
                     "2026-03-09T16:00:00Z", ["carol"])

    # 候补：alice 想约 13:30，被 first 挡住 → pending
    wl = client.post("/api/v1/reservation-waitlist", json={
        "request_id": "req-wait-1",
        "start_utc": "2026-03-09T13:30:00Z",
        "end_utc": "2026-03-09T13:45:00Z",
        "participant_ids": ["alice"],
    })
    assert wl.status_code == 201
    assert wl.json()["status"] == "pending"

    # 交换把 alice 的 first 挪到 15 点段（13 点段由 carol 的 second 接管）
    assert _swap(client, first["reservation_id"], 1,
                 second["reservation_id"], 1).status_code == 200

    # 交换不触发晋升：候补仍是 pending
    got = client.get("/api/v1/reservation-waitlist/req-wait-1")
    assert got.status_code == 200
    assert got.json()["status"] == "pending"
    assert got.json()["reservation"] is None


# ------------------------------------------------------------------ 并发


def test_concurrent_swaps_single_winner(db_path):
    """6 个并发交换（双方相同 expected_version）：恰一个成功，其余版本冲突。

    两个目标时段互不相同且不存在第三方，唯一失败原因是乐观锁版本过期，
    证明“双版本校验 + 互换”在 BEGIN IMMEDIATE 下被串行化。
    """
    first, _ = core.create_reservation(
        request_id="req-1",
        start_utc=_utc(2026, 3, 17, 9),
        end_utc=_utc(2026, 3, 17, 10),
        participant_ids=["quinn"],
        db_path=db_path,
    )
    second, _ = core.create_reservation(
        request_id="req-2",
        start_utc=_utc(2026, 3, 17, 11),
        end_utc=_utc(2026, 3, 17, 12),
        participant_ids=["ruby"],
        db_path=db_path,
    )
    barrier = threading.Barrier(6)

    def worker():
        barrier.wait()
        return core.swap_reservations(
            first_id=first.id, first_expected_version=1,
            second_id=second.id, second_expected_version=1,
            db_path=db_path,
        )

    oks = stale = 0
    with ThreadPoolExecutor(max_workers=6) as pool:
        for f in [pool.submit(worker) for _ in range(6)]:
            try:
                f.result()
                oks += 1
            except core.VersionConflictError:
                stale += 1

    assert oks == 1
    assert stale == 5
    conn = db_module.connect(db_path)
    try:
        assert core.load_reservation(conn, first.id).version == 2
        assert core.load_reservation(conn, second.id).version == 2
    finally:
        conn.close()
