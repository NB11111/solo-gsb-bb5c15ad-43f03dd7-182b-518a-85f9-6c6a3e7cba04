"""预约临时保留（reservation holds）测试。

覆盖：创建/幂等、半开区间冲突（预约与保留双向）、冲突无部分写入、
惰性过期与 410、过期保留被忽略且不阻止新预约/同 key 重申请、
确认转正式预约（同事务 created 审计事件）、重复确认幂等、
request_id 冲突 409、改期被保留阻止、并发仅一个赢家、旧库无损建新表。
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app import db as db_module
from app import holds as hold_core
from app import reservations as reservation_core
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


def _hold_payload(**overrides):
    payload = {
        "hold_key": "hold-1",
        "start_utc": "2026-03-09T13:00:00Z",
        "end_utc": "2026-03-09T14:00:00Z",
        "participant_ids": ["alice", "bob"],
        "ttl_seconds": 300,
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


def _utc(y, mo, d, h, mi=0):
    return datetime(y, mo, d, h, mi, tzinfo=timezone.utc)


def _force_hold_expired(path: str, hold_id: int) -> None:
    """直接把保留的 expires_at 改到过去（不翻转 status），模拟 TTL 到期。

    下一个写事务会在入口惰性过期；用于不 sleep 的确定性测试。
    """
    import sqlite3
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "UPDATE reservation_holds SET expires_at = ? WHERE id = ?",
            ("2000-01-01T00:00:00.000000Z", hold_id),
        )
        conn.commit()
    finally:
        conn.close()


# ------------------------------------------------------------------ 创建保留

def test_create_hold_returns_201_and_normalizes(client):
    resp = client.post("/api/v1/reservation-holds",
                       json=_hold_payload(participant_ids=["bob", "alice", "bob"]))
    assert resp.status_code == 201, resp.text
    data = resp.json()
    assert data["hold_id"] == 1
    assert data["hold_key"] == "hold-1"
    assert data["start_utc"] == "2026-03-09T13:00:00Z"
    assert data["end_utc"] == "2026-03-09T14:00:00Z"
    assert data["participants"] == ["alice", "bob"]
    assert data["ttl_seconds"] == 300
    assert data["status"] == "pending"
    assert data["reservation_id"] is None
    assert data["confirmed_at"] is None
    assert data["created_at"]
    assert data["expires_at"] > data["created_at"]


@pytest.mark.parametrize("ttl", [0, -1, 901, 10000])
def test_ttl_must_be_between_1_and_900(client, ttl):
    resp = client.post("/api/v1/reservation-holds",
                       json=_hold_payload(ttl_seconds=ttl))
    assert resp.status_code == 422


def test_ttl_boundaries_accepted(client):
    assert client.post("/api/v1/reservation-holds",
                       json=_hold_payload(ttl_seconds=1)).status_code == 201
    assert client.post("/api/v1/reservation-holds",
                       json=_hold_payload(hold_key="hold-2",
                                          start_utc="2026-03-10T13:00:00Z",
                                          end_utc="2026-03-10T14:00:00Z",
                                          ttl_seconds=900)
                       ).status_code == 201


def test_hold_window_validation_reuses_reservation_rules(client):
    # end <= start
    assert client.post("/api/v1/reservation-holds",
                       json=_hold_payload(end_utc="2026-03-09T13:00:00Z")
                       ).status_code == 422
    # 缺时区偏移
    assert client.post("/api/v1/reservation-holds",
                       json=_hold_payload(start_utc="2026-03-09T13:00:00")
                       ).status_code == 422
    # 空参与者
    assert client.post("/api/v1/reservation-holds",
                       json=_hold_payload(participant_ids=[])
                       ).status_code == 422
    # 空 hold_key
    assert client.post("/api/v1/reservation-holds",
                       json=_hold_payload(hold_key="")
                       ).status_code == 422


# ---------------------------------------------------------------- 幂等/冲突

def test_hold_idempotent_replay_returns_same_hold(client):
    first = client.post("/api/v1/reservation-holds", json=_hold_payload())
    assert first.status_code == 201
    replay = client.post("/api/v1/reservation-holds",
                         json=_hold_payload(participant_ids=["bob", "alice"]))
    assert replay.status_code == 200
    assert replay.json() == first.json()


def test_same_hold_key_different_content_returns_409(client):
    assert client.post("/api/v1/reservation-holds",
                       json=_hold_payload()).status_code == 201
    resp = client.post("/api/v1/reservation-holds",
                       json=_hold_payload(start_utc="2026-03-09T15:00:00Z",
                                          end_utc="2026-03-09T16:00:00Z"))
    assert resp.status_code == 409
    assert resp.json()["existing_hold_id"] == 1

    # ttl 不同也算不同内容
    resp = client.post("/api/v1/reservation-holds",
                       json=_hold_payload(ttl_seconds=600))
    assert resp.status_code == 409


def test_hold_conflicts_with_existing_reservation(client):
    created = client.post("/api/v1/reservations", json=_resv_payload()).json()
    resp = client.post("/api/v1/reservation-holds", json=_hold_payload(
        hold_key="hold-2",
        start_utc="2026-03-09T13:30:00Z",
        end_utc="2026-03-09T13:45:00Z",
        participant_ids=["alice"],
    ))
    assert resp.status_code == 409, resp.text
    assert resp.json()["conflicts"] == [
        {"reservation_id": created["reservation_id"], "participant_id": "alice"}
    ]


def test_reservation_conflicts_with_existing_hold(client):
    hold = client.post("/api/v1/reservation-holds", json=_hold_payload()).json()
    resp = client.post("/api/v1/reservations", json=_resv_payload(
        request_id="req-2",
        start_utc="2026-03-09T13:30:00Z",
        end_utc="2026-03-09T13:45:00Z",
        participant_ids=["bob"],
    ))
    assert resp.status_code == 409
    assert resp.json()["conflicts"] == [
        {"hold_id": hold["hold_id"], "participant_id": "bob"}
    ]


def test_two_holds_conflict_with_each_other(client):
    first = client.post("/api/v1/reservation-holds", json=_hold_payload()).json()
    resp = client.post("/api/v1/reservation-holds", json=_hold_payload(
        hold_key="hold-2",
        start_utc="2026-03-09T13:30:00Z",
        end_utc="2026-03-09T13:45:00Z",
        participant_ids=["alice"],
    ))
    assert resp.status_code == 409
    assert resp.json()["conflicts"] == [
        {"hold_id": first["hold_id"], "participant_id": "alice"}
    ]


def test_adjacent_windows_do_not_conflict(client):
    assert client.post("/api/v1/reservation-holds",
                       json=_hold_payload()).status_code == 201
    # 紧邻其后
    assert client.post("/api/v1/reservation-holds", json=_hold_payload(
        hold_key="hold-after",
        start_utc="2026-03-09T14:00:00Z",
        end_utc="2026-03-09T15:00:00Z",
    )).status_code == 201
    # 紧邻其前
    assert client.post("/api/v1/reservation-holds", json=_hold_payload(
        hold_key="hold-before",
        start_utc="2026-03-09T12:00:00Z",
        end_utc="2026-03-09T13:00:00Z",
    )).status_code == 201


def test_disjoint_participants_same_slot_ok(client):
    assert client.post("/api/v1/reservation-holds",
                       json=_hold_payload()).status_code == 201
    resp = client.post("/api/v1/reservation-holds",
                       json=_hold_payload(hold_key="hold-2",
                                          participant_ids=["carol", "dave"]))
    assert resp.status_code == 201, resp.text


def test_hold_conflict_leaves_no_writes(client, db_path):
    assert client.post("/api/v1/reservation-holds",
                       json=_hold_payload()).status_code == 201
    resp = client.post("/api/v1/reservation-holds", json=_hold_payload(
        hold_key="hold-2",
        start_utc="2026-03-09T13:30:00Z",
        end_utc="2026-03-09T13:45:00Z",
        participant_ids=["alice", "carol"],
    ))
    assert resp.status_code == 409

    import sqlite3
    conn = sqlite3.connect(db_path)
    try:
        # 冲突的 hold-2 没有留下保留行
        assert conn.execute(
            "SELECT COUNT(*) FROM reservation_holds WHERE hold_key = 'hold-2'"
        ).fetchone()[0] == 0
        # carol 没有残留占用
        assert conn.execute(
            "SELECT COUNT(*) FROM reservation_hold_participants"
            " WHERE participant_id = 'carol'"
        ).fetchone()[0] == 0
    finally:
        conn.close()

    # carol 在同一时段可立即保留成功
    resp = client.post("/api/v1/reservation-holds", json=_hold_payload(
        hold_key="hold-carol",
        start_utc="2026-03-09T13:30:00Z",
        end_utc="2026-03-09T13:45:00Z",
        participant_ids=["carol"],
    ))
    assert resp.status_code == 201, resp.text


def test_reschedule_is_blocked_by_active_hold(client):
    # 预约 #1 在次日，随后保留占用 3/9 13:30
    assert client.post("/api/v1/reservations", json=_resv_payload(
        request_id="req-A",
        start_utc="2026-03-10T13:00:00Z",
        end_utc="2026-03-10T14:00:00Z",
        participant_ids=["alice"],
    )).status_code == 201
    assert client.post("/api/v1/reservation-holds", json=_hold_payload(
        hold_key="hold-block",
        start_utc="2026-03-09T13:00:00Z",
        end_utc="2026-03-09T14:00:00Z",
        participant_ids=["alice"],
    )).status_code == 201

    resp = client.put("/api/v1/reservations/1", json={
        "expected_version": 1,
        "start_utc": "2026-03-09T13:30:00Z",
        "end_utc": "2026-03-09T13:45:00Z",
        "participant_ids": ["alice"],
    })
    assert resp.status_code == 409
    assert resp.json()["conflicts"] == [
        {"hold_id": 1, "participant_id": "alice"}
    ]


# ------------------------------------------------------------------ 过期

def test_expired_hold_confirm_returns_410(client, db_path):
    hid = client.post("/api/v1/reservation-holds",
                      json=_hold_payload(ttl_seconds=1)).json()["hold_id"]
    _force_hold_expired(db_path, hid)

    resp = client.post(f"/api/v1/reservation-holds/{hid}/confirm",
                       json={"request_id": "req-late"})
    assert resp.status_code == 410
    assert str(hid) in resp.json()["detail"]


def test_expired_hold_is_ignored_for_conflicts(client, db_path):
    hid = client.post("/api/v1/reservation-holds",
                      json=_hold_payload(ttl_seconds=1)).json()["hold_id"]
    _force_hold_expired(db_path, hid)

    # 任何写事务都会先惰性过期；直接在同一时段创建正式预约应成功
    resp = client.post("/api/v1/reservations", json=_resv_payload(
        request_id="req-rebook"))
    assert resp.status_code == 201, resp.text


def test_expired_hold_ignored_by_new_hold_and_key_reusable(client, db_path):
    first = client.post("/api/v1/reservation-holds",
                        json=_hold_payload(ttl_seconds=1)).json()
    _force_hold_expired(db_path, first["hold_id"])

    # 同 participants 同时段的新保留不与过期保留冲突
    other = client.post("/api/v1/reservation-holds",
                        json=_hold_payload(hold_key="hold-other"))
    assert other.status_code == 201, other.text

    # 同一个 hold_key 在原 pending 行过期后可重新申请（换到次日时段，
    # 避开上一条未过期的 hold-other）
    reuse = client.post("/api/v1/reservation-holds", json=_hold_payload(
        start_utc="2026-03-10T13:00:00Z",
        end_utc="2026-03-10T14:00:00Z",
        ttl_seconds=600))
    assert reuse.status_code == 201, reuse.text
    assert reuse.json()["hold_id"] != first["hold_id"]


def test_expired_hold_confirm_410_even_without_other_writer(db_path):
    """未被其他写事务触发惰性过期时，confirm 自身事务也会过期并返回 410。"""
    hold, _ = hold_core.create_hold(
        hold_key="h", start_utc=_utc(2026, 3, 9, 13), end_utc=_utc(2026, 3, 9, 14),
        participant_ids=["alice"], ttl_seconds=1, db_path=db_path)
    _force_hold_expired(db_path, hold.id)
    with pytest.raises(hold_core.HoldExpiredError):
        hold_core.confirm_hold(hold.id, "req", db_path=db_path)


# ------------------------------------------------------------------ 确认

def test_confirm_creates_reservation_with_created_event(client):
    hold = client.post("/api/v1/reservation-holds",
                       json=_hold_payload()).json()
    resp = client.post(f"/api/v1/reservation-holds/{hold['hold_id']}/confirm",
                       json={"request_id": "req-final"})
    assert resp.status_code == 201, resp.text
    res = resp.json()
    assert res["reservation_id"] >= 1
    assert res["request_id"] == "req-final"
    assert res["start_utc"] == hold["start_utc"]
    assert res["end_utc"] == hold["end_utc"]
    assert res["participants"] == ["alice", "bob"]
    assert res["version"] == 1
    assert res["status"] == "active"

    # 同事务追加了一条 created 审计事件，链可验
    history = client.get(
        f"/api/v1/reservations/{res['reservation_id']}/history").json()
    actions = [e["action"] for e in history["events"]]
    assert actions == ["created"]
    assert history["events"][0]["snapshot"]["request_id"] == "req-final"


def test_confirm_unknown_hold_returns_404(client):
    resp = client.post("/api/v1/reservation-holds/999/confirm",
                       json={"request_id": "req-x"})
    assert resp.status_code == 404
    assert "999" in resp.json()["detail"]


def test_confirm_requires_nonempty_request_id(client):
    hold = client.post("/api/v1/reservation-holds",
                       json=_hold_payload()).json()
    resp = client.post(f"/api/v1/reservation-holds/{hold['hold_id']}/confirm",
                       json={"request_id": ""})
    assert resp.status_code == 422


def test_duplicate_confirm_returns_same_reservation(client):
    hold = client.post("/api/v1/reservation-holds",
                       json=_hold_payload()).json()
    first = client.post(f"/api/v1/reservation-holds/{hold['hold_id']}/confirm",
                        json={"request_id": "req-final"})
    assert first.status_code == 201
    # 重复确认（即使带不同 request_id）也返回同一预约，200
    replay = client.post(f"/api/v1/reservation-holds/{hold['hold_id']}/confirm",
                         json={"request_id": "req-final-retry"})
    assert replay.status_code == 200
    assert replay.json() == first.json()

    # 只有一条 created 事件：重复确认不新增审计事件
    history = client.get(
        f"/api/v1/reservations/{first.json()['reservation_id']}/history").json()
    assert len(history["events"]) == 1


def test_confirm_then_slot_is_formally_occupied(client):
    hold = client.post("/api/v1/reservation-holds",
                       json=_hold_payload()).json()
    res = client.post(f"/api/v1/reservation-holds/{hold['hold_id']}/confirm",
                      json={"request_id": "req-final"}).json()

    # 正式预约占用该时段
    blocked = client.post("/api/v1/reservations", json=_resv_payload(
        request_id="req-other",
        start_utc="2026-03-09T13:30:00Z",
        end_utc="2026-03-09T13:45:00Z",
        participant_ids=["alice"],
    ))
    assert blocked.status_code == 409
    assert blocked.json()["conflicts"] == [
        {"reservation_id": res["reservation_id"], "participant_id": "alice"}
    ]

    # 新保留同样不能与已转正式预约的时段冲突
    blocked_hold = client.post("/api/v1/reservation-holds", json=_hold_payload(
        hold_key="hold-late",
        start_utc="2026-03-09T13:30:00Z",
        end_utc="2026-03-09T13:45:00Z",
        participant_ids=["bob"],
    ))
    assert blocked_hold.status_code == 409


def test_confirm_with_existing_request_id_returns_409(client):
    hold = client.post("/api/v1/reservation-holds", json=_hold_payload(
        hold_key="hold-a")).json()
    # 先占用 request_id
    other = client.post("/api/v1/reservations", json=_resv_payload(
        request_id="dup-req",
        start_utc="2026-04-01T13:00:00Z",
        end_utc="2026-04-01T14:00:00Z",
        participant_ids=["zed"],
    ))
    assert other.status_code == 201

    resp = client.post(f"/api/v1/reservation-holds/{hold['hold_id']}/confirm",
                       json={"request_id": "dup-req"})
    assert resp.status_code == 409
    assert resp.json()["existing_reservation_id"] == other.json()["reservation_id"]

    # 失败后保留仍为 pending，可换 request_id 重试确认
    retry = client.post(f"/api/v1/reservation-holds/{hold['hold_id']}/confirm",
                        json={"request_id": "good-req"})
    assert retry.status_code == 201, retry.text


def test_confirm_request_id_conflict_rolls_back(client, db_path):
    """request_id 冲突时不写预约、保留仍为 pending。"""
    hold = client.post("/api/v1/reservation-holds",
                       json=_hold_payload()).json()
    client.post("/api/v1/reservations", json=_resv_payload(
        request_id="dup",
        start_utc="2026-04-01T13:00:00Z",
        end_utc="2026-04-01T14:00:00Z",
        participant_ids=["zed"],
    ))
    resp = client.post(f"/api/v1/reservation-holds/{hold['hold_id']}/confirm",
                       json={"request_id": "dup"})
    assert resp.status_code == 409

    import sqlite3
    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute(
            "SELECT status, reservation_id FROM reservation_holds WHERE id = ?",
            (hold["hold_id"],),
        ).fetchone() == ("pending", None)
        # 只有 zed 那条正式预约
        assert conn.execute("SELECT COUNT(*) FROM reservations").fetchone()[0] == 1
    finally:
        conn.close()


def test_confirm_hold_releases_temporary_occupancy_rows(client, db_path):
    hold = client.post("/api/v1/reservation-holds",
                       json=_hold_payload()).json()
    assert client.post(
        f"/api/v1/reservation-holds/{hold['hold_id']}/confirm",
        json={"request_id": "req-final"}).status_code == 201

    import sqlite3
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT status, reservation_id, confirmed_at FROM reservation_holds"
            " WHERE id = ?", (hold["hold_id"],)).fetchone()
        assert row[0] == "confirmed" and row[1] == 1 and row[2] is not None
        # 临时占用行已删除（占用由正式预约接管）
        assert conn.execute(
            "SELECT COUNT(*) FROM reservation_hold_participants"
        ).fetchone()[0] == 0
    finally:
        conn.close()


# ------------------------------------------------------------------ 并发

def test_concurrent_holds_single_winner(db_path):
    """8 个并发保留抢同一参与者同一时段：恰一个成功，其余 409。"""
    barrier = threading.Barrier(8)

    def worker(i: int):
        barrier.wait()
        return hold_core.create_hold(
            hold_key=f"k{i}",
            start_utc=_utc(2026, 3, 9, 13),
            end_utc=_utc(2026, 3, 9, 14),
            participant_ids=["alice"],
            ttl_seconds=300,
            db_path=db_path,
        )

    created, conflicts = [], []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(worker, i) for i in range(8)]
        for f in futures:
            try:
                created.append(f.result())
            except reservation_core.ReservationConflictError as exc:
                conflicts.append(exc)

    assert len(created) == 1
    assert len(conflicts) == 7
    winner_id = created[0][0].id
    for exc in conflicts:
        assert exc.conflicts == [
            {"hold_id": winner_id, "participant_id": "alice"}
        ]


def test_concurrent_confirms_same_hold_single_reservation(db_path):
    """同一保留并发确认：恰一个 201，其余幂等拿到同一条预约。"""
    hold, _ = hold_core.create_hold(
        hold_key="k", start_utc=_utc(2026, 3, 9, 13), end_utc=_utc(2026, 3, 9, 14),
        participant_ids=["alice"], ttl_seconds=300, db_path=db_path)
    barrier = threading.Barrier(8)

    def worker(i: int):
        barrier.wait()
        return hold_core.confirm_hold(
            hold.id, f"req-{i}", db_path=db_path)

    outcomes = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(worker, i) for i in range(8)]
        for f in futures:
            outcomes.append(f.result())

    ids = {r.id for r, _ in outcomes}
    assert len(ids) == 1
    assert sum(1 for _, did in outcomes if did) == 1
    reservation_id = ids.pop()

    import sqlite3
    conn = sqlite3.connect(db_path)
    try:
        # 只产生一条正式预约、一条 created 事件
        assert conn.execute("SELECT COUNT(*) FROM reservations").fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM reservation_events WHERE action = 'created'"
        ).fetchone()[0] == 1
        row = conn.execute(
            "SELECT status, reservation_id FROM reservation_holds WHERE id = ?",
            (hold.id,)).fetchone()
        assert row == ("confirmed", reservation_id)
    finally:
        conn.close()


# -------------------------------------------------------- 旧数据库无损建新表

def test_init_creates_hold_tables_losslessly(tmp_path, monkeypatch):
    """旧版库（只有 reservations 等表）初始化后新增保留表，既有数据不动。"""
    import sqlite3

    path = str(tmp_path / "legacy.db")
    # 旧 schema（无保留表，无 version 列）
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
    db_module.init_db(path)  # 幂等

    conn = sqlite3.connect(path)
    try:
        names = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        assert {"reservation_holds", "reservation_hold_participants"} <= names
        # 既有预约无损
        assert conn.execute(
            "SELECT request_id, participants_key FROM reservations"
        ).fetchone() == ("old", "[\"alice\"]")
    finally:
        conn.close()

    # 新表立即可用
    hold, created = hold_core.create_hold(
        hold_key="k", start_utc=_utc(2026, 5, 1, 13), end_utc=_utc(2026, 5, 1, 14),
        participant_ids=["alice"], ttl_seconds=60, db_path=path)
    assert created is True
    reservation, did = hold_core.confirm_hold(hold.id, "new-req", db_path=path)
    assert did is True and reservation.request_id == "new-req"


# ------------------------------------------------------------------ 核心层

def test_core_create_and_confirm_roundtrip(db_path):
    hold, created = hold_core.create_hold(
        hold_key="k", start_utc=_utc(2026, 3, 9, 13), end_utc=_utc(2026, 3, 9, 14),
        participant_ids=["bob", "alice"], ttl_seconds=120, db_path=db_path)
    assert created is True
    assert hold.participants == ("alice", "bob")
    assert (hold.expires_at - hold.created_at).total_seconds() == 120

    reservation, did = hold_core.confirm_hold(hold.id, "req", db_path=db_path)
    assert did is True
    assert reservation.participants == ("alice", "bob")
    again, did2 = hold_core.confirm_hold(hold.id, "req-other", db_path=db_path)
    assert did2 is False and again.id == reservation.id
