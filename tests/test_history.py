"""预约变更历史（哈希链审计）测试：

- 创建/改期/取消各在同事务追加事件，序号、动作、时间、快照正确；
- 哈希链可独立重算验证（canonical_event 规范化规则、首条 64 个 0）；
- GET history 按序返回、404、篡改后 409 且不返回历史；
- 失败请求与幂等重放不新增事件；
- 既有数据库初始化时无损补写 baseline 事件（含旧版 schema）。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app import audit
from app import db as db_module
from app import reservations as core
from app.main import app


@pytest.fixture()
def db_path(tmp_path, monkeypatch) -> str:
    path = str(tmp_path / "reservations.db")
    monkeypatch.setenv("RESERVATIONS_DB_PATH", path)
    return path


@pytest.fixture()
def client(db_path) -> TestClient:
    return TestClient(app)


def _payload(**overrides):
    payload = {
        "request_id": "req-1",
        "start_utc": "2026-03-09T13:00:00Z",
        "end_utc": "2026-03-09T14:00:00Z",
        "participant_ids": ["alice", "bob"],
    }
    payload.update(overrides)
    return payload


def _put(client, reservation_id, **overrides):
    payload = {
        "expected_version": 1,
        "start_utc": "2026-03-09T15:00:00Z",
        "end_utc": "2026-03-09T16:00:00Z",
        "participant_ids": ["alice", "carol"],
    }
    payload.update(overrides)
    return client.put(f"/api/v1/reservations/{reservation_id}", json=payload)


def _delete(client, reservation_id, expected_version):
    return client.delete(
        f"/api/v1/reservations/{reservation_id}",
        params={"expected_version": expected_version},
    )


def _history(client, reservation_id):
    return client.get(f"/api/v1/reservations/{reservation_id}/history")


# ------------------------------------------------------------- 单元：规范化与哈希


def test_canonical_json_is_compact_sorted_unescaped():
    payload = {
        "snapshot": {"参与者": ["张三", "李四"], "b": 2, "a": [1, 2]},
        "action": "created",
        "seq": 1,
        "timestamp": "2026-03-09T13:00:00.000000Z",
    }
    canon = audit.canonical_json(payload)
    # 无空白
    assert " " not in canon and "\n" not in canon
    # 非 ASCII 不转义（直接出现中文字符，而不是 张三）
    assert "张三" in canon and "\\u" not in canon
    # 键排序：顶层 seq 在前；snapshot 内 a 在 b 前
    assert canon.index('"action"') < canon.index('"snapshot"')
    assert canon.index('"a"') < canon.index('"b"')
    # 与标准库按相同参数序列化的结果一致
    assert canon == json.dumps(
        payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def test_chain_hash_formula_and_genesis():
    event = {"seq": 1, "action": "created"}
    canon = audit.canonical_json(event)
    expected = hashlib.sha256(
        ("0" * 64).encode("ascii") + canon.encode("utf-8")).hexdigest()
    assert audit.chain_hash("0" * 64, canon) == expected
    # 第二条：prev_hash 为 ASCII
    h2 = audit.chain_hash(expected, canon)
    assert h2 == hashlib.sha256(
        expected.encode("ascii") + canon.encode("utf-8")).hexdigest()


# ------------------------------------------------------------- 事件追加与链形态


def test_create_appends_created_event_seq_1(client):
    created = client.post("/api/v1/reservations", json=_payload()).json()
    rid = created["reservation_id"]
    resp = _history(client, rid)
    assert resp.status_code == 200, resp.text
    events = resp.json()["events"]
    assert len(events) == 1
    ev = events[0]
    assert ev["seq"] == 1
    assert ev["action"] == "created"
    assert ev["prev_hash"] == "0" * 64
    assert len(ev["hash"]) == 64
    # 快照即创建响应
    assert ev["snapshot"] == created
    # canonical_event 不含哈希字段
    payload = json.loads(ev["canonical_event"])
    assert set(payload.keys()) == {"seq", "action", "timestamp", "snapshot"}
    assert payload["seq"] == 1 and payload["action"] == "created"
    assert payload["snapshot"]["reservation_id"] == rid
    # 时间存在且为 UTC
    assert ev["timestamp"].endswith("Z")


def test_full_lifecycle_history_order_and_chain(client):
    created = client.post("/api/v1/reservations",
                          json=_payload(participant_ids=["bob", "alice"]))
    assert created.status_code == 201
    rid = created.json()["reservation_id"]

    moved = _put(client, rid)
    assert moved.status_code == 200, moved.text
    moved2 = _put(client, rid, expected_version=2,
                  start_utc="2026-03-09T16:00:00Z",
                  end_utc="2026-03-09T16:30:00Z")
    assert moved2.status_code == 200, moved2.text
    canceled = _delete(client, rid, expected_version=3)
    assert canceled.status_code == 200, canceled.text

    resp = _history(client, rid)
    assert resp.status_code == 200, resp.text
    events = resp.json()["events"]
    assert [e["seq"] for e in events] == [1, 2, 3, 4]
    assert [e["action"] for e in events] == [
        "created", "rescheduled", "rescheduled", "canceled"]

    # 快照跟随每次变更
    assert events[1]["snapshot"]["version"] == 2
    assert events[1]["snapshot"]["participants"] == ["alice", "carol"]
    assert events[2]["snapshot"]["start_utc"] == "2026-03-09T16:00:00Z"
    assert events[3]["snapshot"]["status"] == "canceled"
    assert events[3]["snapshot"]["version"] == 4
    assert events[3]["snapshot"]["canceled_at"] is not None
    # 事件时间单调不减
    stamps = [e["timestamp"] for e in events]
    assert stamps == sorted(stamps)

    # 独立重算整条链：prev 衔接 genesis 与逐环哈希
    prev = "0" * 64
    for ev in events:
        assert ev["prev_hash"] == prev
        digest = hashlib.sha256(
            ev["prev_hash"].encode("ascii")
            + ev["canonical_event"].encode("utf-8")
        ).hexdigest()
        assert digest == ev["hash"]
        # 服务端返回的 canonical_event 必须本身就是规范化形态
        reparsed = {
            "seq": ev["seq"],
            "action": ev["action"],
            "timestamp": _to_canon_ts(ev["timestamp"]),
            "snapshot": _snapshot_to_canon(ev["snapshot"]),
        }
        assert ev["canonical_event"] == json.dumps(
            reparsed, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        prev = ev["hash"]


def _to_canon_ts(iso_z: str) -> str:
    dt = datetime.fromisoformat(iso_z.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc).strftime(audit.CANON_FMT)


def _snapshot_to_canon(s: dict) -> dict:
    return {
        "reservation_id": s["reservation_id"],
        "request_id": s["request_id"],
        "start_utc": _to_canon_ts(s["start_utc"]),
        "end_utc": _to_canon_ts(s["end_utc"]),
        "participants": s["participants"],
        "version": s["version"],
        "status": s["status"],
        "canceled_at": _to_canon_ts(s["canceled_at"])
            if s["canceled_at"] is not None else None,
        "created_at": _to_canon_ts(s["created_at"]),
    }


# ------------------------------------------------------------- 404 / 409 语义


def test_history_unknown_reservation_returns_404(client):
    resp = _history(client, 999)
    assert resp.status_code == 404
    assert "999" in resp.json()["detail"]


def test_history_tampered_hash_returns_409_and_no_events(client, db_path):
    rid = client.post("/api/v1/reservations", json=_payload()).json()[
        "reservation_id"]
    assert _put(client, rid).status_code == 200

    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE reservation_events SET hash = ? WHERE seq = 1",
                     ("a" * 64,))
        conn.commit()
    # 关闭 WAL 中可能残留的连接不影响：重新打开的客户端会读到已提交数据

    resp = _history(client, rid)
    assert resp.status_code == 409
    body = resp.json()
    assert "events" not in body
    assert "chain verification failed" in body["detail"]
    # 其余预约接口仍正常（用新的 request_id；req-1 已被改期为不同内容）
    assert client.post("/api/v1/reservations", json=_payload(
        request_id="req-other",
        start_utc="2026-03-09T18:00:00Z",
        end_utc="2026-03-09T19:00:00Z",
        participant_ids=["zoe"],
    )).status_code == 201


def test_history_tampered_canonical_event_returns_409(client, db_path):
    rid = client.post("/api/v1/reservations", json=_payload()).json()[
        "reservation_id"]
    with sqlite3.connect(db_path) as conn:
        # 改快照但保留旧 hash：重算必然不一致
        row = conn.execute(
            "SELECT canonical_event FROM reservation_events WHERE seq=1"
        ).fetchone()
        tampered = row[0].replace("13:00:00", "18:00:00")
        conn.execute("UPDATE reservation_events SET canonical_event = ? WHERE seq = 1",
                     (tampered,))
        conn.commit()

    resp = _history(client, rid)
    assert resp.status_code == 409
    assert "events" not in resp.json()


def test_history_seq_gap_returns_409(client, db_path):
    rid = client.post("/api/v1/reservations", json=_payload()).json()[
        "reservation_id"]
    assert _put(client, rid).status_code == 200
    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM reservation_events WHERE seq = 1")
        conn.commit()
    resp = _history(client, rid)
    assert resp.status_code == 409
    assert "seq gap" in resp.json()["detail"]


def test_history_snapshot_column_divergence_returns_409(client, db_path):
    rid = client.post("/api/v1/reservations", json=_payload()).json()[
        "reservation_id"]
    with sqlite3.connect(db_path) as conn:
        good = conn.execute(
            "SELECT snapshot FROM reservation_events WHERE seq=1").fetchone()[0]
        diverged = json.loads(good)
        diverged["version"] = 99
        conn.execute("UPDATE reservation_events SET snapshot = ? WHERE seq = 1",
                     (json.dumps(diverged, ensure_ascii=False),))
        conn.commit()
    resp = _history(client, rid)
    assert resp.status_code == 409


# ------------------------------------------------------------- 失败与幂等不写事件


def test_failed_create_writes_no_event(client, db_path):
    assert client.post("/api/v1/reservations", json=_payload()).status_code == 201
    # 冲突失败
    resp = client.post("/api/v1/reservations", json=_payload(
        request_id="req-2", participant_ids=["alice"]))
    assert resp.status_code == 409
    # request_id 内容不同失败
    resp = client.post("/api/v1/reservations", json=_payload(
        participant_ids=["alice"]))
    assert resp.status_code == 409
    # 只有成功的那一条 created
    rid = 1
    events = _history(client, rid).json()["events"]
    assert len(events) == 1 and events[0]["action"] == "created"
    # 事件表里没有指向“不存在的预约”的孤儿行
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT count(*) FROM reservation_events").fetchone()[0] == 1


def test_idempotent_replay_adds_no_event(client):
    created = client.post("/api/v1/reservations", json=_payload())
    assert created.status_code == 201
    rid = created.json()["reservation_id"]
    replay = client.post("/api/v1/reservations",
                         json=_payload(participant_ids=["bob", "alice", "bob"]))
    assert replay.status_code == 200
    events = _history(client, rid).json()["events"]
    assert len(events) == 1
    assert events[0]["action"] == "created"
    assert events[0]["hash"]  # 链仍完好


def test_failed_reschedule_adds_no_event(client, db_path):
    rid = client.post("/api/v1/reservations", json=_payload()).json()[
        "reservation_id"]
    # 版本过期
    assert _put(client, rid, expected_version=5).status_code == 409
    # 与其他预约冲突
    assert client.post("/api/v1/reservations", json=_payload(
        request_id="req-other",
        start_utc="2026-03-09T15:00:00Z",
        end_utc="2026-03-09T16:00:00Z",
        participant_ids=["zoe"],
    )).status_code == 201
    assert _put(client, rid, participant_ids=["zoe"]).status_code == 409
    # 事件仍只有 created
    events = _history(client, rid).json()["events"]
    assert [e["action"] for e in events] == ["created"]


def test_failed_cancel_adds_no_event(client):
    rid = client.post("/api/v1/reservations", json=_payload()).json()[
        "reservation_id"]
    assert _delete(client, rid, expected_version=9).status_code == 409
    events = _history(client, rid).json()["events"]
    assert [e["action"] for e in events] == ["created"]


def test_cancel_idempotent_retry_adds_no_event(client):
    rid = client.post("/api/v1/reservations", json=_payload()).json()[
        "reservation_id"]
    assert _delete(client, rid, 1).status_code == 200
    assert _delete(client, rid, 1).status_code == 200  # 幂等重试
    events = _history(client, rid).json()["events"]
    assert [e["action"] for e in events] == ["created", "canceled"]


def test_business_write_and_event_share_one_transaction(db_path):
    """事件插入失败时业务写入一并回滚（不留下无事件的预约）。"""
    # 先建表，再用触发器让审计写入必失败
    db_module.init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TRIGGER fail_event_insert BEFORE INSERT ON reservation_events"
            " BEGIN SELECT RAISE(ABORT, 'audit unavailable'); END")
        conn.commit()
    failing_client = TestClient(app, raise_server_exceptions=False)
    resp = failing_client.post("/api/v1/reservations", json=_payload())
    assert resp.status_code == 500
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT count(*) FROM reservations").fetchone()[0] == 0
        assert conn.execute(
            "SELECT count(*) FROM reservation_participants").fetchone()[0] == 0
        assert conn.execute(
            "SELECT count(*) FROM reservation_events").fetchone()[0] == 0


# ------------------------------------------------------------- 非 ASCII 参与者


def test_non_ascii_participant_ids_round_trip_through_chain(client):
    created = client.post("/api/v1/reservations", json=_payload(
        participant_ids=["张三", "李四"]))
    assert created.status_code == 201, created.text
    rid = created.json()["reservation_id"]
    events = _history(client, rid).json()["events"]
    assert events[0]["snapshot"]["participants"] == ["张三", "李四"]
    assert "张三" in events[0]["canonical_event"]
    # 服务端已重验通过（200）即证明 UTF-8 哈希一致；这里再独立验一遍
    digest = hashlib.sha256(
        events[0]["prev_hash"].encode("ascii")
        + events[0]["canonical_event"].encode("utf-8")).hexdigest()
    assert digest == events[0]["hash"]


# ------------------------------------------------------- 并发下序号仍连续无缺


def test_concurrent_reschedules_produce_contiguous_chain(db_path):
    import threading
    from concurrent.futures import ThreadPoolExecutor

    from datetime import timedelta

    base_start = datetime(2026, 3, 9, 13, tzinfo=timezone.utc)
    reservation, _ = core.create_reservation(
        request_id="req-base",
        start_utc=base_start,
        end_utc=base_start + timedelta(hours=1),
        participant_ids=["alice"],
        db_path=db_path,
    )

    def worker(i: int, expected: int):
        start = base_start + timedelta(days=10 + i)
        try:
            core.reschedule_reservation(
                reservation_id=reservation.id,
                expected_version=expected,
                start_utc=start,
                end_utc=start + timedelta(hours=1),
                participant_ids=["alice"],
                db_path=db_path,
            )
            return True
        except core.VersionConflictError:
            return False

    # 6 轮乐观锁竞争：每轮可能多个输家，赢家后 expected 推进，循环到全部尝试完
    version = 1
    wins = 0
    for round_i in range(6):
        with ThreadPoolExecutor(max_workers=4) as pool:
            barrier = threading.Barrier(4)

            def w(i):
                barrier.wait()
                return worker(round_i * 10 + i, version)

            results = list(pool.map(w, range(4)))
        wins += sum(results)
        version += 1

    events = core.get_reservation_history(reservation.id, db_path=db_path)
    seqs = [e["seq"] for e in events]
    assert seqs == list(range(1, len(events) + 1))
    assert [e["action"] for e in events].count("rescheduled") == wins
    assert events[-1]["snapshot"]["version"] == 1 + wins


# ------------------------------------------- 初始化补写 baseline（当前版 schema）


def test_startup_backfills_baseline_for_reservations_without_events(tmp_path,
                                                                    monkeypatch):
    import sqlite3

    path = str(tmp_path / "existing.db")
    monkeypatch.setenv("RESERVATIONS_DB_PATH", path)
    # 第一次启动：正常建库并产生两条预约（各自带 created 事件）
    db_module.init_db(path)
    r1, _ = core.create_reservation(
        request_id="req-1",
        start_utc=datetime(2026, 3, 9, 13, tzinfo=timezone.utc),
        end_utc=datetime(2026, 3, 9, 14, tzinfo=timezone.utc),
        participant_ids=["alice"],
        db_path=path,
    )
    r2, _ = core.create_reservation(
        request_id="req-2",
        start_utc=datetime(2026, 3, 9, 15, tzinfo=timezone.utc),
        end_utc=datetime(2026, 3, 9, 16, tzinfo=timezone.utc),
        participant_ids=["bob"],
        db_path=path,
    )

    # 模拟“审计表上线前的旧库”：删除全部事件（预约与占用保留）
    with sqlite3.connect(path) as conn:
        conn.execute("DELETE FROM reservation_events")
        conn.commit()

    # 再次初始化：应为两条既有预约各补一条 baseline
    db_module.init_db(path)
    db_module.init_db(path)  # 幂等：不重复补写

    for rid in (r1.id, r2.id):
        events = core.get_reservation_history(rid, db_path=path)
        assert len(events) == 1
        ev = events[0]
        assert ev["seq"] == 1
        assert ev["action"] == "baseline"
        assert ev["prev_hash"] == "0" * 64
        assert ev["snapshot"]["reservation_id"] == rid
        assert ev["snapshot"]["status"] == "active"
        # baseline 时间即迁移初始化时刻（不早于预约 created_at），非伪造旧操作
        assert ev["timestamp"] >= ev["snapshot"]["created_at"]

    # 初始化之后的新操作正常接到 baseline 之后（seq=2，prev 衔接）
    moved = core.reschedule_reservation(
        reservation_id=r1.id,
        expected_version=1,
        start_utc=datetime(2026, 3, 9, 17, tzinfo=timezone.utc),
        end_utc=datetime(2026, 3, 9, 18, tzinfo=timezone.utc),
        participant_ids=["alice"],
        db_path=path,
    )
    assert moved.version == 2
    events = core.get_reservation_history(r1.id, db_path=path)
    assert [e["seq"] for e in events] == [1, 2]
    assert [e["action"] for e in events] == ["baseline", "rescheduled"]
    assert events[1]["prev_hash"] == events[0]["hash"]


# ------------------------------------------- 旧版数据库（无 version 列）迁移


_LEGACY_SCHEMA = """\
CREATE TABLE reservations (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id       TEXT NOT NULL UNIQUE,
    start_utc        TEXT NOT NULL,
    end_utc          TEXT NOT NULL,
    participants_key TEXT NOT NULL,
    created_at       TEXT NOT NULL
);
CREATE TABLE reservation_participants (
    reservation_id INTEGER NOT NULL REFERENCES reservations(id) ON DELETE CASCADE,
    participant_id TEXT NOT NULL,
    start_utc      TEXT NOT NULL,
    end_utc        TEXT NOT NULL,
    PRIMARY KEY (reservation_id, participant_id)
);
CREATE INDEX idx_rp_participant_window
    ON reservation_participants (participant_id, start_utc, end_utc);
"""


def test_legacy_db_startup_backfills_columns_and_baseline(tmp_path, monkeypatch):
    path = str(tmp_path / "legacy.db")
    conn = sqlite3.connect(path)
    try:
        conn.executescript(_LEGACY_SCHEMA)
        conn.execute(
            "INSERT INTO reservations"
            " (request_id, start_utc, end_utc, participants_key, created_at)"
            " VALUES ('old-req', '2026-03-09T13:00:00.000000Z',"
            " '2026-03-09T14:00:00.000000Z', '[\"alice\", \"bob\"]',"
            " '2026-03-01T00:00:00.000000Z')")
        conn.executemany(
            "INSERT INTO reservation_participants"
            " (reservation_id, participant_id, start_utc, end_utc)"
            " VALUES (1, ?, '2026-03-09T13:00:00.000000Z',"
            " '2026-03-09T14:00:00.000000Z')",
            [("alice",), ("bob",)],
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setenv("RESERVATIONS_DB_PATH", path)
    db_module.init_db(path)  # 补列 + 补 baseline，同一初始化流程
    db_module.init_db(path)  # 幂等

    client = TestClient(app)
    resp = _history(client, 1)
    assert resp.status_code == 200, resp.text
    events = resp.json()["events"]
    assert len(events) == 1
    ev = events[0]
    assert ev["action"] == "baseline"
    assert ev["seq"] == 1 and ev["prev_hash"] == "0" * 64
    snap = ev["snapshot"]
    # 如实记录初始化时观测到的状态（列默认值补齐），不伪造 created 事件
    assert snap["version"] == 1
    assert snap["status"] == "active"
    assert snap["canceled_at"] is None
    assert snap["participants"] == ["alice", "bob"]
    assert snap["created_at"] == "2026-03-01T00:00:00Z"
    # baseline 时间晚于旧 created_at：明确是迁移时补写
    assert ev["timestamp"] > snap["created_at"]

    # 既有预约仍可改期，新事件接在 baseline 之后，链完好
    moved = _put(client, 1, participant_ids=["alice", "bob"],
                 start_utc="2026-03-09T16:00:00Z",
                 end_utc="2026-03-09T17:00:00Z")
    assert moved.status_code == 200, moved.text
    events2 = _history(client, 1).json()["events"]
    assert [e["action"] for e in events2] == ["baseline", "rescheduled"]
    assert events2[1]["prev_hash"] == events2[0]["hash"]


# ------------------------------------------------------- baseline 取消态也如实记录


def test_backfilled_baseline_preserves_canceled_state(tmp_path, monkeypatch):
    """旧库（有 status 列、无事件表数据）中已取消的预约，baseline 如实记录终态。"""
    path = str(tmp_path / "pre-audit.db")
    db_module.init_db(path)
    # 用核心层制造一条已取消预约，再抹掉事件，模拟审计功能上线前的状态
    r, _ = core.create_reservation(
        request_id="req-c",
        start_utc=datetime(2026, 3, 9, 13, tzinfo=timezone.utc),
        end_utc=datetime(2026, 3, 9, 14, tzinfo=timezone.utc),
        participant_ids=["alice"],
        db_path=path,
    )
    core.cancel_reservation(r.id, expected_version=1, db_path=path)
    with sqlite3.connect(path) as conn:
        conn.execute("DELETE FROM reservation_events")
        conn.commit()

    monkeypatch.setenv("RESERVATIONS_DB_PATH", path)
    db_module.init_db(path)

    events = core.get_reservation_history(r.id, db_path=path)
    assert len(events) == 1
    assert events[0]["action"] == "baseline"
    snap = events[0]["snapshot"]
    assert snap["status"] == "canceled"
    assert snap["version"] == 2
    assert snap["canceled_at"] is not None
    # 取消是终态：baseline 之后改期仍被拒绝，且不产生事件
    with pytest.raises(core.ReservationCanceledError):
        core.reschedule_reservation(
            reservation_id=r.id, expected_version=2,
            start_utc=datetime(2026, 3, 9, 18, tzinfo=timezone.utc),
            end_utc=datetime(2026, 3, 9, 19, tzinfo=timezone.utc),
            participant_ids=["alice"],
            db_path=path,
        )
    events = core.get_reservation_history(r.id, db_path=path)
    assert [e["action"] for e in events] == ["baseline"]
