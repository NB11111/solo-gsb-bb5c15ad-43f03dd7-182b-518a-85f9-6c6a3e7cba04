"""FastAPI 接口层测试。"""

from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def _payload(**overrides):
    payload = {
        "start_date": "2026-03-09",
        "end_date": "2026-03-09",
        "meeting_minutes": 60,
        "min_attendees": 2,
        "participants": [
            {
                "id": "tokyo",
                "timezone": "Asia/Tokyo",
                "availability": [
                    {"weekday": 1, "start_local": "22:00", "end_local": "02:00"}
                ],
            },
            {
                "id": "ny",
                "timezone": "America/New_York",
                "availability": [
                    {"weekday": 1, "start_local": "09:00", "end_local": "12:00"}
                ],
            },
        ],
    }
    payload.update(overrides)
    return payload


def test_health():
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_aggregate_happy_path():
    resp = client.post("/api/v1/availability/aggregate", json=_payload())
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["meeting_minutes"] == 60
    assert len(data["segments"]) == 1
    seg = data["segments"][0]
    assert seg["start_utc"] == "2026-03-09T13:00:00Z"
    assert seg["end_utc"] == "2026-03-09T16:00:00Z"
    assert seg["duration_minutes"] == 180
    assert seg["attendee_count"] == 2
    assert seg["attendees"] == ["ny", "tokyo"]
    locals_ = {lt["participant_id"]: lt for lt in seg["local_times"]}
    assert locals_["tokyo"]["start_local"] == "2026-03-09T22:00:00+09:00"
    assert locals_["tokyo"]["end_local"] == "2026-03-10T01:00:00+09:00"
    assert locals_["tokyo"]["start_weekday"] == "Monday"
    assert locals_["tokyo"]["end_weekday"] == "Tuesday"
    assert locals_["ny"]["start_local"] == "2026-03-09T09:00:00-04:00"
    assert locals_["ny"]["end_weekday"] == "Monday"


def test_weekday_accepts_names():
    payload = _payload()
    payload["participants"][0]["availability"][0]["weekday"] = "Monday"
    resp = client.post("/api/v1/availability/aggregate", json=payload)
    assert resp.status_code == 200, resp.text
    assert len(resp.json()["segments"]) == 1


def test_nonexistent_local_time_returns_400():
    payload = _payload()
    # 美东 2026-03-08 周日 02:30 不存在（春跳变）；搜索该日
    payload["start_date"] = payload["end_date"] = "2026-03-08"
    payload["participants"] = [
        {
            "id": "ny",
            "timezone": "America/New_York",
            "availability": [
                {"weekday": 7, "start_local": "02:30", "end_local": "04:00"}
            ],
        }
    ]
    payload["min_attendees"] = 1
    resp = client.post("/api/v1/availability/aggregate", json=payload)
    assert resp.status_code == 400
    assert "does not exist" in resp.json()["detail"]


def test_unknown_timezone_returns_400():
    payload = _payload()
    payload["participants"][0]["timezone"] = "Mars/Olympus"
    resp = client.post("/api/v1/availability/aggregate", json=payload)
    assert resp.status_code == 400
    assert "timezone" in resp.json()["detail"]


def test_fold_zero_via_api():
    # 美东 2025-11-02 周日 01:00-02:00 歧义窗口，fold=0 -> 05:00Z 开始
    payload = _payload(
        start_date="2025-11-02",
        end_date="2025-11-02",
        min_attendees=1,
        participants=[
            {
                "id": "ny",
                "timezone": "America/New_York",
                "availability": [
                    {"weekday": 7, "start_local": "01:00", "end_local": "02:00"}
                ],
            }
        ],
    )
    resp = client.post("/api/v1/availability/aggregate", json=payload)
    assert resp.status_code == 200, resp.text
    seg, = resp.json()["segments"]
    assert seg["start_utc"] == "2025-11-02T05:00:00Z"
    assert seg["end_utc"] == "2025-11-02T07:00:00Z"
    assert seg["duration_minutes"] == 120


def test_validation_errors_return_422():
    # 日期倒置
    resp = client.post("/api/v1/availability/aggregate",
                       json=_payload(start_date="2026-03-10", end_date="2026-03-09"))
    assert resp.status_code == 422

    # min_attendees 超过参与者数量
    resp = client.post("/api/v1/availability/aggregate",
                       json=_payload(min_attendees=5))
    assert resp.status_code == 422

    # 重复的参与者 id
    payload = _payload()
    payload["participants"][1]["id"] = "tokyo"
    resp = client.post("/api/v1/availability/aggregate", json=payload)
    assert resp.status_code == 422

    # 非法 weekday
    payload = _payload()
    payload["participants"][0]["availability"][0]["weekday"] = 9
    resp = client.post("/api/v1/availability/aggregate", json=payload)
    assert resp.status_code == 422


def test_empty_result_200():
    resp = client.post("/api/v1/availability/aggregate",
                       json=_payload(meeting_minutes=480))
    assert resp.status_code == 200
    assert resp.json()["segments"] == []


def test_result_sorted_count_desc_then_start_asc():
    participants = [
        {"id": "p1", "timezone": "UTC",
         "availability": [{"weekday": 1, "start_local": "00:00", "end_local": "12:00"}]},
        {"id": "p2", "timezone": "UTC",
         "availability": [{"weekday": 1, "start_local": "06:00", "end_local": "18:00"}]},
        {"id": "p3", "timezone": "UTC",
         "availability": [{"weekday": 1, "start_local": "10:00", "end_local": "11:00"}]},
    ]
    payload = _payload(min_attendees=1, participants=participants)
    resp = client.post("/api/v1/availability/aggregate", json=payload)
    assert resp.status_code == 200, resp.text
    counts = [s["attendee_count"] for s in resp.json()["segments"]]
    assert counts == sorted(counts, reverse=True)
    assert counts[0] == 3
