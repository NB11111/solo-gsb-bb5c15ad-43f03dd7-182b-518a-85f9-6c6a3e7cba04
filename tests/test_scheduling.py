"""核心调度逻辑测试：DST 跳变、歧义时刻、跨午夜、扫描线排序。"""

from __future__ import annotations

from datetime import date, time, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from app.scheduling import (
    Participant,
    SchedulingError,
    Window,
    aggregate,
    local_to_utc,
)

UTC = timezone.utc
NY = ZoneInfo("America/New_York")
BERLIN = ZoneInfo("Europe/Berlin")
TOKYO = ZoneInfo("Asia/Tokyo")


def _utc(y, mo, d, h, mi=0):
    from datetime import datetime
    return datetime(y, mo, d, h, mi, tzinfo=UTC)


# ---------------------------------------------------------------- local_to_utc

def test_nonexistent_time_spring_forward_raises():
    # 美东 2026-03-08（三月第二个周日）02:00 -> 03:00 跳变，02:30 不存在
    with pytest.raises(SchedulingError, match="does not exist"):
        local_to_utc(date(2026, 3, 8), time(2, 30), NY, "America/New_York")


def test_nonexistent_time_exactly_at_spring_forward_raises():
    # 02:00 整也是被跳过的时刻
    with pytest.raises(SchedulingError):
        local_to_utc(date(2026, 3, 8), time(2, 0), NY, "America/New_York")


def test_ambiguous_time_uses_fold_zero():
    # 美东 2025-11-02 秋回退，01:30 出现两次：
    # fold=0 -> EDT(-04:00) = 05:30Z；fold=1 -> EST(-05:00) = 06:30Z
    assert local_to_utc(date(2025, 11, 2), time(1, 30), NY,
                        "America/New_York") == _utc(2025, 11, 2, 5, 30)


def test_normal_time_roundtrip():
    assert local_to_utc(date(2026, 3, 9), time(9, 0), NY,
                        "America/New_York") == _utc(2026, 3, 9, 13, 0)  # EDT -4


def test_unknown_timezone_raises():
    from app.scheduling import get_timezone
    with pytest.raises(SchedulingError, match="unknown IANA timezone"):
        get_timezone("Not/AZone")


# ----------------------------------------------------------------- aggregate

def test_fold_zero_full_window_fall_back():
    """周日 01:00-02:00（美东）落在回退夜：

    fold=0 取第一次（EDT），区间为 05:00Z-07:00Z，时长 2 小时；
    若错误使用 fold=1，则只会得到 06:00Z-07:00Z 的 1 小时。
    """
    p = Participant("ny", "America/New_York",
                    (Window(7, time(1, 0), time(2, 0)),))
    segs = aggregate([p], date(2025, 11, 2), date(2025, 11, 2), 30, 1)
    assert len(segs) == 1
    seg = segs[0]
    assert seg.start_utc == _utc(2025, 11, 2, 5, 0)
    assert seg.end_utc == _utc(2025, 11, 2, 7, 0)
    assert seg.duration == timedelta(hours=2)


def test_spring_forward_window_nonexistent_rejected():
    # 周日 02:30-04:00，起点落在跳变间隙 -> 整体请求报错
    p = Participant("ny", "America/New_York",
                    (Window(7, time(2, 30), time(4, 0)),))
    with pytest.raises(SchedulingError, match="does not exist"):
        aggregate([p], date(2026, 3, 8), date(2026, 3, 8), 30, 1)


def test_spring_forward_shortens_wall_clock_window():
    """柏林 2026-03-29（周日）00:00-04:00，02:00->03:00 跳变：

    实际 UTC 时长只有 3 小时（23:00Z 前日 - 02:00Z 当日）。
    """
    p = Participant("berlin", "Europe/Berlin",
                    (Window(7, time(0, 0), time(4, 0)),))
    segs = aggregate([p], date(2026, 3, 28), date(2026, 3, 29), 60, 1)
    seg, = segs
    assert seg.start_utc == _utc(2026, 3, 28, 23, 0)
    assert seg.end_utc == _utc(2026, 3, 29, 2, 0)
    assert seg.duration == timedelta(hours=3)


def test_overnight_window_split_and_overlap():
    """东京周一 22:00-次日02:00 跨午夜，与纽约周一 09:00-12:00 重叠 3 小时。"""
    tokyo = Participant("tokyo", "Asia/Tokyo",
                        (Window(1, time(22, 0), time(2, 0)),))
    ny = Participant("ny", "America/New_York",
                     (Window(1, time(9, 0), time(12, 0)),))
    segs = aggregate([tokyo, ny], date(2026, 3, 9), date(2026, 3, 9), 60, 2)
    seg, = segs
    assert seg.start_utc == _utc(2026, 3, 9, 13, 0)
    assert seg.end_utc == _utc(2026, 3, 9, 16, 0)
    assert seg.duration == timedelta(hours=3)
    assert seg.attendees == ("ny", "tokyo")

    # 各自本地时刻
    local = {pid: (seg.start_utc.astimezone(z), seg.end_utc.astimezone(z))
             for pid, z in (("tokyo", TOKYO), ("ny", NY))}
    assert local["tokyo"][0].isoformat() == "2026-03-09T22:00:00+09:00"
    assert local["tokyo"][1].isoformat() == "2026-03-10T01:00:00+09:00"
    assert local["ny"][0].isoformat() == "2026-03-09T09:00:00-04:00"
    assert local["ny"][1].isoformat() == "2026-03-09T12:00:00-04:00"


def test_overnight_segment_too_short_filtered_out():
    # 东京 23:30-00:30，纽约只在 15:00-15:15Z 有空 -> 共同段仅 15 分钟，
    # 不足 30 分钟会议，不返回
    tokyo = Participant("tokyo", "Asia/Tokyo",
                        (Window(1, time(23, 30), time(0, 30)),))
    ny = Participant("ny", "America/New_York",
                     (Window(1, time(11, 0), time(11, 15)),))  # 15:00-15:15Z
    segs = aggregate([tokyo, ny], date(2026, 3, 9), date(2026, 3, 9), 30, 2)
    assert segs == []


def test_sweep_ordering_count_desc_start_asc():
    p1 = Participant("p1", "UTC", (Window(1, time(0), time(12)),))
    p2 = Participant("p2", "UTC", (Window(1, time(6), time(18)),))
    p3 = Participant("p3", "UTC", (Window(1, time(10), time(11)),))
    segs = aggregate([p1, p2, p3], date(2026, 3, 9), date(2026, 3, 9), 30, 1)
    counts = [len(s.attendees) for s in segs]
    # 00-06:{p1} 06-10:{p1,p2} 10-11:三人 11-12:{p1,p2} 12-18:{p2}
    assert counts == [3, 2, 2, 1, 1]
    # 同人数内 UTC 开始时间升序
    two_person = [s for s in segs if len(s.attendees) == 2]
    assert [s.start_utc for s in two_person] == sorted(s.start_utc for s in two_person)
    assert two_person[0].start_utc == _utc(2026, 3, 9, 6)
    assert two_person[1].start_utc == _utc(2026, 3, 9, 11)
    # 三人段
    assert segs[0].attendees == ("p1", "p2", "p3")
    assert segs[0].start_utc == _utc(2026, 3, 9, 10)


def test_horizon_clipping_utc_anchored():
    """搜索区间以 UTC 日期为界：东京周一 00:00-01:00 实际是周日 15:00Z，

    当 start_date=周一（UTC）时落在区间外，被裁剪掉。
    """
    p = Participant("tokyo", "Asia/Tokyo",
                    (Window(1, time(0, 0), time(1, 0)),))
    segs = aggregate([p], date(2026, 3, 9), date(2026, 3, 9), 30, 1)
    assert segs == []


def test_horizon_inclusive_both_date_bounds():
    # UTC 参与者周一与周二各一小时，范围两天都要保留
    p = Participant("u", "UTC",
                    (Window(1, time(10), time(11)),
                     Window(2, time(10), time(11))))
    segs = aggregate([p], date(2026, 3, 9), date(2026, 3, 10), 30, 1)
    assert [s.start_utc for s in segs] == [
        _utc(2026, 3, 9, 10), _utc(2026, 3, 10, 10)
    ]


def test_multi_day_recurring_windows():
    # 连续两天重复窗口产生两个独立段
    p = Participant("u", "UTC", (Window(1, time(9), time(10)),))
    segs = aggregate([p], date(2026, 3, 9), date(2026, 3, 16), 30, 1)
    assert len(segs) == 2


def test_overlapping_windows_same_participant_merged():
    # 同一参与者重叠窗口不应在扫描线中被计为两人
    p = Participant("u", "UTC",
                    (Window(1, time(9), time(11)),
                     Window(1, time(10), time(12))))
    segs = aggregate([p], date(2026, 3, 9), date(2026, 3, 9), 30, 1)
    seg, = segs
    assert seg.start_utc == _utc(2026, 3, 9, 9)
    assert seg.end_utc == _utc(2026, 3, 9, 12)
    assert seg.attendees == ("u",)


def test_invalid_arguments():
    p = Participant("u", "UTC", (Window(1, time(9), time(10)),))
    with pytest.raises(SchedulingError):
        aggregate([p], date(2026, 3, 10), date(2026, 3, 9), 30, 1)
    with pytest.raises(SchedulingError):
        aggregate([p], date(2026, 3, 9), date(2026, 3, 9), 0, 1)
    with pytest.raises(SchedulingError):
        aggregate([p], date(2026, 3, 9), date(2026, 3, 9), 30, 0)
