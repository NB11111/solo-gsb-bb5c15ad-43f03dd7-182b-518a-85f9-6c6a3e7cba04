"""跨时区可用时段聚合核心逻辑。

输入每位参与者按本地星期/时刻填写的重复可用区间，经过以下步骤得到候选时段：

1. 按具体日期将本地时刻附加 IANA 时区；DST 规则由 zoneinfo 按日期应用。
2. 不存在的本地时刻（春季跳变被跳过的钟面时间）抛出错误；
   歧义时刻（秋季回退重叠时间）统一采用 ``fold=0``，即偏移量较大的第一次出现。
3. 跨午夜区间（``end <= start``）拆分成跨日的两个半开区间，统一换算到 UTC。
4. 在 [start_date 00:00 UTC, end_date+1 00:00 UTC) 的时间轴上用扫描线
   求每个时刻的出席者集合，过滤出席人数不足或时长短于会议时长的段。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# Python isoweekday()：周一=1 ... 周日=7
WEEKDAYS = frozenset(range(1, 8))

# 时区最大偏移为 UTC+14/-12，窗口还可能向前/后跨午夜；
# 实例化候选日期时在请求日期范围两侧各多取两天即可完整覆盖搜索区间。
_SIDE_PADDING_DAYS = 2


class SchedulingError(ValueError):
    """输入数据无法转换为有效排期（未知时区、不存在的本地时刻等）。"""


ZoneInfoType = type(ZoneInfo("UTC"))


def get_timezone(tz_name: str) -> ZoneInfoType:
    """按名称获取 IANA 时区，未知名称抛 :class:`SchedulingError`。"""
    try:
        return ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, KeyError, ValueError) as exc:
        raise SchedulingError(f"unknown IANA timezone: {tz_name!r}") from exc


def local_to_utc(local_date: date, local_time: time, tzinfo: ZoneInfoType,
                 tz_name: str) -> datetime:
    """把某个本地日期 + 钟面时刻换算为 UTC datetime。

    - 歧义时刻（秋季回退）使用 ``fold=0``（偏移量较大、较早的那一次）；
    - 不存在的时刻（春季跳变被跳过）抛 :class:`SchedulingError`。
    """
    local_dt = datetime.combine(local_date, local_time, tzinfo=tzinfo).replace(fold=0)
    # fold=0 下做一次 UTC 往返：不存在的钟面时间会被映射到跳变之后，
    # 往返回来时 fold 变为 1，以此识别不存在时刻。
    roundtrip = local_dt.astimezone(timezone.utc).astimezone(tzinfo)
    if roundtrip.replace(tzinfo=None) != local_dt.replace(tzinfo=None) or roundtrip.fold != 0:
        raise SchedulingError(
            f"local time {local_date.isoformat()} {local_time.isoformat()} "
            f"does not exist in timezone {tz_name!r} due to a DST forward gap"
        )
    return local_dt.astimezone(timezone.utc)


@dataclass(frozen=True)
class Window:
    """某参与者一条本地重复可用区间（半开）。

    ``end_local <= start_local`` 表示跨午夜：起点在 weekday 当天，
    终点落在次日的 ``end_local``（end 为 00:00 时即午夜整）。
    """

    weekday: int  # isoweekday: 1=Mon ... 7=Sun
    start_local: time
    end_local: time


@dataclass(frozen=True)
class Participant:
    participant_id: str
    timezone: str
    windows: tuple[Window, ...]


@dataclass(frozen=True)
class Segment:
    """扫描线产出的一段恒定出席集合的候选时段。"""

    start_utc: datetime
    end_utc: datetime
    attendees: tuple[str, ...]

    @property
    def duration(self) -> timedelta:
        return self.end_utc - self.start_utc


def participant_intervals(participant: Participant,
                          d0: date, d1: date) -> list[tuple[datetime, datetime]]:
    """把一位参与者在候选日期范围内的重复窗口实例化为 UTC 区间（未裁剪）。"""
    tzinfo = get_timezone(participant.timezone)
    intervals: list[tuple[datetime, datetime]] = []
    day = d0 - timedelta(days=_SIDE_PADDING_DAYS)
    last = d1 + timedelta(days=_SIDE_PADDING_DAYS)
    while day <= last:
        for win in participant.windows:
            if day.isoweekday() != win.weekday:
                continue
            start_utc = local_to_utc(day, win.start_local, tzinfo, participant.timezone)
            # end <= start 表示跨午夜：结束钟面落在次日；否则就在当日
            end_date = day + (timedelta(days=1)
                              if win.end_local <= win.start_local
                              else timedelta(0))
            end_utc = local_to_utc(end_date, win.end_local, tzinfo, participant.timezone)
            # end 钟面时间必须晚于起点（00:00 终点因日期 +1 天然成立；
            # 若两者恰好落在 DST 间隙两侧导致反转，说明该窗口无法在该日成立）
            if end_utc <= start_utc:
                raise SchedulingError(
                    f"window of participant {participant.participant_id!r} on "
                    f"{day.isoformat()} ({win.start_local.isoformat()} - "
                    f"{win.end_local.isoformat()}) has non-positive duration "
                    f"after applying DST rules in {participant.timezone!r}"
                )
            intervals.append((start_utc, end_utc))
        day += timedelta(days=1)

    # 同一参与者可能有窗口重叠（相邻跨午夜 + 当日早窗口等），先按人合并，
    # 保证扫描线里一个人在任意时刻最多贡献一份出席。
    intervals.sort()
    merged: list[tuple[datetime, datetime]] = []
    for start, end in intervals:
        if merged and start <= merged[-1][1]:
            if end > merged[-1][1]:
                merged[-1] = (merged[-1][0], end)
        else:
            merged.append((start, end))
    return merged


def _clip(intervals: list[tuple[datetime, datetime]],
          horizon_start: datetime, horizon_end: datetime) -> list[tuple[datetime, datetime]]:
    out = []
    for start, end in intervals:
        s = max(start, horizon_start)
        e = min(end, horizon_end)
        if s < e:
            out.append((s, e))
    return out


def sweep(intervals_by_participant: dict[str, list[tuple[datetime, datetime]]],
          horizon_start: datetime, horizon_end: datetime,
          min_attendees: int) -> list[Segment]:
    """扫描线：求所有出席者集合恒定、且人数 >= min_attendees 的最大时段。"""
    # 事件：(时刻, 类型排序, 参与者)。同一时刻先结束后开始，
    # 这样首尾相接的区间不会产生零长度段。
    events: list[tuple[datetime, int, int, str]] = []
    for pid, intervals in intervals_by_participant.items():
        for start, end in intervals:
            events.append((start, 0, 1, pid))   # 开始事件
            events.append((end, 0, 0, pid))     # 结束事件
    events.sort(key=lambda ev: (ev[0], ev[2]))

    active: set[str] = set()
    segments: list[Segment] = []
    prev = horizon_start

    def flush(t: datetime) -> None:
        if prev < t and len(active) >= min_attendees:
            segments.append(Segment(prev, t, tuple(sorted(active))))

    for t, _, kind, pid in events:
        if t >= horizon_end:
            break
        if t < horizon_start:
            # 裁剪后理论上不会出现；防御性地直接更新出席集合
            if kind == 1:
                active.add(pid)
            else:
                active.discard(pid)
            continue
        if t > prev:
            flush(t)
        if kind == 1:
            active.add(pid)
        else:
            active.discard(pid)
        prev = t

    flush(horizon_end)
    return segments


def aggregate(participants: list[Participant], start_date: date, end_date: date,
              meeting_minutes: int, min_attendees: int) -> list[Segment]:
    """主入口：返回满足人数与时长的候选段。

    搜索区间为 ``[start_date 00:00 UTC, end_date + 1天 00:00 UTC)``。
    结果按出席人数降序、UTC 开始时间升序稳定排列。
    """
    if end_date < start_date:
        raise SchedulingError("end_date must not be before start_date")
    if meeting_minutes <= 0:
        raise SchedulingError("meeting_minutes must be positive")
    if min_attendees <= 0:
        raise SchedulingError("min_attendees must be positive")

    horizon_start = datetime.combine(start_date, time.min, tzinfo=timezone.utc)
    horizon_end = datetime.combine(end_date + timedelta(days=1), time.min,
                                   tzinfo=timezone.utc)

    clipped: dict[str, list[tuple[datetime, datetime]]] = {}
    for participant in participants:
        intervals = participant_intervals(participant, start_date, end_date)
        clipped[participant.participant_id] = _clip(
            intervals, horizon_start, horizon_end
        )

    segments = sweep(clipped, horizon_start, horizon_end, min_attendees)

    min_duration = timedelta(minutes=meeting_minutes)
    segments = [seg for seg in segments if seg.duration >= min_duration]

    # 稳定排序：出席人数降序，平局时 UTC 开始时间升序
    segments.sort(key=lambda seg: (-len(seg.attendees), seg.start_utc))
    return segments
