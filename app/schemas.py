"""请求/响应 Pydantic 模型。"""

from __future__ import annotations

from datetime import date, datetime, time, timezone
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

# 1=周一 ... 7=周日（对应 date.isoweekday）
WEEKDAY_NAMES = {
    1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday",
    5: "Friday", 6: "Saturday", 7: "Sunday",
}
WEEKDAY_FROM_NAME = {v.lower(): k for k, v in WEEKDAY_NAMES.items()}
WEEKDAY_ALIASES = {
    "mon": 1, "tue": 2, "wed": 3, "thu": 4,
    "fri": 5, "sat": 6, "sun": 7,
}


def _parse_weekday(value: int | str) -> int:
    if isinstance(value, int):
        if value not in WEEKDAY_NAMES:
            raise ValueError("weekday must be 1 (Monday) .. 7 (Sunday)")
        return value
    key = value.strip().lower()
    if key in WEEKDAY_FROM_NAME:
        return WEEKDAY_FROM_NAME[key]
    if key in WEEKDAY_ALIASES:
        return WEEKDAY_ALIASES[key]
    raise ValueError(f"invalid weekday: {value!r}")


class AvailabilityWindowIn(BaseModel):
    """按本地星期与钟面时刻填写的每周重复可用区间（半开）。

    ``end_local <= start_local`` 表示跨午夜：窗口从 weekday 当天的
    start_local 持续到次日的 end_local。
    """

    weekday: int | str = Field(
        description="1..7（1=周一），或 'Monday'/'mon' 这样的名称",
        examples=[1],
    )
    start_local: time = Field(description="本地开始钟面时刻 HH:MM[:SS]", examples=["09:00"])
    end_local: time = Field(description="本地结束钟面时刻；<= 开始时刻表示跨午夜", examples=["17:30"])

    @field_validator("weekday")
    @classmethod
    def _validate_weekday(cls, v: int | str) -> int:
        return _parse_weekday(v)


class ParticipantIn(BaseModel):
    id: str = Field(min_length=1, description="参与者唯一标识", examples=["alice"])
    timezone: str = Field(description="IANA 时区名，如 Asia/Shanghai", examples=["Asia/Shanghai"])
    availability: list[AvailabilityWindowIn] = Field(min_length=1)


class AggregationRequest(BaseModel):
    start_date: date = Field(description="搜索日期范围起点（含，UTC）", examples=["2026-03-07"])
    end_date: date = Field(description="搜索日期范围终点（含，UTC）", examples=["2026-03-09"])
    meeting_minutes: int = Field(gt=0, description="会议时长（分钟）", examples=[60])
    min_attendees: int = Field(gt=0, description="最低出席人数", examples=[2])
    participants: list[ParticipantIn] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate(self) -> "AggregationRequest":
        if self.end_date < self.start_date:
            raise ValueError("end_date must not be before start_date")
        ids = [p.id for p in self.participants]
        if len(ids) != len(set(ids)):
            raise ValueError("participant ids must be unique")
        if self.min_attendees > len(self.participants):
            raise ValueError("min_attendees cannot exceed the number of participants")
        return self


class LocalTimeOut(BaseModel):
    participant_id: str
    timezone: str
    start_local: str = Field(description="本地开始时刻（含 UTC 偏移），如 2026-03-08T10:00:00+09:00")
    end_local: str
    start_weekday: Literal["Monday", "Tuesday", "Wednesday", "Thursday",
                           "Friday", "Saturday", "Sunday"]
    end_weekday: Literal["Monday", "Tuesday", "Wednesday", "Thursday",
                         "Friday", "Saturday", "Sunday"]


class SegmentOut(BaseModel):
    start_utc: str = Field(description="UTC 开始时间 ISO 8601", examples=["2026-03-08T01:00:00Z"])
    end_utc: str
    duration_minutes: int
    attendees: list[str] = Field(description="出席者 id，按字典序排列")
    attendee_count: int
    local_times: list[LocalTimeOut] = Field(description="每位出席者在各自时区的本地时刻")


class AggregationResponse(BaseModel):
    start_date: date
    end_date: date
    meeting_minutes: int
    min_attendees: int
    segments: list[SegmentOut]


class _ReservationWindowBase(BaseModel):
    """预约时间与参与者字段（创建/改期共用），规范化规则保持一致。"""

    start_utc: datetime = Field(
        description="UTC 开始时刻 ISO 8601（需带时区偏移，如 ...Z）",
        examples=["2026-03-09T13:00:00Z"],
    )
    end_utc: datetime = Field(
        description="UTC 结束时刻，必须晚于开始时刻",
        examples=["2026-03-09T14:00:00Z"],
    )
    participant_ids: list[str] = Field(
        min_length=1,
        description="参与者 ID 列表；去重排序后存储并参与冲突检查",
        examples=[["alice", "bob"]],
    )

    @field_validator("start_utc", "end_utc")
    @classmethod
    def _require_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None or v.utcoffset() is None:
            raise ValueError(
                "timestamp must include a timezone offset (e.g. 2026-03-09T13:00:00Z)"
            )
        # 统一换算到 UTC，等价偏移（如 +09:00）视为同一时刻
        return v.astimezone(timezone.utc)

    @field_validator("participant_ids")
    @classmethod
    def _validate_ids(cls, v: list[str]) -> list[str]:
        if any(not pid.strip() for pid in v):
            raise ValueError("participant ids must be non-empty")
        return v

    @model_validator(mode="after")
    def _validate_window(self) -> "_ReservationWindowBase":
        if self.end_utc <= self.start_utc:
            raise ValueError("end_utc must be later than start_utc")
        return self


class ReservationIn(_ReservationWindowBase):
    """会议预约创建请求：幂等创建，参与者区间占用检查。"""

    request_id: str = Field(
        min_length=1, max_length=200,
        description="客户端幂等请求标识；相同标识 + 相同规范化内容重试返回原预约",
        examples=["req-9f3a"],
    )


class ReservationRescheduleIn(_ReservationWindowBase):
    """会议预约改期请求：乐观锁版本 + 新时间/参与者，沿用相同规范化规则。"""

    expected_version: int = Field(
        ge=1,
        description="客户端上次读到的预约版本；与当前版本一致才执行改期",
        examples=[1],
    )


class ReservationHoldIn(_ReservationWindowBase):
    """临时保留创建请求：幂等 hold_key + 半开区间参与者占用 + TTL 秒数。"""

    hold_key: str = Field(
        min_length=1, max_length=200,
        description=(
            "客户端幂等键：相同键 + 相同规范化内容（含 ttl_seconds）重试"
            "返回原 pending 保留；键相同但内容不同返回 409"
        ),
        examples=["hold-7c21"],
    )
    ttl_seconds: int = Field(
        ge=1, le=900,
        description="保留有效期（秒），1..900；到期后保留在写事务内惰性过期",
        examples=[300],
    )


class ReservationHoldConfirmIn(BaseModel):
    """临时保留确认请求：request_id 作为转成正式预约后的幂等请求标识。"""

    request_id: str = Field(
        min_length=1, max_length=200,
        description="正式预约的客户端幂等请求标识（写入 reservations.request_id）",
        examples=["req-confirm-1"],
    )


class ReservationWaitlistIn(_ReservationWindowBase):
    """候补请求：幂等 request_id + 半开区间参与者占用；冲突时排队而非报错。"""

    request_id: str = Field(
        min_length=1, max_length=200,
        description=(
            "客户端幂等请求标识（与正式预约共用同一命名空间）："
            "时段当前可预约则直接创建正式预约，否则按创建时间进入 pending 队列；"
            "相同标识 + 相同规范化内容重试返回当前候补状态"
        ),
        examples=["req-wait-1"],
    )


class ReservationHoldOut(BaseModel):
    hold_id: int
    hold_key: str
    start_utc: str = Field(description="UTC 开始时间 ISO 8601", examples=["2026-03-09T13:00:00Z"])
    end_utc: str
    participants: list[str] = Field(description="参与者 id，去重后按字典序排列")
    ttl_seconds: int = Field(description="保留有效期（秒，1..900）", examples=[300])
    status: Literal["pending", "confirmed", "expired"] = Field(
        description="保留状态：pending=待确认（未过期）；confirmed=已转正式预约；expired=已过期",
        examples=["pending"],
    )
    reservation_id: int | None = Field(
        default=None,
        description="confirmed 后关联的正式预约 ID；pending/expired 为 null",
    )
    created_at: str
    expires_at: str = Field(description="过期时刻 ISO 8601（created_at + ttl_seconds）")
    confirmed_at: str | None = None


class ReservationOut(BaseModel):
    reservation_id: int
    request_id: str
    start_utc: str = Field(description="UTC 开始时间 ISO 8601", examples=["2026-03-09T13:00:00Z"])
    end_utc: str
    participants: list[str] = Field(description="参与者 id，去重后按字典序排列")
    version: int = Field(description="乐观锁版本：创建为 1，每次成功改期/取消加 1", examples=[1])
    status: Literal["active", "canceled"] = Field(
        description="预约状态：active=进行中；canceled=已取消（软删除，终态）",
        examples=["active"],
    )
    canceled_at: str | None = Field(
        default=None,
        description="取消时刻 ISO 8601；未取消为 null",
        examples=[None],
    )
    created_at: str


class ReservationWaitlistOut(BaseModel):
    """候补请求状态（pending 候补中 / promoted 已转正式预约）。"""

    waitlist_id: int
    request_id: str
    start_utc: str = Field(description="UTC 开始时间 ISO 8601", examples=["2026-03-09T13:00:00Z"])
    end_utc: str
    participants: list[str] = Field(description="参与者 id，去重后按字典序排列")
    created_at: str = Field(
        description="入队时刻（UTC ISO 8601），即候补 FIFO 排序键",
    )
    status: Literal["pending", "promoted"] = Field(
        description="候补状态：pending=候补中；promoted=已转为正式预约",
        examples=["pending"],
    )
    reservation: ReservationOut | None = Field(
        default=None,
        description="promoted 后关联的正式预约（含其当前最新状态）；pending 为 null",
    )
    promoted_at: str | None = Field(
        default=None,
        description="转为正式预约的时刻（UTC ISO 8601）；pending 为 null",
    )


class HistoryEventOut(BaseModel):
    """预约变更历史中的单条审计事件（哈希链一环）。"""

    seq: int = Field(description="预约内从 1 递增的事件序号", examples=[1])
    action: Literal["created", "rescheduled", "canceled", "baseline"] = Field(
        description=(
            "动作：created=创建、rescheduled=改期、canceled=取消；"
            "baseline=升级初始化时为既有预约补写的快照（非伪造的历史操作）"
        ),
        examples=["created"],
    )
    timestamp: str = Field(description="事件时间（UTC ISO 8601）")
    snapshot: ReservationOut = Field(
        description="事件发生后的预约完整快照（即参与哈希的快照）"
    )
    prev_hash: str = Field(
        description="上一条事件的哈希（64 位十六进制）；首条为 64 个 0",
        examples=["0" * 64],
    )
    hash: str = Field(
        description=(
            "本事件哈希 = SHA-256(prev_hash 的 ASCII 字节 +"
            " canonical_event 的 UTF-8 字节)，64 位十六进制"
        ),
    )
    canonical_event: str = Field(
        description=(
            "实际参与哈希的规范化 JSON：不含哈希字段、键按字典序排序、"
            "非 ASCII 不转义、无空白"
        ),
    )


class ReservationHistoryOut(BaseModel):
    reservation_id: int
    events: list[HistoryEventOut] = Field(
        description="按 seq 升序的审计事件；返回前服务端已重验整条哈希链"
    )
