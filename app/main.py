"""FastAPI 应用：跨时区可用时段聚合 + SQLite 会议预约。

启动（容器内）：uvicorn app.main:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Query, Response
from fastapi.responses import JSONResponse

from . import db as db_module
from . import holds as hold_core
from . import reservations as reservation_core
from . import scheduling
from . import waitlist as waitlist_core
from .audit import CANON_FMT as _AUDIT_CANON_FMT, ChainVerificationError
from .holds import (
    HoldExpiredError,
    HoldKeyConflictError,
    HoldNotFoundError,
)
from .waitlist import (
    WaitlistContentConflictError,
    WaitlistNotFoundError,
)
from .reservations import (
    RequestIdConflictError,
    ReservationCanceledError,
    ReservationConflictError,
    ReservationNotFoundError,
    VersionConflictError,
)
from .scheduling import SchedulingError
from .schemas import (
    AggregationRequest,
    AggregationResponse,
    HistoryEventOut,
    LocalTimeOut,
    ParticipantIn,
    ReservationHistoryOut,
    ReservationHoldConfirmIn,
    ReservationHoldIn,
    ReservationHoldOut,
    ReservationIn,
    ReservationOut,
    ReservationRescheduleIn,
    ReservationWaitlistIn,
    ReservationWaitlistOut,
    SegmentOut,
    WEEKDAY_NAMES,
)
from .scheduling import Participant, Window as CoreWindow


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Compose 启动时自动初始化 SQLite（建表幂等）；
    # 数据库文件路径由 RESERVATIONS_DB_PATH 配置，默认落在挂载卷上
    db_module.init_db()
    yield


app = FastAPI(
    title="跨时区可用时段聚合 API",
    description=(
        "接收日期范围、会议时长、最低出席人数及每位参与者按本地星期/时刻填写的 "
        "重复可用区间与 IANA 时区，返回可安排会议的 UTC 候选时段及各自本地时刻。"
        "DST 按具体日期应用：不存在的本地时刻返回 400，歧义时刻统一取 fold=0。"
        "另提供 SQLite 持久化的会议预约接口：单事务冲突检测 + request_id 幂等，"
        "支持带乐观版本号的改期（PUT）与软删除取消（DELETE，取消后时段立即释放）；"
        "每次成功的创建/改期/取消都在同一事务追加 SHA-256 哈希链审计事件，"
        "可通过 GET 历史接口按序获取并由服务端重验整链。"
        "还提供预约临时保留（reservation hold）：TTL 1..900 秒的短时占位，"
        "与进行中预约及其他未过期保留在 BEGIN IMMEDIATE 事务内按半开区间检查冲突，"
        "到期保留在事务内惰性过期；未过期 pending 保留可确认为正式预约"
        "（同事务写入并追加 created 审计事件），重复确认幂等返回同一预约。"
        "另提供预约候补队列（reservation waitlist）：时段当前可预约则直接创建"
        "正式预约，否则按创建时间进入 pending 队列（相同 request_id 幂等）；"
        "取消正式预约时在同一 BEGIN IMMEDIATE 事务内按 FIFO 扫描受影响候补，"
        "跳过仍冲突项，把首个可满足项原子转为正式预约并写入现有哈希链审计。"
    ),
    version="1.6.0",
    lifespan=lifespan,
)


@app.exception_handler(SchedulingError)
async def scheduling_error_handler(request, exc: SchedulingError) -> JSONResponse:
    return JSONResponse(status_code=400, content={"detail": str(exc)})


@app.exception_handler(ReservationConflictError)
async def reservation_conflict_handler(request, exc: ReservationConflictError) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={
            "detail": str(exc),
            "conflicts": exc.conflicts,
        },
    )


@app.exception_handler(RequestIdConflictError)
async def request_id_conflict_handler(request, exc: RequestIdConflictError) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={
            "detail": str(exc),
            "existing_reservation_id": exc.existing_reservation_id,
        },
    )


@app.exception_handler(ReservationNotFoundError)
async def reservation_not_found_handler(request, exc: ReservationNotFoundError) -> JSONResponse:
    return JSONResponse(status_code=404, content={"detail": str(exc)})


@app.exception_handler(HoldNotFoundError)
async def hold_not_found_handler(request, exc: HoldNotFoundError) -> JSONResponse:
    return JSONResponse(status_code=404, content={"detail": str(exc)})


@app.exception_handler(HoldExpiredError)
async def hold_expired_handler(request, exc: HoldExpiredError) -> JSONResponse:
    return JSONResponse(status_code=410, content={"detail": str(exc)})


@app.exception_handler(HoldKeyConflictError)
async def hold_key_conflict_handler(request, exc: HoldKeyConflictError) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={"detail": str(exc), "existing_hold_id": exc.hold_id},
    )


@app.exception_handler(WaitlistNotFoundError)
async def waitlist_not_found_handler(request, exc: WaitlistNotFoundError) -> JSONResponse:
    return JSONResponse(status_code=404, content={"detail": str(exc)})


@app.exception_handler(WaitlistContentConflictError)
async def waitlist_content_conflict_handler(
    request, exc: WaitlistContentConflictError
) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={"detail": str(exc), "existing_waitlist_id": exc.waitlist_id},
    )


@app.exception_handler(VersionConflictError)
async def version_conflict_handler(request, exc: VersionConflictError) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={
            "detail": str(exc),
            "current_version": exc.current_version,
        },
    )


@app.exception_handler(ReservationCanceledError)
async def reservation_canceled_handler(request, exc: ReservationCanceledError) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={
            "detail": str(exc),
            "current_version": exc.current_version,
        },
    )


@app.exception_handler(ChainVerificationError)
async def chain_verification_error_handler(
    request, exc: ChainVerificationError
) -> JSONResponse:
    # 验链失败：不返回任何历史内容，仅告知链已损坏的位置/原因
    return JSONResponse(
        status_code=409,
        content={"detail": f"reservation history chain verification failed: {exc}"},
    )


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


def _to_core_participant(p: ParticipantIn) -> Participant:
    return Participant(
        participant_id=p.id,
        timezone=p.timezone,
        windows=tuple(
            CoreWindow(weekday=w.weekday,
                       start_local=w.start_local,
                       end_local=w.end_local)
            for w in p.availability
        ),
    )


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _segment_to_out(seg: scheduling.Segment,
                    tz_by_id: dict[str, ZoneInfo]) -> SegmentOut:
    local_times: list[LocalTimeOut] = []
    for pid in seg.attendees:
        tzinfo = tz_by_id[pid]
        start_local = seg.start_utc.astimezone(tzinfo)
        end_local = seg.end_utc.astimezone(tzinfo)
        local_times.append(LocalTimeOut(
            participant_id=pid,
            timezone=str(tzinfo),
            start_local=start_local.isoformat(),
            end_local=end_local.isoformat(),
            start_weekday=WEEKDAY_NAMES[start_local.isoweekday()],
            end_weekday=WEEKDAY_NAMES[end_local.isoweekday()],
        ))
    return SegmentOut(
        start_utc=_iso_utc(seg.start_utc),
        end_utc=_iso_utc(seg.end_utc),
        duration_minutes=int(seg.duration.total_seconds() // 60),
        attendees=list(seg.attendees),
        attendee_count=len(seg.attendees),
        local_times=local_times,
    )


@app.post("/api/v1/availability/aggregate", response_model=AggregationResponse)
async def aggregate_availability(req: AggregationRequest) -> AggregationResponse:
    participants = [_to_core_participant(p) for p in req.participants]
    # 提前解析所有时区，未知名称统一返回 400
    tz_by_id = {p.participant_id: scheduling.get_timezone(p.timezone)
                for p in participants}

    segments = scheduling.aggregate(
        participants=participants,
        start_date=req.start_date,
        end_date=req.end_date,
        meeting_minutes=req.meeting_minutes,
        min_attendees=req.min_attendees,
    )

    return AggregationResponse(
        start_date=req.start_date,
        end_date=req.end_date,
        meeting_minutes=req.meeting_minutes,
        min_attendees=req.min_attendees,
        segments=[_segment_to_out(seg, tz_by_id) for seg in segments],
    )


# ---------------------------------------------------------------- reservations


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _reservation_to_out(r: reservation_core.Reservation) -> ReservationOut:
    return ReservationOut(
        reservation_id=r.id,
        request_id=r.request_id,
        start_utc=_iso_z(r.start_utc),
        end_utc=_iso_z(r.end_utc),
        participants=list(r.participants),
        version=r.version,
        status=r.status,
        canceled_at=_iso_z(r.canceled_at) if r.canceled_at is not None else None,
        created_at=_iso_z(r.created_at),
    )


def _snapshot_to_out(snapshot: dict) -> ReservationOut:
    """审计事件中的快照（定宽 UTC 文本）转 API 输出（ISO 8601 带 Z）。"""
    def _z(ts: str) -> str:
        return _iso_z(datetime.strptime(ts, _AUDIT_CANON_FMT)
                      .replace(tzinfo=timezone.utc))

    return ReservationOut(
        reservation_id=snapshot["reservation_id"],
        request_id=snapshot["request_id"],
        start_utc=_z(snapshot["start_utc"]),
        end_utc=_z(snapshot["end_utc"]),
        participants=list(snapshot["participants"]),
        version=snapshot["version"],
        status=snapshot["status"],
        canceled_at=_z(snapshot["canceled_at"])
            if snapshot["canceled_at"] is not None else None,
        created_at=_z(snapshot["created_at"]),
    )


@app.post("/api/v1/reservations", response_model=ReservationOut, status_code=201)
def create_reservation(req: ReservationIn, response: Response) -> ReservationOut:
    """创建会议预约（同步函数，在线程池中执行以避免阻塞事件循环）。

    - 任一参与者与已有预约半开区间重叠 → 409 + 冲突预约/参与者，整体不写库；
    - 相同 request_id + 相同规范化内容 → 200 返回原预约，不新增记录；
    - 相同 request_id + 不同内容 → 409。
    """
    reservation, created = reservation_core.create_reservation(
        request_id=req.request_id,
        start_utc=req.start_utc,
        end_utc=req.end_utc,
        participant_ids=req.participant_ids,
    )
    if not created:
        # 幂等重放：返回原预约
        response.status_code = 200
    return _reservation_to_out(reservation)


@app.put("/api/v1/reservations/{reservation_id}", response_model=ReservationOut)
def reschedule_reservation(
    reservation_id: int, req: ReservationRescheduleIn
) -> ReservationOut:
    """改期（乐观并发控制）。

    - 预约不存在 → 404；
    - 预约已取消 → 409（取消是终态，改期不会使其复活）；
    - ``expected_version`` 与当前版本不一致 → 409，响应给出 ``current_version``；
    - 任一新参与者与**其他**预约半开区间重叠（自动排除自身）→ 409 + 冲突明细；
    - 成功 → 原子替换时间与参与者占用，``version`` 加 1，返回更新后的预约。

    所有检查与写入在单个 ``BEGIN IMMEDIATE`` 事务内完成；任一步失败，
    预约及其参与者占用均保持原状。
    """
    reservation = reservation_core.reschedule_reservation(
        reservation_id=reservation_id,
        expected_version=req.expected_version,
        start_utc=req.start_utc,
        end_utc=req.end_utc,
        participant_ids=req.participant_ids,
    )
    return _reservation_to_out(reservation)


@app.delete("/api/v1/reservations/{reservation_id}", response_model=ReservationOut)
def cancel_reservation(
    reservation_id: int,
    expected_version: int = Query(
        ge=1,
        description="客户端上次读到的预约版本；与当前版本一致才执行取消",
    ),
) -> ReservationOut:
    """取消预约（软删除，乐观并发控制）。

    - 预约不存在 → 404；
    - ``expected_version`` 与当前版本不一致 → 409，响应给出 ``current_version``；
    - 成功 → 单个 ``BEGIN IMMEDIATE`` 事务内把预约置为 ``canceled``、记录
      ``canceled_at``、``version`` 加 1，并删除全部参与者占用，
      时段提交后立即可再次预约；
    - 相同 ``reservation_id`` + 原 ``expected_version`` 的重试 → 幂等返回
      同一取消结果（同样的 ``canceled_at`` 与版本），不重复变更；
    - 取消是终态：之后 PUT 改期一律 409，不会复活。
    """
    reservation, _ = reservation_core.cancel_reservation(
        reservation_id=reservation_id,
        expected_version=expected_version,
    )
    return _reservation_to_out(reservation)


@app.get(
    "/api/v1/reservations/{reservation_id}/history",
    response_model=ReservationHistoryOut,
)
def get_reservation_history(reservation_id: int) -> ReservationHistoryOut:
    """预约变更历史：按 ``seq`` 升序返回哈希链审计事件，返回前重验整链。

    - 预约不存在 → ``404``；
    - 哈希链任一环重验失败（序号断档、``prev_hash`` 不衔接、
      ``canonical_event`` 与哈希不符等）→ ``409``，**不返回任何历史**；
    - 校验通过 → ``200``，事件包含动作（``created`` / ``rescheduled`` /
      ``canceled`` / ``baseline``）、序号、时间、预约快照、前后哈希与
      canonical_event。``baseline`` 为升级初始化时为既有预约补写的快照，
      不是伪造的历史操作。

    仅成功的创建/改期/取消会产生事件；失败请求与幂等重放不产生事件。
    """
    events = reservation_core.get_reservation_history(reservation_id)
    return ReservationHistoryOut(
        reservation_id=reservation_id,
        events=[
            HistoryEventOut(
                seq=e["seq"],
                action=e["action"],
                timestamp=_iso_z(datetime.strptime(e["timestamp"], _AUDIT_CANON_FMT)
                                 .replace(tzinfo=timezone.utc)),
                snapshot=_snapshot_to_out(e["snapshot"]),
                prev_hash=e["prev_hash"],
                hash=e["hash"],
                canonical_event=e["canonical_event"],
            )
            for e in events
        ],
    )


# --------------------------------------------------------- reservation holds


def _hold_to_out(h: hold_core.Hold) -> ReservationHoldOut:
    return ReservationHoldOut(
        hold_id=h.id,
        hold_key=h.hold_key,
        start_utc=_iso_z(h.start_utc),
        end_utc=_iso_z(h.end_utc),
        participants=list(h.participants),
        ttl_seconds=h.ttl_seconds,
        status=h.status,
        reservation_id=h.reservation_id,
        created_at=_iso_z(h.created_at),
        expires_at=_iso_z(h.expires_at),
        confirmed_at=_iso_z(h.confirmed_at) if h.confirmed_at is not None else None,
    )


@app.post(
    "/api/v1/reservation-holds",
    response_model=ReservationHoldOut,
    status_code=201,
)
def create_reservation_hold(req: ReservationHoldIn, response: Response) -> ReservationHoldOut:
    """创建预约临时保留（短时占位）。

    整个“惰性过期 + 冲突检查 + 写入”在单个 ``BEGIN IMMEDIATE`` 事务内完成：

    - 事务入口先把所有已到期 pending 保留标记为 ``expired``（忽略已过期保留）；
    - 与**进行中预约**及**其他未过期 pending 保留**按半开区间
      ``[start, end)`` 检查参与者重叠（首尾相接不冲突）；冲突 → ``409``
      且整体回滚、**不写入**，``conflicts`` 给出冲突预约（``reservation_id``）
      或保留（``hold_id``）与对应参与者；
    - 相同 ``hold_key`` + 相同规范化内容（起止、去重排序参与者、TTL）重试
      → ``200`` 返回原 pending 保留；同键内容不同 → ``409``；
    - 成功 → ``201``，保留状态为 ``pending``，``expires_at = now + ttl_seconds``。
    """
    hold, created = hold_core.create_hold(
        hold_key=req.hold_key,
        start_utc=req.start_utc,
        end_utc=req.end_utc,
        participant_ids=req.participant_ids,
        ttl_seconds=req.ttl_seconds,
    )
    if not created:
        response.status_code = 200
    return _hold_to_out(hold)


@app.post(
    "/api/v1/reservation-holds/{hold_id}/confirm",
    response_model=ReservationOut,
    status_code=201,
)
def confirm_reservation_hold(
    hold_id: int, req: ReservationHoldConfirmIn, response: Response
) -> ReservationOut:
    """确认临时保留：未过期 pending → 同事务转正式预约。

    - 保留不存在 → ``404``；
    - 保留已过期（含惰性过期翻转）→ ``410``；
    - ``request_id`` 已被其他正式预约占用 → ``409``（TTL 内换 request_id 可重试）；
    - 成功 → ``201``：同一事务写入正式预约与 ``created`` 审计事件，
      保留标记为 ``confirmed`` 并回填 ``reservation_id``，其临时占用交由
      正式预约接管；
    - 重复确认（同一保留再次调用，``request_id`` 可不同）→ ``200``
      返回**同一预约**，不重复写库、不新增审计事件。
    """
    reservation, confirmed = hold_core.confirm_hold(
        hold_id=hold_id,
        request_id=req.request_id,
    )
    if not confirmed:
        response.status_code = 200
    return _reservation_to_out(reservation)


# ------------------------------------------------------ reservation waitlist


def _waitlist_to_out(
    entry: waitlist_core.WaitlistEntry,
    reservation: reservation_core.Reservation | None,
) -> ReservationWaitlistOut:
    return ReservationWaitlistOut(
        waitlist_id=entry.id,
        request_id=entry.request_id,
        start_utc=_iso_z(entry.start_utc),
        end_utc=_iso_z(entry.end_utc),
        participants=list(entry.participants),
        created_at=_iso_z(entry.created_at),
        status=entry.status,
        reservation=_reservation_to_out(reservation)
        if reservation is not None else None,
        promoted_at=_iso_z(entry.promoted_at)
        if entry.promoted_at is not None else None,
    )


@app.post(
    "/api/v1/reservation-waitlist",
    response_model=ReservationWaitlistOut,
    status_code=201,
)
def join_reservation_waitlist(
    req: ReservationWaitlistIn, response: Response
) -> ReservationWaitlistOut:
    """加入预约候补队列：当前可预约则直接创建正式预约，否则按创建时间排队。

    整个“惰性过期 + 判定 + 写入”在单个 ``BEGIN IMMEDIATE`` 事务内完成：

    - 请求时段与**进行中预约**及**未过期 pending 保留**均无半开区间重叠
      → ``201`` 直接创建正式预约（同事务写入并追加 ``created`` 哈希链审计
      事件），候补行状态为 ``promoted`` 并关联该预约；
    - 否则不报错：``201`` 写入 ``pending`` 候补行（``created_at`` 即
      FIFO 排序键）。pending 候补**不占位**，不会阻止任何预约/保留/改期；
    - 相同 ``request_id`` + 相同规范化内容（起止、去重排序参与者）重试
      → ``200`` 返回候补当前状态（仍 pending，或已 promoted 及关联预约，
      含该预约后续改期/取消后的最新状态）；同 ``request_id`` 内容不同
      → ``409``；该 ``request_id`` 已被其他正式预约/候补占用且内容不同
      同样 ``409``。

    候补在被取消预约释放出时段后，由取消事务按 FIFO 自动晋升
    （见 ``DELETE /api/v1/reservations/{id}``）。
    """
    result = waitlist_core.join_waitlist(
        request_id=req.request_id,
        start_utc=req.start_utc,
        end_utc=req.end_utc,
        participant_ids=req.participant_ids,
    )
    if not result.inserted:
        # 幂等重放 / 跨端点同内容：返回已有候补/预约
        response.status_code = 200
    return _waitlist_to_out(result.entry, result.reservation)


@app.get(
    "/api/v1/reservation-waitlist/{request_id}",
    response_model=ReservationWaitlistOut,
)
def get_reservation_waitlist(request_id: str) -> ReservationWaitlistOut:
    """查询候补状态：``pending`` 候补中或 ``promoted`` 已转正式预约。

    - ``request_id`` 从未通过候补接口登记 → ``404``；
    - ``pending`` → ``200``，``reservation`` 为 ``null``；
    - ``promoted`` → ``200``，``reservation`` 为关联正式预约的**当前最新
      状态**（晋升后可正常改期/取消），``promoted_at`` 为晋升时刻。
    """
    entry, reservation = waitlist_core.get_waitlist_entry(request_id)
    return _waitlist_to_out(entry, reservation)
