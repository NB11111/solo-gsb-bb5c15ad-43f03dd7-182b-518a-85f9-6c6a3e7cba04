# 跨时区可用时段聚合 API

基于 **Python + FastAPI + 标准库 `zoneinfo`** 的会议候选时段聚合服务，
并附带 **SQLite 持久化的会议预约接口**（幂等创建、乐观锁版本改期、
软删除取消、**两条预约时段原子交换**、单事务冲突检测、
**可校验的 SHA-256 哈希链变更历史**、
**预约临时保留 / 确认**、**预约候补队列**）。

每位参与者按**本地星期与钟面时刻**填写每周重复的可用区间并给出 IANA 时区
（如 `Asia/Shanghai`），服务按具体日期应用夏令时（DST）规则，统一换算到 UTC，
用**扫描线**算法计算所有满足最低出席人数与会议时长的候选时段。

- 交互式文档（Swagger UI）：<http://localhost:8000/docs>
- ReDoc：<http://localhost:8000/redoc>
- 健康检查：`GET /health`

服务默认监听 **8000 端口**（Docker Compose 一键启动：`docker compose up --build`，
随后访问 <http://localhost:8000>；端口被占用时可用 `HOST_PORT` 改宿主机映射）。

## 语义说明

- **搜索区间**：`[start_date 00:00 UTC, end_date + 1 天 00:00 UTC)`，起止日期均含（以 UTC 日期为界）。
  这意味着 UTC+14 时区里落在本地周一凌晨的窗口，其 UTC 时刻可能处于周日、从而被裁剪在区间之外。
- **跨午夜窗口**：`end_local <= start_local` 时视为跨午夜，自动拆成“当日起点 → 次日终点”的半开区间后换算 UTC。
- **DST 处理**（按每个具体日期分别计算）：
  - **不存在的本地时刻**（春季跳变被跳过的钟面时间，如美东 2026-03-08 02:30）→ 返回 `400` 错误；
  - **歧义时刻**（秋季回退出现两次的钟面时间）→ 统一选择 `fold=0`，即 UTC 偏移量较大、较早出现的那一次。
- **候选段**：出席者集合保持恒定的最大区间；时长必须 `>= meeting_minutes`。
- **排序**：出席人数**降序**，人数相同时按 UTC 开始时间**升序**（稳定排序）。
- 同一参与者互相重叠的窗口会先按人合并，扫描线中一人在任一时刻至多计一份出席。

## 目录结构

```
app/
  scheduling.py    # 聚合核心：本地时刻换算、跨午夜拆分、扫描线、排序
  reservations.py  # 预约核心：规范化、幂等创建、乐观锁版本改期/取消、时段原子交换、单事务冲突检测、历史读取、取消后 FIFO 候补晋升
  holds.py         # 临时保留核心：TTL 占位、事务内惰性过期、确认转正式预约
  waitlist.py      # 候补核心：可预约直订/否则入队、request_id 幂等、取消事务内 FIFO 扫描晋升
  audit.py         # 审计事件：canonical JSON、SHA-256 哈希链追加与整链重验
  db.py            # SQLite 连接管理、建表与无损迁移（补列 / 补 baseline 事件）
  schemas.py       # Pydantic 请求/响应模型
  main.py          # FastAPI 路由
tests/             # pytest 测试（DST、跨午夜、排序、预约冲突/幂等/并发、时段交换、临时保留、候补 FIFO 晋升、哈希链/篡改/迁移）
Dockerfile
docker-compose.yml
requirements.txt  # 运行依赖
requirements-dev.txt  # 测试依赖（含 pytest、httpx）
```

## 配置

| 配置项 | 位置 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `APP_PORT` | 容器环境变量 | `8000` | 容器内 uvicorn 监听端口 |
| `HOST_PORT` | 宿主机环境变量 / `.env` | `8000` | 映射到宿主机的端口 |
| `RESERVATIONS_DB_PATH` | 容器环境变量 / `.env` | `/data/reservations.db`（Compose）<br>`./data/reservations.db`（本地） | SQLite 数据库文件路径 |
| `TZ` | 容器环境变量 | `UTC` | 容器系统时区（不影响按 IANA 时区进行的换算） |

构建时镜像内会安装 `tzdata`（IANA 时区数据库），Python 的 `tzdata` pip 包也作为兜底依赖安装。

## 用 Docker Compose 一条命令启动

```bash
docker compose up --build
```

启动时应用会**自动初始化 SQLite 数据库**（建表幂等）；数据库文件落在命名卷
`reservations-data`（容器内 `/data`）上，容器重建后预约数据不丢失。
升级到带 `version` / 取消能力的版本后首次启动时，会为**既有数据无损补齐**
`version=1`、`status='active'`、`canceled_at=NULL`
（`ALTER TABLE ADD COLUMN ... DEFAULT ...`），无需手工迁移。
升级到带**变更历史**的版本后首次启动时，还会为每条尚无审计事件的既有预约
在同一迁移事务内补写一条 `baseline` 事件（序号 1、首哈希为 64 个 0，
快照如实记录初始化时观测到的预约状态，事件时间取迁移时刻——**不伪造**
过去的创建/改期/取消操作）；迁移可重复执行，已有事件不会重复补写。
升级到带**临时保留**的版本后，`reservation_holds` 与
`reservation_hold_participants` 两张新表以 `CREATE TABLE IF NOT EXISTS`
无损建立：不改动任何既有预约/事件数据，建表幂等可重复执行。
升级到带**候补队列**的版本后，`reservation_waitlist` 与
`reservation_waitlist_participants` 两张新表同样以
`CREATE TABLE IF NOT EXISTS` 幂等无损建立，不影响任何既有数据。

服务启动后访问 <http://localhost:8000>（Swagger UI 在 <http://localhost:8000/docs>）。
若 8000 端口被占用：

```bash
HOST_PORT=9000 docker compose up --build   # 访问 http://localhost:9000
```

后台运行与停止：

```bash
docker compose up -d --build
docker compose logs -f
docker compose down          # 加 -v 会连同数据库卷一起删除
```

## 本地开发（不使用 Docker）

需要 Python 3.11+。

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt

# 启动开发服务器（端口 8000）
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# 运行测试
pytest -q
```

## API

### `POST /api/v1/availability/aggregate`

请求体：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `start_date` / `end_date` | `YYYY-MM-DD` | 搜索日期范围，均含（UTC 日期为界） |
| `meeting_minutes` | 整数 > 0 | 会议时长（分钟） |
| `min_attendees` | 整数 > 0 | 最低出席人数（不能超过参与者总数） |
| `participants[].id` | 字符串 | 参与者唯一标识 |
| `participants[].timezone` | 字符串 | IANA 时区名，如 `Europe/Berlin` |
| `participants[].availability[].weekday` | `1..7` 或名称 | `1`=周一 … `7`=周日；也接受 `Monday`/`mon` |
| `...start_local` / `end_local` | `HH:MM[:SS]` | 本地钟面时刻；`end <= start` 表示跨午夜 |

请求示例（东京周一 22:00 跨午夜到次日 02:00，纽约周一 09:00–12:00）：

```bash
curl -s -X POST http://localhost:8000/api/v1/availability/aggregate \
  -H 'Content-Type: application/json' \
  -d '{
    "start_date": "2026-03-09",
    "end_date": "2026-03-09",
    "meeting_minutes": 60,
    "min_attendees": 2,
    "participants": [
      {"id": "tokyo", "timezone": "Asia/Tokyo",
       "availability": [{"weekday": 1, "start_local": "22:00", "end_local": "02:00"}]},
      {"id": "ny", "timezone": "America/New_York",
       "availability": [{"weekday": 1, "start_local": "09:00", "end_local": "12:00"}]}
    ]
  }'
```

响应（共同可用段为 UTC 13:00–16:00；东京本地已跨到周二凌晨）：

```json
{
  "start_date": "2026-03-09",
  "end_date": "2026-03-09",
  "meeting_minutes": 60,
  "min_attendees": 2,
  "segments": [
    {
      "start_utc": "2026-03-09T13:00:00Z",
      "end_utc": "2026-03-09T16:00:00Z",
      "duration_minutes": 180,
      "attendees": ["ny", "tokyo"],
      "attendee_count": 2,
      "local_times": [
        {
          "participant_id": "ny",
          "timezone": "America/New_York",
          "start_local": "2026-03-09T09:00:00-04:00",
          "end_local": "2026-03-09T12:00:00-04:00",
          "start_weekday": "Monday",
          "end_weekday": "Monday"
        },
        {
          "participant_id": "tokyo",
          "timezone": "Asia/Tokyo",
          "start_local": "2026-03-09T22:00:00+09:00",
          "end_local": "2026-03-10T01:00:00+09:00",
          "start_weekday": "Monday",
          "end_weekday": "Tuesday"
        }
      ]
    }
  ]
}
```

错误：

- `400`：本地时刻因 DST 春跳变不存在，或 IANA 时区名未知；
- `422`：请求结构校验失败（日期倒置、人数非法、id 重复、weekday 越界等）。

### `POST /api/v1/reservations`

创建会议预约（SQLite 持久化）。请求体：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `request_id` | 非空字符串 | 客户端幂等请求标识 |
| `start_utc` / `end_utc` | ISO 8601（须带时区偏移，如 `...Z`） | UTC 起止时刻；`end_utc` 必须晚于 `start_utc` |
| `participant_ids` | 非空字符串数组 | 参与者 ID；**去重并排序后**存储并参与幂等比较 |

语义：

- **原子占用**：检查与写入在单个 `BEGIN IMMEDIATE` 事务内完成，并发请求被串行化；
  只要任一参与者与已有预约存在**半开区间重叠**（`existing.start < new.end` 且
  `new.start < existing.end`），整个请求被拒绝，不会留下部分写入。
  **相邻区间不冲突**（`end == start` 首尾相接可以共存）。
- **版本号**：每条预约带单调递增的 `version`，创建时为 `1`，每次成功改期或取消加 `1`。
- **状态**：`status` 为 `active`（进行中）或 `canceled`（已取消，软删除终态）；
  未取消时 `canceled_at` 为 `null`。
- **幂等**：相同 `request_id` + 相同规范化内容（起止时刻、去重排序后的参与者）
  重试 → `200` 返回原预约，不新增记录；同一 `request_id` 内容不同 → `409`。
- **审计**：仅成功创建时在**同一事务**追加 `created` 哈希链事件（见
  [变更历史](#get-apiv1reservationsreservation_idhistory)）；
  冲突/幂等失败与重放不产生事件。

请求示例：

```bash
curl -s -X POST http://localhost:8000/api/v1/reservations \
  -H 'Content-Type: application/json' \
  -d '{
    "request_id": "req-9f3a",
    "start_utc": "2026-03-09T13:00:00Z",
    "end_utc": "2026-03-09T14:00:00Z",
    "participant_ids": ["alice", "bob"]
  }'
```

成功响应（`201`，幂等重放为 `200`）：

```json
{
  "reservation_id": 1,
  "request_id": "req-9f3a",
  "start_utc": "2026-03-09T13:00:00Z",
  "end_utc": "2026-03-09T14:00:00Z",
  "participants": ["alice", "bob"],
  "version": 1,
  "status": "active",
  "canceled_at": null,
  "created_at": "2026-09-17T20:42:39.634009Z"
}
```

冲突响应（`409`，含冲突预约 ID 与参与者）：

```json
{
  "detail": "reservation conflict: participant 'bob' overlaps reservation #1",
  "conflicts": [{"reservation_id": 1, "participant_id": "bob"}]
}
```

`request_id` 被不同内容占用时（`409`）：

```json
{
  "detail": "request_id is already used by reservation #1 with different content",
  "existing_reservation_id": 1
}
```

其他错误：`422`（`end_utc` 不晚于 `start_utc`、时间缺时区偏移、参与者列表为空等）。

### `PUT /api/v1/reservations/{reservation_id}`

改期：原子替换预约的起止时间与参与者占用，并推进乐观锁版本。请求体：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `expected_version` | 正整数 | 客户端上次读到的版本号；与当前版本一致才执行 |
| `start_utc` / `end_utc` | ISO 8601（须带时区偏移） | 新的 UTC 起止时刻；沿用相同校验（`end > start`、等价偏移归一） |
| `participant_ids` | 非空字符串数组 | 新的参与者列表；**同样去重并排序**后存储 |

语义：

- 整个流程（存在性 → 状态 → 版本匹配 → 冲突检查 → 替换）在单个 `BEGIN IMMEDIATE`
  事务内完成；**任一步失败都整体回滚**，预约与参与者占用保持原状。
- 预约不存在 → `404`。
- 预约已取消 → `409`：取消是终态，改期**不会**使其复活。
- `expected_version` 与当前版本不一致（已被别人改过）→ `409`，响应给出
  `current_version`；客户端可用它重新读取后重试。
- 冲突检查沿用半开区间规则，并**排除自身**（`reservation_id != 当前预约`）：
  自己改期前的旧占用不算冲突，相邻区间也不冲突；与其他预约重叠 → `409`
  及冲突明细。
- 成功时 `request_id` 与 `created_at` 不变，`version` 加 `1`，返回更新后的预约
  （结构同创建响应）。
- **审计**：成功改期在同一事务追加 `rescheduled` 事件（快照为改期后的新状态）；
  任一步失败或并发落败都不产生事件。

请求示例（把预约 #1 改到 15:00–16:00，并把参与者调整为 alice / carol）：

```bash
curl -s -X PUT http://localhost:8000/api/v1/reservations/1 \
  -H 'Content-Type: application/json' \
  -d '{
    "expected_version": 1,
    "start_utc": "2026-03-09T15:00:00Z",
    "end_utc": "2026-03-09T16:00:00Z",
    "participant_ids": ["carol", "alice"]
  }'
```

成功响应（`200`，参与者已按字典序规范化，版本推进到 2）：

```json
{
  "reservation_id": 1,
  "request_id": "req-9f3a",
  "start_utc": "2026-03-09T15:00:00Z",
  "end_utc": "2026-03-09T16:00:00Z",
  "participants": ["alice", "carol"],
  "version": 2,
  "status": "active",
  "canceled_at": null,
  "created_at": "2026-09-17T20:42:39.634009Z"
}
```

版本过期（`409`，可用 `current_version` 重试）：

```json
{
  "detail": "expected_version does not match current version of reservation #1",
  "current_version": 2
}
```

预约不存在（`404`）：

```json
{ "detail": "reservation #999 not found" }
```

参与者冲突时返回的 `409` 结构与创建接口一致（`detail` + `conflicts` 明细）。
其他错误：`422`（`expected_version < 1`、时间/参与者非法等）。

### `DELETE /api/v1/reservations/{reservation_id}`

取消预约（**软删除**，乐观并发控制）。查询参数：

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| `expected_version` | 正整数，**必填** | 客户端上次读到的版本号；与当前版本一致才执行取消 |

语义：

- 整个流程（存在性 → 状态 → 版本匹配 → 取消 → 释放占用）在单个
  `BEGIN IMMEDIATE` 事务内完成；任一步失败都整体回滚。
- 预约不存在 → `404`。
- `expected_version` 与当前版本不一致 → `409`，响应给出 `current_version`。
- 成功 → `200`：`status` 置为 `canceled`、记录 `canceled_at`、`version` 加 `1`，
  并**同事务删除全部参与者占用**——时段提交后立即可被再次预约；
  预约行本身保留（软删除），`request_id`、`created_at` 等不变。
- **候补晋升**：删除占用、追加 `canceled` 事件后，在**同一 `BEGIN IMMEDIATE`
  事务**内按 FIFO（`created_at, id`）扫描与被取消预约“参与者相交且时段重叠”的
  pending 候补，**跳过仍与其他进行中预约或未过期保留冲突的项**，把首个可满足
  候补原子转为正式预约（同事务写入并追加 `created` 哈希链事件）；
  一次取消至多晋升一条候补，无候补可满足则不晋升（详见
  [预约候补队列](#post-apiv1reservation-waitlist)）。
- **幂等重试**：相同 `reservation_id` + 原 `expected_version` 重试 → `200`
  返回**同一取消结果**（同样的 `canceled_at` 与版本），不重复变更；
  取消后使用其他版本号 → `409` + `current_version`。
- 取消是**终态**：之后 `PUT` 改期一律 `409`，预约不会复活；
  创建请求的迟到重试（同 `request_id` + 同内容）幂等返回该已取消记录。
- **审计**：成功取消在同一事务追加 `canceled` 事件（快照记录取消终态）；
  版本不符等失败与幂等重试不产生事件。

请求示例：

```bash
curl -s -X DELETE 'http://localhost:8000/api/v1/reservations/1?expected_version=2'
```

成功/幂等重放响应（`200`）：

```json
{
  "reservation_id": 1,
  "request_id": "req-9f3a",
  "start_utc": "2026-03-09T15:00:00Z",
  "end_utc": "2026-03-09T16:00:00Z",
  "participants": ["alice", "carol"],
  "version": 3,
  "status": "canceled",
  "canceled_at": "2026-09-17T21:05:12.345678Z",
  "created_at": "2026-09-17T20:42:39.634009Z"
}
```

版本不匹配（`409`）：

```json
{
  "detail": "expected_version does not match current version of reservation #1",
  "current_version": 3
}
```

其他错误：`404`（预约不存在）、`422`（缺少 `expected_version` 或其值 `< 1`）。

### `POST /api/v1/reservations/swap`

**时段原子交换**：两条不同的 `active` 预约在**单个事务**内互换
`start_utc` / `end_utc`；参与者、`request_id`、`created_at` 均保持不变。
请求体：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `first.reservation_id` | 正整数 | 第一条预约 ID |
| `first.expected_version` | 正整数 | 客户端上次读到的第一条预约版本；与当前版本一致才执行 |
| `second.reservation_id` | 正整数 | 第二条预约 ID（**必须与第一条不同**） |
| `second.expected_version` | 正整数 | 客户端上次读到的第二条预约版本；与当前版本一致才执行 |

语义：

- 整个流程（双方存在性 → 均未取消 → 双版本匹配 → 冲突检查 → 互换）
  在单个 `BEGIN IMMEDIATE` 事务内完成；**任一步失败都整体回滚**，
  两条预约及其参与者占用、审计链均保持原状。
- 任一预约不存在 → `404`。
- 任一预约已取消 → `409`：取消是终态，交换**不会**使其复活。
- 任一 `expected_version` 与对应预约当前版本不一致 → `409`，
  响应给出该预约的 `current_version`；客户端可重新读取后重试。
- **冲突检查**：交换后第一条预约的参与者落在第二条的原时段、反之亦然。
  对**双方各自的新时段**按半开区间规则检查与**其他进行中预约**
  （`reservation_id NOT IN (first, second)`，**排除交换双方**自身旧占用）
  及**未过期 pending 保留**的参与者重叠；任一冲突 → `409` + `conflicts`
  明细，整体回滚。交换双方互为“新时段上的另一方”不算冲突
  （双方交换前均为 active，共享参与者的占用集合交换后不变）。
- 成功 → `200`：两条预约的 `start_utc` / `end_utc` 互换，`version`
  **各加 1**，返回双方交换后的最新状态。
- **审计**：成功交换在同一事务为**双方各追加一条 `rescheduled` 哈希链事件**
  （快照为各自交换后的新状态）；任一步失败不产生事件。

请求示例（把预约 #1 与 #2 的时段互换）：

```bash
curl -s -X POST http://localhost:8000/api/v1/reservations/swap \
  -H 'Content-Type: application/json' \
  -d '{
    "first":  {"reservation_id": 1, "expected_version": 1},
    "second": {"reservation_id": 2, "expected_version": 3}
  }'
```

成功响应（`200`，#1 拿到 #2 的原时段 16:00–17:00，#2 拿到 #1 的原时段
13:00–14:00，双方版本各 +1）：

```json
{
  "first": {
    "reservation_id": 1,
    "request_id": "req-9f3a",
    "start_utc": "2026-03-09T16:00:00Z",
    "end_utc": "2026-03-09T17:00:00Z",
    "participants": ["alice", "bob"],
    "version": 2,
    "status": "active",
    "canceled_at": null,
    "created_at": "2026-09-17T20:42:39.634009Z"
  },
  "second": {
    "reservation_id": 2,
    "request_id": "req-77bc",
    "start_utc": "2026-03-09T13:00:00Z",
    "end_utc": "2026-03-09T14:00:00Z",
    "participants": ["carol"],
    "version": 4,
    "status": "active",
    "canceled_at": null,
    "created_at": "2026-09-18T08:11:02.111111Z"
  }
}
```

错误示例——任一预约不存在（`404`）：

```json
{ "detail": "reservation #2 not found" }
```

任一预约已取消（`409`）：

```json
{ "detail": "reservation #1 has been canceled", "current_version": 2 }
```

任一版本过期（`409`，`current_version` 属于版本不符的那一条预约）：

```json
{
  "detail": "expected_version does not match current version of reservation #2",
  "current_version": 3
}
```

交换后的新时段与第三方预约或未过期保留冲突（`409`，整体回滚）：

```json
{
  "detail": "reservation conflict: participant 'alice' overlaps reservation #3",
  "conflicts": [{"reservation_id": 3, "participant_id": "alice"}]
}
```

其他错误：`422`（两条预约 ID 相同、`reservation_id` / `expected_version < 1`、
缺少字段等）。

### `POST /api/v1/reservation-holds`

创建**预约临时保留**（正式预约前的短时占位）。请求体：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `hold_key` | 非空字符串（≤200） | 客户端幂等键 |
| `start_utc` / `end_utc` | ISO 8601（须带时区偏移，如 `...Z`） | UTC 起止时刻；`end_utc` 必须晚于 `start_utc` |
| `participant_ids` | 非空字符串数组 | 参与者 ID；**去重并排序后**存储 |
| `ttl_seconds` | 整数，**1..900** | 保留有效期（秒）；到期后保留在写事务内惰性过期 |

语义：

- **原子“过期清理 + 冲突检查 + 写入”**：整个流程在单个 `BEGIN IMMEDIATE`
  事务内完成。事务入口先把所有已到期的 `pending` 保留标记为 `expired`
  （**忽略已过期保留**），随后与**进行中预约**及**其他未过期 pending 保留**
  按统一的**半开区间**规则检查参与者重叠（`existing.start < new.end` 且
  `new.start < existing.end`，**首尾相接不冲突**）。任一参与者冲突 → `409`，
  整体回滚、**不写入**；`conflicts` 中冲突项用 `reservation_id` 或 `hold_id`
  指明挡路的是正式预约还是另一条保留。
- **有效期**：创建时固化 `expires_at = created_at + ttl_seconds`。过期不需要
  后台任务：下一个写事务（创建/确认/改期）在事务入口统一惰性翻转；
  冲突查询还带 `expires_at > now` 过滤作为双重保险。
- **未过期保留同样占位**：在 TTL 内，直接 `POST /reservations` 创建或
  `PUT` 改期撞上某条未过期保留也会得到 `409`，从而保证持有者确认时不会被抢占。
- **幂等**：相同 `hold_key` + 相同规范化内容（起止、去重排序参与者、TTL）
  重试 → `200` 返回原 `pending` 保留；同键内容不同 → `409`（`existing_hold_id`）。
  `hold_key` 的唯一约束只针对 `pending` 行，保留**过期或确认后可重新申请同名键**。

请求示例：

```bash
curl -s -X POST http://localhost:8000/api/v1/reservation-holds \
  -H 'Content-Type: application/json' \
  -d '{
    "hold_key": "hold-7c21",
    "start_utc": "2026-03-09T13:00:00Z",
    "end_utc": "2026-03-09T14:00:00Z",
    "participant_ids": ["bob", "alice"],
    "ttl_seconds": 300
  }'
```

成功响应（`201`，幂等重放为 `200`）：

```json
{
  "hold_id": 1,
  "hold_key": "hold-7c21",
  "start_utc": "2026-03-09T13:00:00Z",
  "end_utc": "2026-03-09T14:00:00Z",
  "participants": ["alice", "bob"],
  "ttl_seconds": 300,
  "status": "pending",
  "reservation_id": null,
  "created_at": "2026-09-18T04:30:41.174548Z",
  "expires_at": "2026-09-18T04:35:41.174548Z",
  "confirmed_at": null
}
```

与未过期保留冲突时（`409`，挡路的是保留 #1）：

```json
{
  "detail": "reservation conflict: participant 'alice' overlaps reservation hold #1",
  "conflicts": [{"hold_id": 1, "participant_id": "alice"}]
}
```

与正式预约冲突时 `conflicts` 项为 `{"reservation_id": ..., "participant_id": ...}`，
结构与预约创建接口一致。其他错误：`422`（`ttl_seconds` 超出 1..900、
`end_utc` 不晚于 `start_utc`、时间缺时区偏移、参与者/`hold_key` 为空等）。

### `POST /api/v1/reservation-holds/{hold_id}/confirm`

确认保留：把**未过期的 `pending` 保留**在**同一事务**内转为正式预约。请求体：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `request_id` | 非空字符串（≤200） | 正式预约的客户端幂等请求标识（写入 `reservations.request_id`） |

语义：

- 保留不存在 → `404`；
- 保留**已过期**（确认事务入口惰性过期后，或状态已是 `expired`）→ `410 Gone`；
- `request_id` 已被**其他正式预约**占用 → `409`（`existing_reservation_id`），
  本次确认不落库；在 TTL 内更换 `request_id` 可重试；
- 成功 → `201`：同一 `BEGIN IMMEDIATE` 事务内写入正式预约与参与者占用、
  追加 **`created` 哈希链审计事件**，随后把保留置为 `confirmed`、回填
  `reservation_id` / `confirmed_at`，并删除保留的临时占用行（占用语义
  已由正式预约接管）——任一步失败整体回滚；
- **重复确认**：对同一条保留再次调用（`request_id` 可不同）→ `200`
  返回**同一预约**，不重复写库、不新增审计事件。

请求 / 成功响应：

```bash
curl -s -X POST http://localhost:8000/api/v1/reservation-holds/1/confirm \
  -H 'Content-Type: application/json' \
  -d '{"request_id": "req-confirm-1"}'
```

```json
{
  "reservation_id": 1,
  "request_id": "req-confirm-1",
  "start_utc": "2026-03-09T13:00:00Z",
  "end_utc": "2026-03-09T14:00:00Z",
  "participants": ["alice", "bob"],
  "version": 1,
  "status": "active",
  "canceled_at": null,
  "created_at": "2026-09-18T04:31:02.881004Z"
}
```

过期（`410`）：

```json
{ "detail": "reservation hold #1 has expired" }
```

确认产生的正式预约与直接创建的完全等价：可正常 `GET .../history`
验链（首条事件为 `created`）、`PUT` 改期、`DELETE` 取消。

### `POST /api/v1/reservation-waitlist`

加入**预约候补队列**：时段当前可预约则直接创建正式预约，否则按创建时间进入
pending 队列（不报错、不占位）。请求体与创建预约完全一致：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `request_id` | 非空字符串（≤200） | 客户端幂等请求标识；**与正式预约/候补共用同一命名空间** |
| `start_utc` / `end_utc` | ISO 8601（须带时区偏移，如 `...Z`） | UTC 起止时刻；`end_utc` 必须晚于 `start_utc` |
| `participant_ids` | 非空字符串数组 | 参与者 ID；**去重并排序后**存储 |

语义（整个“惰性过期 + 判定 + 写入”在单个 `BEGIN IMMEDIATE` 事务内完成）：

- **当前可预约**（与进行中预约及未过期 pending 保留均无半开区间重叠）
  → `201` **直接创建正式预约**：同事务写入预约、参与者占用与 **`created`
  哈希链审计事件**，候补行状态直接为 `promoted` 并关联该预约；
- **当前被占用** → `201` 写入 `pending` 候补行，`created_at` 即 FIFO 排序键。
  pending 候补**不占位**：不会阻止任何人的正式预约、临时保留或改期；
  候补之间也互不冲突（同一时段可有多条候补，按创建时间排队）；
- **幂等**：相同 `request_id` + 相同规范化内容重试 → `200` 返回候补当前状态
  （仍为 `pending`，或已 `promoted` 及关联预约的**最新状态**，含晋升后又被
  改期/取消的结果）；相同 `request_id` 但内容不同 → `409`。
  该 `request_id` 已被正式预约（`POST /reservations` 或保留确认）占用时：
  同内容 `200` 幂等返回该预约（候补行回填为 `promoted`），不同内容 `409`；
  反过来，pending 候补占用的 `request_id` 也不能被内容不同的正式预约/保留确认
  使用（同样 `409`，响应给出 `existing_waitlist_id`）。

请求示例：

```bash
curl -s -X POST http://localhost:8000/api/v1/reservation-waitlist \
  -H 'Content-Type: application/json' \
  -d '{
    "request_id": "req-wait-1",
    "start_utc": "2026-03-09T13:00:00Z",
    "end_utc": "2026-03-09T14:00:00Z",
    "participant_ids": ["alice", "bob"]
  }'
```

当前可预约时（`201`，直接成为正式预约）：

```json
{
  "waitlist_id": 1,
  "request_id": "req-wait-1",
  "start_utc": "2026-03-09T13:00:00Z",
  "end_utc": "2026-03-09T14:00:00Z",
  "participants": ["alice", "bob"],
  "created_at": "2026-09-18T09:10:00.123456Z",
  "status": "promoted",
  "reservation": {
    "reservation_id": 1,
    "request_id": "req-wait-1",
    "start_utc": "2026-03-09T13:00:00Z",
    "end_utc": "2026-03-09T14:00:00Z",
    "participants": ["alice", "bob"],
    "version": 1,
    "status": "active",
    "canceled_at": null,
    "created_at": "2026-09-18T09:10:00.123456Z"
  },
  "promoted_at": "2026-09-18T09:10:00.123456Z"
}
```

时段被占用时（`201`，进入 pending 队列，`reservation` 为 `null`）：

```json
{
  "waitlist_id": 2,
  "request_id": "req-wait-2",
  "start_utc": "2026-03-09T13:00:00Z",
  "end_utc": "2026-03-09T14:00:00Z",
  "participants": ["alice", "bob"],
  "created_at": "2026-09-18T09:11:20.654321Z",
  "status": "pending",
  "reservation": null,
  "promoted_at": null
}
```

同 `request_id` 内容不同（`409`）：

```json
{
  "detail": "request_id is already used by reservation waitlist entry #2 with different content",
  "existing_waitlist_id": 2
}
```

其他错误：`422`（`end_utc` 不晚于 `start_utc`、时间缺时区偏移、参与者/`request_id` 为空等）。

### `GET /api/v1/reservation-waitlist/{request_id}`

查询候补状态及其关联预约：

- `request_id` 从未通过候补接口登记 → `404`；
- `pending` → `200`，`reservation` 为 `null`；
- `promoted` → `200`，`reservation` 为关联正式预约的**当前最新状态**
  （晋升后仍可正常改期/取消，查询始终反映最新版本），`promoted_at` 为晋升时刻。

```bash
curl -s http://localhost:8000/api/v1/reservation-waitlist/req-wait-2
```

取消正式预约后的自动晋升规则（见
[DELETE 预约](#delete-apiv1reservationsreservation_id)）：

1. 仅扫描与被取消预约**至少一位参与者相同**且候补时段与其时段
   **半开重叠**（`existing.start < new.end` 且 `new.start < existing.end`，
   首尾相接不算受影响）的 pending 候补；
2. 按 **FIFO**（`created_at` 升序，相同则按候补 id 升序）依次检查；
3. 候选项若仍与**其他进行中预约**或**未过期 pending 保留**重叠 →
   **跳过该项**继续向后扫描；
4. **首个**不再冲突的候选项在同一 `BEGIN IMMEDIATE` 事务内原子转为正式预约
   （写入预约、参与者占用并追加现有 `created` 哈希链事件；候补行置为
   `promoted` 并回填 `reservation_id` / `promoted_at`），随后立即停止——
   一次取消至多晋升一条；无候选或全部仍冲突则不晋升。
5. 版本不符等失败取消整体回滚，候补不发生任何变化；原 `expected_version`
   的取消幂等重试不会重复晋升。
6. 候补只在**取消**时触发晋升（改期不触发）；晋升后的预约与普通创建完全等价
   （可验链、改期、再次取消——再次取消时同样会尝试晋升后续候补）。

### `GET /api/v1/reservations/{reservation_id}/history`

返回预约的**可校验变更历史**：按序（`seq` 升序）列出审计事件，服务端在返回前
**重验整条哈希链**。

- 预约不存在 → `404`；
- 哈希链任一环重验失败（序号断档、`prev_hash` 不衔接、`canonical_event`
  被篡改导致哈希不符、快照列与链上内容不一致等）→ `409`，
  **响应不包含任何历史**，仅在 `detail` 中给出失败位置；
- 校验通过 → `200`。

**什么时候产生事件**：仅在创建/改期/取消/交换**成功提交**时，于**同一数据库事务**
内追加一条事件（与业务写入同生共死，不会出现有预约无事件或反之；交换会在
两条预约各自的链上各追加一条 `rescheduled` 事件）；
失败请求（冲突、版本不符等）与**幂等重放**（相同 `request_id` 重试创建、
原 `expected_version` 重试取消）**不产生事件**。

**事件字段与哈希链规则**：

| 字段 | 说明 |
| --- | --- |
| `seq` | 该预约内**从 1 递增**的序号 |
| `action` | `created`（创建）、`rescheduled`（改期或时段交换）、`canceled`（取消）、`baseline`（初始化时为既有预约补写） |
| `timestamp` | 事件时间（UTC ISO 8601；与业务写入同一时钟读数） |
| `snapshot` | 事件发生后的预约完整快照（结构同预约响应：id、request_id、起止时刻、参与者、version、status、canceled_at、created_at） |
| `prev_hash` | 上一条事件的哈希（64 位十六进制）；**首条为 64 个 `0`** |
| `hash` | 本条事件哈希 |
| `canonical_event` | 实际参与哈希的规范化 JSON |

- `canonical_event` 为**不含哈希字段**（无 `prev_hash`/`hash`）、**键按字典序
  排序**、**非 ASCII 不转义**（中文原样保留）、**无任何多余空白**的紧凑 JSON：
  `json.dumps(event, sort_keys=True, ensure_ascii=False, separators=(",", ":"))`；
- 链公式：`hash = SHA-256(prev_hash 的 ASCII 字节 + canonical_event 的 UTF-8 字节)`；
- 每条事件的 `snapshot.version` 与当时的预约版本一致：`created` 后为 1，
  每次 `rescheduled`/`canceled` 递增；`baseline` 记录迁移时观测到的版本。

请求示例：

```bash
curl -s http://localhost:8000/api/v1/reservations/1/history
```

成功响应（`200`，创建 → 改期 → 取消后）：

```json
{
  "reservation_id": 1,
  "events": [
    {
      "seq": 1,
      "action": "created",
      "timestamp": "2026-09-17T20:42:39.634009Z",
      "snapshot": {
        "reservation_id": 1,
        "request_id": "req-9f3a",
        "start_utc": "2026-03-09T13:00:00Z",
        "end_utc": "2026-03-09T14:00:00Z",
        "participants": ["alice", "bob"],
        "version": 1,
        "status": "active",
        "canceled_at": null,
        "created_at": "2026-09-17T20:42:39.634009Z"
      },
      "prev_hash": "0000000000000000000000000000000000000000000000000000000000000000",
      "hash": "e917cbcc0f0032480091db37122254e3f71d7808ca2c79da9fde57b40be4d69b",
      "canonical_event": "{\"action\":\"created\",\"seq\":1,\"snapshot\":{...},\"timestamp\":\"...\"}"
    },
    { "seq": 2, "action": "rescheduled", "prev_hash": "e917cb…", "hash": "4f1ad5…", "...": "..." },
    { "seq": 3, "action": "canceled",    "prev_hash": "4f1ad5…", "hash": "db3ccb…", "...": "..." }
  ]
}
```

验链异常（`409`，无 `events` 字段）：

```json
{ "detail": "reservation history chain verification failed: hash mismatch at seq 1" }
```

客户端可用任意语言按同一规则独立复核（Python 示例）：

```python
import hashlib, json

prev = "0" * 64
for ev in history["events"]:
    assert ev["prev_hash"] == prev
    digest = hashlib.sha256(
        prev.encode("ascii") + ev["canonical_event"].encode("utf-8")
    ).hexdigest()
    assert digest == ev["hash"]          # canonical_event 被任何改动都会失配
    prev = ev["hash"]
```

**升级迁移语义**：升级前已存在的预约没有真实操作历史，初始化时补写的
`baseline` 事件只表示“启动时观测到的快照”——其 `timestamp` 是迁移时刻
（晚于预约 `created_at`），快照里保留既有的 `version`/`status`/`canceled_at`，
不会伪造出过去的 `created`/`rescheduled`/`canceled` 事件；迁移后新的成功操作
正常追加为 seq=2、3……并与 baseline 的哈希衔接。

## 测试覆盖要点

- **春季跳变**：不存在的钟面时刻报错；跨越跳变间隙的窗口 UTC 时长缩短（柏林 2026-03-29 00:00–04:00 实际只有 3 小时）。
- **秋季回退**：歧义窗口取 `fold=0`（美东 2025-11-02 01:00–02:00 → UTC 05:00–07:00，共 2 小时）。
- **跨午夜**：窗口拆分、与另一时区窗口求交、各自本地时刻回显。
- 扫描线排序（人数降序、开始时间升序）、同参与者重叠窗口合并、UTC 边界裁剪、时长过滤。
- **预约**：半开区间各方向重叠均 409、相邻区间不冲突、冲突无部分写入、
  幂等重放返回原预约且不新增记录、`request_id` 内容不同 409、
  多线程并发下同一时段仅一个赢家、并发重放只建一条记录；
  **改期**：不存在 404、版本过期 409 并回传 `current_version`、排除自身的
  半开区间冲突 409、失败不改预约/占用、成功原子替换且版本递增、
  并发改期仅一个赢家；
  **取消**：软删除置 `canceled` 并记录 `canceled_at`、版本递增、
  占用同事务释放使时段立即可再约、原 `expected_version` 重试幂等返回
  同一结果、其他版本 409、取消后改期 409 不复活、并发取消仅一次实际变更；
  以及**旧版数据库启动时无损补齐 `version=1`、`status='active'`、
  `canceled_at=NULL`** 的迁移。
- **时段原子交换（swap）**：成功交换双方 `start_utc`/`end_utc` 互换、
  版本各 +1、`request_id`/参与者/`created_at` 不变、占用随新时段平移
  （旧窗口立即可再约、新窗口被占用）；任一不存在 404、任一已取消 409、
  任一版本不符 409 并回传该预约 `current_version`；排除交换双方后与
  第三方预约/未过期保留的新时段冲突 409（相邻不冲突、共享参与者的
  交换双方互不冲突、过期保留不阻止）；失败整体回滚（双方预约、占用、
  审计链均不变且不产生事件）；成功为双方各追加一条 `rescheduled`
  哈希链事件且链可重验；两条 ID 相同 422；并发交换恰一个赢家。
- **变更历史（哈希链）**：成功创建/改期/取消各在同事务追加事件、序号从 1
  连续递增、快照与版本跟随；`canonical_event` 紧凑/键排序/非 ASCII 不转义；
  首条 `prev_hash` 为 64 个 0，可按公式独立重算整链；GET 历史按序返回、
  不存在 404、篡改哈希/事件/快照列/序号断档均 409 且不返回历史；
  失败请求、幂等重放（创建重放、取消重试）不新增事件；事件写入失败时业务
  写入同事务回滚；非 ASCII 参与者端到端验签；并发改期后链仍连续；
  **旧版库（含无 version 列的最旧 schema）初始化时无损补写 `baseline`**、
  迁移幂等不重复补写、baseline 如实记录迁移时刻与取消态、迁移后改期正常接链。
- **临时保留（holds）**：创建返回 201 并规范化参与者、TTL 仅接受 1..900、
  时间/参与者校验沿用预约规则；同 `hold_key` 幂等重放 200、同键异内容 409、
  过期/确认后键可重用；与正式预约双向半开区间冲突（含相邻不冲突、
  不相交参与者可共存）、保留之间互相冲突、冲突 409 无部分写入、
  改期被未过期保留阻止；到期保留在写事务内惰性过期——确认返回 410、
  过期保留不阻止新预约/新保留；确认 201 转正式预约且同事务仅追加一条
  `created` 事件、重复确认 200 返回同一预约不重复写库/事件、404/422、
  `request_id` 撞车 409 且可在 TTL 内换键重试、确认后临时占用行释放；
  并发保留仅一个赢家、并发确认只产生一条预约与一条事件；
  旧版库初始化无损新建保留表且既有数据不动。
- **候补队列（waitlist）**：空闲时段直订 201 为 promoted 且同事务写入
  `created` 事件、被占用则 201 入 pending 且不占位（不阻止预约/保留/改期、
  候补之间不冲突）；同 `request_id` 幂等重放 200（pending / promoted 均返回
  最新状态）、同键异内容 409、GET 404/最新预约状态（含晋升后改期/取消）；
  取消在同一事务内按 `(created_at, id)` FIFO 晋升**受影响**候补（参与者相交
  + 时段半开重叠，相邻不扫描）、跳过仍与其他预约/未过期保留冲突的项、
  一次取消至多晋升一条、晋升与取消同事务原子（版本不符回滚、幂等重试不重复
  晋升）、晋升产生的预约可正常验链/改期/再取消；跨端点 `request_id` 一致
  （直接预约/保留确认与候补互相识别同内容幂等、异内容 409）；并发入队与
  并发取消下恰好一个赢家；旧版库初始化无损新建候补表、迁移幂等。

```bash
pytest -q
```
