# 本地持久化任务队列（durable-task-queue-java）

基于 Spring Boot 3 / Java 21 的**本地任务队列**：无需任何外部中间件（不依赖
Redis/RabbitMQ/Kafka/数据库等独立部署的服务），任务数据以 JSON Lines 文件持久化在
本地磁盘，覆盖「提交 → 并发消费 → 失败重试/死信 → 重启恢复 → 优雅停机」完整链路。

## 快速开始

```bash
# 构建与测试（全部单测/集成测试，不依赖外部服务，可重复运行）
mvn test

# 启动（默认端口 8080，本地存储目录 data/task-queue）
mvn spring-boot:run

# 提交一个任务
curl -X POST localhost:8080/api/tasks \
  -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"order-1001","type":"demo.echo","payload":"hello"}'
```

## 任务状态流转

```
                 提交(幂等键)
                     │
                     ▼
                ┌─────────┐  claim（原子领取，带租约）   ┌─────────┐
                │ PENDING │ ───────────────────────────▶ │ RUNNING │
                └─────────┘                                └─────────┘
                     ▲                                    │   │   │
        可重试失败退避到期 │                  成功确认 ────────┘   │   └──────────────┐
                     │                    ▼                       │                  │
                     │              ┌───────────┐  不可重试失败 /   │ 租约过期(失联)    │
                     └──────────────┤ SUCCEEDED │  重试次数耗尽     │ /停机强制回收     │
                                    └───────────┘  ▼                ▼                  │
                                               ┌──────┐  重新入队(requeue) 回到 PENDING ┘
                                               │ DEAD │ ─────────────────────────────┘
                                               └──────┘
```

- **PENDING**：已入队等待消费；重试时 `visibleAt = 失败时刻 + 退避延迟`，未到期不会被领取。
- **RUNNING**：已被某个消费者领取执行。领取与状态转换在队列内部单锁内原子完成，
  **同一任务在判定失败前不可能被两个消费者同时处理**。
- **SUCCEEDED / DEAD**：终态，保存在本地文件中，**永远不会再被投递**。

## 幂等规则

- 提交必须带 `idempotencyKey` 与 `type`，否则返回 `400 INVALID_REQUEST`。
- 相同幂等键在任务终结前（PENDING/RUNNING/等待重试）只保留一条活动任务：
  - 并发重复提交也是同样保证（内部加锁，先到先得）；
  - 重复提交返回 `409`，原因为 **`DUPLICATE_ACTIVE`**，并带已有任务 ID，绝不静默丢弃。
- 已 **SUCCEEDED** 的幂等键再次提交返回 `409`，原因为 **`ALREADY_SUCCEEDED`**——
  成功任务不会因重复投递或重启被再次执行。
- 容量（活动任务数，不含终态）超限返回 `429`，原因为 **`CAPACITY_FULL`**，与重复提交明确区分。
- **DEAD** 的幂等键允许重新提交（也可调用死信重放入口），作为全新任务重新计数；
  这是死信的人工/自动修复通道。

拒绝原因枚举：`CAPACITY_FULL` / `DUPLICATE_ACTIVE` / `ALREADY_SUCCEEDED` / `INVALID_REQUEST`。

## 失败分类、重试与死信

业务处理器实现 `core.TaskHandler` 并注册为 Spring Bean，按 `type` 分发：

```java
@Component
public class OrderHandler implements TaskHandler {
    public String type() { return "order.pay"; }

    public void handle(String payload) throws Exception {
        // 1) 外部依赖抖动等可自愈错误 → 抛 TransientTaskException：按指数退避重试
        // 2) 其它任何异常（含 RuntimeException）→ 不可重试：直接进入死信
    }
}
```

- **可重试失败**：抛出 `TaskHandler.TransientTaskException`，任务回到 PENDING，
  退避延迟 = `backoff × backoffMultiplier^(attempt-1)`，封顶 `max-backoff`。
- **重试上限**：尝试次数（含首次执行）达到 `max-attempts` 后转入 **DEAD**，
  记录保留**最后一次失败原因**（`lastError`，HTTP 可查）。
- **不可重试失败**：任何其它异常立即 DEAD，不浪费后续尝试；未注册的 `type` 同样直接死信。
- **毒消息不阻塞**：死信和反复失败的任务不占用消费者——其它 PENDING 任务照常被领取执行。
- 死信可通过 `POST /api/dead-letters/{id}/requeue` 重新入队，尝试次数清零再走完整策略。

## 租约与并发消费

- 领取任务时生成带 TTL 的租约（`leasedBy` + `leaseExpireAt`），执行期间由独立心跳线程
  每 `lease-ttl/3` 续约一次。
- 只有租约持有者能确认成功/上报失败；心跳证明消费者存活。
- 消费者进程被 kill / 长时间 STW 等导致租约过期时，下一轮领取会把失联任务原子地
  回收为 PENDING 并重新投递（at-least-once，执行次数计数器递增）。

## 重启恢复语义

- 每次状态变更都会把全量快照写入 `tasks.jsonl.tmp`、fsync 后**原子替换** `tasks.jsonl`，
  主文件任何时刻都是完整一致的状态。
- 启动恢复时：
  - PENDING（含等待退避，`visibleAt` 保持不变）继续等待领取；
  - **RUNNING 一律回到 PENDING 重新投递**（上一进程已死，无人持有租约）；
  - SUCCEEDED/DEAD 终态原样保留，成功任务不会重复执行。
- **损坏记录处理**：无法解析的 JSON 行、字段非法（缺 id/幂等键/type/status、
  RUNNING 缺租约等）不会被静默忽略：原始行会原样写入 `corrupt-<时间戳>.log` 隔离文件，
  并在 `GET /api/admin/recovery` 中报告行号与原因；其余正常记录照常加载，队列继续可用。

## 优雅停机关机

Spring 容器关闭（`@PreDestroy`）时调度器按顺序执行：

1. 停止领取新任务（同时拒绝新提交）；
2. 等待在途任务执行完毕，宽限期 `task-queue.shutdown-await`（默认 30s，
   宽限期内跑完的任务正常确认成功/失败）；
3. 超时仍未结束的任务中断执行，并且不再上报结果，统一由队列把 RUNNING **兜底回收为
   PENDING 并落盘**；
4. 重启后这些任务被重新投递，最终一定得到 SUCCEEDED 或 DEAD 的确定结果——
   不丢任务、不卡在中间状态。

## 配置项（`application.properties`，均有默认值，支持环境变量覆盖）

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `task-queue.storage-dir` | `data/task-queue` | 本地持久化目录 |
| `task-queue.capacity` | `1000` | 活动任务容量上限，超限拒绝入队 |
| `task-queue.max-attempts` | `5` | 最大尝试次数（含首次），耗尽后死信 |
| `task-queue.backoff` | `10s` | 首次重试退避 |
| `task-queue.backoff-multiplier` | `2.0` | 退避指数倍数 |
| `task-queue.max-backoff` | `30m` | 退避上限 |
| `task-queue.concurrency` | `2` | 并发消费者线程数 |
| `task-queue.lease-ttl` | `5m` | 租约时长（执行期间自动续约） |
| `task-queue.poll-interval` | `200ms` | 空闲轮询间隔 |
| `task-queue.shutdown-await` | `30s` | 停机等待在途任务的宽限期 |

均可用同名环境变量覆盖，例如 `TASK_QUEUE_MAX_ATTEMPTS=8`、`TASK_QUEUE_BACKOFF=5s`。

## HTTP 接口

| 方法与路径 | 说明 |
| --- | --- |
| `POST /api/tasks` | 提交任务，body：`{"idempotencyKey","type","payload"}` |
| `GET /api/tasks/{id}` | 按任务 ID 查询 |
| `GET /api/tasks?idempotencyKey={key}` | 按幂等键查询 |
| `GET /api/tasks?status=PENDING`（RUNNING/SUCCEEDED/DEAD） | 按状态列出 |
| `GET /api/dead-letters` | 死信列表（含最后失败原因、尝试次数） |
| `POST /api/dead-letters/{id}/requeue` | 死信重新入队 |
| `GET /api/admin/recovery` | 启动恢复报告（活动数、损坏记录行号与原因） |

状态码：`201` 入队成功；`409` 重复（`DUPLICATE_ACTIVE`/`ALREADY_SUCCEEDED`）；
`429` 容量满（`CAPACITY_FULL`）；`400` 非法请求；`404` 任务不存在。

## 本地验证

```bash
mvn test                     # 31 个用例：幂等/并发/重试死信/恢复/停机/REST 全链路
mvn test -Dtest=RetryDeadLetterTest     # 只跑「失败重试与死信判定」业务
```

测试日志中以 `BUSINESS-COVERAGE` logger 输出业务场景 → 用例 → 通过/失败与耗时，
可直接看出覆盖了哪些业务链路，例如：

```
====== 业务场景开始: 任务提交与幂等去重 ======
>>> 用例开始: ...多线程并发提交相同幂等键：只有一条成功，其余全部 DUPLICATE_ACTIVE
<<< 用例通过: ... (13 ms)
```

测试分布：

- `SubmissionIdempotencyTest`：提交、按标识查询、并发重复提交、容量/重复拒绝区分、成功后拒绝重放
- `RetryDeadLetterTest`：指数退避、退避窗口不可领取、重试耗尽死信、不可重试立即死信、毒消息不阻塞、死信重放
- `ConcurrentConsumptionTest`：100 任务 8 消费者互斥领取、租约归属校验、心跳续约、租约过期回收
- `StorageRecoveryTest`：重启后四种状态一致性、成功不重投、退避截止时间保持、损坏行隔离报告、空存储初始化
- `GracefulShutdownTest`：在途任务跑完确认、超时任务回收后重启完成
- `DispatcherEndToEndTest`：端到端重试→成功/死信、毒消息不阻塞
- `ApplicationIntegrationTest` / `ApplicationRestartIntegrationTest`：REST 拒绝码、真实 Spring 上下文停机重启
