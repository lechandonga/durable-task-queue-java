# 本地持久化任务队列（durable-task-queue）

纯本地实现的可靠任务队列：**追加式 CRC 校验事件日志（journal）+ 内存状态机**。
不引入任何需要单独部署的中间件或外部服务（无数据库、无 Redis、无 MQ），
持久化文件只落在本地磁盘（默认 `./data/task-queue/task-queue.journal`）。

## 1. 任务状态流转

```
PENDING ──领取(写租约)──▶ RUNNING ──成功确认────────────────────▶ SUCCEEDED（终态）
   ▲                        │
   │                        ├─ 可重试失败：按指数退避回到 PENDING，到期再投递
   │                        ├─ 达到最大尝试次数 / 不可重试失败 ──▶ DEAD（终态，死信）
   │                        ├─ 租约过期（消费者宕机/卡死）被回收 ──┘
   │                        └─ 优雅停机时在途任务释放 ───────────┘
   └────────────────────────────┘
```

- `PENDING`：已持久化、等待 `visibleAt`（退避到期）后被领取。
- `RUNNING`：已被某个消费者持有租约；租约内**同一任务绝不会被第二个消费者领取**。
- `SUCCEEDED`：成功确认后的终态。任何重复投递、租约回收、重启都不会再次执行。
- `DEAD`：死信终态。保留 `attempts` 与最后一次失败原因 `lastError`，不再投递、不占用并发。

## 2. 提交、幂等与拒绝原因

- `POST /api/tasks`，body：`{"type":"...","payload":"...","idempotencyKey":"..."}`
  - `idempotencyKey` 可空；非空时会做幂等去重。
  - **同一幂等键在任务终结（SUCCEEDED/DEAD）前只保留一条**，并发提交同样保证只有一条成功。
  - 任务终结后，允许用同一幂等键重新提交（返回的是新任务）。
- 拒绝均返回 **HTTP 409**，响应体含机器可读原因，绝不静默丢弃：
  - `DUPLICATE_ACTIVE_KEY`：相同幂等键已有未终结任务；同时返回 `existingTaskId`。
  - `QUEUE_FULL`：活跃（未终结）任务数达到 `taskqueue.capacity`。
- 查询：
  - `GET /api/tasks/{id}` 按任务 id 查询。
  - `GET /api/tasks/idempotency/{key}` 按幂等键查询最近一次提交（含已终结任务）。
  - `GET /api/tasks/metrics` 队列指标；`GET /api/tasks/storage-issues` 存储异常明细。

## 3. 失败分类、重试与死信

业务处理器实现 `TaskHandler`（返回 `TaskOutcome`）：

- `TaskOutcome.success()`：成功确认。
- `TaskOutcome.retryable(reason)`：可重试失败。退避到期后重新投递；
  **达到最大尝试次数后转入死信**，并保留最后一次失败原因。
- `TaskOutcome.dead(reason)`：不可重试失败，**首次即进死信**，不重试。
- 处理器抛出未捕获异常等价于一次 `retryable(异常信息)`。
- 没有注册处理器的任务类型按不可重试失败直接进死信。

退避公式（指数递增 + 抖动，避免重试压垮下游）：

```
delay(n) = base-backoff * backoff-multiplier^(n-1) * (1 ± backoff-jitter)
```

- 死信任务与反复失败的“毒消息”不会阻塞其他任务：失败即释放并发槽，
  其他到期任务继续被消费；毒消息只在自己的退避时刻被重试，最终进死信。

## 4. 租约与并发消费

- 领取任务即生成租约（`taskqueue.lease-duration`），消费者每 `lease/3` 自动续租。
- 消费者宕机 / 卡死导致租约过期后，心跳巡检会把任务回收为 PENDING 重新投递。
- 并发度由 `taskqueue.concurrency` 控制；不同任务可并行，同一任务在判定成功/失败前互斥。
- 语义为**至少一次（at-least-once）**：只有在“处理完成 + 成功确认落盘”后才不会再执行；
  若执行结果尚未确认进程就崩溃，任务会重新投递，业务侧需自行保证处理幂等。

## 5. 重启恢复与优雅停机

持久化保证：每条状态事件先追加到 journal 并 `fsync(FileChannel.force)`，再更新内存。

- **重启恢复**：重放 journal 重建全部任务。
  - PENDING 任务继续等待/投递；退避到期时间、attempts 一并保留。
  - 上次退出时仍处于 RUNNING（在途）的任务，恢复为 PENDING 并重新投递，
    最终一定会得到 SUCCEEDED 或 DEAD 的确定结果。
  - SUCCEEDED/DEAD 终态保持不变，成功任务不会因重启再次执行。
- **优雅停机**（Spring 容器关闭触发）：
  - 消费者立即停止领取新任务；等待在途任务执行并确认，最长等待 `taskqueue.shutdown-timeout`。
  - 超时仍未结束的执行线程被中断：任务租约释放回 PENDING（不计失败、不耗尝试次数），
    下次启动立即可恢复执行——不丢任务，也不卡在 RUNNING。
- **损坏记录处理**：每行带 CRC32。启动重放时损坏行（CRC 不匹配/JSON 非法/字段缺失）
  会被**逐条隔离、明确记录 WARN 日志**，可通过 `/api/tasks/storage-issues` 与 metrics 查看；
  其余记录正常恢复、队列继续可用——既不悄悄忽略，也不让整个队列不可用。

## 6. 配置项（`application.properties`，均有默认值）

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `taskqueue.data-dir` | `./data/task-queue` | 本地 journal 目录 |
| `taskqueue.capacity` | `1000` | 活跃任务容量上限，超限返回 QUEUE_FULL |
| `taskqueue.max-attempts` | `5` | 最大尝试次数（含首次），达到后进死信 |
| `taskqueue.base-backoff` | `2s` | 首次重试基础退避 |
| `taskqueue.backoff-multiplier` | `2.0` | 退避递增倍数 |
| `taskqueue.backoff-jitter` | `0.2` | 退避抖动比例（0~0.9） |
| `taskqueue.lease-duration` | `30s` | 单次执行租约时长（自动续租） |
| `taskqueue.concurrency` | `2` | 并发消费者线程数 |
| `taskqueue.shutdown-timeout` | `20s` | 优雅停机最长等待 |
| `taskqueue.compact-threshold-bytes` | `1048576` | journal 超过该大小触发快照压缩，`-1` 关闭 |

## 7. 如何实现自定义任务处理器

实现 `TaskHandler` 并注册为 Spring Bean，用 `@TaskType("类型名")` 绑定类型；
或实现 `NamedTaskHandler.typeName()`。内置示例为类型 `echo`（见 `DemoEchoHandler`）。

```java
@TaskType("order-paid")
public class OrderPaidHandler implements TaskHandler {
    public TaskOutcome handle(TaskContext ctx) {
        // ctx.getType() / getPayload() / getAttempt() / getLeaseExpiresAt()
        return TaskOutcome.success();      // 或 retryable(reason) / dead(reason)
    }
}
```

## 8. 本地验证方法

无需任何外部服务，JDK 17+、Maven 即可。

```bash
# 运行全部单元/集成测试（测试使用 JUnit 临时目录，可重复运行）
mvn test

# 打包并启动（默认随机 HTTP 端口由原工程 server.port=0 决定；可显式指定）
mvn package -DskipTests
java -jar target/durable-task-queue-java-1.0-SNAPSHOT.jar --server.port=18080

# 手工冒烟
curl -s -X POST localhost:18080/api/tasks -H 'Content-Type: application/json' \
  -d '{"type":"echo","payload":"hello","idempotencyKey":"k1"}'
curl -s localhost:18080/api/tasks/idempotency/k1
curl -s localhost:18080/api/tasks/metrics
```

测试日志（`mvn test` 输出）按业务分组，每个用例都有
`>>> [业务名] 用例：...` 与 `<<< [业务名] 通过：...` 的中文日志，可直观看出覆盖范围：

- **提交/查询/并发幂等**：50 线程同键并发只成功 1 条、拒绝原因区分、无键共存。
- **重试/死信/毒消息**：指数退避递增、最大尝试次数、不可重试即死信、毒消息不阻塞、异常重试。
- **并发消费**：单任务租约互斥、不同任务并行、租约过期回收、成功不重放。
- **重启恢复**：PENDING/在途 RUNNING/退避中任务跨重启恢复、成功任务不重复执行。
- **优雅停机**：等待在途完成、超时中断释放回 PENDING 并可恢复。
- **损坏存储**：CRC 损坏行/垃圾行被隔离报错，好记录照常恢复，队列继续可用。
- **HTTP 端到端**：提交→消费成功→查询，409 两种拒绝原因，metrics/storage-issues。
