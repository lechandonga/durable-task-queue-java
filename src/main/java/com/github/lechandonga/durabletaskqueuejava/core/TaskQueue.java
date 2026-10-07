package com.github.lechandonga.durabletaskqueuejava.core;

import com.github.lechandonga.durabletaskqueuejava.config.TaskQueueProperties;
import com.github.lechandonga.durabletaskqueuejava.storage.CorruptRecord;
import com.github.lechandonga.durabletaskqueuejava.storage.LocalJsonTaskStore;
import com.github.lechandonga.durabletaskqueuejava.storage.StorageLoadResult;
import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

/**
 * 本地持久化任务队列的核心状态机。
 * <p>
 * 所有状态变更都在单把监视器锁内完成并立即落盘，保证：
 * <ul>
 *     <li>幂等键维度：同一幂等键在任务终结前只存在一条活动任务；
 *     已成功的幂等键拒绝再次执行；死信任务允许重新入队（视为全新任务）。</li>
 *     <li>领取维度：任务被领取后带租约进入 RUNNING，不会同时被两个消费者处理；
 *     租约由执行中的心跳持续续约，过期失联任务会被安全回收重新投递。</li>
 *     <li>恢复维度：启动时所有 RUNNING 一律回到 PENDING 重新投递，
 *     停机时有兜底回收，任务不会卡在中间状态。</li>
 * </ul>
 */
public class TaskQueue {

    private static final Logger log = LoggerFactory.getLogger(TaskQueue.class);
    private static final int MAX_ERROR_LENGTH = 4000;

    private final TaskQueueProperties properties;
    private final LocalJsonTaskStore store;
    private final Clock clock;
    private final String nodeId = UUID.randomUUID().toString();

    private final Map<String, TaskRecord> byId = new LinkedHashMap<>();
    /** 非终态任务：幂等键 -> 任务 ID。 */
    private final Map<String, String> activeKeyIndex = new LinkedHashMap<>();
    /** 幂等键 -> 最近一条终态任务（用于拒绝已成功键的重复执行）。 */
    private final Map<String, TaskRecord> terminalByKey = new LinkedHashMap<>();

    private volatile boolean shutdown;
    private List<CorruptRecord> corruptRecordsOnStartup = List.of();

    public TaskQueue(TaskQueueProperties properties, LocalJsonTaskStore store, Clock clock) {
        this.properties = properties;
        this.store = store;
        this.clock = clock;
    }

    /**
     * 从本地存储恢复全部任务。必须在接受提交 / 启动消费者之前调用一次。
     *
     * @return 加载阶段发现的损坏记录报告
     */
    public synchronized List<CorruptRecord> recover() {
        StorageLoadResult result = store.loadAll();
        long now = clock.millis();
        int recoveredRunning = 0;
        List<CorruptRecord> corrupt = new ArrayList<>(result.corruptRecords());
        for (TaskRecord record : result.records()) {
            if (byId.containsKey(record.getId())) {
                corrupt.add(new CorruptRecord(-1, record.getId(),
                        "Duplicate task id detected in storage, record skipped"));
                log.error("Duplicate task id {} detected in storage, skipping duplicate record",
                        record.getId());
                continue;
            }
            byId.put(record.getId(), record);
            if (record.getStatus().isTerminal()) {
                terminalByKey.put(record.getIdempotencyKey(), record);
            } else {
                String existing = activeKeyIndex.putIfAbsent(
                        record.getIdempotencyKey(), record.getId());
                if (existing != null) {
                    corrupt.add(new CorruptRecord(-1, record.getId(),
                            "Duplicate active idempotencyKey '" + record.getIdempotencyKey()
                                    + "', record skipped"));
                    log.error("Duplicate active idempotencyKey {} for task {} during recovery",
                            record.getIdempotencyKey(), record.getId());
                    byId.remove(record.getId());
                    continue;
                }
                if (record.getStatus() == TaskStatus.RUNNING) {
                    requeue(record, now, "recovered after restart");
                    recoveredRunning++;
                }
            }
        }
        corruptRecordsOnStartup = List.copyOf(corrupt);
        log.info("Task store recovered: {} task(s) loaded, {} in-flight task(s) requeued, "
                        + "{} corrupt record(s) reported",
                byId.size(), recoveredRunning, corruptRecordsOnStartup.size());
        persist();
        return corruptRecordsOnStartup;
    }

    /**
     * 提交任务。
     *
     * @throws TaskRejectedException 容量满 / 活动重复 / 已成功重复 / 参数非法 / 队列已关闭
     */
    public synchronized TaskRecord submit(String idempotencyKey, String type, String payload) {
        if (shutdown) {
            throw new TaskRejectedException(RejectReason.INVALID_REQUEST,
                    "Task queue is shutting down, submission is rejected");
        }
        if (!StringUtils.hasText(idempotencyKey)) {
            throw new TaskRejectedException(RejectReason.INVALID_REQUEST,
                    "idempotencyKey must not be blank");
        }
        if (!StringUtils.hasText(type)) {
            throw new TaskRejectedException(RejectReason.INVALID_REQUEST,
                    "type must not be blank");
        }
        if (activeKeyIndex.containsKey(idempotencyKey)) {
            throw new TaskRejectedException(RejectReason.DUPLICATE_ACTIVE,
                    "An active task with idempotencyKey '" + idempotencyKey
                            + "' already exists: " + activeKeyIndex.get(idempotencyKey));
        }
        TaskRecord terminal = terminalByKey.get(idempotencyKey);
        if (terminal != null && terminal.getStatus() == TaskStatus.SUCCEEDED) {
            throw new TaskRejectedException(RejectReason.ALREADY_SUCCEEDED,
                    "Task with idempotencyKey '" + idempotencyKey
                            + "' already succeeded: " + terminal.getId());
        }
        if (activeKeyIndex.size() >= properties.getCapacity()) {
            throw new TaskRejectedException(RejectReason.CAPACITY_FULL,
                    "Queue capacity " + properties.getCapacity()
                            + " for active tasks is exhausted");
        }

        long now = clock.millis();
        TaskRecord record = new TaskRecord();
        record.setId(UUID.randomUUID().toString());
        record.setIdempotencyKey(idempotencyKey);
        record.setType(type);
        record.setPayload(payload);
        record.setStatus(TaskStatus.PENDING);
        record.setAttempts(0);
        record.setCreatedAt(now);
        record.setVisibleAt(now);
        record.setUpdatedAt(now);

        byId.put(record.getId(), record);
        activeKeyIndex.put(idempotencyKey, record.getId());
        if (terminal != null) {
            terminalByKey.remove(idempotencyKey);
        }
        persist();
        log.info("Task enqueued: id={}, key={}, type={}, active={}/{}",
                record.getId(), idempotencyKey, type,
                activeKeyIndex.size(), properties.getCapacity());
        return record.copy();
    }

    /**
     * 领取一个到期可执行的任务，同时回收租约过期的失联任务。
     * 整个领取是原子的：同一任务不可能被两个消费者同时拿到。
     */
    public synchronized Optional<TaskRecord> claim() {
        long now = clock.millis();
        TaskRecord candidate = null;
        for (TaskRecord record : byId.values()) {
            if (record.getStatus() == TaskStatus.RUNNING
                    && record.getLeaseExpireAt() > 0 && record.getLeaseExpireAt() < now) {
                requeue(record, now, "lease expired (worker lost)");
                log.warn("Task {} lease expired, requeued for redelivery", record.getId());
                if (candidate == null) {
                    candidate = record;
                }
            } else if (candidate == null
                    && record.getStatus() == TaskStatus.PENDING
                    && record.getVisibleAt() <= now) {
                candidate = record;
            }
        }
        if (candidate == null) {
            return Optional.empty();
        }
        candidate.setStatus(TaskStatus.RUNNING);
        candidate.setAttempts(candidate.getAttempts() + 1);
        candidate.setLeasedBy(nodeId + "#" + Thread.currentThread().getId());
        candidate.setLeaseExpireAt(now + properties.getLeaseTtl().toMillis());
        candidate.setUpdatedAt(now);
        persist();
        log.info("Task claimed: id={}, type={}, attempt={}/{}",
                candidate.getId(), candidate.getType(),
                candidate.getAttempts(), properties.getMaxAttempts());
        return Optional.of(candidate.copy());
    }

    /**
     * 执行期间续约，证明消费者仍然存活。
     *
     * @return true 表示租约仍属于该消费者；false 表示已过期被回收，调用方应放弃执行
     */
    public synchronized boolean heartbeat(String taskId, String leaseOwner) {
        TaskRecord record = byId.get(taskId);
        if (record == null || record.getStatus() != TaskStatus.RUNNING
                || !leaseOwner.equals(record.getLeasedBy())) {
            return false;
        }
        record.setLeaseExpireAt(clock.millis() + properties.getLeaseTtl().toMillis());
        record.setUpdatedAt(clock.millis());
        persist();
        return true;
    }

    /**
     * 确认任务成功。仅当前租约持有者可以确认；成功任务为终态，永不再投递。
     */
    public synchronized boolean succeed(String taskId, String leaseOwner) {
        TaskRecord record = byId.get(taskId);
        if (record == null || record.getStatus() != TaskStatus.RUNNING
                || !leaseOwner.equals(record.getLeasedBy())) {
            return false;
        }
        long now = clock.millis();
        record.setStatus(TaskStatus.SUCCEEDED);
        record.setLeasedBy(null);
        record.setLeaseExpireAt(0);
        record.setLastError(null);
        record.setUpdatedAt(now);
        moveToTerminal(record);
        persist();
        log.info("Task succeeded: id={}, attempts={}", taskId, record.getAttempts());
        return true;
    }

    /**
     * 上报任务失败。
     *
     * @param retryable true 表示可重试（瞬时）失败；false 表示不可重试，直接死信
     * @param errorMessage 失败原因，死信时保留最后一次的原因
     * @return 失败后的任务快照
     */
    public synchronized TaskRecord fail(String taskId, String leaseOwner, boolean retryable,
                                        String errorMessage) {
        TaskRecord record = byId.get(taskId);
        if (record == null || record.getStatus() != TaskStatus.RUNNING
                || !leaseOwner.equals(record.getLeasedBy())) {
            throw new IllegalStateException(
                    "Task " + taskId + " is no longer owned by this consumer");
        }
        long now = clock.millis();
        record.setLastError(truncate(errorMessage));
        record.setLeasedBy(null);
        record.setLeaseExpireAt(0);
        record.setUpdatedAt(now);

        boolean attemptsExhausted = record.getAttempts() >= properties.getMaxAttempts();
        if (!retryable || attemptsExhausted) {
            record.setStatus(TaskStatus.DEAD);
            record.setVisibleAt(now);
            moveToTerminal(record);
            persist();
            log.error("Task moved to dead-letter: id={}, attempts={}/{}, retryable={}, reason={}",
                    taskId, record.getAttempts(), properties.getMaxAttempts(),
                    retryable, record.getLastError());
        } else {
            long delayMillis = backoffDelayMillis(record.getAttempts());
            record.setStatus(TaskStatus.PENDING);
            record.setVisibleAt(now + delayMillis);
            persist();
            log.warn("Task failed (retryable): id={}, attempt={}/{}, retry in {} ms, reason={}",
                    taskId, record.getAttempts(), properties.getMaxAttempts(),
                    delayMillis, record.getLastError());
        }
        return record.copy();
    }

    /**
     * 死信任务重新入队：删除旧的终态记录并提交一条全新任务，
     * 尝试次数清零、重新走完整的退避/重试策略。
     */
    public synchronized TaskRecord requeueDeadLetter(String taskId) {
        TaskRecord dead = byId.get(taskId);
        if (dead == null || dead.getStatus() != TaskStatus.DEAD) {
            throw new IllegalArgumentException("Task " + taskId + " is not in dead-letter");
        }
        String key = dead.getIdempotencyKey();
        if (activeKeyIndex.containsKey(key)) {
            throw new TaskRejectedException(RejectReason.DUPLICATE_ACTIVE,
                    "An active task with idempotencyKey '" + key + "' already exists");
        }
        String type = dead.getType();
        String payload = dead.getPayload();
        byId.remove(taskId);
        terminalByKey.remove(key);
        return submit(key, type, payload);
    }

    /**
     * 停机兜底：把所有仍处于 RUNNING 的任务安全地退回 PENDING 并落盘。
     * 消费者线程应先停止（成功确认或按失败处理在途任务），再调用本方法。
     */
    public synchronized int requeueRunningTasks(String reason) {
        long now = clock.millis();
        int count = 0;
        for (TaskRecord record : byId.values()) {
            if (record.getStatus() == TaskStatus.RUNNING) {
                requeue(record, now, reason);
                count++;
            }
        }
        if (count > 0) {
            persist();
            log.info("{} in-flight task(s) requeued during shutdown ({})", count, reason);
        }
        return count;
    }

    /** 停止接受新提交。正在执行的任务不受影响。 */
    public synchronized void shutdown() {
        shutdown = true;
    }

    public synchronized boolean isShutdown() {
        return shutdown;
    }

    public synchronized Optional<TaskRecord> findById(String taskId) {
        TaskRecord record = byId.get(taskId);
        return record == null ? Optional.empty() : Optional.of(record.copy());
    }

    /** 按幂等键查询：优先返回活动任务，否则返回最近的终态任务。 */
    public synchronized Optional<TaskRecord> findByIdempotencyKey(String idempotencyKey) {
        String activeId = activeKeyIndex.get(idempotencyKey);
        if (activeId != null) {
            return Optional.of(byId.get(activeId).copy());
        }
        TaskRecord terminal = terminalByKey.get(idempotencyKey);
        return terminal == null ? Optional.empty() : Optional.of(terminal.copy());
    }

    public synchronized List<TaskRecord> listByStatus(TaskStatus status) {
        List<TaskRecord> result = new ArrayList<>();
        for (TaskRecord record : byId.values()) {
            if (record.getStatus() == status) {
                result.add(record.copy());
            }
        }
        return result;
    }

    public synchronized List<TaskRecord> listAll() {
        return byId.values().stream().map(TaskRecord::copy).toList();
    }

    public synchronized int activeCount() {
        return activeKeyIndex.size();
    }

    public synchronized List<CorruptRecord> getCorruptRecordsOnStartup() {
        return corruptRecordsOnStartup;
    }

    /**
     * 距离下一个可领取任务的建议等待毫秒数；无任务或全部在退避时返回轮询间隔上限。
     */
    public synchronized long nextDelayMillis() {
        long now = clock.millis();
        long earliest = Long.MAX_VALUE;
        for (TaskRecord record : byId.values()) {
            if (record.getStatus() == TaskStatus.PENDING) {
                earliest = Math.min(earliest, record.getVisibleAt());
            }
        }
        long maxWait = properties.getPollInterval().toMillis();
        if (earliest == Long.MAX_VALUE) {
            return maxWait;
        }
        return Math.min(maxWait, Math.max(0, earliest - now));
    }

    /** 第 N 次尝试失败后的指数退避延迟（N 从 1 开始）。 */
    public long backoffDelayMillis(int failedAttempt) {
        double factor = Math.pow(properties.getBackoffMultiplier(), Math.max(0, failedAttempt - 1));
        long delay = (long) (properties.getBackoff().toMillis() * factor);
        return Math.min(delay, properties.getMaxBackoff().toMillis());
    }

    private void requeue(TaskRecord record, long now, String reason) {
        record.setStatus(TaskStatus.PENDING);
        record.setLeasedBy(null);
        record.setLeaseExpireAt(0);
        record.setVisibleAt(now);
        record.setUpdatedAt(now);
        log.info("Task requeued: id={}, attempts={}, reason={}", record.getId(),
                record.getAttempts(), reason);
    }

    private void moveToTerminal(TaskRecord record) {
        String previous = activeKeyIndex.remove(record.getIdempotencyKey());
        if (previous == null) {
            log.warn("Terminal task {} had no active index entry", record.getId());
        }
        terminalByKey.put(record.getIdempotencyKey(), record);
    }

    private void persist() {
        store.rewriteAll(byId.values().stream().toList());
    }

    private static String truncate(String message) {
        if (message == null) {
            return null;
        }
        return message.length() <= MAX_ERROR_LENGTH ? message
                : message.substring(0, MAX_ERROR_LENGTH) + "...(truncated)";
    }
}
