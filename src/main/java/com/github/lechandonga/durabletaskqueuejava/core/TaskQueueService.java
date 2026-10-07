package com.github.lechandonga.durabletaskqueuejava.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 本地持久化任务队列核心：事件溯源状态机。
 *
 * <p>所有状态变更都在同一把 {@link ReentrantLock} 内完成，并且先落 journal 后改内存，
 * 因此入队幂等判定、容量判定、领取、确认天然原子；消费侧通过租约保证同一任务
 * 在判定失败前不会被两个消费者同时处理。</p>
 */
public class TaskQueueService {

    private static final Logger log = LoggerFactory.getLogger(TaskQueueService.class);

    private final TaskQueueProperties properties;
    private final JournalStore journal;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition taskAvailable = lock.newCondition();

    private final Map<String, Task> tasksById = new HashMap<>();
    /** 仅收录未终结任务的幂等键 -> 任务 id；终结后移除，允许同键重新提交。 */
    private final Map<String, String> activeKeyIndex = new HashMap<>();
    /** 按可见时间排序的待处理堆，只包含 PENDING 任务。 */
    private final PriorityQueue<Task> pendingHeap =
            new PriorityQueue<>(Comparator.comparingLong(Task::getVisibleAt)
                    .thenComparing(Task::getId));

    private long recoveredInFlight;

    /** 重放时语义异常（如引用不存在的任务）但 CRC 合法的事件，明确报错并隔离，不拖垮队列。 */
    private final List<String> recoveryAnomalies = new ArrayList<>();

    public TaskQueueService(TaskQueueProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.journal = new JournalStore(Path.of(properties.getDataDir()), objectMapper);
    }


    @PostConstruct
    public void start() {
        lock.lock();
        try {
            recover();
        } finally {
            lock.unlock();
        }
    }

    ReentrantLock queueLock() {
        return lock;
    }

    Condition availability() {
        return taskAvailable;
    }

    // ------------------------------------------------------------ 提交 / 查询

    /**
     * 提交任务。
     *
     * @param idempotencyKey 幂等键，null/空白表示不做幂等去重
     * @throws TaskRejectedException 容量满或幂等键冲突（原因可区分，不静默丢弃）
     */
    public Task enqueue(String type, String payload, String idempotencyKey) {
        Objects.requireNonNull(type, "type 不能为空");
        String key = normalizeKey(idempotencyKey);
        long now = System.currentTimeMillis();
        lock.lock();
        try {
            if (key != null) {
                String existingId = activeKeyIndex.get(key);
                if (existingId != null) {
                    Task existing = tasksById.get(existingId);
                    if (existing != null && !existing.isTerminal()) {
                        throw new TaskRejectedException(RejectReason.DUPLICATE_ACTIVE_KEY,
                                "幂等键 '" + key + "' 已有未终结任务 " + existingId
                                        + "（状态 " + existing.getStatus() + "），任务终结前重复提交被拒绝",
                                existingId);
                    }
                }
            }
            if (countActive() >= properties.getCapacity()) {
                throw new TaskRejectedException(RejectReason.QUEUE_FULL,
                        "队列活跃任务数已达容量上限 " + properties.getCapacity()
                                + "，请等待任务终结后再提交",
                        null);
            }
            String id = UUID.randomUUID().toString().replace("-", "");
            Task task = new Task();
            task.setId(id);
            task.setIdempotencyKey(key);
            task.setType(type);
            task.setPayload(payload);
            task.setStatus(TaskStatus.PENDING);
            task.setAttempts(0);
            task.setVisibleAt(now);
            task.setUpdatedAt(now);

            Map<String, Object> data = new HashMap<>();
            data.put("id", id);
            data.put("key", key);
            data.put("type", type);
            data.put("payload", payload);
            data.put("createdAt", task.getCreatedAt());
            data.put("visibleAt", now);
            journal.append(JournalEvent.ENQUEUED, data);

            tasksById.put(id, task);
            if (key != null) {
                activeKeyIndex.put(key, id);
            }
            pendingHeap.add(task);
            taskAvailable.signalAll();
            log.info("任务入队成功 id={} type={} key={} (活跃 {}/{})",
                    id, type, key, countActive(), properties.getCapacity());
            maybeCompact();
            return snapshot(task);
        } finally {
            lock.unlock();
        }
    }

    public Optional<Task> findById(String id) {
        lock.lock();
        try {
            return Optional.ofNullable(tasksById.get(id)).map(this::snapshot);
        } finally {
            lock.unlock();
        }
    }

    /** 按幂等键查询最近一次提交的任务（含已终结任务，便于调用方判断此前处理结果）。 */
    public Optional<Task> findByIdempotencyKey(String idempotencyKey) {
        String key = normalizeKey(idempotencyKey);
        if (key == null) {
            return Optional.empty();
        }
        lock.lock();
        try {
            Task found = null;
            for (Task task : tasksById.values()) {
                if (key.equals(task.getIdempotencyKey())
                        && (found == null || task.getCreatedAt() > found.getCreatedAt())) {
                    found = task;
                }
            }
            return Optional.ofNullable(found).map(this::snapshot);
        } finally {
            lock.unlock();
        }
    }

    public QueueMetrics metrics() {
        lock.lock();
        try {
            int pending = 0;
            int running = 0;
            int succeeded = 0;
            int dead = 0;
            for (Task task : tasksById.values()) {
                switch (task.getStatus()) {
                    case PENDING -> pending++;
                    case RUNNING -> running++;
                    case SUCCEEDED -> succeeded++;
                    case DEAD -> dead++;
                }
            }
            return new QueueMetrics(pending + running, properties.getCapacity(),
                    pending, running, succeeded, dead,
                    journal.corruptRecords().size() + recoveryAnomalies.size());
        } finally {
            lock.unlock();
        }
    }

    public List<CorruptRecord> corruptRecords() {
        return journal.corruptRecords();
    }

    public List<String> recoveryAnomalies() {
        return List.copyOf(recoveryAnomalies);
    }

    // ------------------------------------------------------------ 消费侧操作

    /**
     * 阻塞等待并领取一个到期任务，领取即加租约。超时或无任务时返回 null。
     * 同一任务在租约有效期内只会被一个消费者领取。
     */
    public Task claimNext(long waitMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + waitMillis;
        lock.lock();
        try {
            while (true) {
                reclaimExpiredLeases(System.currentTimeMillis());
                Task next = pendingHeap.peek();
                long now = System.currentTimeMillis();
                if (next != null && next.getVisibleAt() <= now) {
                    pendingHeap.poll();
                    next.setStatus(TaskStatus.RUNNING);
                    next.setAttempts(next.getAttempts() + 1);
                    long expiresAt = now + properties.getLeaseDuration().toMillis();
                    next.setLeaseExpiresAt(expiresAt);
                    next.setUpdatedAt(now);
                    journal.append(JournalEvent.LEASED, Map.of(
                            "id", next.getId(),
                            "attempts", next.getAttempts(),
                            "leaseExpiresAt", expiresAt));
                    log.info("任务被领取 id={} attempt={} 租约至 {}",
                            next.getId(), next.getAttempts(), expiresAt);
                    return snapshot(next);
                }
                long wait;
                if (next == null) {
                    wait = deadline - now;
                } else {
                    wait = Math.min(deadline - now, Math.max(1, next.getVisibleAt() - now));
                }
                if (wait <= 0) {
                    return null;
                }
                taskAvailable.awaitNanos(Duration.ofMillis(wait).toNanos());
                if (System.currentTimeMillis() >= deadline) {
                    return null;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /** 续租；仅当任务仍处于 RUNNING 时成功。 */
    public boolean renewLease(String taskId) {
        lock.lock();
        try {
            Task task = tasksById.get(taskId);
            if (task == null || task.getStatus() != TaskStatus.RUNNING) {
                return false;
            }
            long expiresAt = System.currentTimeMillis() + properties.getLeaseDuration().toMillis();
            task.setLeaseExpiresAt(expiresAt);
            task.setUpdatedAt(System.currentTimeMillis());
            journal.append(JournalEvent.LEASE_RENEWED,
                    Map.of("id", taskId, "leaseExpiresAt", expiresAt));
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** 成功确认：任务进入 SUCCEEDED 终态，此后任何重复投递/重启都不会再次执行。 */
    public void acknowledgeSuccess(String taskId) {
        lock.lock();
        try {
            Task task = requireRunning(taskId);
            long now = System.currentTimeMillis();
            task.setStatus(TaskStatus.SUCCEEDED);
            task.setLeaseExpiresAt(0);
            task.setUpdatedAt(now);
            journal.append(JournalEvent.SUCCEEDED, Map.of("id", taskId, "at", now));
            releaseKeyIndex(task);
            log.info("任务成功确认 id={} 共尝试 {} 次", taskId, task.getAttempts());
            maybeCompact();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 报告失败：不可重试或已达最大尝试次数 -> 死信（保留最后一次失败原因）；
     * 否则按指数递增退避重新排队。死信/反复失败的任务不占用并发，也不阻塞其他任务。
     */
    public void reportFailure(String taskId, boolean retryable, String reason) {
        lock.lock();
        try {
            Task task = requireRunning(taskId);
            long now = System.currentTimeMillis();
            task.setLastError(reason);
            task.setLeaseExpiresAt(0);
            task.setUpdatedAt(now);

            boolean giveUp = !retryable || task.getAttempts() >= properties.getMaxAttempts();
            Map<String, Object> data = new HashMap<>();
            data.put("id", taskId);
            data.put("attempts", task.getAttempts());
            data.put("lastError", reason);
            if (giveUp) {
                task.setStatus(TaskStatus.DEAD);
                journal.append(JournalEvent.DEAD_LETTERED, data);
                releaseKeyIndex(task);
                log.warn("任务进入死信 id={} 尝试 {} 次 retryable={} 最后失败原因: {}",
                        taskId, task.getAttempts(), retryable, reason);
            } else {
                long backoff = computeBackoffMillis(task.getAttempts());
                long visibleAt = now + backoff;
                task.setStatus(TaskStatus.PENDING);
                task.setVisibleAt(visibleAt);
                data.put("visibleAt", visibleAt);
                data.put("backoffMillis", backoff);
                journal.append(JournalEvent.RETRY_SCHEDULED, data);
                pendingHeap.add(task);
                taskAvailable.signalAll();
                log.warn("任务执行失败，{}ms 后第 {} 次重试 id={} 原因: {}",
                        backoff, task.getAttempts() + 1, taskId, reason);
            }
            maybeCompact();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 主动释放租约（优雅停机时使用）：任务回到 PENDING 并立即可见，
     * 停机过程既不丢任务，也不让任务卡在 RUNNING。
     */
    public void releaseLease(String taskId) {
        lock.lock();
        try {
            Task task = tasksById.get(taskId);
            if (task == null || task.getStatus() != TaskStatus.RUNNING) {
                return;
            }
            long now = System.currentTimeMillis();
            task.setStatus(TaskStatus.PENDING);
            task.setLeaseExpiresAt(0);
            task.setVisibleAt(now);
            task.setUpdatedAt(now);
            journal.append(JournalEvent.LEASE_RELEASED, Map.of("id", taskId, "visibleAt", now));
            pendingHeap.add(task);
            taskAvailable.signalAll();
            log.info("任务租约释放，重新进入待处理 id={}", taskId);
        } finally {
            lock.unlock();
        }
    }

    /** 回收租约过期仍处于 RUNNING 的任务（消费者卡死/掉线场景）。 */
    public int reclaimExpiredLeases() {
        lock.lock();
        try {
            return reclaimExpiredLeases(System.currentTimeMillis());
        } finally {
            lock.unlock();
        }
    }

    private int reclaimExpiredLeases(long now) {
        int reclaimed = 0;
        for (Task task : tasksById.values()) {
            if (task.getStatus() == TaskStatus.RUNNING && task.getLeaseExpiresAt() > 0
                    && task.getLeaseExpiresAt() <= now) {
                long expiredAt = task.getLeaseExpiresAt();
                task.setStatus(TaskStatus.PENDING);
                task.setLeaseExpiresAt(0);
                task.setVisibleAt(now);
                task.setUpdatedAt(now);
                journal.append(JournalEvent.LEASE_RELEASED,
                        Map.of("id", task.getId(), "visibleAt", now, "reason", "LEASE_EXPIRED"));
                pendingHeap.add(task);
                reclaimed++;
                log.warn("任务租约过期被回收，重新进入待处理 id={} 过期于 {}",
                        task.getId(), expiredAt);
            }
        }
        if (reclaimed > 0) {
            taskAvailable.signalAll();
        }
        return reclaimed;
    }

    // ------------------------------------------------------------ 恢复（事件重放）

    private void recover() {
        List<JournalEvent> events = journal.replay();
        tasksById.clear();
        activeKeyIndex.clear();
        pendingHeap.clear();
        recoveredInFlight = 0;

        for (JournalEvent event : events) {
            try {
                applyEvent(event);
            } catch (RuntimeException e) {
                String anomaly = "事件 seq=" + event.seq() + " type=" + event.type()
                        + " 无法应用，已隔离: " + e.getMessage();
                recoveryAnomalies.add(anomaly);
                log.error(anomaly);
            }
        }

        long now = System.currentTimeMillis();
        for (Task task : new ArrayList<>(tasksById.values())) {
            if (task.getStatus() == TaskStatus.RUNNING) {
                // 上次进程退出时仍在处理的任务：释放回 PENDING 并落盘，重启后继续得到确定结果。
                task.setStatus(TaskStatus.PENDING);
                task.setLeaseExpiresAt(0);
                task.setVisibleAt(now);
                task.setUpdatedAt(now);
                pendingHeap.add(task);
                journal.append(JournalEvent.LEASE_RELEASED,
                        Map.of("id", task.getId(), "visibleAt", now, "reason", "RECOVERY"));
                recoveredInFlight++;
                log.info("恢复在途任务，重新进入待处理 id={} 上次已尝试 {} 次",
                        task.getId(), task.getAttempts());
            } else if (task.getStatus() == TaskStatus.PENDING) {
                pendingHeap.add(task);
            }
        }
        // 重建未终结幂等键索引（终结任务允许同键重新提交）。
        for (Task task : tasksById.values()) {
            if (!task.isTerminal() && task.getIdempotencyKey() != null) {
                activeKeyIndex.put(task.getIdempotencyKey(), task.getId());
            }
        }
        log.info("本地任务队列恢复完成：任务总数 {}，活跃 {}，待处理 {}，在途恢复 {}，损坏记录隔离 {}",
                tasksById.size(), activeKeyIndex.size(), pendingHeap.size(),
                recoveredInFlight, journal.corruptRecords().size());
        if (!pendingHeap.isEmpty()) {
            taskAvailable.signalAll();
        }
    }

    private void applyEvent(JournalEvent event) {
        Map<String, Object> d = event.data();
        switch (event.type()) {
            case JournalEvent.ENQUEUED, JournalEvent.SNAPSHOT -> applyUpsert(d, event.type());
            case JournalEvent.LEASED -> {
                Task task = requireEventTask(d);
                task.setStatus(TaskStatus.RUNNING);
                task.setAttempts(asInt(d.get("attempts"), task.getAttempts()));
                task.setLeaseExpiresAt(asLong(d.get("leaseExpiresAt"), 0));
            }
            case JournalEvent.LEASE_RENEWED -> {
                Task task = requireEventTask(d);
                task.setLeaseExpiresAt(asLong(d.get("leaseExpiresAt"), 0));
            }
            case JournalEvent.LEASE_RELEASED, JournalEvent.RETRY_SCHEDULED -> {
                Task task = requireEventTask(d);
                task.setStatus(TaskStatus.PENDING);
                task.setLeaseExpiresAt(0);
                task.setVisibleAt(asLong(d.get("visibleAt"), System.currentTimeMillis()));
                if (d.get("attempts") instanceof Number) {
                    task.setAttempts(asInt(d.get("attempts"), task.getAttempts()));
                }
                if (d.get("lastError") != null) {
                    task.setLastError(String.valueOf(d.get("lastError")));
                }
            }
            case JournalEvent.SUCCEEDED -> {
                Task task = requireEventTask(d);
                task.setStatus(TaskStatus.SUCCEEDED);
                task.setLeaseExpiresAt(0);
            }
            case JournalEvent.DEAD_LETTERED -> {
                Task task = requireEventTask(d);
                task.setStatus(TaskStatus.DEAD);
                task.setLeaseExpiresAt(0);
                if (d.get("attempts") instanceof Number) {
                    task.setAttempts(asInt(d.get("attempts"), task.getAttempts()));
                }
                if (d.get("lastError") != null) {
                    task.setLastError(String.valueOf(d.get("lastError")));
                }
            }
            case JournalEvent.COMPACTION_BOUNDARY -> {
                // 仅作为重放分界标记，无状态需要应用。
            }
            default -> log.warn("忽略未知类型的存储事件: {}", event.type());
        }
    }

    private void applyUpsert(Map<String, Object> d, String eventType) {
        Object idObj = d.get("id");
        if (!(idObj instanceof String id)) {
            log.warn("存储事件缺少合法 id 字段，已跳过: {}", d);
            return;
        }
        Task task = tasksById.get(id);
        if (task == null) {
            task = new Task();
            task.setId(id);
            tasksById.put(id, task);
        }
        if (d.containsKey("key")) {
            Object keyObj = d.get("key");
            String key = keyObj == null ? null : String.valueOf(keyObj);
            task.setIdempotencyKey(key != null && key.isEmpty() ? null : key);
        }
        if (d.get("type") instanceof String type) {
            task.setType(type);
        }
        if (d.containsKey("payload")) {
            Object payload = d.get("payload");
            task.setPayload(payload == null ? null : String.valueOf(payload));
        }
        if (JournalEvent.ENQUEUED.equals(eventType)) {
            task.setStatus(TaskStatus.PENDING);
        } else if (d.get("status") instanceof String status) {
            try {
                task.setStatus(TaskStatus.valueOf(status));
            } catch (IllegalArgumentException e) {
                log.warn("任务 {} 的存储状态非法: {}，保持 {}", id, status, task.getStatus());
            }
        }
        if (d.get("attempts") instanceof Number) {
            task.setAttempts(asInt(d.get("attempts"), 0));
        }
        if (d.get("visibleAt") instanceof Number) {
            task.setVisibleAt(asLong(d.get("visibleAt"), 0L));
        }
        if (d.get("leaseExpiresAt") instanceof Number) {
            task.setLeaseExpiresAt(asLong(d.get("leaseExpiresAt"), 0L));
        }
        if (d.get("lastError") != null) {
            task.setLastError(String.valueOf(d.get("lastError")));
        }
    }

    private Task requireEventTask(Map<String, Object> d) {
        Object idObj = d.get("id");
        if (!(idObj instanceof String id)) {
            throw new IllegalStateException("存储事件缺少合法 id 字段: " + d);
        }
        Task task = tasksById.get(id);
        if (task == null) {
            throw new IllegalStateException("存储事件引用了不存在的任务 id=" + id
                    + "（journal 可能不完整，该事件被跳过）");
        }
        return task;
    }

    // ------------------------------------------------------------ 压缩

    private void maybeCompact() {
        long threshold = properties.getCompactThresholdBytes();
        if (threshold <= 0 || journal.sizeBytes() <= threshold) {
            return;
        }
        List<JournalEvent> snapshots = new ArrayList<>();
        for (Task task : tasksById.values()) {
            Map<String, Object> d = new HashMap<>();
            d.put("id", task.getId());
            d.put("key", task.getIdempotencyKey());
            d.put("type", task.getType());
            d.put("payload", task.getPayload());
            d.put("status", task.getStatus().name());
            d.put("attempts", task.getAttempts());
            d.put("visibleAt", task.getVisibleAt());
            d.put("leaseExpiresAt", task.getLeaseExpiresAt());
            d.put("lastError", task.getLastError());
            d.put("createdAt", task.getCreatedAt());
            snapshots.add(new JournalEvent(-1, JournalEvent.SNAPSHOT, d));
        }
        journal.compact(snapshots);
    }

    // ------------------------------------------------------------ 内部工具

    long computeBackoffMillis(int failedAttempt) {
        long base = properties.getBaseBackoff().toMillis();
        double factor = Math.pow(properties.getBackoffMultiplier(), Math.max(0, failedAttempt - 1));
        long delay = (long) Math.min((double) Long.MAX_VALUE, base * factor);
        double jitterRatio = Math.max(0.0, Math.min(0.9, properties.getBackoffJitter()));
        double factorJitter = 1.0 + (jitterRatio == 0.0 ? 0.0
                : ThreadLocalRandom.current().nextDouble(-jitterRatio, jitterRatio));
        long result = (long) Math.max(0, delay * factorJitter);
        return Math.max(1, result);
    }

    private Task requireRunning(String taskId) {
        Task task = tasksById.get(taskId);
        if (task == null) {
            throw new IllegalStateException("任务不存在: " + taskId);
        }
        if (task.getStatus() != TaskStatus.RUNNING) {
            throw new IllegalStateException("任务 " + taskId + " 当前状态为 " + task.getStatus()
                    + "，无法报告执行结果（可能租约已过期被回收）");
        }
        return task;
    }

    private int countActive() {
        int active = 0;
        for (Task t : tasksById.values()) {
            if (!t.isTerminal()) {
                active++;
            }
        }
        return active;
    }

    private void releaseKeyIndex(Task task) {
        if (task.getIdempotencyKey() != null) {
            activeKeyIndex.remove(task.getIdempotencyKey());
        }
    }

    private static String normalizeKey(String idempotencyKey) {
        if (idempotencyKey == null) {
            return null;
        }
        String trimmed = idempotencyKey.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static int asInt(Object value, int fallback) {
        return value instanceof Number n ? n.intValue() : fallback;
    }

    private static long asLong(Object value, long fallback) {
        return value instanceof Number n ? n.longValue() : fallback;
    }

    private Task snapshot(Task source) {
        Task copy = new Task();
        copy.setId(source.getId());
        copy.setIdempotencyKey(source.getIdempotencyKey());
        copy.setType(source.getType());
        copy.setPayload(source.getPayload());
        copy.setStatus(source.getStatus());
        copy.setAttempts(source.getAttempts());
        copy.setVisibleAt(source.getVisibleAt());
        copy.setLeaseExpiresAt(source.getLeaseExpiresAt());
        copy.setLastError(source.getLastError());
        copy.setUpdatedAt(source.getUpdatedAt());
        return copy;
    }

    /** 仅供 WorkerPool 唤醒等待中的消费者（停机时使用）。 */
    public void signalAll() {
        lock.lock();
        try {
            taskAvailable.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
