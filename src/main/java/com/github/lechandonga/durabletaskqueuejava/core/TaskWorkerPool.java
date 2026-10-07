package com.github.lechandonga.durabletaskqueuejava.core;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * 消费者工作池：按 {@code taskqueue.concurrency} 启动并发消费者，
 * 按任务 type 分发到对应 {@link TaskHandler}。
 *
 * <p>租约语义：领取任务后启动心跳定时续租；租约仍有效时，
 * {@link TaskQueueService} 保证同一任务不会被第二个消费者领取。</p>
 *
 * <p>优雅停机：先停止领取新任务，等待在途执行完成/上报结果；
 * 超时未结束的执行被中断，其任务租约释放回 PENDING，保证不丢任务、不卡中间状态。</p>
 */
public class TaskWorkerPool {

    private static final Logger log = LoggerFactory.getLogger(TaskWorkerPool.class);

    private final TaskQueueService queue;
    private final TaskQueueProperties properties;
    private final Map<String, TaskHandler> handlers;
    private final ExecutorService workers;
    private final ScheduledExecutorService heartbeat;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public TaskWorkerPool(TaskQueueService queue, TaskQueueProperties properties,
                          List<TaskHandler> handlerBeans) {
        this.queue = queue;
        this.properties = properties;
        this.handlers = handlerBeans.stream()
                .collect(Collectors.toMap(TaskWorkerPool::typeNameOf, h -> h, (a, b) -> a));
        this.workers = Executors.newFixedThreadPool(Math.max(1, properties.getConcurrency()),
                r -> {
                    Thread t = new Thread(r, "task-queue-worker");
                    t.setDaemon(true);
                    return t;
                });
        this.heartbeat = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "task-queue-heartbeat");
            t.setDaemon(true);
            return t;
        });
    }

    static String typeNameOf(TaskHandler handler) {
        if (handler instanceof NamedTaskHandler named) {
            return named.typeName();
        }
        if (handler.getClass().isAnnotationPresent(TaskType.class)) {
            return handler.getClass().getAnnotation(TaskType.class).value();
        }
        return defaultTypeName(handler);
    }

    private static String defaultTypeName(TaskHandler handler) {
        String name = handler.getClass().getSimpleName();
        if (name.endsWith("Handler")) {
            name = name.substring(0, name.length() - "Handler".length());
        }
        return name.replaceAll("([a-z0-9])([A-Z])", "$1-$2").toLowerCase();
    }

    @PostConstruct
    public void start() {
        running.set(true);
        int concurrency = Math.max(1, properties.getConcurrency());
        for (int i = 0; i < concurrency; i++) {
            final int workerIndex = i + 1;
            workers.submit(() -> workerLoop(workerIndex));
        }
        long leaseMs = properties.getLeaseDuration().toMillis();
        heartbeat.scheduleAtFixedRate(this::reclaimExpired, leaseMs,
                Math.max(500, leaseMs / 3), TimeUnit.MILLISECONDS);
        log.info("任务消费者工作池已启动，并发度 {}，已注册处理器: {}", concurrency, handlers.keySet());
    }

    private void workerLoop(int workerIndex) {
        while (running.get()) {
            Task task;
            try {
                task = queue.claimNext(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (task == null) {
                continue;
            }
            try {
                execute(workerIndex, task);
            } catch (Throwable t) {
                log.error("worker#{} 处理任务 {} 时发生未捕获异常（该任务依赖租约过期回收重投）",
                        workerIndex, task.getId(), t);
            }
        }
    }

    private void execute(int workerIndex, Task task) {
        String taskId = task.getId();
        Optional<TaskHandler> handler = Optional.ofNullable(handlers.get(task.getType()));
        if (handler.isEmpty()) {
            log.error("任务 {} 的类型 '{}' 没有注册处理器，判定为不可重试失败并进入死信",
                    taskId, task.getType());
            queue.reportFailure(taskId, false, "没有注册的任务处理器: " + task.getType());
            return;
        }

        long leaseMs = properties.getLeaseDuration().toMillis();
        ScheduledExecutorService singleHeartbeat = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "task-heartbeat-" + taskId.substring(0, 8));
            t.setDaemon(true);
            return t;
        });
        singleHeartbeat.scheduleAtFixedRate(() -> {
            try {
                queue.renewLease(taskId);
            } catch (RuntimeException e) {
                log.debug("任务 {} 续租失败（可能已终结）: {}", taskId, e.getMessage());
            }
        }, leaseMs / 3, leaseMs / 3, TimeUnit.MILLISECONDS);

        TaskOutcome outcome = null;
        boolean interruptedDuringShutdown = false;
        try {
            log.info("worker#{} 开始执行任务 id={} type={} attempt={}",
                    workerIndex, taskId, task.getType(), task.getAttempts());
            outcome = handler.get().handle(new TaskContext(task, task.getLeaseExpiresAt()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (!running.get()) {
                // 优雅停机触发的中断：不按失败计入尝试次数，直接释放租约，任务下次立即可重新执行。
                interruptedDuringShutdown = true;
                outcome = null;
            } else {
                outcome = TaskOutcome.retryable("执行线程被中断");
            }
        } catch (Exception e) {
            outcome = TaskOutcome.retryable(e.getClass().getSimpleName()
                    + (e.getMessage() == null ? "" : ": " + e.getMessage()));
        } finally {
            singleHeartbeat.shutdownNow();
        }

        try {
            if (outcome == null) {
                queue.releaseLease(taskId);
            } else {
                log.debug("worker#{} 上报任务 {} 的执行结果 {}", workerIndex, taskId,
                        outcome.getClass().getSimpleName());
                outcome.applyTo(queue, taskId);
            }
        } catch (IllegalStateException e) {
            // 任务可能已因租约过期被回收（本次执行结果不再可信），记录即可，避免任务被重复确认。
            log.warn("worker#{} 对任务 {} 的执行结果被拒绝: {}", workerIndex, taskId, e.getMessage());
            if (interruptedDuringShutdown) {
                queue.releaseLease(taskId);
            }
        }
    }

    private void reclaimExpired() {
        try {
            int reclaimed = queue.reclaimExpiredLeases();
            if (reclaimed > 0) {
                log.warn("心跳巡检回收了 {} 个租约过期任务", reclaimed);
            }
        } catch (RuntimeException e) {
            log.error("租约回收巡检失败（不影响队列继续运行）", e);
        }
    }

    @PreDestroy
    public void shutdown() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        log.info("任务队列开始优雅停机：停止领取新任务，等待在途任务结束，最长 {} ...",
                properties.getShutdownTimeout());
        queue.signalAll();
        heartbeat.shutdownNow();
        workers.shutdown();
        boolean terminated;
        try {
            terminated = workers.awaitTermination(
                    properties.getShutdownTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            terminated = false;
        }
        if (!terminated) {
            List<Runnable> dropped = workers.shutdownNow();
            log.warn("优雅停机超时，强制中断剩余 {} 个消费者；在途任务将按租约/恢复语义重新投递",
                    dropped.size());
        }
        log.info("任务队列优雅停机完成：没有任务丢失，停机时仍在处理的任务会在下次启动时恢复");
    }

    /** 当前注册的任务类型 -> 处理器，供诊断接口使用。 */
    public Map<String, String> registeredTypes() {
        return handlers.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getClass().getName()));
    }
}
