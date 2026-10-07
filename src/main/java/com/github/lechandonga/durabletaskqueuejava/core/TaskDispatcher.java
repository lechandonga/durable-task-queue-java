package com.github.lechandonga.durabletaskqueuejava.core;

import com.github.lechandonga.durabletaskqueuejava.config.TaskQueueProperties;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 消费者调度器：固定数量的工作线程并发领取并执行任务，
 * 每个在途任务由单线程调度器周期性发送心跳续约。
 * <p>
 * 优雅停机顺序：拒绝新领取 -> 等待在途任务执行完毕（或超时）->
 * 仍未终结的 RUNNING 任务由 {@link TaskQueue#requeueRunningTasks} 兜底回收，
 * 保证不丢任务、不卡在中间状态。
 */
public class TaskDispatcher {

    private static final Logger log = LoggerFactory.getLogger(TaskDispatcher.class);

    private final TaskQueue queue;
    private final TaskHandlerRegistry registry;
    private final TaskQueueProperties properties;

    private ExecutorService workers;
    private ScheduledExecutorService heartbeats;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean forced = new AtomicBoolean(false);
    

    public TaskDispatcher(TaskQueue queue, TaskHandlerRegistry registry,
                          TaskQueueProperties properties) {
        this.queue = queue;
        this.registry = registry;
        this.properties = properties;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        int concurrency = Math.max(1, properties.getConcurrency());
        workers = Executors.newFixedThreadPool(concurrency, runnable -> {
            Thread thread = new Thread(runnable, "task-worker");
            thread.setDaemon(true);
            return thread;
        });
        heartbeats = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "task-heartbeat");
            thread.setDaemon(true);
            return thread;
        });
        for (int i = 0; i < concurrency; i++) {
            workers.submit(this::workerLoop);
        }
        log.info("Task dispatcher started with {} consumer(s)", concurrency);
    }

    /** Spring 容器关闭时执行优雅停机。 */
    @PreDestroy
    public void onContainerShutdown() {
        shutdown();
    }

    public boolean isRunning() {
        return running.get();
    }


    private void workerLoop() {
        String workerName = Thread.currentThread().getName() + "-" + Thread.currentThread().getId();
        while (running.get()) {
            try {
                Optional<TaskRecord> claimed = queue.claim();
                if (claimed.isEmpty()) {
                    sleepQuietly(queue.nextDelayMillis());
                    continue;
                }
                execute(claimed.get(), workerName);
            } catch (Exception e) {
                log.error("Unexpected error in worker loop of {}", workerName, e);
                sleepQuietly(properties.getPollInterval().toMillis());
            }
        }
    }

    private void execute(TaskRecord task, String workerName) {
        String leaseOwner = task.getLeasedBy();
        AtomicBoolean leaseLost = new AtomicBoolean(false);
        long heartbeatPeriodMillis = Math.max(1, properties.getLeaseTtl().toMillis() / 3);
        var heartbeatFuture = heartbeats.scheduleAtFixedRate(() -> {
            try {
                if (!queue.heartbeat(task.getId(), leaseOwner)) {
                    leaseLost.set(true);
                }
            } catch (Exception e) {
                log.warn("Heartbeat failed for task {}", task.getId(), e);
            }
        }, heartbeatPeriodMillis, heartbeatPeriodMillis, TimeUnit.MILLISECONDS);

        log.info("Executing task: id={}, type={}, worker={}", task.getId(), task.getType(), workerName);
        try {
            if (registry.find(task.getType()).isEmpty()) {
                completeFailure(task, leaseOwner, false,
                        "No handler registered for task type: " + task.getType());
                return;
            }
            TaskHandler handler = registry.find(task.getType()).orElseThrow();
            try {
                handler.handle(task.getPayload());
                safelyReportSuccess(task, leaseOwner, leaseLost);
            } catch (TaskHandler.TransientTaskException transientError) {
                safelyReportFailure(task, leaseOwner, leaseLost, true,
                        transientError.getClass().getSimpleName() + ": "
                                + transientError.getMessage());
            } catch (Exception otherError) {
                safelyReportFailure(task, leaseOwner, leaseLost, false,
                        otherError.getClass().getSimpleName() + ": " + otherError.getMessage());
            }
        } finally {
            heartbeatFuture.cancel(false);
        }
    }

    private void completeFailure(TaskRecord task, String leaseOwner, boolean retryable,
                                 String message) {
        queue.fail(task.getId(), leaseOwner, retryable, message);
    }

    private void safelyReportSuccess(TaskRecord task, String leaseOwner,
                                     AtomicBoolean leaseLost) {
        if (forced.get() || leaseLost.get()) {
            log.info("Task {} finished during shutdown/reclaim, leaving it for redelivery",
                    task.getId());
            return;
        }
        if (!queue.succeed(task.getId(), leaseOwner)) {
            log.warn("Task {} could not be acknowledged (lease changed)", task.getId());
        }
    }

    private void safelyReportFailure(TaskRecord task, String leaseOwner,
                                     AtomicBoolean leaseLost, boolean retryable,
                                     String message) {
        if (forced.get() || leaseLost.get()) {
            log.info("Task {} failed during shutdown/reclaim, leaving it for redelivery: {}",
                    task.getId(), message);
            return;
        }
        queue.fail(task.getId(), leaseOwner, retryable, message);
    }

    /**
     * 优雅停机。幂等，可被 Spring 生命周期与测试重复调用。
     */
    public void shutdown() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        log.info("Shutting down task dispatcher, no new tasks will be claimed");
        queue.shutdown();
        workers.shutdown();
        heartbeats.shutdown();
        try {
            if (!workers.awaitTermination(properties.getShutdownAwait().toMillis(),
                    TimeUnit.MILLISECONDS)) {
                log.warn("In-flight tasks did not finish within {}, forcing requeue",
                        properties.getShutdownAwait());
                forced.set(true);
                workers.shutdownNow();
                workers.awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            workers.shutdownNow();
        } finally {
            int requeued = queue.requeueRunningTasks("graceful shutdown");
            heartbeats.shutdownNow();
            log.info("Task dispatcher stopped, {} in-flight task(s) requeued", requeued);
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(Math.max(1, millis));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
