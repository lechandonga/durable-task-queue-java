package com.github.lechandonga.durabletaskqueuejava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.durabletaskqueuejava.core.TaskHandler;
import com.github.lechandonga.durabletaskqueuejava.core.TaskQueueProperties;
import com.github.lechandonga.durabletaskqueuejava.core.TaskQueueService;
import com.github.lechandonga.durabletaskqueuejava.core.TaskWorkerPool;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

/**
 * 测试工具：构造使用临时目录、短退避、短租约的本地队列（无任何外部服务依赖）。
 */
final class TaskQueueTestSupport {

    private TaskQueueTestSupport() {
    }

    static TaskQueueProperties properties(Path dataDir) {
        TaskQueueProperties p = new TaskQueueProperties();
        p.setDataDir(dataDir.toString());
        p.setCapacity(10);
        p.setMaxAttempts(3);
        p.setBaseBackoff(Duration.ofMillis(50));
        p.setBackoffMultiplier(2.0);
        p.setBackoffJitter(0.0);
        p.setLeaseDuration(Duration.ofSeconds(2));
        p.setConcurrency(1);
        p.setShutdownTimeout(Duration.ofSeconds(5));
        p.setCompactThresholdBytes(-1);
        return p;
    }

    static TaskQueueService newService(Path dataDir, TaskQueueProperties properties) {
        TaskQueueService service = new TaskQueueService(dataDir == null ? properties : withDir(properties, dataDir),
                new ObjectMapper());
        service.start();
        return service;
    }

    private static TaskQueueProperties withDir(TaskQueueProperties source, Path dataDir) {
        source.setDataDir(dataDir.toString());
        return source;
    }

    static TaskWorkerPool newPool(TaskQueueService service, TaskQueueProperties properties,
                                  TaskHandler... handlers) {
        TaskWorkerPool pool = new TaskWorkerPool(service, properties, List.of(handlers));
        pool.start();
        return pool;
    }

    static void await(BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        if (!condition.getAsBoolean()) {
            throw new AssertionError("等待条件在 " + timeoutMs + "ms 内未满足");
        }
    }

    @FunctionalInterface
    interface BooleanSupplier {
        boolean getAsBoolean() throws InterruptedException;
    }
}
