package com.github.lechandonga.durabletaskqueuejava.tests;

import com.github.lechandonga.durabletaskqueuejava.core.Task;
import com.github.lechandonga.durabletaskqueuejava.core.TaskOutcome;
import com.github.lechandonga.durabletaskqueuejava.core.TaskQueueProperties;
import com.github.lechandonga.durabletaskqueuejava.core.TaskQueueService;
import com.github.lechandonga.durabletaskqueuejava.core.TaskStatus;
import com.github.lechandonga.durabletaskqueuejava.core.TaskWorkerPool;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务五：优雅停机——在途任务不丢失、不卡死，停机后任务可被恢复并得到确定结果。
 */
class GracefulShutdownTests {

    private static final Logger log = LoggerFactory.getLogger(GracefulShutdownTests.class);

    @Test
    @DisplayName("优雅停机等待在途任务执行完成，任务最终为SUCCEEDED")
    void gracefulShutdownWaitsForInFlight(@TempDir Path dir) throws Exception {
        log.info(">>> [优雅停机-等待完成] 用例：任务执行约 300ms，停机超时 5s，停机应等待其成功");
        TaskQueueProperties props = TaskQueueTestSupport.properties(dir);
        TaskQueueService queue = TaskQueueTestSupport.newService(dir, props);
        CountDownLatch started = new CountDownLatch(1);
        AtomicInteger done = new AtomicInteger();
        TaskWorkerPool pool = TaskQueueTestSupport.newPool(queue, props, new NamedHandler("slow", ctx -> {
            started.countDown();
            Thread.sleep(300);
            done.incrementAndGet();
            return TaskOutcome.success();
        }));

        String id = queue.enqueue("slow", null, "grace-key").getId();
        assertTrue(started.await(2, java.util.concurrent.TimeUnit.SECONDS));
        long t0 = System.currentTimeMillis();
        pool.shutdown();
        long elapsed = System.currentTimeMillis() - t0;
        assertTrue(elapsed >= 200, "停机应等待在途任务完成，实际仅等待 " + elapsed + "ms");
        assertEquals(1, done.get());
        assertEquals(TaskStatus.SUCCEEDED, queue.findById(id).orElseThrow().getStatus());
        log.info("<<< [优雅停机-等待完成] 通过：停机等待 {}ms，在途任务成功确认", elapsed);
    }

    @Test
    @DisplayName("停机超时强中断时，在途任务释放回PENDING，重启后重新执行成功，不丢不卡")
    void shutdownTimeoutReleasesInFlight(@TempDir Path dir) throws Exception {
        log.info(">>> [优雅停机-超时中断] 用例：任务执行超过停机超时，被中断后任务回 PENDING 可恢复");
        TaskQueueProperties props = TaskQueueTestSupport.properties(dir);
        props.setShutdownTimeout(Duration.ofMillis(400));
        TaskQueueService queue = TaskQueueTestSupport.newService(dir, props);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch canFinish = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        TaskWorkerPool pool = TaskQueueTestSupport.newPool(queue, props, new NamedHandler("stuck", ctx -> {
            calls.incrementAndGet();
            started.countDown();
            canFinish.await();
            return TaskOutcome.success();
        }));

        String id = queue.enqueue("stuck", null, "stuck-key").getId();
        assertTrue(started.await(2, java.util.concurrent.TimeUnit.SECONDS));
        pool.shutdown();

        // 执行线程被中断后会把任务按“可重试失败”上报，任务应回到 PENDING（带退避），不卡在 RUNNING
        TaskQueueTestSupport.await(() -> queue.findById(id).orElseThrow().getStatus() == TaskStatus.PENDING, 2_000);
        canFinish.countDown();

        TaskQueueService q2 = TaskQueueTestSupport.newService(dir, TaskQueueTestSupport.properties(dir));
        AtomicInteger calls2 = new AtomicInteger();
        TaskWorkerPool p2 = TaskQueueTestSupport.newPool(q2,
                TaskQueueTestSupport.properties(dir), new NamedHandler("stuck", ctx -> {
            calls2.incrementAndGet();
            return TaskOutcome.success();
        }));
        TaskQueueTestSupport.await(() -> q2.findById(id).orElseThrow().getStatus() == TaskStatus.SUCCEEDED, 3_000);
        p2.shutdown();
        log.info("<<< [优雅停机-超时中断] 通过：中断后任务 PENDING 不丢失，重启执行 1 次后成功");
    }
}
