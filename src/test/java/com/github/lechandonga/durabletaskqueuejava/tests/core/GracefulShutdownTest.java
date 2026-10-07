package com.github.lechandonga.durabletaskqueuejava.tests.core;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lechandonga.durabletaskqueuejava.config.TaskQueueProperties;
import com.github.lechandonga.durabletaskqueuejava.core.TaskDispatcher;
import com.github.lechandonga.durabletaskqueuejava.core.TaskQueue;
import com.github.lechandonga.durabletaskqueuejava.core.TaskStatus;
import com.github.lechandonga.durabletaskqueuejava.tests.support.BusinessScenario;
import com.github.lechandonga.durabletaskqueuejava.tests.support.ManualClock;
import com.github.lechandonga.durabletaskqueuejava.tests.support.QueueTestSupport;
import com.github.lechandonga.durabletaskqueuejava.tests.support.RecordingHandler;
import com.github.lechandonga.durabletaskqueuejava.tests.support.ScenarioLoggingExtension;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * 业务：优雅停机。在途任务要么执行到终态，要么被安全退回队列，重启后一定有确定结果。
 */
@BusinessScenario("优雅停机与在途任务回收")
@ExtendWith(ScenarioLoggingExtension.class)
class GracefulShutdownTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("停机时正在执行的任务正常跑完并确认成功，停机后不接受新提交")
    void inFlightTaskCompletesDuringGracefulShutdown() throws Exception {
        TaskQueueProperties properties = QueueTestSupport.fastProperties();
        ManualClock clock = new ManualClock();
        TaskQueue queue = QueueTestSupport.newQueue(tempDir, properties, clock);

        CountDownLatch handlerEntered = new CountDownLatch(1);
        CountDownLatch allowFinish = new CountDownLatch(1);
        RecordingHandler handler = new RecordingHandler("slow.echo");
        handler.setBarrier(() -> {
            handlerEntered.countDown();
            allowFinish.await(10, TimeUnit.SECONDS);
        });
        TaskDispatcher dispatcher = QueueTestSupport.newDispatcher(queue, properties,
                List.of(handler));

        queue.submit("slow-key", "slow.echo", "payload");
        assertTrue(handlerEntered.await(10, TimeUnit.SECONDS));

        Thread shutdownThread = new Thread(dispatcher::shutdown);
        shutdownThread.start();
        // 给停机流程一点时间进入等待，然后放行任务
        Thread.sleep(200);
        allowFinish.countDown();
        shutdownThread.join(15_000);
        assertFalse(shutdownThread.isAlive(), "停机必须在宽限期内完成");

        assertEquals(TaskStatus.SUCCEEDED,
                queue.findByIdempotencyKey("slow-key").orElseThrow().getStatus());
        assertEquals(1, handler.executionCount());

        // 重启后终态保持，不再执行
        TaskQueue reopened = QueueTestSupport.reopenQueue(tempDir, properties, clock);
        assertTrue(reopened.claim().isEmpty());
        reopened.shutdown();
    }

    @Test
    @DisplayName("停机宽限期内未完成的在途任务被退回 PENDING，重启后可重新执行完成")
    void unfinishedTaskIsRequeuedAndCompletesAfterRestart() throws Exception {
        TaskQueueProperties properties = QueueTestSupport.fastProperties();
        ManualClock clock = new ManualClock();
        TaskQueue queue = QueueTestSupport.newQueue(tempDir, properties, clock);

        CountDownLatch handlerEntered = new CountDownLatch(1);
        CountDownLatch neverRelease = new CountDownLatch(1);
        RecordingHandler stuckHandler = new RecordingHandler("stuck.echo");
        stuckHandler.setBarrier(() -> {
            handlerEntered.countDown();
            neverRelease.await();
        });
        properties.setShutdownAwait(Duration.ofSeconds(1));
        TaskDispatcher dispatcher = QueueTestSupport.newDispatcher(queue, properties,
                List.of(stuckHandler));

        queue.submit("stuck-key", "stuck.echo", null);
        assertTrue(handlerEntered.await(10, TimeUnit.SECONDS));

        dispatcher.shutdown();
        assertEquals(TaskStatus.PENDING,
                queue.findByIdempotencyKey("stuck-key").orElseThrow().getStatus(),
                "卡在中间状态的任务停机时必须退回 PENDING");

        // 重启恢复：新的处理器执行成功，任务得到确定终态
        neverRelease.countDown();
        RecordingHandler healthy = new RecordingHandler("stuck.echo");
        TaskQueue reopened = QueueTestSupport.reopenQueue(tempDir, properties, clock);
        TaskDispatcher restarted = QueueTestSupport.newDispatcher(reopened, properties,
                List.of(healthy));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertEquals(TaskStatus.SUCCEEDED,
                        reopened.findByIdempotencyKey("stuck-key").orElseThrow().getStatus()));
        restarted.shutdown();
    }
}
