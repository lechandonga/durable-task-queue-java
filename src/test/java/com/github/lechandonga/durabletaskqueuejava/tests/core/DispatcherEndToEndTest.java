package com.github.lechandonga.durabletaskqueuejava.tests.core;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * 业务：调度器端到端链路——提交 -> 自动消费 -> 瞬时失败重试 -> 成功 / 死信，
 * 毒消息不阻塞其他任务。
 */
@BusinessScenario("入队消费到死信端到端链路")
@ExtendWith(ScenarioLoggingExtension.class)
class DispatcherEndToEndTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("系统时钟下：瞬时失败重试后成功、不可重试立即死信、其他任务不被阻塞")
    void endToEndWithSystemClock() {
        TaskQueueProperties properties = QueueTestSupport.fastProperties();
        properties.setBackoff(Duration.ofMillis(20));
        properties.setMaxAttempts(3);
        properties.setLeaseTtl(Duration.ofSeconds(5));
        TaskQueue queue = QueueTestSupport.newQueue(tempDir.resolve("e2e"), properties,
                java.time.Clock.systemDefaultZone());

        AtomicInteger fatalAttempts = new AtomicInteger();
        RecordingHandler flaky = new RecordingHandler("flaky.echo",
                attempt -> fatalAttempts.incrementAndGet() <= 2, false) {
        };
        RecordingHandler poison = new RecordingHandler("poison.echo",
                attempt -> false, true);
        RecordingHandler echo = new RecordingHandler("demo.echo");
        TaskDispatcher dispatcher = QueueTestSupport.newDispatcher(queue, properties,
                List.of(flaky, poison, echo));

        queue.submit("flaky", "flaky.echo", null);
        queue.submit("poison", "poison.echo", null);
        queue.submit("healthy-1", "demo.echo", "a");
        queue.submit("healthy-2", "demo.echo", "b");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertEquals(TaskStatus.SUCCEEDED,
                    queue.findByIdempotencyKey("flaky").orElseThrow().getStatus());
            assertEquals(TaskStatus.DEAD,
                    queue.findByIdempotencyKey("poison").orElseThrow().getStatus());
            assertEquals(TaskStatus.SUCCEEDED,
                    queue.findByIdempotencyKey("healthy-1").orElseThrow().getStatus());
            assertEquals(TaskStatus.SUCCEEDED,
                    queue.findByIdempotencyKey("healthy-2").orElseThrow().getStatus());
        });

        assertEquals(3, flaky.executionCount(), "前两次瞬时失败，第三次成功");
        assertEquals(1, poison.executionCount(), "不可重试失败不重试");
        assertEquals(2, echo.executionCount());
        assertEquals("RuntimeException: fatal failure from handler",
                queue.listByStatus(TaskStatus.DEAD).get(0).getLastError());

        dispatcher.shutdown();
        assertEquals(0, queue.listByStatus(TaskStatus.RUNNING).size(),
                "停机后不存在卡在 RUNNING 的任务");
    }

    @Test
    @DisplayName("反复可重试失败的毒消息耗尽尝试后进入死信，期间不阻塞后续任务")
    void alwaysFailingTaskDiesAndOthersProgress() {
        TaskQueueProperties properties = QueueTestSupport.fastProperties();
        properties.setBackoff(Duration.ofMillis(10));
        properties.setMaxAttempts(2);
        TaskQueue queue = QueueTestSupport.newQueue(tempDir.resolve("poison"), properties,
                java.time.Clock.systemDefaultZone());

        RecordingHandler alwaysTransient = new RecordingHandler("retry-forever.echo",
                attempt -> true, false);
        RecordingHandler echo = new RecordingHandler("demo.echo");
        TaskDispatcher dispatcher = QueueTestSupport.newDispatcher(queue, properties,
                List.of(alwaysTransient, echo));

        queue.submit("poison", "retry-forever.echo", null);
        queue.submit("ok", "demo.echo", null);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertEquals(TaskStatus.DEAD,
                    queue.findByIdempotencyKey("poison").orElseThrow().getStatus());
            assertEquals(TaskStatus.SUCCEEDED,
                    queue.findByIdempotencyKey("ok").orElseThrow().getStatus());
        });
        assertEquals(2, alwaysTransient.executionCount());
        dispatcher.shutdown();
    }
}
