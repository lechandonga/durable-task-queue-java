package com.github.lechandonga.durabletaskqueuejava.tests.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lechandonga.durabletaskqueuejava.config.TaskQueueProperties;
import com.github.lechandonga.durabletaskqueuejava.core.TaskQueue;
import com.github.lechandonga.durabletaskqueuejava.core.TaskRecord;
import com.github.lechandonga.durabletaskqueuejava.core.TaskStatus;
import com.github.lechandonga.durabletaskqueuejava.tests.support.BusinessScenario;
import com.github.lechandonga.durabletaskqueuejava.tests.support.ManualClock;
import com.github.lechandonga.durabletaskqueuejava.tests.support.QueueTestSupport;
import com.github.lechandonga.durabletaskqueuejava.tests.support.ScenarioLoggingExtension;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * 业务：失败分类、指数退避重试、超过最大尝试次数进入死信并保留最后原因。
 */
@BusinessScenario("失败重试与死信判定")
@ExtendWith(ScenarioLoggingExtension.class)
class RetryDeadLetterTest {

    @TempDir
    Path tempDir;

    private TaskQueueProperties properties;
    private ManualClock clock;
    private TaskQueue queue;

    @BeforeEach
    void setUp() {
        properties = QueueTestSupport.fastProperties();
        clock = new ManualClock();
        queue = QueueTestSupport.newQueue(tempDir, properties, clock);
    }

    @Test
    @DisplayName("退避延迟按倍数递增并受最大退避封顶")
    void backoffGrowsExponentially() {
        properties.setMaxAttempts(10);
        TaskQueue q = QueueTestSupport.newQueue(tempDir.resolve("backoff"), properties, clock);
        long first = q.backoffDelayMillis(1);
        long second = q.backoffDelayMillis(2);
        long third = q.backoffDelayMillis(3);
        assertEquals(100, first);
        assertEquals(200, second);
        assertEquals(400, third);
        assertEquals(properties.getMaxBackoff().toMillis(),
                q.backoffDelayMillis(20), "退避必须被 maxBackoff 封顶");
        q.shutdown();
    }

    @Test
    @DisplayName("可重试失败按退避重新变为可领取，成功后不再重试")
    void retryableFailureIsRetriedWithBackoff() {
        queue.submit("retry-key", "demo.echo", null);

        TaskRecord first = queue.claim().orElseThrow();
        assertEquals(1, first.getAttempts());
        queue.fail(first.getId(), first.getLeasedBy(), true, "boom-1");

        assertTrue(queue.claim().isEmpty(), "退避未到期前不得被再次领取");
        clock.advanceMillis(99);
        assertTrue(queue.claim().isEmpty(), "退避窗口内不得被再次领取");
        clock.advanceMillis(2);
        TaskRecord second = queue.claim().orElseThrow();
        assertEquals(2, second.getAttempts());
        assertEquals("boom-1", queue.findById(second.getId()).orElseThrow().getLastError());

        assertTrue(queue.succeed(second.getId(), second.getLeasedBy()));
        assertEquals(TaskStatus.SUCCEEDED,
                queue.findById(second.getId()).orElseThrow().getStatus());
        assertTrue(queue.claim().isEmpty(), "成功任务不得重复投递");
    }

    @Test
    @DisplayName("可重试失败达到最大尝试次数后进入死信，保留最后一次失败原因")
    void retriesExhaustedMovesToDeadLetter() {
        queue.submit("exhaust-key", "demo.echo", null);
        TaskRecord claimed;
        for (int attempt = 1; attempt <= properties.getMaxAttempts(); attempt++) {
            if (attempt > 1) {
                clock.advanceMillis(properties.getMaxBackoff().toMillis());
            }
            claimed = queue.claim().orElseThrow();
            assertEquals(attempt, claimed.getAttempts());
            queue.fail(claimed.getId(), claimed.getLeasedBy(), true,
                    "retryable failure #" + attempt);
        }
        TaskRecord dead = queue.findByIdempotencyKey("exhaust-key").orElseThrow();
        assertEquals(TaskStatus.DEAD, dead.getStatus());
        assertEquals(properties.getMaxAttempts(), dead.getAttempts());
        assertEquals("retryable failure #" + properties.getMaxAttempts(),
                dead.getLastError());
        assertEquals(0, queue.activeCount());
        clock.advanceMillis(properties.getMaxBackoff().toMillis());
        assertTrue(queue.claim().isEmpty(), "死信不得再被投递");
    }

    @Test
    @DisplayName("不可重试失败直接进入死信，不消耗后续尝试机会")
    void nonRetryableFailureGoesStraightToDeadLetter() {
        queue.submit("fatal-key", "demo.echo", null);
        TaskRecord claimed = queue.claim().orElseThrow();
        queue.fail(claimed.getId(), claimed.getLeasedBy(), false, "poison message");

        TaskRecord dead = queue.findByIdempotencyKey("fatal-key").orElseThrow();
        assertEquals(TaskStatus.DEAD, dead.getStatus());
        assertEquals(1, dead.getAttempts());
        assertEquals("poison message", dead.getLastError());
    }

    @Test
    @DisplayName("死信/反复失败的毒消息不会阻塞其他任务")
    void poisonMessageDoesNotBlockOthers() {
        queue.submit("poison", "demo.echo", null);
        queue.submit("healthy-1", "demo.echo", null);
        queue.submit("healthy-2", "demo.echo", null);

        TaskRecord poison = queue.claim().orElseThrow();
        assertEquals("poison", poison.getIdempotencyKey());
        queue.fail(poison.getId(), poison.getLeasedBy(), false, "poison");

        queue.claim().orElseThrow();
        queue.claim().orElseThrow();
        assertEquals(1, queue.listByStatus(TaskStatus.DEAD).size());
        assertEquals(2, queue.listByStatus(TaskStatus.RUNNING).size());
        assertFalse(queue.listByStatus(TaskStatus.PENDING).stream()
                .anyMatch(r -> "poison".equals(r.getIdempotencyKey())));
    }

    @Test
    @DisplayName("死信任务重新入队后尝试次数清零并可重新执行")
    void deadLetterCanBeRequeued() {
        queue.submit("dlq-key", "demo.echo", null);
        TaskRecord claimed = queue.claim().orElseThrow();
        queue.fail(claimed.getId(), claimed.getLeasedBy(), false, "fatal");
        String deadId = queue.findByIdempotencyKey("dlq-key").orElseThrow().getId();

        TaskRecord fresh = queue.requeueDeadLetter(deadId);
        assertEquals(0, fresh.getAttempts());
        assertEquals(TaskStatus.PENDING, fresh.getStatus());
        TaskRecord retried = queue.claim().orElseThrow();
        assertTrue(queue.succeed(retried.getId(), retried.getLeasedBy()));
        assertEquals(TaskStatus.SUCCEEDED,
                queue.findByIdempotencyKey("dlq-key").orElseThrow().getStatus());
    }
}
