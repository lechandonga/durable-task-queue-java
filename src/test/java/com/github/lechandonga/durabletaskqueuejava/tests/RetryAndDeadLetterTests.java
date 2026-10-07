package com.github.lechandonga.durabletaskqueuejava.tests;

import com.github.lechandonga.durabletaskqueuejava.core.Task;
import com.github.lechandonga.durabletaskqueuejava.core.TaskContext;
import com.github.lechandonga.durabletaskqueuejava.core.TaskHandler;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务二：失败分类、递增退避重试、最大尝试次数、死信与毒消息不阻塞。
 */
class RetryAndDeadLetterTests {

    private static final Logger log = LoggerFactory.getLogger(RetryAndDeadLetterTests.class);

    @Test
    @DisplayName("可重试失败按指数退避重试，超过最大尝试次数(3)后进入死信并保留最后原因")
    void retryWithBackoffThenDeadLetter(@TempDir Path dir) throws Exception {
        log.info(">>> [重试->死信] 用例：处理器始终报可重试失败，maxAttempts=3，验证退避递增与死信");
        TaskQueueProperties props = TaskQueueTestSupport.properties(dir);
        props.setConcurrency(1);
        TaskQueueService queue = TaskQueueTestSupport.newService(dir, props);
        AtomicInteger attempts = new AtomicInteger();
        long[] startTimes = new long[4];

        TaskHandler alwaysFail = ctx -> {
            int n = attempts.incrementAndGet();
            startTimes[Math.min(n, 3)] = System.currentTimeMillis();
            return TaskOutcome.retryable("模拟临时失败#" + n);
        };
        TaskWorkerPool pool = TaskQueueTestSupport.newPool(queue, props, new NamedHandler("flaky", alwaysFail));

        String id = queue.enqueue("flaky", null, "retry-key").getId();
        TaskQueueTestSupport.await(() -> queue.findById(id).orElseThrow().getStatus() == TaskStatus.DEAD, 5_000);
        pool.shutdown();

        Task dead = queue.findById(id).orElseThrow();
        assertEquals(TaskStatus.DEAD, dead.getStatus());
        assertEquals(3, dead.getAttempts(), "最大尝试次数应为 3");
        assertTrue(dead.getLastError().contains("模拟临时失败#3"), "死信必须保留最后一次失败原因");
        long gap1 = startTimes[2] - startTimes[1];
        long gap2 = startTimes[3] - startTimes[2];
        assertTrue(gap1 >= 45, "第一次退避应约为 50ms，实际 " + gap1);
        assertTrue(gap2 >= 95, "第二次退避应约为 100ms（递增），实际 " + gap2);
        assertTrue(gap2 > gap1, "退避必须递增: %d -> %d".formatted(gap1, gap2));
        log.info("<<< [重试->死信] 通过：尝试 3 次，退避 {}ms -> {}ms 递增，死信原因保留: {}",
                gap1, gap2, dead.getLastError());
    }

    @Test
    @DisplayName("不可重试失败直接进入死信，不进行重试")
    void nonRetryableGoesDeadImmediately(@TempDir Path dir) throws Exception {
        log.info(">>> [不可重试] 用例：处理器返回 dead，首次执行即进死信");
        TaskQueueProperties props = TaskQueueTestSupport.properties(dir);
        TaskQueueService queue = TaskQueueTestSupport.newService(dir, props);
        AtomicInteger calls = new AtomicInteger();
        TaskHandler fatal = ctx -> {
            calls.incrementAndGet();
            return TaskOutcome.dead("参数非法，不可重试");
        };
        TaskWorkerPool pool = TaskQueueTestSupport.newPool(queue, props, new NamedHandler("fatal", fatal));

        String id = queue.enqueue("fatal", null, "fatal-key").getId();
        TaskQueueTestSupport.await(() -> queue.findById(id).orElseThrow().getStatus() == TaskStatus.DEAD, 3_000);
        pool.shutdown();

        Task dead = queue.findById(id).orElseThrow();
        assertEquals(1, dead.getAttempts());
        assertEquals(1, calls.get());
        assertEquals("参数非法，不可重试", dead.getLastError());
        log.info("<<< [不可重试] 通过：仅执行 1 次即进入死信，原因: {}", dead.getLastError());
    }

    @Test
    @DisplayName("毒消息(持续失败)不阻塞其他任务：毒消息死信后，后续任务照常成功")
    void poisonMessageDoesNotBlockOthers(@TempDir Path dir) throws Exception {
        log.info(">>> [毒消息隔离] 用例：一个永远失败的任务不得阻塞同队列的正常任务");
        TaskQueueProperties props = TaskQueueTestSupport.properties(dir);
        props.setBaseBackoff(java.time.Duration.ofMillis(10));
        props.setConcurrency(1);
        TaskQueueService queue = TaskQueueTestSupport.newService(dir, props);
        AtomicInteger poisonAttempts = new AtomicInteger();
        AtomicInteger goodDone = new AtomicInteger();

        TaskHandler mixed = ctx -> {
            if ("poison".equals(ctx.getPayload())) {
                poisonAttempts.incrementAndGet();
                return TaskOutcome.retryable("毒消息持续失败");
            }
            goodDone.incrementAndGet();
            return TaskOutcome.success();
        };
        TaskWorkerPool pool = TaskQueueTestSupport.newPool(queue, props, new NamedHandler("mixed", mixed));

        String poisonId = queue.enqueue("mixed", "poison", null).getId();
        // 在毒消息重试间隙插入正常任务
        Thread.sleep(30);
        String goodId1 = queue.enqueue("mixed", "good-1", null).getId();
        String goodId2 = queue.enqueue("mixed", "good-2", "good-key").getId();

        TaskQueueTestSupport.await(() -> queue.findById(poisonId).orElseThrow().getStatus() == TaskStatus.DEAD, 5_000);
        TaskQueueTestSupport.await(() -> goodDone.get() >= 2, 5_000);
        pool.shutdown();

        assertEquals(TaskStatus.DEAD, queue.findById(poisonId).orElseThrow().getStatus());
        assertEquals(TaskStatus.SUCCEEDED, queue.findById(goodId1).orElseThrow().getStatus());
        assertEquals(TaskStatus.SUCCEEDED, queue.findById(goodId2).orElseThrow().getStatus());
        assertNotNull(queue.findByIdempotencyKey("good-key").orElseThrow());
        log.info("<<< [毒消息隔离] 通过：毒消息尝试 {} 次后死信，期间 2 个正常任务成功，未被阻塞",
                poisonAttempts.get());
    }

    @Test
    @DisplayName("处理器抛出未捕获异常等价于可重试失败，最终仍能成功")
    void exceptionMeansRetryableThenSuccess(@TempDir Path dir) throws Exception {
        log.info(">>> [异常重试] 用例：处理器前两次抛异常，第三次成功");
        TaskQueueProperties props = TaskQueueTestSupport.properties(dir);
        props.setBaseBackoff(java.time.Duration.ofMillis(10));
        TaskQueueService queue = TaskQueueTestSupport.newService(dir, props);
        AtomicInteger calls = new AtomicInteger();
        TaskHandler flaky = ctx -> {
            if (calls.incrementAndGet() < 3) {
                throw new IllegalStateException("boom-" + calls.get());
            }
            return TaskOutcome.success();
        };
        TaskWorkerPool pool = TaskQueueTestSupport.newPool(queue, props, new NamedHandler("boom", flaky));

        String id = queue.enqueue("boom", null, null).getId();
        TaskQueueTestSupport.await(() -> queue.findById(id).orElseThrow().getStatus() == TaskStatus.SUCCEEDED, 5_000);
        pool.shutdown();

        Task done = queue.findById(id).orElseThrow();
        assertEquals(3, done.getAttempts());
        assertEquals(TaskStatus.SUCCEEDED, done.getStatus());
        log.info("<<< [异常重试] 通过：2 次异常按可重试失败退避，第 3 次成功，共尝试 3 次");
    }
}
