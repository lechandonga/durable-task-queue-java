package com.github.lechandonga.durabletaskqueuejava.tests;

import com.github.lechandonga.durabletaskqueuejava.core.RejectReason;
import com.github.lechandonga.durabletaskqueuejava.core.Task;
import com.github.lechandonga.durabletaskqueuejava.core.TaskQueueProperties;
import com.github.lechandonga.durabletaskqueuejava.core.TaskQueueService;
import com.github.lechandonga.durabletaskqueuejava.core.TaskRejectedException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务一：任务提交、按标识查询、并发消费前置约束。
 * 覆盖用例：普通提交与查询；并发重复提交只保留一条；串行重复提交被明确拒绝；
 * 容量超限给出 QUEUE_FULL；两种拒绝原因可区分。
 */
class EnqueueAndIdempotencyTests {

    private static final Logger log = LoggerFactory.getLogger(EnqueueAndIdempotencyTests.class);

    @Test
    @DisplayName("提交后可按任务id与幂等键查询到同一条任务")
    void submitAndQuery(@TempDir Path dir) {
        log.info(">>> [提交/查询] 用例：提交任务后按 id 和幂等键查询");
        TaskQueueProperties props = TaskQueueTestSupport.properties(dir);
        TaskQueueService queue = TaskQueueTestSupport.newService(dir, props);

        Task task = queue.enqueue("echo", "hello", "key-1");
        assertNotNull(task.getId());
        assertEquals("key-1", queue.findById(task.getId()).orElseThrow().getIdempotencyKey());
        assertEquals(task.getId(), queue.findByIdempotencyKey("key-1").orElseThrow().getId());
        log.info("<<< [提交/查询] 通过：id={} 与幂等键 key-1 指向同一任务", task.getId());
    }

    @Test
    @DisplayName("相同幂等键并发提交(50线程)：成功仅1条，其余全部 DUPLICATE_ACTIVE_KEY")
    void concurrentDuplicateSubmit(@TempDir Path dir) throws Exception {
        log.info(">>> [并发幂等] 用例：50 个线程用同一幂等键并发提交，只能保留一条");
        TaskQueueProperties props = TaskQueueTestSupport.properties(dir);
        TaskQueueService queue = TaskQueueTestSupport.newService(dir, props);

        int threads = 50;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        AtomicInteger accepted = new AtomicInteger();
        ConcurrentLinkedQueue<String> acceptedIds = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<RejectReason> reasons = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    Task t = queue.enqueue("echo", "p", "concurrent-key");
                    accepted.incrementAndGet();
                    acceptedIds.add(t.getId());
                } catch (TaskRejectedException e) {
                    reasons.add(e.getReason());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS), "并发提交应全部返回");
        pool.shutdown();

        assertEquals(1, accepted.get(), "并发重复提交只能成功 1 条");
        assertEquals(threads - 1, reasons.size());
        assertTrue(reasons.stream().allMatch(r -> r == RejectReason.DUPLICATE_ACTIVE_KEY),
                "其余提交必须全部以 DUPLICATE_ACTIVE_KEY 拒绝，实际: " + reasons);
        assertEquals(1, acceptedIds.stream().distinct().count());
        log.info("<<< [并发幂等] 通过：成功 1 条 (id={})，拒绝 {} 条且原因全部为 DUPLICATE_ACTIVE_KEY",
                acceptedIds.peek(), reasons.size());
    }

    @Test
    @DisplayName("容量超限拒绝原因为 QUEUE_FULL，与重复提交原因可区分")
    void capacityFullAndDuplicateReasonsDiffer(@TempDir Path dir) {
        log.info(">>> [拒绝原因] 用例：打满容量得到 QUEUE_FULL，重复提交得到 DUPLICATE_ACTIVE_KEY");
        TaskQueueProperties props = TaskQueueTestSupport.properties(dir);
        props.setCapacity(3);
        TaskQueueService queue = TaskQueueTestSupport.newService(dir, props);

        queue.enqueue("echo", null, "k1");
        queue.enqueue("echo", null, "k2");
        queue.enqueue("echo", null, "k3");

        TaskRejectedException full = assertThrows(TaskRejectedException.class,
                () -> queue.enqueue("echo", null, "k4"));
        assertEquals(RejectReason.QUEUE_FULL, full.getReason());

        TaskRejectedException dup = assertThrows(TaskRejectedException.class,
                () -> queue.enqueue("echo", null, "k1"));
        assertEquals(RejectReason.DUPLICATE_ACTIVE_KEY, dup.getReason());
        assertEquals(queue.findByIdempotencyKey("k1").orElseThrow().getId(),
                dup.getExistingTaskId());
        log.info("<<< [拒绝原因] 通过：第 4 条拒绝为 {}，重复提交拒绝为 {}，两者可区分，均未静默丢弃",
                full.getReason(), dup.getReason());
    }

    @Test
    @DisplayName("无幂等键任务不受去重影响，终态后同键允许重新提交(由重试死信用例间接覆盖)")
    void noKeyTasksCoexist(@TempDir Path dir) {
        log.info(">>> [无键提交] 用例：不带幂等键的多条任务可以共存");
        TaskQueueProperties props = TaskQueueTestSupport.properties(dir);
        TaskQueueService queue = TaskQueueTestSupport.newService(dir, props);
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            ids.add(queue.enqueue("echo", null, null).getId());
        }
        assertEquals(5, ids.stream().distinct().count());
        assertEquals(5, queue.metrics().active());
        log.info("<<< [无键提交] 通过：5 条无键任务全部共存，活跃数=5");
    }
}
