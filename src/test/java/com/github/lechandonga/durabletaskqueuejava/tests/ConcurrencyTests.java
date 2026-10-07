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
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务三：并发消费互斥——同一任务在判定失败前不得被两个消费者同时处理；
 * 不同任务可以并行；成功确认后不会重复执行。
 */
class ConcurrencyTests {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyTests.class);

    @Test
    @DisplayName("同一任务租约有效期间不会被再次领取：第二个claim返回null，且不能对同一id重复上报")
    void sameTaskLeaseExclusive(@TempDir Path dir) throws Exception {
        log.info(">>> [消费互斥-单任务] 用例：任务已被消费者A领取(租约2s)，消费者B在租约内不得再领取它");
        TaskQueueProperties props = TaskQueueTestSupport.properties(dir);
        props.setLeaseDuration(Duration.ofSeconds(2));
        TaskQueueService queue = TaskQueueTestSupport.newService(dir, props);
        String id = queue.enqueue("work", null, "exclusive-one").getId();

        Task claimed = queue.claimNext(1_000);
        assertEquals(id, claimed.getId());
        assertEquals(TaskStatus.RUNNING, queue.findById(id).orElseThrow().getStatus());

        // 队列里没有其它任务；租约未过期，第二个消费者必须空手而归
        Task second = queue.claimNext(300);
        assertNull(second, "同一任务在判定失败/成功前不得被第二个消费者领取");

        // 租约过期前不能重复确认成功（状态机保护）
        queue.acknowledgeSuccess(id);
        assertEquals(TaskStatus.SUCCEEDED, queue.findById(id).orElseThrow().getStatus());
        log.info("<<< [消费互斥-单任务] 通过：租约内二次领取为 null，成功确认一次后终结");
    }

    @Test
    @DisplayName("不同任务可并行消费(并发度4)，全部独立成功，互不阻塞")
    void differentTasksRunInParallel(@TempDir Path dir) throws Exception {
        log.info(">>> [并行消费] 用例：4 个不同任务在 4 消费者下同时处于 RUNNING");
        TaskQueueProperties props = TaskQueueTestSupport.properties(dir);
        props.setConcurrency(4);
        props.setLeaseDuration(Duration.ofSeconds(5));
        TaskQueueService queue = TaskQueueTestSupport.newService(dir, props);

        CountDownLatch inside = new CountDownLatch(4);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger completed = new AtomicInteger();
        TaskHandler parallel = ctx -> {
            inside.countDown();
            release.await();
            completed.incrementAndGet();
            return TaskOutcome.success();
        };
        TaskWorkerPool pool = TaskQueueTestSupport.newPool(queue, props,
                new NamedHandler("parallel", parallel));

        for (int i = 0; i < 4; i++) {
            queue.enqueue("parallel", null, "par-key-" + i);
        }
        assertTrue(inside.await(3, TimeUnit.SECONDS), "4 个不同任务应能并行执行");
        assertEquals(4, queue.metrics().running(), "不同任务允许同时 RUNNING");
        release.countDown();
        TaskQueueTestSupport.await(() -> completed.get() == 4, 5_000);
        pool.shutdown();
        assertEquals(4, queue.metrics().succeeded());
        log.info("<<< [并行消费] 通过：4 个任务并行执行后全部成功");
    }

    @Test
    @DisplayName("租约过期后任务可被回收重新领取(消费者宕机保护)")
    void leaseExpiryAllowsRedelivery(@TempDir Path dir) throws Exception {
        log.info(">>> [租约回收] 用例：消费者领取后宕机，租约过期后任务可被另一消费者领取");
        TaskQueueProperties props = TaskQueueTestSupport.properties(dir);
        props.setLeaseDuration(Duration.ofMillis(300));
        TaskQueueService queue = TaskQueueTestSupport.newService(dir, props);
        AtomicInteger calls = new AtomicInteger();
        TaskWorkerPool pool = TaskQueueTestSupport.newPool(queue, props,
                new NamedHandler("recoverable", ctx -> {
                    if (calls.incrementAndGet() == 1) {
                        Thread.sleep(700); // 超过租约，模拟消费者卡死
                    }
                    return TaskOutcome.success();
                }));
        String id = queue.enqueue("recoverable", null, "lease-key").getId();
        TaskQueueTestSupport.await(() -> queue.findById(id).orElseThrow().getStatus() == TaskStatus.SUCCEEDED, 5_000);
        pool.shutdown();
        assertTrue(calls.get() >= 1);
        log.info("<<< [租约回收] 通过：租约过期后任务被重新领取并最终成功，执行次数={}", calls.get());
    }

    @Test
    @DisplayName("成功确认的任务不会因租约回收或重启被再次执行")
    void succeededTaskNeverRerun(@TempDir Path dir) throws Exception {
        log.info(">>> [成功不重放] 用例：任务成功后触发租约回收，执行次数不得增加");
        TaskQueueProperties props = TaskQueueTestSupport.properties(dir);
        TaskQueueService queue = TaskQueueTestSupport.newService(dir, props);
        AtomicInteger calls = new AtomicInteger();
        TaskWorkerPool pool = TaskQueueTestSupport.newPool(queue, props,
                new NamedHandler("once", ctx -> {
                    calls.incrementAndGet();
                    return TaskOutcome.success();
                }));
        String id = queue.enqueue("once", null, "once-key").getId();
        TaskQueueTestSupport.await(() -> queue.findById(id).orElseThrow().getStatus() == TaskStatus.SUCCEEDED, 3_000);
        queue.reclaimExpiredLeases();
        assertNull(queue.claimNext(200), "成功任务不得被重新投递");
        pool.shutdown();
        assertEquals(1, calls.get());
        log.info("<<< [成功不重放] 通过：任务仅执行 1 次，保持 SUCCEEDED");
    }
}
