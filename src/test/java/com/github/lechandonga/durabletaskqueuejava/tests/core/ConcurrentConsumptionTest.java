package com.github.lechandonga.durabletaskqueuejava.tests.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * 业务：并发消费下的租约互斥——同一任务在失败判定前绝不被两个消费者同时处理。
 */
@BusinessScenario("并发消费与租约互斥")
@ExtendWith(ScenarioLoggingExtension.class)
class ConcurrentConsumptionTest {

    @TempDir
    Path tempDir;

    private TaskQueue queue;
    private ManualClock clock;

    @AfterEach
    void tearDown() {
        queue.shutdown();
    }

    @Test
    @DisplayName("多线程并发领取不同任务：同一任务只会被一个消费者拿到")
    void eachTaskClaimedByExactlyOneConsumer() throws Exception {
        TaskQueueProperties properties = QueueTestSupport.fastProperties();
        clock = new ManualClock();
        queue = QueueTestSupport.newQueue(tempDir, properties, clock);
        int taskCount = 100;
        for (int i = 0; i < taskCount; i++) {
            queue.submit("key-" + i, "demo.echo", "p" + i);
        }

        int consumers = 8;
        ExecutorService pool = Executors.newFixedThreadPool(consumers);
        CountDownLatch ready = new CountDownLatch(consumers);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> claimedIds = new ConcurrentLinkedQueue<>();
        List<Future<?>> futures = new ArrayList<>();
        for (int c = 0; c < consumers; c++) {
            futures.add(pool.submit(() -> {
                ready.countDown();
                start.await();
                while (true) {
                    var maybe = queue.claim();
                    if (maybe.isEmpty()) {
                        break;
                    }
                    claimedIds.add(maybe.get().getId());
                }
                return null;
            }));
        }
        ready.await();
        start.countDown();
        for (Future<?> future : futures) {
            future.get();
        }
        pool.shutdown();

        long distinct = claimedIds.stream().distinct().count();
        assertEquals(taskCount, claimedIds.size(), "每个任务恰好被领取一次");
        assertEquals(taskCount, distinct, "不允许同一任务被两个消费者领取");
        assertEquals(taskCount, queue.listByStatus(TaskStatus.RUNNING).size());
    }

    @Test
    @DisplayName("租约有效时其他消费者无法替该任务上报结果，必须由持有者完成")
    void onlyLeaseOwnerCanReportOutcome() {
        TaskQueueProperties properties = QueueTestSupport.fastProperties();
        clock = new ManualClock();
        queue = QueueTestSupport.newQueue(tempDir, properties, clock);
        queue.submit("owner-key", "demo.echo", null);
        TaskRecord claimed = queue.claim().orElseThrow();

        assertThrows(IllegalStateException.class,
                () -> queue.fail(claimed.getId(), "someone-else", true, "stolen"));
        // 非持有者 succeed 返回 false 而不是把任务改成成功
        assertTrue(!queue.succeed(claimed.getId(), "someone-else"));
        assertEquals(TaskStatus.RUNNING,
                queue.findById(claimed.getId()).orElseThrow().getStatus());

        assertTrue(queue.succeed(claimed.getId(), claimed.getLeasedBy()));
        assertEquals(TaskStatus.SUCCEEDED,
                queue.findById(claimed.getId()).orElseThrow().getStatus());
    }

    @Test
    @DisplayName("心跳在租约期内持续续约，任务不会被误判失联")
    void heartbeatRenewsLease() {
        TaskQueueProperties properties = QueueTestSupport.fastProperties();
        properties.setLeaseTtl(java.time.Duration.ofSeconds(1));
        clock = new ManualClock();
        queue = QueueTestSupport.newQueue(tempDir, properties, clock);
        queue.submit("hb-key", "demo.echo", null);
        TaskRecord claimed = queue.claim().orElseThrow();
        long firstExpire = claimed.getLeaseExpireAt();

        clock.advanceMillis(500);
        assertTrue(queue.heartbeat(claimed.getId(), claimed.getLeasedBy()));
        long renewedExpire = queue.findById(claimed.getId()).orElseThrow().getLeaseExpireAt();
        assertTrue(renewedExpire > firstExpire);

        // claim 扫描时不应回收仍有租约的任务，队列里没有可领取的新任务
        assertTrue(queue.claim().isEmpty());
        assertEquals(TaskStatus.RUNNING,
                queue.findById(claimed.getId()).orElseThrow().getStatus());
    }

    @Test
    @DisplayName("租约过期的失联任务会被安全回收重新投递")
    void expiredLeaseIsReclaimed() {
        TaskQueueProperties properties = QueueTestSupport.fastProperties();
        properties.setLeaseTtl(java.time.Duration.ofSeconds(1));
        clock = new ManualClock();
        queue = QueueTestSupport.newQueue(tempDir, properties, clock);
        queue.submit("lost-key", "demo.echo", null);
        TaskRecord claimed = queue.claim().orElseThrow();

        clock.advanceMillis(1_001);
        // 没有新任务，claim 内部应先回收失联任务并将其重新领取
        TaskRecord redelivered = queue.claim().orElseThrow();
        assertEquals(claimed.getId(), redelivered.getId());
        assertEquals(2, redelivered.getAttempts(), "重新投递是一次新的尝试");
    }
}
