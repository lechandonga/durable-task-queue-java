package com.github.lechandonga.durabletaskqueuejava.tests.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lechandonga.durabletaskqueuejava.config.TaskQueueProperties;
import com.github.lechandonga.durabletaskqueuejava.core.TaskQueue;
import com.github.lechandonga.durabletaskqueuejava.core.TaskRecord;
import com.github.lechandonga.durabletaskqueuejava.core.TaskRejectedException;
import com.github.lechandonga.durabletaskqueuejava.core.RejectReason;
import com.github.lechandonga.durabletaskqueuejava.core.TaskStatus;
import com.github.lechandonga.durabletaskqueuejava.tests.support.BusinessScenario;
import com.github.lechandonga.durabletaskqueuejava.tests.support.ManualClock;
import com.github.lechandonga.durabletaskqueuejava.tests.support.QueueTestSupport;
import com.github.lechandonga.durabletaskqueuejava.tests.support.ScenarioLoggingExtension;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * 业务：任务提交与幂等键去重（含并发重复提交、容量拒绝、成功后拒绝重放）。
 */
@BusinessScenario("任务提交与幂等去重")
@ExtendWith(ScenarioLoggingExtension.class)
class SubmissionIdempotencyTest {

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

    @AfterEach
    void tearDown() {
        queue.shutdown();
    }

    @Test
    @DisplayName("提交后可按任务ID和幂等键查询")
    void submitAndQuery() {
        TaskRecord record = queue.submit("key-1", "demo.echo", "payload-1");
        assertNotNull(record.getId());
        assertEquals(TaskStatus.PENDING, record.getStatus());
        assertEquals(record.getId(), queue.findById(record.getId()).orElseThrow().getId());
        assertEquals(TaskStatus.PENDING,
                queue.findByIdempotencyKey("key-1").orElseThrow().getStatus());
    }

    @Test
    @DisplayName("缺少幂等键或任务类型时以 INVALID_REQUEST 拒绝")
    void invalidRequestRejected() {
        TaskRejectedException blankKey = assertThrows(TaskRejectedException.class,
                () -> queue.submit("  ", "demo.echo", null));
        assertEquals(RejectReason.INVALID_REQUEST, blankKey.getReason());
        TaskRejectedException blankType = assertThrows(TaskRejectedException.class,
                () -> queue.submit("key-x", " ", null));
        assertEquals(RejectReason.INVALID_REQUEST, blankType.getReason());
    }

    @Test
    @DisplayName("同一幂等键在任务终结前重复提交只保留一条，拒绝原因为 DUPLICATE_ACTIVE")
    void duplicateActiveRejected() {
        queue.submit("dup-key", "demo.echo", "first");
        TaskRejectedException error = assertThrows(TaskRejectedException.class,
                () -> queue.submit("dup-key", "demo.echo", "second"));
        assertEquals(RejectReason.DUPLICATE_ACTIVE, error.getReason());
        assertEquals(1, queue.activeCount());
        assertEquals("first",
                queue.findByIdempotencyKey("dup-key").orElseThrow().getPayload());
    }

    @Test
    @DisplayName("多线程并发提交相同幂等键：只有一条成功，其余全部 DUPLICATE_ACTIVE")
    void concurrentDuplicateSubmission() throws Exception {
        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            final int index = i;
            futures.add(pool.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                    queue.submit("concurrent-key", "demo.echo", "payload-" + index);
                } catch (TaskRejectedException rejected) {
                    if (rejected.getReason() != RejectReason.DUPLICATE_ACTIVE) {
                        throw rejected;
                    }
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

        assertEquals(1, queue.activeCount(), "并发重复提交必须只保留一条活动任务");
        assertEquals(1, queue.listByStatus(TaskStatus.PENDING).size());
    }

    @Test
    @DisplayName("容量超限时以 CAPACITY_FULL 拒绝，且拒绝原因与重复提交可区分")
    void capacityFullRejected() {
        properties.setCapacity(2);
        TaskQueue smallQueue = QueueTestSupport.newQueue(
                tempDir.resolve("small-" + UUID.randomUUID()), properties, clock);
        smallQueue.submit("k1", "demo.echo", null);
        smallQueue.submit("k2", "demo.echo", null);

        TaskRejectedException capacityError = assertThrows(TaskRejectedException.class,
                () -> smallQueue.submit("k3", "demo.echo", null));
        assertEquals(RejectReason.CAPACITY_FULL, capacityError.getReason());

        TaskRejectedException duplicateError = assertThrows(TaskRejectedException.class,
                () -> smallQueue.submit("k1", "demo.echo", null));
        assertEquals(RejectReason.DUPLICATE_ACTIVE, duplicateError.getReason());
        assertEquals(2, smallQueue.activeCount());
        smallQueue.shutdown();
    }

    @Test
    @DisplayName("已成功的幂等键再次提交被 ALREADY_SUCCEEDED 拒绝，保证不重复执行")
    void succeededKeyCannotBeReplayed() {
        queue.submit("done-key", "demo.echo", null);
        TaskRecord claimed = queue.claim().orElseThrow();
        assertTrue(queue.succeed(claimed.getId(), claimed.getLeasedBy()));
        TaskRejectedException error = assertThrows(TaskRejectedException.class,
                () -> queue.submit("done-key", "demo.echo", null));
        assertEquals(RejectReason.ALREADY_SUCCEEDED, error.getReason());
    }
}
