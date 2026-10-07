package com.github.lechandonga.durabletaskqueuejava.tests;

import com.github.lechandonga.durabletaskqueuejava.core.Task;
import com.github.lechandonga.durabletaskqueuejava.core.TaskContext;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务四：重启恢复——PENDING 任务不丢，停机时仍在 RUNNING 的任务回到 PENDING 并最终得到确定结果；
 * 成功任务重启后保持 SUCCEEDED 且不重复执行。
 */
class RestartRecoveryTests {

    private static final Logger log = LoggerFactory.getLogger(RestartRecoveryTests.class);

    @Test
    @DisplayName("重启后未完成任务继续处理并成功；已成功任务保持终态不重复执行")
    void pendingAndSucceededSurviveRestart(@TempDir Path dir) throws Exception {
        log.info(">>> [重启恢复-待处理] 用例：第一次运行成功1个+遗留1个未处理，重启后遗留任务继续成功");
        TaskQueueProperties props = TaskQueueTestSupport.properties(dir);
        AtomicInteger calls = new AtomicInteger();

        TaskQueueService q1 = TaskQueueTestSupport.newService(dir, props);
        TaskWorkerPool p1 = TaskQueueTestSupport.newPool(q1, props,
                new NamedHandler("work", ctx -> {
                    calls.incrementAndGet();
                    return TaskOutcome.success();
                }));
        String doneId = q1.enqueue("work", null, "done-key").getId();
        String pendingId;
        // 用容量构造一个尚未处理的任务更简单：直接在第一个任务成功后，停机前再入队一个
        TaskQueueTestSupport.await(() -> q1.findById(doneId).orElseThrow().getStatus() == TaskStatus.SUCCEEDED, 3_000);
        pendingId = q1.enqueue("work", null, "pending-key").getId();
        // 给一点时间让它被领取（也可能来不及），随后直接销毁工作池模拟进程退出
        Thread.sleep(30);
        p1.shutdown();

        TaskQueueService q2 = TaskQueueTestSupport.newService(dir, props);
        assertEquals(TaskStatus.SUCCEEDED, q2.findById(doneId).orElseThrow().getStatus());
        Task pending = q2.findById(pendingId).orElseThrow();
        assertTrue(pending.getStatus() == TaskStatus.PENDING || pending.getStatus() == TaskStatus.RUNNING
                        || pending.getStatus() == TaskStatus.SUCCEEDED,
                "重启后未完成任务必须处于可恢复状态，实际 " + pending.getStatus());

        TaskWorkerPool p2 = TaskQueueTestSupport.newPool(q2, props,
                new NamedHandler("work", ctx -> {
                    calls.incrementAndGet();
                    return TaskOutcome.success();
                }));
        TaskQueueTestSupport.await(() -> q2.findById(pendingId).orElseThrow().getStatus() == TaskStatus.SUCCEEDED, 3_000);
        p2.shutdown();

        assertEquals(2, calls.get(), "两个任务各执行一次，成功任务重启后不得重复执行");
        assertEquals(2, q2.metrics().succeeded());
        log.info("<<< [重启恢复-待处理] 通过：已成功任务保持 SUCCEEDED，遗留任务重启后成功，总执行次数=2");
    }

    @Test
    @DisplayName("停机时仍在处理的任务：重启后回到PENDING并重新执行，attempts记录延续")
    void inFlightTaskRecoveredAfterCrash(@TempDir Path dir) throws Exception {
        log.info(">>> [重启恢复-在途] 用例：任务被领取(RUNNING)后模拟宕机，重启后必须恢复并成功");
        TaskQueueProperties props = TaskQueueTestSupport.properties(dir);
        props.setLeaseDuration(Duration.ofSeconds(30));
        TaskQueueService q1 = TaskQueueTestSupport.newService(dir, props);
        CountDownLatch leased = new CountDownLatch(1);
        CountDownLatch block = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        TaskWorkerPool p1 = TaskQueueTestSupport.newPool(q1, props, new NamedHandler("slow", ctx -> {
            calls.incrementAndGet();
            leased.countDown();
            block.await();
            return TaskOutcome.success();
        }));

        String id = q1.enqueue("slow", null, "slow-key").getId();
        assertTrue(leased.await(3, java.util.concurrent.TimeUnit.SECONDS));
        // 模拟进程被 kill：处理器线程仍被阻塞，不释放租约直接丢弃整个队列实例
        Task running = q1.findById(id).orElseThrow();
        assertEquals(TaskStatus.RUNNING, running.getStatus());
        assertEquals(1, running.getAttempts());
        log.info("任务 {} 已在 RUNNING(attempt=1)，直接模拟宕机（不释放租约）", id);

        TaskQueueService q2 = TaskQueueTestSupport.newService(dir, props);
        Task recovered = q2.findById(id).orElseThrow();
        assertEquals(TaskStatus.PENDING, recovered.getStatus(), "重启恢复时在途任务必须回到 PENDING");
        assertEquals(0, recovered.getLeaseExpiresAt());

        TaskWorkerPool p2 = TaskQueueTestSupport.newPool(q2, props, new NamedHandler("slow", ctx -> {
            calls.incrementAndGet();
            return TaskOutcome.success();
        }));
        TaskQueueTestSupport.await(() -> q2.findById(id).orElseThrow().getStatus() == TaskStatus.SUCCEEDED, 3_000);
        p2.shutdown();
        block.countDown();
        p1.shutdown();

        assertEquals(2, calls.get(), "宕机前开始 1 次 + 重启后重新执行 1 次");
        log.info("<<< [重启恢复-在途] 通过：RUNNING 任务重启后回 PENDING，重新执行并成功，总执行次数=2");
    }

    @Test
    @DisplayName("退避中的重试任务重启后保留剩余退避语义，最终成功")
    void retryingTaskSurvivesRestart(@TempDir Path dir) throws Exception {
        log.info(">>> [重启恢复-重试中] 用例：任务首次失败进入退避，重启后仍会再次尝试直至成功");
        TaskQueueProperties props = TaskQueueTestSupport.properties(dir);
        props.setBaseBackoff(Duration.ofSeconds(2));
        CountDownLatch firstFailureDone = new CountDownLatch(1);
        TaskQueueService q1 = TaskQueueTestSupport.newService(dir, props);
        TaskWorkerPool p1 = TaskQueueTestSupport.newPool(q1, props, new NamedHandler("r", ctx -> {
            firstFailureDone.countDown();
            return TaskOutcome.retryable("先失败");
        }));
        String id = q1.enqueue("r", null, "r-key").getId();
        assertTrue(firstFailureDone.await(3, java.util.concurrent.TimeUnit.SECONDS));
        // 第一次失败已进入 2s 退避；立即停机，确保重试尚未发生
        p1.shutdown();
        Task beforeRestart = q1.findById(id).orElseThrow();
        assertEquals(1, beforeRestart.getAttempts());
        assertTrue(beforeRestart.getVisibleAt() > System.currentTimeMillis());

        TaskQueueProperties props2 = TaskQueueTestSupport.properties(dir);
        props2.setBaseBackoff(Duration.ofSeconds(2));
        TaskQueueService q2 = TaskQueueTestSupport.newService(dir, props2);
        Task afterRestart = q2.findById(id).orElseThrow();
        assertEquals(1, afterRestart.getAttempts(), "attempts 必须在重启后延续");
        assertEquals(TaskStatus.PENDING, afterRestart.getStatus());
        assertTrue(afterRestart.getVisibleAt() > System.currentTimeMillis(),
                "退避到期时间必须随重启保留，任务不应立刻被重新执行");
        assertNull(q2.claimNext(200), "退避未到期不得提前投递");
        AtomicInteger secondCalls = new AtomicInteger();
        TaskWorkerPool p2 = TaskQueueTestSupport.newPool(q2, props2, new NamedHandler("r", ctx -> {
            secondCalls.incrementAndGet();
            return TaskOutcome.success();
        }));
        TaskQueueTestSupport.await(() -> q2.findById(id).orElseThrow().getStatus() == TaskStatus.SUCCEEDED, 5_000);
        p2.shutdown();
        assertEquals(1, secondCalls.get(), "退避到期后应恰好再执行 1 次");
        log.info("<<< [重启恢复-重试中] 通过：attempts 与退避到期时间跨重启保留，到期后执行 1 次即成功");
    }
}
