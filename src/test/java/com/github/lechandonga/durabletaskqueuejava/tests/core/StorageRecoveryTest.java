package com.github.lechandonga.durabletaskqueuejava.tests.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lechandonga.durabletaskqueuejava.config.TaskQueueProperties;
import com.github.lechandonga.durabletaskqueuejava.core.TaskQueue;
import com.github.lechandonga.durabletaskqueuejava.core.TaskStatus;
import com.github.lechandonga.durabletaskqueuejava.storage.CorruptRecord;
import com.github.lechandonga.durabletaskqueuejava.tests.support.BusinessScenario;
import com.github.lechandonga.durabletaskqueuejava.tests.support.ManualClock;
import com.github.lechandonga.durabletaskqueuejava.tests.support.QueueTestSupport;
import com.github.lechandonga.durabletaskqueuejava.tests.support.ScenarioLoggingExtension;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * 业务：重启恢复（在途任务重投、终态保持）以及损坏记录隔离报错。
 */
@BusinessScenario("重启恢复与损坏记录处理")
@ExtendWith(ScenarioLoggingExtension.class)
class StorageRecoveryTest {

    @TempDir
    Path tempDir;

    private TaskQueueProperties properties;
    private ManualClock clock;

    @BeforeEach
    void setUp() {
        properties = QueueTestSupport.fastProperties();
        clock = new ManualClock();
    }

    @Test
    @DisplayName("重启后 PENDING 可继续领取、RUNNING 全部回到 PENDING 重投、终态保持不变")
    void pendingRunningAndTerminalStatesSurviveRestart() {
        TaskQueue first = QueueTestSupport.newQueue(tempDir, properties, clock);
        first.submit("pending-key", "demo.echo", "p");
        first.submit("running-key", "demo.echo", "r");
        first.submit("succeeded-key", "demo.echo", "s");
        first.submit("dead-key", "demo.echo", "d");

        var pendingClaim = first.claim().orElseThrow();
        var runningClaim = first.claim().orElseThrow();
        assertEquals("pending-key", pendingClaim.getIdempotencyKey());
        assertEquals("running-key", runningClaim.getIdempotencyKey());
        var succeededClaim = first.claim().orElseThrow();
        first.succeed(succeededClaim.getId(), succeededClaim.getLeasedBy());
        var deadClaim = first.claim().orElseThrow();
        first.fail(deadClaim.getId(), deadClaim.getLeasedBy(), false, "dead reason");
        assertEquals(2, first.listByStatus(TaskStatus.RUNNING).size());

        TaskQueue reopened = QueueTestSupport.reopenQueue(tempDir, properties, clock);
        assertEquals(TaskStatus.PENDING,
                reopened.findByIdempotencyKey("pending-key").orElseThrow().getStatus());
        assertEquals(TaskStatus.PENDING,
                reopened.findByIdempotencyKey("running-key").orElseThrow().getStatus());
        assertEquals(TaskStatus.SUCCEEDED,
                reopened.findByIdempotencyKey("succeeded-key").orElseThrow().getStatus());
        assertEquals(TaskStatus.DEAD,
                reopened.findByIdempotencyKey("dead-key").orElseThrow().getStatus());

        // 恢复后两个在途任务都能被重新执行，终态任务不参与
        assertTrue(reopened.claim().isPresent());
        assertTrue(reopened.claim().isPresent());
        assertFalse(reopened.claim().isPresent());
        reopened.shutdown();
    }


    @Test
    @DisplayName("成功确认的任务重启后不会再次执行")
    void succeededTaskIsNotRedeliveredAfterRestart() {
        TaskQueue first = QueueTestSupport.newQueue(tempDir, properties, clock);
        first.submit("once-key", "demo.echo", null);
        var claimed = first.claim().orElseThrow();
        first.succeed(claimed.getId(), claimed.getLeasedBy());

        TaskQueue reopened = QueueTestSupport.reopenQueue(tempDir, properties, clock);
        assertTrue(reopened.claim().isEmpty());
        assertEquals(TaskStatus.SUCCEEDED,
                reopened.findByIdempotencyKey("once-key").orElseThrow().getStatus());
        reopened.shutdown();
    }

    @Test
    @DisplayName("等待退避的任务重启后仍遵守剩余退避，时间到了才能领取")
    void backoffDeadlineSurvivesRestart() {
        TaskQueue first = QueueTestSupport.newQueue(tempDir, properties, clock);
        first.submit("backoff-key", "demo.echo", null);
        var claimed = first.claim().orElseThrow();
        first.fail(claimed.getId(), claimed.getLeasedBy(), true, "temporary");
        long visibleAt = first.findByIdempotencyKey("backoff-key").orElseThrow().getVisibleAt();

        TaskQueue reopened = QueueTestSupport.reopenQueue(tempDir, properties, clock);
        assertTrue(reopened.claim().isEmpty());
        assertEquals(visibleAt,
                reopened.findByIdempotencyKey("backoff-key").orElseThrow().getVisibleAt());
        clock.advanceMillis(200);
        assertTrue(reopened.claim().isPresent());
        reopened.shutdown();
    }

    @Test
    @DisplayName("存储中的损坏行被隔离并明确报告，其余任务正常加载，队列仍可用")
    void corruptRecordsAreQuarantinedButQueueStaysAvailable() throws Exception {
        TaskQueue first = QueueTestSupport.newQueue(tempDir, properties, clock);
        first.submit("good-key", "demo.echo", null);

        Path dataFile = tempDir.resolve("tasks.jsonl");
        List<String> lines = Files.readAllLines(dataFile, StandardCharsets.UTF_8);
        assertEquals(1, lines.size());
        lines.add("{this is not valid json");
        lines.add("{\"id\":\"bad-1\",\"idempotencyKey\":\"\",\"type\":\"x\",\"status\":\"PENDING\","
                + "\"attempts\":0,\"createdAt\":1}");
        Files.write(dataFile, lines, StandardCharsets.UTF_8);

        TaskQueue reopened = QueueTestSupport.reopenQueue(tempDir, properties, clock);
        List<CorruptRecord> corrupt = reopened.getCorruptRecordsOnStartup();
        assertEquals(2, corrupt.size());
        assertTrue(corrupt.get(0).reason().contains("JSON parse error"));
        assertTrue(corrupt.get(1).reason().contains("Invalid record"));

        // 正常记录仍可用
        assertEquals(TaskStatus.PENDING,
                reopened.findByIdempotencyKey("good-key").orElseThrow().getStatus());
        reopened.submit("after-corrupt", "demo.echo", null);
        assertEquals(2, reopened.activeCount());

        // 原始损坏内容已落隔离文件
        try (var quarantineFiles = Files.list(tempDir)) {
            long count = quarantineFiles.filter(p -> p.getFileName().toString()
                    .startsWith("corrupt-")).count();
            assertEquals(1, count);
        }
        // 重写后损坏行不再残留在主文件
        List<String> rewritten = Files.readAllLines(dataFile, StandardCharsets.UTF_8);
        assertTrue(rewritten.stream().noneMatch(line -> line.contains("not valid json")));
        reopened.shutdown();
    }

    @Test
    @DisplayName("存储文件不存在时视为空队列，正常初始化不报错")
    void missingFileStartsEmpty() {
        Path emptyDir = tempDir.resolve("empty");
        TaskQueue queue = QueueTestSupport.newQueue(emptyDir, properties, clock);
        assertEquals(0, queue.activeCount());
        assertTrue(queue.claim().isEmpty());
        queue.shutdown();
    }
}
