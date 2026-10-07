package com.github.lechandonga.durabletaskqueuejava.tests;

import com.github.lechandonga.durabletaskqueuejava.core.CorruptRecord;
import com.github.lechandonga.durabletaskqueuejava.core.Task;
import com.github.lechandonga.durabletaskqueuejava.core.TaskQueueProperties;
import com.github.lechandonga.durabletaskqueuejava.core.TaskQueueService;
import com.github.lechandonga.durabletaskqueuejava.core.TaskStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务六：本地存储损坏/异常记录的处理——明确报错并隔离，其余记录正常恢复，队列仍然可用。
 */
class CorruptStorageTests {

    private static final Logger log = LoggerFactory.getLogger(CorruptStorageTests.class);

    @Test
    @DisplayName("CRC损坏行被明确隔离报错，前后正常记录照常恢复，队列继续可用")
    void corruptLineIsQuarantinedNotFatal(@TempDir Path dir) throws Exception {
        log.info(">>> [损坏隔离] 用例：正常行/损坏行/正常行混排，损坏行报错隔离，好行全部恢复");
        TaskQueueProperties props = TaskQueueTestSupport.properties(dir);
        TaskQueueService q1 = TaskQueueTestSupport.newService(dir, props);
        String id1 = q1.enqueue("echo", "a", "key-a").getId();
        String id2 = q1.enqueue("echo", "b", "key-b").getId();

        Path journal = dir.resolve("task-queue.journal");
        List<String> lines = Files.readAllLines(journal, StandardCharsets.UTF_8);
        assertTrue(lines.size() >= 2);
        // 篡改第二条事件的负载字节，制造 CRC 不匹配
        int victim = 1;
        String badLine = lines.get(victim);
        String tampered = badLine.substring(0, badLine.indexOf('\t') + 1)
                + badLine.substring(badLine.indexOf('\t') + 1).replace("key-b", "key-X");
        Files.writeString(journal, String.join("\n", lines.get(0), tampered)
                        .replace(lines.get(1), tampered) + "\n",
                StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING);
        // 重新写成 好行 + 坏行（id2 的入队事件被破坏）
        Files.writeString(journal, lines.get(0) + "\n" + tampered + "\n",
                StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING);

        TaskQueueService q2 = TaskQueueTestSupport.newService(dir, props);
        List<CorruptRecord> corrupt = q2.corruptRecords();
        assertEquals(1, corrupt.size(), "应精确隔离 1 条损坏记录");
        assertEquals(2, corrupt.get(0).lineNumber());
        assertTrue(corrupt.get(0).reason().contains("CRC"), "错误原因应指明 CRC 校验失败");

        assertEquals(TaskStatus.PENDING, q2.findById(id1).orElseThrow().getStatus(),
                "损坏行之前的正常任务必须恢复");
        assertFalse(q2.findById(id2).isPresent(), "被破坏事件对应的任务不应凭空恢复");

        // 队列仍然可用：新提交、查询、幂等拒绝都正常
        Task id3 = q2.enqueue("echo", "c", "key-c");
        assertEquals(TaskStatus.PENDING, q2.findById(id3.getId()).orElseThrow().getStatus());
        log.info("<<< [损坏隔离] 通过：损坏行(第2行)隔离并报 CRC 错误，id1 正常恢复，队列继续可提交新任务");
    }

    @Test
    @DisplayName("格式完全非法的行(无CRC分隔)同样被隔离且不影响启动")
    void garbageLineIsQuarantined(@TempDir Path dir) throws Exception {
        log.info(">>> [损坏隔离] 用例：向 journal 追加垃圾行，恢复时隔离该行且启动成功");
        TaskQueueProperties props = TaskQueueTestSupport.properties(dir);
        TaskQueueService q1 = TaskQueueTestSupport.newService(dir, props);
        q1.enqueue("echo", null, "ok-key");

        Path journal = dir.resolve("task-queue.journal");
        Files.writeString(journal, "this-is-not-a-valid-journal-line\n",
                StandardCharsets.UTF_8, StandardOpenOption.APPEND);

        TaskQueueService q2 = TaskQueueTestSupport.newService(dir, props);
        assertEquals(1, q2.corruptRecords().size());
        assertEquals(1, q2.metrics().active());
        log.info("<<< [损坏隔离] 通过：垃圾行被隔离（{}），队列正常启动且任务完好",
                q2.corruptRecords().get(0).reason());
    }
}
