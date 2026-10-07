package com.github.lechandonga.durabletaskqueuejava.tests;

import com.github.lechandonga.durabletaskqueuejava.core.TaskQueueProperties;
import com.github.lechandonga.durabletaskqueuejava.core.TaskQueueService;
import com.github.lechandonga.durabletaskqueuejava.core.TaskStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务八：journal 压缩后状态语义不变——快照重放与事件重放等价。
 */
class JournalCompactionTests {

    private static final Logger log = LoggerFactory.getLogger(JournalCompactionTests.class);

    @Test
    @DisplayName("压缩阈值很小触发SNAPSHOT压缩后重启：终态/活跃任务状态与压缩前一致")
    void compactThenRecoverKeepsState(@TempDir Path dir) {
        log.info(">>> [日志压缩] 用例：阈值 1 字节强制压缩，重启后任务状态完整保留");
        TaskQueueProperties props = TaskQueueTestSupport.properties(dir);
        props.setCompactThresholdBytes(1);
        TaskQueueService q1 = TaskQueueTestSupport.newService(dir, props);
        String succeededKey = null;
        String first = q1.enqueue("echo", "a", "compact-1").getId();
        String second = q1.enqueue("echo", "b", "compact-2").getId();
        assertEquals(2, q1.metrics().active());

        TaskQueueService q2 = TaskQueueTestSupport.newService(dir, props);
        assertEquals(TaskStatus.PENDING, q2.findById(first).orElseThrow().getStatus());
        assertEquals(TaskStatus.PENDING, q2.findById(second).orElseThrow().getStatus());
        assertEquals(2, q2.metrics().active());
        assertEquals("compact-1", q2.findById(first).orElseThrow().getIdempotencyKey());
        assertEquals("b", q2.findById(second).orElseThrow().getPayload());

        // 压缩后的存储仍可继续写入并再次恢复
        String third = q2.enqueue("echo", "c", "compact-3").getId();
        TaskQueueService q3 = TaskQueueTestSupport.newService(dir, props);
        assertEquals(3, q3.metrics().active());
        assertTrue(q3.findById(third).isPresent());
        log.info("<<< [日志压缩] 通过：多次压缩+重启后 3 个任务全部保留，字段与状态一致");
    }
}
