package com.github.lechandonga.durabletaskqueuejava.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * 基于本地文件的追加式事件日志（append-only journal）。
 *
 * <p>每行磁盘格式：{@code <16位小写十六进制CRC32>\t<事件JSON>}。CRC 覆盖 JSON 字节，
 * 可以检出截断/位翻转等损坏。每条事件 append 后立即 {@code flush + fsync}，保证
 * 进程崩溃或机器掉电后已确认的状态不丢失。</p>
 *
 * <p>损坏处理：重放时逐行隔离坏记录，收集到 {@link #corruptRecords()} 并打 WARN 日志，
 * 其余记录照常恢复——不会悄悄忽略，也不会让整个队列不可用。</p>
 */
public class JournalStore {

    private static final Logger log = LoggerFactory.getLogger(JournalStore.class);
    private static final int RAW_PREVIEW_LIMIT = 200;

    private final Path journalFile;
    private final ObjectMapper objectMapper;
    private final List<CorruptRecord> corruptRecords = new ArrayList<>();

    private volatile long lastSeq;

    public JournalStore(Path dataDirectory, ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        try {
            Files.createDirectories(dataDirectory);
            this.journalFile = dataDirectory.resolve("task-queue.journal");
            if (Files.notExists(journalFile)) {
                Files.createFile(journalFile);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("无法初始化本地任务队列存储目录: " + dataDirectory, e);
        }
    }

    /**
     * 追加一条事件并持久化（fsync）。序列号由本方法分配，从 1 开始单调递增。
     */
    public synchronized JournalEvent append(String type, Map<String, Object> data) {
        long seq = lastSeq + 1;
        JournalEvent event = new JournalEvent(seq, type, new HashMap<>(data));
        String line = encode(event);
        try (FileChannel channel = FileChannel.open(journalFile,
                StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            byte[] bytes = (line + "\n").getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            // 强制将内容刷到物理存储，进程崩溃/掉电后已确认事件不丢。
            channel.force(false);
        } catch (IOException e) {
            throw new UncheckedIOException("写入本地任务队列日志失败: " + journalFile, e);
        }
        lastSeq = seq;
        return event;
    }

    /**
     * 从磁盘重放全部事件。遇到坏行只隔离该条并继续；若文件中存在
     * {@link JournalEvent#COMPACTION_BOUNDARY}，则只回放边界之后的事件
     * （边界之前的状态已由 SNAPSHOT 折叠）。
     */
    public synchronized List<JournalEvent> replay() {
        corruptRecords.clear();
        List<JournalEvent> allEvents = new ArrayList<>();
        long totalLines = 0;
        try (BufferedReader reader = Files.newBufferedReader(journalFile, StandardCharsets.UTF_8)) {
            String raw;
            while ((raw = reader.readLine()) != null) {
                long lineNumber = ++totalLines;
                if (raw.isEmpty()) {
                    quarantine(lineNumber, raw, "空行");
                    continue;
                }
                try {
                    allEvents.add(decode(raw, lineNumber));
                } catch (CorruptRecordException e) {
                    quarantine(lineNumber, raw, e.getMessage());
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("读取本地任务队列日志失败: " + journalFile, e);
        }
        // 压缩是原子整体写入：内容为「SNAPSHOT * N + COMPACTION_BOUNDARY」。
        // 因此若存在边界，只保留最后一个边界及其之前的事件；边界之后若还有事件，
        // 只可能来自一次未完成替换（正常情况下不会出现），一并忽略。
        int endExclusive = allEvents.size();
        for (int i = allEvents.size() - 1; i >= 0; i--) {
            if (JournalEvent.COMPACTION_BOUNDARY.equals(allEvents.get(i).type())) {
                endExclusive = i + 1;
                break;
            }
        }
        List<JournalEvent> effective = new ArrayList<>(allEvents.subList(0, endExclusive));
        if (!corruptRecords.isEmpty()) {
            log.warn("本地任务队列日志重放完成：共 {} 行，隔离损坏记录 {} 条（其余记录已正常恢复，队列继续可用）",
                    totalLines, corruptRecords.size());
            corruptRecords.forEach(c -> log.warn("  隔离记录 第{}行: {} | 原因: {}",
                    c.lineNumber(), c.rawPreview(), c.reason()));
        }
        for (JournalEvent event : effective) {
            if (event.seq() > lastSeq) {
                lastSeq = event.seq();
            }
        }
        return effective;
    }

    /**
     * 用给定快照事件序列压缩日志：原子替换为「SNAPSHOT * N + COMPACTION_BOUNDARY」，
     * 替换后旧日志的语义完整保留。仅在调用方持有队列锁时使用。
     */
    public synchronized void compact(List<JournalEvent> snapshotEvents) {
        Path temp = journalFile.resolveSibling(journalFile.getFileName() + ".compact.tmp");
        long boundaryLine;
        try (FileChannel channel = FileChannel.open(temp,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            long seq = lastSeq;
            long line = 0;
            for (JournalEvent snapshot : snapshotEvents) {
                Map<String, Object> data = new HashMap<>(snapshot.data());
                data.remove("__line");
                JournalEvent renumbered = new JournalEvent(++seq, JournalEvent.SNAPSHOT, data);
                channel.write(ByteBuffer.wrap((encode(renumbered) + "\n").getBytes(StandardCharsets.UTF_8)));
                line++;
            }
            JournalEvent boundary = new JournalEvent(++seq, JournalEvent.COMPACTION_BOUNDARY,
                    Map.of("at", System.currentTimeMillis()));
            channel.write(ByteBuffer.wrap((encode(boundary) + "\n").getBytes(StandardCharsets.UTF_8)));
            boundaryLine = ++line;
            channel.force(false);
            lastSeq = seq;
        } catch (IOException e) {
            throw new UncheckedIOException("压缩本地任务队列日志失败", e);
        }
        try {
            Files.move(temp, journalFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            try {
                Files.deleteIfExists(temp);
            } catch (IOException ignored) {
                // 删除临时文件失败不影响原日志，忽略
            }
            throw new UncheckedIOException("替换本地任务队列日志失败", e);
        }
        log.info("本地任务队列日志已压缩：{} 条任务快照，边界行 {}", snapshotEvents.size(), boundaryLine);
    }

    public synchronized long sizeBytes() {
        try {
            return Files.size(journalFile);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public List<CorruptRecord> corruptRecords() {
        return List.copyOf(corruptRecords);
    }

    private JournalEvent decode(String raw, long lineNumber) {
        int tab = raw.indexOf('\t');
        if (tab <= 0 || tab >= raw.length() - 1) {
            throw new CorruptRecordException(lineNumber, "缺少 CRC 分隔符或 JSON 负载为空");
        }
        String checksumHex = raw.substring(0, tab);
        String json = raw.substring(tab + 1);
        long expected;
        try {
            expected = Long.parseUnsignedLong(checksumHex, 16);
        } catch (NumberFormatException e) {
            throw new CorruptRecordException(lineNumber, "CRC 不是十六进制数字: " + checksumHex);
        }
        long actual = crc32(json.getBytes(StandardCharsets.UTF_8));
        if (expected != actual) {
            throw new CorruptRecordException(lineNumber,
                    "CRC 校验失败，记录可能已被截断或损坏 (期望 " + checksumHex + ", 实际 " + toHex(actual) + ")");
        }
        Map<?, ?> root;
        try {
            root = objectMapper.readValue(json, Map.class);
        } catch (JsonProcessingException e) {
            throw new CorruptRecordException(lineNumber, "JSON 无法解析: " + e.getOriginalMessage());
        }
        Object seqObj = root.get("seq");
        Object typeObj = root.get("type");
        Object dataObj = root.get("data");
        if (!(seqObj instanceof Number) || !(typeObj instanceof String) || !(dataObj instanceof Map)) {
            throw new CorruptRecordException(lineNumber, "必填字段缺失或类型错误 (seq/type/data)");
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> data = (Map<String, Object>) dataObj;
        return new JournalEvent(((Number) seqObj).longValue(), (String) typeObj, new HashMap<>(data));
    }

    private String encode(JournalEvent event) {
        try {
            Map<String, Object> root = new HashMap<>();
            Map<String, Object> data = new HashMap<>(event.data());
            data.remove("__line");
            root.put("seq", event.seq());
            root.put("type", event.type());
            root.put("data", data);
            String json = objectMapper.copy()
                    .setSerializationInclusion(JsonInclude.Include.ALWAYS)
                    .writeValueAsString(root);
            return toHex(crc32(json.getBytes(StandardCharsets.UTF_8))) + "\t" + json;
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("事件无法序列化为 JSON: " + event.type(), e);
        }
    }

    private void quarantine(long lineNumber, String raw, String reason) {
        String preview = raw.length() > RAW_PREVIEW_LIMIT ? raw.substring(0, RAW_PREVIEW_LIMIT) + "..." : raw;
        CorruptRecord record = new CorruptRecord(lineNumber, preview, reason);
        corruptRecords.add(record);
        log.warn("隔离损坏的本地存储记录: {}", record);
    }

    static long crc32(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return crc.getValue();
    }

    private static String toHex(long value) {
        return String.format("%08x", value);
    }
}
