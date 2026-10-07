package com.github.lechandonga.durabletaskqueuejava.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.durabletaskqueuejava.core.TaskRecord;
import com.github.lechandonga.durabletaskqueuejava.core.TaskStatus;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于单个 JSON Lines 文件的本地任务存储。
 * <p>
 * 持久化语义：
 * <ul>
 *     <li>每次状态变更都把全量任务快照写入临时文件、{@code fsync} 后原子替换主文件，
 *     因此任何时刻主文件要么是变更前、要么是变更后的完整内容，不会写到一半。</li>
 *     <li>启动加载时逐行解析：无法解析、字段非法、状态缺失的行会被原样复制到
 *     {@code corrupt-<时间戳>.log} 隔离文件并在恢复报告中返回，不影响其余任务加载，
 *     既不静默忽略，也不会让整个队列不可用。</li>
 * </ul>
 */
public class LocalJsonTaskStore {

    private static final Logger log = LoggerFactory.getLogger(LocalJsonTaskStore.class);

    private final Path dataFile;
    private final Path storageDir;
    private final ObjectMapper objectMapper;

    public LocalJsonTaskStore(Path storageDir, ObjectMapper objectMapper) {
        this.storageDir = storageDir;
        this.dataFile = storageDir.resolve("tasks.jsonl");
        this.objectMapper = objectMapper;
        try {
            Files.createDirectories(storageDir);
        } catch (IOException e) {
            throw new TaskStoreException("Cannot create storage directory: " + storageDir, e);
        }
    }

    public Path getDataFile() {
        return dataFile;
    }

    /**
     * 加载全部任务记录。损坏行会被隔离并报告，见类注释。
     */
    public StorageLoadResult loadAll() {
        if (Files.notExists(dataFile)) {
            return new StorageLoadResult(List.of(), List.of());
        }
        List<TaskRecord> valid = new ArrayList<>();
        List<CorruptRecord> corrupt = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(dataFile, StandardCharsets.UTF_8)) {
            int lineNumber = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.isBlank()) {
                    continue;
                }
                try {
                    TaskRecord record = objectMapper.readValue(line, TaskRecord.class);
                    validate(record, lineNumber, line, corrupt);
                    if (record.getStatus() != null) {
                        valid.add(record);
                    }
                } catch (Exception parseError) {
                    corrupt.add(new CorruptRecord(lineNumber, line,
                            "JSON parse error: " + parseError.getMessage()));
                }
            }
        } catch (IOException e) {
            throw new TaskStoreException("Failed to read task store: " + dataFile, e);
        }
        if (!corrupt.isEmpty()) {
            quarantine(corrupt);
        }
        return new StorageLoadResult(List.copyOf(valid), List.copyOf(corrupt));
    }

    private static void validate(TaskRecord record, int lineNumber, String line,
                                 List<CorruptRecord> corrupt) {
        String problem = null;
        if (isBlank(record.getId())) {
            problem = "missing id";
        } else if (isBlank(record.getIdempotencyKey())) {
            problem = "missing idempotencyKey";
        } else if (isBlank(record.getType())) {
            problem = "missing type";
        } else if (record.getStatus() == null) {
            problem = "missing status";
        } else if (record.getAttempts() < 0) {
            problem = "negative attempts";
        } else if (record.getCreatedAt() <= 0) {
            problem = "invalid createdAt";
        } else if (record.getStatus() == TaskStatus.RUNNING
                && (isBlank(record.getLeasedBy()) || record.getLeaseExpireAt() <= 0)) {
            problem = "RUNNING record without lease";
        }
        if (problem != null) {
            corrupt.add(new CorruptRecord(lineNumber, line, "Invalid record: " + problem));
            record.setStatus(null);
        }
    }

    private void quarantine(List<CorruptRecord> corruptRecords) {
        Path quarantineFile = storageDir.resolve(
                "corrupt-" + System.currentTimeMillis() + ".log");
        StringBuilder builder = new StringBuilder();
        for (CorruptRecord corrupt : corruptRecords) {
            builder.append("# line ").append(corrupt.lineNumber())
                    .append(": ").append(corrupt.reason()).append('\n')
                    .append(corrupt.rawLine()).append('\n');
        }
        try {
            Files.writeString(quarantineFile, builder.toString(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            log.error("Detected {} corrupt record(s) in {}; raw content quarantined to {}",
                    corruptRecords.size(), dataFile, quarantineFile);
        } catch (IOException e) {
            throw new TaskStoreException(
                    "Failed to write quarantine file: " + quarantineFile, e);
        }
    }

    /**
     * 全量、原子地持久化所有记录。
     */
    public synchronized void rewriteAll(List<TaskRecord> records) {
        Path temp = storageDir.resolve("tasks.jsonl.tmp." + Thread.currentThread().getId());
        try {
            List<String> lines = new ArrayList<>(records.size());
            for (TaskRecord record : records) {
                lines.add(objectMapper.writeValueAsString(record));
            }
            Files.write(temp, lines, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);
            try (FileChannel channel = FileChannel.open(temp, StandardOpenOption.WRITE)) {
                channel.force(true);
            }
            try {
                Files.move(temp, dataFile, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temp, dataFile, StandardCopyOption.REPLACE_EXISTING);
            }
            try (var dir = Files.newDirectoryStream(storageDir)) {
                // touch directory entry to ensure the rename is durable
                dir.iterator().forEachRemaining(p -> { });
            }
        } catch (IOException e) {
            throw new TaskStoreException("Failed to persist task store: " + dataFile, e);
        } catch (UncheckedIOException e) {
            throw new TaskStoreException("Failed to persist task store: " + dataFile, e.getCause());
        } finally {
            try {
                Files.deleteIfExists(temp);
            } catch (IOException ignored) {
                // best effort cleanup
            }
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
