package com.github.lechandonga.durabletaskqueuejava.storage;

import com.github.lechandonga.durabletaskqueuejava.core.TaskRecord;
import java.util.List;

/**
 * 存储文件加载结果：可正常使用的记录 + 损坏记录报告。
 */
public record StorageLoadResult(List<TaskRecord> records, List<CorruptRecord> corruptRecords) {
}
