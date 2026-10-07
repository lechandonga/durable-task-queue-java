package com.github.lechandonga.durabletaskqueuejava.core;

/**
 * 本地存储中的某条记录损坏或语义非法（CRC 不匹配、JSON 无法解析、必填字段缺失等）。
 * 单条坏记录只导致该记录被隔离，不会让整个队列不可用；
 * 详细信息通过 {@link JournalStore#corruptRecords()} 暴露并记录日志。
 */
public class CorruptRecordException extends RuntimeException {

    private final long lineNumber;

    public CorruptRecordException(long lineNumber, String message) {
        super("损坏的本地存储记录 (第 " + lineNumber + " 行): " + message);
        this.lineNumber = lineNumber;
    }

    public long getLineNumber() {
        return lineNumber;
    }
}
