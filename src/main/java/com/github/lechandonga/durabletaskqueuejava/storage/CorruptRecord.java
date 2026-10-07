package com.github.lechandonga.durabletaskqueuejava.storage;

/**
 * 本地存储中一条无法解析的损坏记录。原始内容会被原样保留到隔离文件，
 * 并通过恢复报告明确暴露，绝不静默丢弃。
 */
public record CorruptRecord(int lineNumber, String rawLine, String reason) {
}
