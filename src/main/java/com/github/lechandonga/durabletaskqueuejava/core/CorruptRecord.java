package com.github.lechandonga.durabletaskqueuejava.core;

/**
 * 一条被隔离的损坏记录：所在行号、原始内容片段、错误原因。
 */
public record CorruptRecord(long lineNumber, String rawPreview, String reason) {
}
