package com.github.lechandonga.durabletaskqueuejava.core;

/**
 * 队列运行指标快照。
 */
public record QueueMetrics(int active, int capacity, int pending, int running,
                           int succeeded, int dead, int corruptRecords) {
}
