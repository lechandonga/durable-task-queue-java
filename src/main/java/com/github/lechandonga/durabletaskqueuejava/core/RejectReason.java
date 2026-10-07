package com.github.lechandonga.durabletaskqueuejava.core;

/**
 * 入队被拒绝的原因。两种原因对调用方可明确区分，绝不静默丢弃。
 */
public enum RejectReason {
    /**
     * 相同幂等键已有一个尚未终结（PENDING/RUNNING）的任务。
     * 任务终结（成功或进入死信）后允许用同一幂等键重新提交。
     */
    DUPLICATE_ACTIVE_KEY,
    /**
     * 队列中活跃（未终结）任务数量已达到配置的容量上限。
     */
    QUEUE_FULL
}
