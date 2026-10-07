package com.github.lechandonga.durabletaskqueuejava.core;

/**
 * 任务提交被拒绝的可区分原因。
 */
public enum RejectReason {

    /**
     * 队列容量超限：当前活动任务（非终态）数量已达到容量上限。
     */
    CAPACITY_FULL,

    /**
     * 重复提交：相同幂等键的任务尚未终结（PENDING/RUNNING 或等待重试）。
     */
    DUPLICATE_ACTIVE,

    /**
     * 重复提交：相同幂等键的任务此前已经成功完成，禁止再次执行。
     */
    ALREADY_SUCCEEDED,

    /**
     * 入参非法（缺少幂等键 / 任务类型等）。
     */
    INVALID_REQUEST
}
