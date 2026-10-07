package com.github.lechandonga.durabletaskqueuejava.core;

/**
 * 任务提交被拒绝时抛出，携带可区分的 {@link RejectReason} 与冲突任务 id。
 */
public class TaskRejectedException extends RuntimeException {

    private final RejectReason reason;
    private final String existingTaskId;

    public TaskRejectedException(RejectReason reason, String message, String existingTaskId) {
        super(message);
        this.reason = reason;
        this.existingTaskId = existingTaskId;
    }

    public RejectReason getReason() {
        return reason;
    }

    public String getExistingTaskId() {
        return existingTaskId;
    }
}
