package com.github.lechandonga.durabletaskqueuejava.core;

/**
 * 任务提交被拒绝时抛出，携带可区分的 {@link RejectReason}。
 */
public class TaskRejectedException extends RuntimeException {

    private final RejectReason reason;

    public TaskRejectedException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason getReason() {
        return reason;
    }
}
