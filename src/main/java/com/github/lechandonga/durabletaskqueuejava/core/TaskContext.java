package com.github.lechandonga.durabletaskqueuejava.core;

/**
 * 执行任务时传给 {@link TaskHandler} 的上下文，提供任务数据与租约信息。
 */
public class TaskContext {

    private final Task task;
    private final long leaseExpiresAt;

    public TaskContext(Task task, long leaseExpiresAt) {
        this.task = task;
        this.leaseExpiresAt = leaseExpiresAt;
    }

    public String getTaskId() {
        return task.getId();
    }

    public String getType() {
        return task.getType();
    }

    public String getPayload() {
        return task.getPayload();
    }

    public int getAttempt() {
        return task.getAttempts();
    }

    public long getLeaseExpiresAt() {
        return leaseExpiresAt;
    }
}
