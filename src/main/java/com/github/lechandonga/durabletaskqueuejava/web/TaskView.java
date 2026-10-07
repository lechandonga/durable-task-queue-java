package com.github.lechandonga.durabletaskqueuejava.web;

import com.github.lechandonga.durabletaskqueuejava.core.Task;

/**
 * 对外暴露的任务视图（与内部可变对象隔离）。
 */
public record TaskView(String id, String idempotencyKey, String type, String payload,
                       String status, int attempts, long visibleAt, long leaseExpiresAt,
                       String lastError, long createdAt, long updatedAt) {

    public static TaskView from(Task task) {
        return new TaskView(task.getId(), task.getIdempotencyKey(), task.getType(),
                task.getPayload(), task.getStatus().name(), task.getAttempts(),
                task.getVisibleAt(), task.getLeaseExpiresAt(), task.getLastError(),
                task.getCreatedAt(), task.getUpdatedAt());
    }
}
