package com.github.lechandonga.durabletaskqueuejava.web;

import com.github.lechandonga.durabletaskqueuejava.core.TaskRecord;

/**
 * 对外暴露的任务视图。
 */
public record TaskView(
        String id,
        String idempotencyKey,
        String type,
        String payload,
        String status,
        int attempts,
        int maxAttempts,
        long createdAt,
        long visibleAt,
        String leasedBy,
        long leaseExpireAt,
        String lastError,
        long updatedAt) {

    public static TaskView from(TaskRecord record, int maxAttempts) {
        return new TaskView(
                record.getId(),
                record.getIdempotencyKey(),
                record.getType(),
                record.getPayload(),
                record.getStatus().name(),
                record.getAttempts(),
                maxAttempts,
                record.getCreatedAt(),
                record.getVisibleAt(),
                record.getLeasedBy(),
                record.getLeaseExpireAt(),
                record.getLastError(),
                record.getUpdatedAt());
    }
}
