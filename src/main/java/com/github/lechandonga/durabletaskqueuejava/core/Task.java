package com.github.lechandonga.durabletaskqueuejava.core;

import java.time.Instant;

/**
 * 一条任务的完整状态。该类字段的修改只允许在 {@link TaskQueueService} 的锁内进行。
 */
public class Task {

    private String id;
    private String idempotencyKey;
    private String type;
    private String payload;
    private TaskStatus status;

    /** 已尝试执行的次数（含正在进行的这一次）。 */
    private int attempts;
    /** 下次可以被领取的时间（退避到期时间），epoch millis。 */
    private long visibleAt;
    /** 当前租约到期时间；0 表示未被领取。 */
    private long leaseExpiresAt;
    private String lastError;
    private final long createdAt;
    private long updatedAt;

    public Task() {
        this.createdAt = Instant.now().toEpochMilli();
        this.updatedAt = this.createdAt;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public void setStatus(TaskStatus status) {
        this.status = status;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public long getVisibleAt() {
        return visibleAt;
    }

    public void setVisibleAt(long visibleAt) {
        this.visibleAt = visibleAt;
    }

    public long getLeaseExpiresAt() {
        return leaseExpiresAt;
    }

    public void setLeaseExpiresAt(long leaseExpiresAt) {
        this.leaseExpiresAt = leaseExpiresAt;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
    }

    public boolean isTerminal() {
        return status == TaskStatus.SUCCEEDED || status == TaskStatus.DEAD;
    }
}
