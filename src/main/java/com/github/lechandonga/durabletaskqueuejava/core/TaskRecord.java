package com.github.lechandonga.durabletaskqueuejava.core;

/**
 * 任务的持久化快照。一条记录对应队列中的一个任务实例。
 * <p>
 * 该类是可变的，但所有读写都必须在 {@code TaskQueue} 的锁保护下进行；
 * 对外暴露（查询、序列化）时使用 {@link #copy()} 复制，避免逃逸出锁。
 */
public final class TaskRecord {

    private String id;
    private String idempotencyKey;
    private String type;
    private String payload;
    private TaskStatus status;
    private int attempts;
    private long createdAt;
    private long visibleAt;
    private String leasedBy;
    private long leaseExpireAt;
    private String lastError;
    private long updatedAt;

    public TaskRecord() {
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

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    /**
     * 任务最早可被再次领取的时间（epoch millis）。重试时 = 失败时间 + 退避延迟。
     */
    public long getVisibleAt() {
        return visibleAt;
    }

    public void setVisibleAt(long visibleAt) {
        this.visibleAt = visibleAt;
    }

    public String getLeasedBy() {
        return leasedBy;
    }

    public void setLeasedBy(String leasedBy) {
        this.leasedBy = leasedBy;
    }

    public long getLeaseExpireAt() {
        return leaseExpireAt;
    }

    public void setLeaseExpireAt(long leaseExpireAt) {
        this.leaseExpireAt = leaseExpireAt;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
    }

    public TaskRecord copy() {
        TaskRecord r = new TaskRecord();
        r.id = id;
        r.idempotencyKey = idempotencyKey;
        r.type = type;
        r.payload = payload;
        r.status = status;
        r.attempts = attempts;
        r.createdAt = createdAt;
        r.visibleAt = visibleAt;
        r.leasedBy = leasedBy;
        r.leaseExpireAt = leaseExpireAt;
        r.lastError = lastError;
        r.updatedAt = updatedAt;
        return r;
    }
}
