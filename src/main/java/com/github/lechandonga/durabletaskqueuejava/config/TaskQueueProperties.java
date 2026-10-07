package com.github.lechandonga.durabletaskqueuejava.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 本地任务队列配置项，前缀 {@code task-queue}。
 */
@ConfigurationProperties(prefix = "task-queue")
public class TaskQueueProperties {

    /**
     * 本地持久化目录，任务数据以 JSON Lines 文件保存在该目录下。
     */
    private String storageDir = "data/task-queue";

    /**
     * 活动任务（PENDING/RUNNING，含等待重试）的最大数量；超限拒绝新提交。
     * 终态（SUCCEEDED/DEAD）记录不计入容量。
     */
    private int capacity = 1000;

    /**
     * 最大尝试次数（含首次执行）。可重试失败累计到本次数后进入死信。
     */
    private int maxAttempts = 5;

    /**
     * 首次重试的退避延迟，之后按倍数递增：backoff * multiplier^(attempt-1)。
     */
    private Duration backoff = Duration.ofSeconds(10);

    /**
     * 退避递增倍数。
     */
    private double backoffMultiplier = 2.0;

    /**
     * 退避延迟上限，避免长期重试导致间隔过大。
     */
    private Duration maxBackoff = Duration.ofMinutes(30);

    /**
     * 并发消费者（工作线程）数量。
     */
    private int concurrency = 2;

 /**
     * 领取任务后的租约时长：执行时间超过租约的任务会被判定为失联并可被重新领取。
     * 正常执行期间队列会自动续约。
     */
    private Duration leaseTtl = Duration.ofMinutes(5);

    /**
     * 消费者空闲轮询间隔，以及退避到期检查间隔的上限。
     */
    private Duration pollInterval = Duration.ofMillis(200);

    /**
     * 优雅停机时等待在途任务执行完成的最长时间；超时后强制回收仍在途的任务。
     */
    private Duration shutdownAwait = Duration.ofSeconds(30);

    public String getStorageDir() {
        return storageDir;
    }

    public void setStorageDir(String storageDir) {
        this.storageDir = storageDir;
    }

    public int getCapacity() {
        return capacity;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public Duration getBackoff() {
        return backoff;
    }

    public void setBackoff(Duration backoff) {
        this.backoff = backoff;
    }

    public double getBackoffMultiplier() {
        return backoffMultiplier;
    }

    public void setBackoffMultiplier(double backoffMultiplier) {
        this.backoffMultiplier = backoffMultiplier;
    }

    public Duration getMaxBackoff() {
        return maxBackoff;
    }

    public void setMaxBackoff(Duration maxBackoff) {
        this.maxBackoff = maxBackoff;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    public Duration getLeaseTtl() {
        return leaseTtl;
    }

    public void setLeaseTtl(Duration leaseTtl) {
        this.leaseTtl = leaseTtl;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    public Duration getShutdownAwait() {
        return shutdownAwait;
    }

    public void setShutdownAwait(Duration shutdownAwait) {
        this.shutdownAwait = shutdownAwait;
    }

    public void setPollInterval(Duration pollInterval) {
        this.pollInterval = pollInterval;
    }
}
