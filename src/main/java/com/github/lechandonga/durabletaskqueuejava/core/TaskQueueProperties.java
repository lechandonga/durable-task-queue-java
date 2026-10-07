package com.github.lechandonga.durabletaskqueuejava.core;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * 任务队列可调参数，前缀 {@code taskqueue.*}，均有合理默认值。
 */
@ConfigurationProperties(prefix = "taskqueue")
public class TaskQueueProperties {

    /** 本地持久化目录（journal 文件所在目录）。 */
    private String dataDir = "./data/task-queue";

    /** 活跃（未终结）任务容量上限；达到后新提交返回 QUEUE_FULL。 */
    private int capacity = 1000;

    /** 最大尝试次数（首次执行为第 1 次）；可重试失败达到此次数后进入死信。 */
    private int maxAttempts = 5;

    /** 基础退避时长；第 n 次重试退避 = baseBackoff * multiplier^(n-1)，并叠加抖动。 */
    private Duration baseBackoff = Duration.ofSeconds(2);

    /** 退避递增倍数。 */
    private double backoffMultiplier = 2.0;

    /** 退避抖动比例（0~1），实际退避在计算值基础上随机增减该比例，避免重试压垮下游。 */
    private double backoffJitter = 0.2;

    /** 单次执行的租约时长；消费者会定期续租，租约过期未续租的任务可被回收重领。 */
    private Duration leaseDuration = Duration.ofSeconds(30);

    /** 消费者工作线程数量（并发度）。 */
    private int concurrency = 2;

    /** 优雅停机最长等待时间。 */
    private Duration shutdownTimeout = Duration.ofSeconds(20);

    /** journal 文件超过该大小时触发一次压缩（-1 表示不压缩）。 */
    private long compactThresholdBytes = 1024L * 1024L;

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
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

    public Duration getBaseBackoff() {
        return baseBackoff;
    }

    public void setBaseBackoff(Duration baseBackoff) {
        this.baseBackoff = baseBackoff;
    }

    public double getBackoffMultiplier() {
        return backoffMultiplier;
    }

    public void setBackoffMultiplier(double backoffMultiplier) {
        this.backoffMultiplier = backoffMultiplier;
    }

    public double getBackoffJitter() {
        return backoffJitter;
    }

    public void setBackoffJitter(double backoffJitter) {
        this.backoffJitter = backoffJitter;
    }

    public Duration getLeaseDuration() {
        return leaseDuration;
    }

    public void setLeaseDuration(Duration leaseDuration) {
        this.leaseDuration = leaseDuration;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    public Duration getShutdownTimeout() {
        return shutdownTimeout;
    }

    public void setShutdownTimeout(Duration shutdownTimeout) {
        this.shutdownTimeout = shutdownTimeout;
    }

    public long getCompactThresholdBytes() {
        return compactThresholdBytes;
    }

    public void setCompactThresholdBytes(long compactThresholdBytes) {
        this.compactThresholdBytes = compactThresholdBytes;
    }
}
