package com.github.lechandonga.durabletaskqueuejava.core;

/**
 * 任务在其生命周期中的状态。
 * <ul>
 *     <li>{@link #PENDING}：已入队，等待被消费者领取（含等待退避到期）。</li>
 *     <li>{@link #RUNNING}：已被某个消费者领取，正在执行。</li>
 *     <li>{@link #SUCCEEDED}：终态，处理成功并已确认，不会再被投递。</li>
 *     <li>{@link #DEAD}：终态，死信，不可重试失败或重试次数耗尽后转入。</li>
 * </ul>
 */
public enum TaskStatus {
    PENDING,
    RUNNING,
    SUCCEEDED,
    DEAD;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == DEAD;
    }
}
