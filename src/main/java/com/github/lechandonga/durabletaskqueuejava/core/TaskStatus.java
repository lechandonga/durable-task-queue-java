package com.github.lechandonga.durabletaskqueuejava.core;

/**
 * 任务生命周期状态。
 *
 * <pre>
 * PENDING ──lease──▶ RUNNING ──ack成功──────────────▶ SUCCEEDED（终态）
 *    ▲                  │
 *    │                  ├──可重试失败/退避到期────────┤
 *    │                  ├──超过最大尝试次数/不可重试──▶ DEAD（终态，死信）
 *    │                  ├──租约过期被回收/停机释放────┘
 *    └──────────────────┘
 * </pre>
 */
public enum TaskStatus {
    /** 已入队，等待可见时间到达后被消费者领取。 */
    PENDING,
    /** 已被某个消费者租约领取，正在处理中。 */
    RUNNING,
    /** 处理成功并已确认，终态，不会再被投递或执行。 */
    SUCCEEDED,
    /** 死信：超过最大尝试次数或发生不可重试失败，终态，保留最后失败原因。 */
    DEAD
}
