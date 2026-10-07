package com.github.lechandonga.durabletaskqueuejava.core;

/**
 * 业务侧实现的任务处理器，按任务 {@code type} 注册到 Spring 容器即可被消费者发现。
 * 执行可能耗时较长时，建议周期性检查 {@link TaskContext#getLeaseExpiresAt()}
 * （消费者会自动续租，正常情况下无需关心；租约被回收说明本次执行已经超时失效）。
 */
@FunctionalInterface
public interface TaskHandler {

    TaskOutcome handle(TaskContext context) throws Exception;
}
