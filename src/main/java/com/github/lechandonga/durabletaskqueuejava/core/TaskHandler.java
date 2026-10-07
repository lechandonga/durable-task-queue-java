package com.github.lechandonga.durabletaskqueuejava.core;

import java.util.Objects;

/**
 * 业务任务处理器。实现类通过 {@link TaskHandlerRegistry} 按任务类型注册。
 */
public interface TaskHandler {

    /**
     * 处理任务。
     *
     * @param payload 入队时提交的原始负载（可能为 null）
     * @throws TransientTaskException 可重试失败：队列按退避策略安排重试
     * @throws Exception              其它异常一律视为不可重试失败，直接进入死信
     */
    void handle(String payload) throws Exception;

    /**
     * 处理器支持的任务类型，对应提交时的 {@code type}。
     */
    String type();

    /**
     * 可重试（瞬时）失败。处理器预期外部依赖抖动等可自愈场景抛出本异常。
     */
    class TransientTaskException extends Exception {
        public TransientTaskException(String message) {
            super(Objects.requireNonNull(message, "message"));
        }

        public TransientTaskException(String message, Throwable cause) {
            super(Objects.requireNonNull(message, "message"), cause);
        }
    }
}
