package com.github.lechandonga.durabletaskqueuejava.core;

/**
 * 一次任务执行的结果。
 * <ul>
 *     <li>{@link #success()}：执行成功，任务确认终结；</li>
 *     <li>{@link #retryable(String)}：可重试失败，按递增退避重新投递，超过最大尝试次数后进入死信；</li>
 *     <li>{@link #dead(String)}：不可重试失败，立即进入死信。</li>
 * </ul>
 * 抛出未捕获异常等价于 {@code retryable(异常信息)}。
 */
public sealed interface TaskOutcome permits TaskOutcome.Success, TaskOutcome.Failure {

    static TaskOutcome success() {
        return Success.INSTANCE;
    }

    static TaskOutcome retryable(String reason) {
        return new Failure(true, reason);
    }

    static TaskOutcome dead(String reason) {
        return new Failure(false, reason);
    }

    /** 应用该执行结果到队列（成功确认 / 失败重试或死信）。 */
    void applyTo(TaskQueueService queue, String taskId);

    final class Success implements TaskOutcome {
        static final Success INSTANCE = new Success();
        private Success() {
        }

        @Override
        public void applyTo(TaskQueueService queue, String taskId) {
            queue.acknowledgeSuccess(taskId);
        }
    }

    record Failure(boolean retryable, String reason) implements TaskOutcome {
        @Override
        public void applyTo(TaskQueueService queue, String taskId) {
            queue.reportFailure(taskId, retryable, reason == null ? "未知失败原因" : reason);
        }
    }
}
