package com.github.lechandonga.durabletaskqueuejava.tests.support;

import com.github.lechandonga.durabletaskqueuejava.core.TaskHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;

/** 可编程的测试处理器：记录执行次数、支持前 N 次瞬时失败、可阻塞等待释放。 */
public class RecordingHandler implements TaskHandler {

    private final String type;
    private final Predicate<Integer> transientFailUntilAttempt;
    private final boolean failFatally;
    public final List<String> executions = new CopyOnWriteArrayList<>();

    @FunctionalInterface
    public interface AttemptBarrier {
        void await() throws InterruptedException;
    }

    private volatile AttemptBarrier barrier;

    public RecordingHandler(String type) {
        this(type, attempt -> false, false);
    }

    public RecordingHandler(String type, Predicate<Integer> transientFailUntilAttempt,
                            boolean failFatally) {
        this.type = type;
        this.transientFailUntilAttempt = transientFailUntilAttempt;
        this.failFatally = failFatally;
    }

    public void setBarrier(AttemptBarrier barrier) {
        this.barrier = barrier;
    }

    @Override
    public void handle(String payload) throws Exception {
        AttemptBarrier current = barrier;
        if (current != null) {
            current.await();
        }
        int attemptNumber = executions.size() + 1;
        executions.add(payload);
        if (failFatally) {
            throw new RuntimeException("fatal failure from handler");
        }
        if (transientFailUntilAttempt.test(attemptNumber)) {
            throw new TransientTaskException("transient failure on attempt " + attemptNumber);
        }
    }

    @Override
    public String type() {
        return type;
    }

    public int executionCount() {
        return executions.size();
    }

    public List<String> getExecutions() {
        return new ArrayList<>(executions);
    }
}
