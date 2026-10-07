package com.github.lechandonga.durabletaskqueuejava.tests;

import com.github.lechandonga.durabletaskqueuejava.core.NamedTaskHandler;
import com.github.lechandonga.durabletaskqueuejava.core.TaskContext;
import com.github.lechandonga.durabletaskqueuejava.core.TaskOutcome;

import java.util.concurrent.CountDownLatch;

/**
 * 测试专用阻塞处理器：领取后一直阻塞到 JVM 退出（守护线程），用于制造稳定的 RUNNING 任务，
 * 以验证重复提交/容量拒绝语义。永远不会上报结果，因此任务始终未终结。
 */
public class BlockingTestHandler implements NamedTaskHandler {

    static final String TYPE = "blocking-test";
    static final CountDownLatch EVER = new CountDownLatch(1);

    @Override
    public String typeName() {
        return TYPE;
    }

    @Override
    public TaskOutcome handle(TaskContext context) throws InterruptedException {
        EVER.await();
        return TaskOutcome.success();
    }
}
