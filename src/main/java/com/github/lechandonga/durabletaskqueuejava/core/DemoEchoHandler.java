package com.github.lechandonga.durabletaskqueuejava.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 内置示例处理器：任务类型 {@code echo}，负载原样视为成功。
 * 业务系统可实现自己的 {@link TaskHandler} Bean（配合 {@link TaskType} 注解）。
 */
@TaskType("echo")
public class DemoEchoHandler implements TaskHandler {

    private static final Logger log = LoggerFactory.getLogger(DemoEchoHandler.class);

    @Override
    public TaskOutcome handle(TaskContext context) {
        log.info("执行示例 echo 任务 id={} attempt={} payload={}",
                context.getTaskId(), context.getAttempt(), context.getPayload());
        return TaskOutcome.success();
    }
}
