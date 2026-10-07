package com.github.lechandonga.durabletaskqueuejava.handler;

import com.github.lechandonga.durabletaskqueuejava.core.TaskHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 内置演示处理器：任务类型为 {@code demo.echo}，仅记录负载内容。
 * 可通过 POST /api/tasks 提交 {"idempotencyKey":"...","type":"demo.echo","payload":"hello"} 体验。
 */
@Component
public class DemoEchoHandler implements TaskHandler {

    private static final Logger log = LoggerFactory.getLogger(DemoEchoHandler.class);

    @Override
    public void handle(String payload) {
        log.info("[demo.echo] processed payload: {}", payload);
    }

    @Override
    public String type() {
        return "demo.echo";
    }
}
