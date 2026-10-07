package com.github.lechandonga.durabletaskqueuejava.core;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

/**
 * 任务类型 -> 处理器 的注册表。Spring 容器中所有 {@link TaskHandler}
 * 会在启动时自动注册。
 */
@Component
public class TaskHandlerRegistry {

    private final Map<String, TaskHandler> handlers = new ConcurrentHashMap<>();

    public TaskHandlerRegistry(java.util.List<TaskHandler> handlerBeans) {
        for (TaskHandler handler : handlerBeans) {
            register(handler);
        }
    }

    public void register(TaskHandler handler) {
        TaskHandler previous = handlers.putIfAbsent(handler.type(), handler);
        if (previous != null) {
            throw new IllegalStateException("Duplicate task handler for type: " + handler.type());
        }
    }

    public Optional<TaskHandler> find(String type) {
        return Optional.ofNullable(handlers.get(type));
    }
}
