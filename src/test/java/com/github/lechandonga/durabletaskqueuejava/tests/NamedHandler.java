package com.github.lechandonga.durabletaskqueuejava.tests;

import com.github.lechandonga.durabletaskqueuejava.core.TaskContext;
import com.github.lechandonga.durabletaskqueuejava.core.NamedTaskHandler;
import com.github.lechandonga.durabletaskqueuejava.core.TaskHandler;
import com.github.lechandonga.durabletaskqueuejava.core.TaskOutcome;

/**
 * 测试用处理器：显式绑定任务类型名（等价于 {@code @TaskType}），包装任意 lambda。
 */
class NamedHandler implements NamedTaskHandler {

    private final String type;
    private final TaskHandler delegate;

    NamedHandler(String type, TaskHandler delegate) {
        this.type = type;
        this.delegate = delegate;
    }

    @Override
    public String typeName() {
        return type;
    }

    @Override
    public TaskOutcome handle(TaskContext context) throws Exception {
        return delegate.handle(context);
    }
}
