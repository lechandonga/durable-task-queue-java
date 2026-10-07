package com.github.lechandonga.durabletaskqueuejava.tests.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.durabletaskqueuejava.config.TaskQueueProperties;
import com.github.lechandonga.durabletaskqueuejava.core.TaskDispatcher;
import com.github.lechandonga.durabletaskqueuejava.core.TaskHandler;
import com.github.lechandonga.durabletaskqueuejava.core.TaskHandlerRegistry;
import com.github.lechandonga.durabletaskqueuejava.core.TaskQueue;
import com.github.lechandonga.durabletaskqueuejava.storage.LocalJsonTaskStore;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;

/** 测试用装配工具：快速退避/短租约，隔离的临时目录。 */
public final class QueueTestSupport {

    private QueueTestSupport() {
    }

    public static TaskQueueProperties fastProperties() {
        TaskQueueProperties properties = new TaskQueueProperties();
        properties.setMaxAttempts(3);
        properties.setBackoff(Duration.ofMillis(100));
        properties.setBackoffMultiplier(2.0);
        properties.setMaxBackoff(Duration.ofSeconds(30));
        properties.setLeaseTtl(Duration.ofSeconds(2));
        properties.setPollInterval(Duration.ofMillis(10));
        properties.setShutdownAwait(Duration.ofSeconds(10));
        properties.setConcurrency(2);
        properties.setCapacity(100);
        return properties;
    }

    public static TaskQueue newQueue(Path storageDir, TaskQueueProperties properties, Clock clock) {
        LocalJsonTaskStore store = new LocalJsonTaskStore(storageDir, new ObjectMapper());
        TaskQueue queue = new TaskQueue(properties, store, clock);
        queue.recover();
        return queue;
    }

    public static TaskQueue reopenQueue(Path storageDir, TaskQueueProperties properties,
                                        Clock clock) {
        return newQueue(storageDir, properties, clock);
    }

    public static TaskDispatcher newDispatcher(TaskQueue queue, TaskQueueProperties properties,
                                               List<TaskHandler> handlers) {
        TaskDispatcher dispatcher = new TaskDispatcher(queue,
                new TaskHandlerRegistry(handlers), properties);
        dispatcher.start();
        return dispatcher;
    }
}
