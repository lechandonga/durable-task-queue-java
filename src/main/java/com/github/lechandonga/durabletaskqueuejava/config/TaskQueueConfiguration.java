package com.github.lechandonga.durabletaskqueuejava.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.durabletaskqueuejava.core.TaskDispatcher;
import com.github.lechandonga.durabletaskqueuejava.core.TaskHandlerRegistry;
import com.github.lechandonga.durabletaskqueuejava.core.TaskQueue;
import com.github.lechandonga.durabletaskqueuejava.storage.LocalJsonTaskStore;
import java.nio.file.Path;
import java.time.Clock;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 本地任务队列的 Spring 装配：存储 -> 队列（启动即恢复）-> 消费者调度器。
 */
@Configuration
@EnableConfigurationProperties(TaskQueueProperties.class)
public class TaskQueueConfiguration {

    @Bean
    public Clock taskQueueClock() {
        return Clock.systemDefaultZone();
    }

    @Bean
    public LocalJsonTaskStore localJsonTaskStore(TaskQueueProperties properties,
                                                 ObjectMapper objectMapper) {
        return new LocalJsonTaskStore(Path.of(properties.getStorageDir()), objectMapper);
    }

    @Bean
    public TaskQueue taskQueue(TaskQueueProperties properties, LocalJsonTaskStore store,
                               Clock taskQueueClock) {
        TaskQueue queue = new TaskQueue(properties, store, taskQueueClock);
        queue.recover();
        return queue;
    }

    @Bean
    public TaskDispatcher taskDispatcher(TaskQueue queue, TaskHandlerRegistry registry,
                                         TaskQueueProperties properties) {
        TaskDispatcher dispatcher = new TaskDispatcher(queue, registry, properties);
        dispatcher.start();
        return dispatcher;
    }
}
