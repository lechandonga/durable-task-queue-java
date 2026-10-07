package com.github.lechandonga.durabletaskqueuejava.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

import org.springframework.context.annotation.Bean;

/**
 * 本地任务队列装配：纯本地文件持久化，不依赖任何需要单独部署的中间件。
 */
@Configuration
@EnableConfigurationProperties(TaskQueueProperties.class)
public class TaskQueueConfiguration {

    @Bean
    public TaskQueueService taskQueueService(TaskQueueProperties properties, ObjectMapper objectMapper) {
        return new TaskQueueService(properties, objectMapper);
    }

    @Bean
    public TaskWorkerPool taskWorkerPool(TaskQueueService taskQueueService,
                                         TaskQueueProperties properties,
                                         List<TaskHandler> handlers) {
        return new TaskWorkerPool(taskQueueService, properties, handlers);
    }

    @Bean
    public DemoEchoHandler demoEchoHandler() {
        return new DemoEchoHandler();
    }
}
