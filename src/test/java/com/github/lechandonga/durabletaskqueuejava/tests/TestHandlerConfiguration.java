package com.github.lechandonga.durabletaskqueuejava.tests;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

/**
 * HTTP 集成测试专用处理器装配。
 */
@TestConfiguration
public class TestHandlerConfiguration {

    @Bean
    public BlockingTestHandler blockingTestHandler() {
        return new BlockingTestHandler();
    }
}
