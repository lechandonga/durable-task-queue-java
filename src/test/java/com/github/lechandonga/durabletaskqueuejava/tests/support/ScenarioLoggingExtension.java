package com.github.lechandonga.durabletaskqueuejava.tests.support;

import java.util.Optional;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 输出「业务场景 -> 用例 -> 测试结果」结构化日志。 */
public class ScenarioLoggingExtension implements BeforeAllCallback, BeforeTestExecutionCallback,
        AfterTestExecutionCallback, AfterAllCallback {

    private static final Logger log = LoggerFactory.getLogger("BUSINESS-COVERAGE");
    private static final ExtensionContext.Namespace NAMESPACE =
            ExtensionContext.Namespace.create(ScenarioLoggingExtension.class);

    @Override
    public void beforeAll(ExtensionContext context) {
        log.info("====== 业务场景开始: {} ======", scenarioName(context));
    }

    @Override
    public void beforeTestExecution(ExtensionContext context) {
        log.info(">>> 用例开始: {}.{}", scenarioName(context), context.getDisplayName());
        context.getStore(NAMESPACE).put("startNanos", System.nanoTime());
    }

    @Override
    public void afterTestExecution(ExtensionContext context) {
        long start = context.getStore(NAMESPACE).get("startNanos", long.class);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        Optional<Throwable> failure = context.getExecutionException();
        if (failure.isPresent()) {
            log.error("<<< 用例失败: {}.{} ({} ms) - {}", scenarioName(context),
                    context.getDisplayName(), elapsedMs,
                    failure.get().getClass().getSimpleName() + ": "
                            + failure.get().getMessage());
        } else {
            log.info("<<< 用例通过: {}.{} ({} ms)", scenarioName(context),
                    context.getDisplayName(), elapsedMs);
        }
    }

    @Override
    public void afterAll(ExtensionContext context) {
        log.info("====== 业务场景结束: {} ======", scenarioName(context));
    }

    private static String scenarioName(ExtensionContext context) {
        Class<?> testClass = context.getTestClass().orElseThrow();
        BusinessScenario annotation = testClass.getAnnotation(BusinessScenario.class);
        return annotation != null ? annotation.value() : testClass.getSimpleName();
    }
}
