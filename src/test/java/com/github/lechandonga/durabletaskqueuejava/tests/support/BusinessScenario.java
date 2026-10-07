package com.github.lechandonga.durabletaskqueuejava.tests.support;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标注测试类覆盖的业务场景，{@link ScenarioLoggingExtension} 会把业务名、
 * 用例名以及执行结果打印到测试日志，便于从日志看出覆盖了哪些业务链路。
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface BusinessScenario {
    String value();
}
