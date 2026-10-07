package com.github.lechandonga.durabletaskqueuejava.core;

import org.springframework.stereotype.Component;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标注一个 {@link TaskHandler} Spring Bean 处理的任务类型。
 * 不使用该注解时，类型名由处理器类名推导（去掉 {@code Handler} 后缀并转为 kebab-case）。
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Component
public @interface TaskType {
    String value();
}
