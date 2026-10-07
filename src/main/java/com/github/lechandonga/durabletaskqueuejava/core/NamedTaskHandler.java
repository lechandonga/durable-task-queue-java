package com.github.lechandonga.durabletaskqueuejava.core;

/**
 * 处理器可选实现该接口显式声明任务类型；
 * 否则通过 {@link TaskType} 注解或类名推导。
 */
public interface NamedTaskHandler extends TaskHandler {

    String typeName();
}
