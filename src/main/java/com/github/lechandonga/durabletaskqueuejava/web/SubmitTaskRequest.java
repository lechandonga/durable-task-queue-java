package com.github.lechandonga.durabletaskqueuejava.web;

/**
 * 提交任务请求。
 *
 * @param type           任务类型，对应某个 {@code @TaskType} 处理器
 * @param payload        业务负载（字符串，通常为 JSON），可为空
 * @param idempotencyKey 幂等键，可为空；相同键在任务终结前只保留一条
 */
public record SubmitTaskRequest(String type, String payload, String idempotencyKey) {
}
