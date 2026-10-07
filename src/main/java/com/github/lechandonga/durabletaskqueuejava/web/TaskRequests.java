package com.github.lechandonga.durabletaskqueuejava.web;

/**
 * HTTP 请求/响应 DTO。
 */
public final class TaskRequests {

    private TaskRequests() {
    }

    public record SubmitTaskRequest(String idempotencyKey, String type, String payload) {
    }
}
