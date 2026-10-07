package com.github.lechandonga.durabletaskqueuejava.web;

import com.github.lechandonga.durabletaskqueuejava.core.TaskRejectedException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/**
 * 入队拒绝映射为 409，并在响应体中给出机器可读的拒绝原因：
 * DUPLICATE_ACTIVE_KEY（重复提交）与 QUEUE_FULL（容量超限）可明确区分。
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(TaskRejectedException.class)
    public ResponseEntity<Map<String, Object>> handleRejected(TaskRejectedException e) {
        Map<String, Object> body = new java.util.HashMap<>();
        body.put("rejected", true);
        body.put("reason", e.getReason().name());
        body.put("message", e.getMessage());
        body.put("existingTaskId", e.getExistingTaskId());
        return ResponseEntity.status(HttpStatus.CONFLICT).body(body);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleBadRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of(
                "rejected", true,
                "reason", "INVALID_REQUEST",
                "message", e.getMessage() == null ? "" : e.getMessage()));
    }
}
