package com.github.lechandonga.durabletaskqueuejava.web;

import com.github.lechandonga.durabletaskqueuejava.config.TaskQueueProperties;
import com.github.lechandonga.durabletaskqueuejava.core.TaskQueue;
import com.github.lechandonga.durabletaskqueuejava.core.TaskRecord;
import com.github.lechandonga.durabletaskqueuejava.core.TaskStatus;
import com.github.lechandonga.durabletaskqueuejava.storage.CorruptRecord;
import com.github.lechandonga.durabletaskqueuejava.web.TaskRequests.SubmitTaskRequest;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.github.lechandonga.durabletaskqueuejava.core.TaskRejectedException;

/**
 * 任务队列 REST 接口。
 * <ul>
 *     <li>POST /api/tasks：提交任务</li>
 *     <li>GET  /api/tasks/{id}：按任务 ID 查询</li>
 *     <li>GET  /api/tasks:by-key?key=：按幂等键查询</li>
 *     <li>GET  /api/tasks：按状态列出任务</li>
 *     <li>GET  /api/dead-letters：列出死信</li>
 *     <li>POST /api/dead-letters/{id}/requeue：死信重新入队</li>
 *     <li>GET  /api/admin/recovery：查看启动恢复与损坏记录报告</li>
 * </ul>
 */
@RestController
@RequestMapping("/api")
public class TaskController {

    private final TaskQueue queue;
    private final TaskQueueProperties properties;

    public TaskController(TaskQueue queue, TaskQueueProperties properties) {
        this.queue = queue;
        this.properties = properties;
    }

    @PostMapping("/tasks")
    public ResponseEntity<TaskView> submit(@RequestBody SubmitTaskRequest request) {
        TaskRecord record = queue.submit(request.idempotencyKey(), request.type(),
                request.payload());
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(TaskView.from(record, properties.getMaxAttempts()));
    }

    @GetMapping("/tasks/{id}")
    public TaskView getById(@PathVariable String id) {
        TaskRecord record = queue.findById(id)
                .orElseThrow(() -> new TaskNotFoundException(id));
        return TaskView.from(record, properties.getMaxAttempts());
    }

    @GetMapping("/tasks")
    public List<TaskView> list(@RequestParam(name = "status", required = false) String status) {
        if (status == null || status.isBlank()) {
            return queue.listAll().stream()
                    .map(record -> TaskView.from(record, properties.getMaxAttempts()))
                    .toList();
        }
        TaskStatus parsed = parseStatus(status);
        return queue.listByStatus(parsed).stream()
                .map(record -> TaskView.from(record, properties.getMaxAttempts()))
                .toList();
    }

    @GetMapping(value = "/tasks", params = "idempotencyKey")
    public TaskView getByIdempotencyKey(@RequestParam("idempotencyKey") String idempotencyKey) {
        TaskRecord record = queue.findByIdempotencyKey(idempotencyKey)
                .orElseThrow(() -> new TaskNotFoundException(idempotencyKey));
        return TaskView.from(record, properties.getMaxAttempts());
    }

    @GetMapping("/dead-letters")
    public List<TaskView> deadLetters() {
        return queue.listByStatus(TaskStatus.DEAD).stream()
                .map(record -> TaskView.from(record, properties.getMaxAttempts()))
                .toList();
    }

    @PostMapping("/dead-letters/{id}/requeue")
    public TaskView requeueDeadLetter(@PathVariable String id) {
        return TaskView.from(queue.requeueDeadLetter(id), properties.getMaxAttempts());
    }

    @GetMapping("/admin/recovery")
    public Map<String, Object> recoveryReport() {
        List<CorruptRecord> corrupt = queue.getCorruptRecordsOnStartup();
        return Map.of(
                "activeCount", queue.activeCount(),
                "corruptCount", corrupt.size(),
                "corruptRecords", corrupt.stream()
                        .map(c -> Map.of("lineNumber", c.lineNumber(),
                                "reason", c.reason()))
                        .toList());
    }

    private static TaskStatus parseStatus(String status) {
        try {
            return TaskStatus.valueOf(status.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown task status: " + status);
        }
    }

    @ExceptionHandler(TaskRejectedException.class)
    public ResponseEntity<Map<String, String>> handleRejected(TaskRejectedException e) {
        HttpStatus httpStatus = switch (e.getReason()) {
            case CAPACITY_FULL -> HttpStatus.TOO_MANY_REQUESTS;
            case DUPLICATE_ACTIVE, ALREADY_SUCCEEDED -> HttpStatus.CONFLICT;
            case INVALID_REQUEST -> HttpStatus.BAD_REQUEST;
        };
        return ResponseEntity.status(httpStatus)
                .body(Map.of("reason", e.getReason().name(), "message", e.getMessage()));
    }

    @ExceptionHandler(TaskNotFoundException.class)
    public ResponseEntity<Map<String, String>> handleNotFound(TaskNotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("reason", "NOT_FOUND", "message", e.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> handleBadRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest()
                .body(Map.of("reason", "INVALID_REQUEST", "message", e.getMessage()));
    }

    static class TaskNotFoundException extends RuntimeException {
        TaskNotFoundException(String identifier) {
            super("Task not found: " + identifier);
        }
    }
}
