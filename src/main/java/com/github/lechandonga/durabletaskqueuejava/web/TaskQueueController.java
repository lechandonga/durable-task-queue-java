package com.github.lechandonga.durabletaskqueuejava.web;

import com.github.lechandonga.durabletaskqueuejava.core.QueueMetrics;
import com.github.lechandonga.durabletaskqueuejava.core.Task;
import com.github.lechandonga.durabletaskqueuejava.core.TaskQueueService;
import com.github.lechandonga.durabletaskqueuejava.core.TaskWorkerPool;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 本地任务队列 REST API：
 * <ul>
 *     <li>POST /api/tasks                提交任务</li>
 *     <li>GET  /api/tasks/{id}           按任务 id 查询</li>
 *     <li>GET  /api/tasks/idempotency/{key} 按幂等键查询</li>
 *     <li>GET  /api/tasks/metrics        队列指标（含损坏记录计数）</li>
 *     <li>GET  /api/tasks/storage-issues 本地存储损坏/异常记录明细</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/tasks")
public class TaskQueueController {

    private final TaskQueueService queue;
    private final TaskWorkerPool workerPool;

    public TaskQueueController(TaskQueueService queue, TaskWorkerPool workerPool) {
        this.queue = queue;
        this.workerPool = workerPool;
    }

    @PostMapping
    public TaskView submit(@RequestBody SubmitTaskRequest request) {
        if (request == null || request.type() == null || request.type().isBlank()) {
            throw new IllegalArgumentException("字段 type 不能为空");
        }
        Task task = queue.enqueue(request.type(), request.payload(), request.idempotencyKey());
        return TaskView.from(task);
    }

    @GetMapping("/{id}")
    public TaskView getById(@PathVariable String id) {
        return queue.findById(id).map(TaskView::from)
                .orElseThrow(() -> new TaskNotFoundException(id));
    }

    @GetMapping("/idempotency/{key}")
    public TaskView getByIdempotencyKey(@PathVariable String key) {
        return queue.findByIdempotencyKey(key).map(TaskView::from)
                .orElseThrow(() -> new TaskNotFoundException("idempotencyKey=" + key));
    }

    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        QueueMetrics m = queue.metrics();
        return Map.of(
                "active", m.active(),
                "capacity", m.capacity(),
                "pending", m.pending(),
                "running", m.running(),
                "succeeded", m.succeeded(),
                "dead", m.dead(),
                "storageIssues", m.corruptRecords(),
                "registeredTaskTypes", workerPool.registeredTypes());
    }

    @GetMapping("/storage-issues")
    public Map<String, Object> storageIssues() {
        return Map.of(
                "corruptRecords", queue.corruptRecords(),
                "recoveryAnomalies", queue.recoveryAnomalies());
    }
}
