package com.github.lechandonga.durabletaskqueuejava.tests;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 业务七（端到端）：Spring Boot 启动后通过 HTTP 完成 提交 -> 消费成功 -> 查询，
 * 以及重复提交/容量拒绝的差异化响应。每个类使用独立临时目录，测试可重复运行。
 */
@SpringBootTest
@AutoConfigureMockMvc
@Import(TestHandlerConfiguration.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class TaskQueueApiIntegrationTests {

    private static final Logger log = LoggerFactory.getLogger(TaskQueueApiIntegrationTests.class);
    private static final java.util.concurrent.atomic.AtomicInteger DIR_SEQ =
            new java.util.concurrent.atomic.AtomicInteger();

    private static Path newDataDir() {
        try {
            return Files.createTempDirectory("tq-api-" + DIR_SEQ.incrementAndGet() + "-");
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("taskqueue.data-dir", () -> newDataDir().toString());
        registry.add("taskqueue.base-backoff", () -> "100ms");
        registry.add("taskqueue.capacity", () -> "3");
        registry.add("taskqueue.max-attempts", () -> "2");
        registry.add("taskqueue.concurrency", () -> "4");
        registry.add("taskqueue.lease-duration", () -> "60s");
    }

    private static final String DUP_KEY = "dup-" + System.nanoTime();
    private static final String[] CAP_KEYS = {"cap-a-" + System.nanoTime(), "cap-b-" + System.nanoTime(), "cap-c-" + System.nanoTime(), "cap-d-" + System.nanoTime()};

    @Autowired
    private MockMvc mockMvc;

    @BeforeEach
    void logCaseStart() {
        log.info("---- HTTP 集成测试用例开始 ----");
    }

    @Test
    @DisplayName("HTTP: 提交echo任务后异步消费成功，可按id查询到SUCCEEDED")
    void submitThenSucceedOverHttp() throws Exception {
        log.info(">>> [HTTP 端到端] 用例：POST 提交 echo 任务 -> 轮询 GET 得到 SUCCEEDED");
        String response = mockMvc.perform(post("/api/tasks")
                        .contentType("application/json")
                        .content("""
                                {"type":"echo","payload":"http-hello","idempotencyKey":"http-key"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.idempotencyKey").value("http-key"))
                .andReturn().getResponse().getContentAsString();
        String id = new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree(response).get("id").asText();

        String status = "";
        for (int i = 0; i < 50; i++) {
            status = mockMvc.perform(get("/api/tasks/" + id))
                    .andExpect(status().isOk())
                    .andReturn().getResponse().getContentAsString();
            if (status.contains("SUCCEEDED")) {
                break;
            }
            Thread.sleep(50);
        }
        mockMvc.perform(get("/api/tasks/idempotency/http-key"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCEEDED"));
        log.info("<<< [HTTP 端到端] 通过：任务 {} 最终 SUCCEEDED", id);
    }

    @Test
    @DisplayName("HTTP: 相同幂等键重复提交返回409 DUPLICATE_ACTIVE_KEY 并携带已有任务id")
    void duplicateSubmitOverHttp() throws Exception {
        log.info(">>> [HTTP 重复拒绝] 用例：同键两次提交，第二次 409 且 reason=DUPLICATE_ACTIVE_KEY");
        String body = "{\"type\":\"blocking-test\",\"idempotencyKey\":\"" + DUP_KEY + "\"}";
        mockMvc.perform(post("/api/tasks").contentType("application/json").content(body))
                .andExpect(status().isOk());
        waitUntilRunning(mockMvc, DUP_KEY);
        mockMvc.perform(post("/api/tasks").contentType("application/json").content(body))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.rejected").value(true))
                .andExpect(jsonPath("$.reason").value("DUPLICATE_ACTIVE_KEY"))
                .andExpect(jsonPath("$.existingTaskId").isNotEmpty());
        log.info("<<< [HTTP 重复拒绝] 通过：重复提交被 409 拒绝，原因 DUPLICATE_ACTIVE_KEY 明确可区分");
    }

    @Test
    @DisplayName("HTTP: 容量打满后返回409 QUEUE_FULL；metrics/storage-issues 接口可用")
    void queueFullOverHttp() throws Exception {
        log.info(">>> [HTTP 容量拒绝] 用例：容量3被打满后，新提交 409 QUEUE_FULL，指标接口可见");
        for (int i = 0; i < 3; i++) {
            String body = "{\"type\":\"blocking-test\",\"idempotencyKey\":\"" + CAP_KEYS[i] + "\"}";
            mockMvc.perform(post("/api/tasks").contentType("application/json").content(body))
                    .andExpect(status().isOk());
        }
        waitUntilRunning(mockMvc, CAP_KEYS[2]);
        String overflow = "{\"type\":\"blocking-test\",\"idempotencyKey\":\"" + CAP_KEYS[3] + "\"}";
        mockMvc.perform(post("/api/tasks").contentType("application/json").content(overflow))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.reason").value("QUEUE_FULL"));
        mockMvc.perform(get("/api/tasks/metrics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.capacity").value(3));
        mockMvc.perform(get("/api/tasks/storage-issues"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.corruptRecords").isArray());
        log.info("<<< [HTTP 容量拒绝] 通过：超限得到 QUEUE_FULL，metrics 与 storage-issues 接口正常");
    }

    private static void waitUntilRunning(MockMvc mockMvc, String key) throws Exception {
        for (int i = 0; i < 50; i++) {
            String body = mockMvc.perform(get("/api/tasks/idempotency/" + key))
                    .andReturn().getResponse().getContentAsString();
            if (body.contains("RUNNING")) {
                return;
            }
            Thread.sleep(40);
        }
        throw new AssertionError("任务 " + key + " 未在预期时间内进入 RUNNING");
    }
}
