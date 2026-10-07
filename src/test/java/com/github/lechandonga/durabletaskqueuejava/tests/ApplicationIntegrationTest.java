package com.github.lechandonga.durabletaskqueuejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.durabletaskqueuejava.tests.support.BusinessScenario;
import com.github.lechandonga.durabletaskqueuejava.tests.support.ScenarioLoggingExtension;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

/**
 * 业务：Spring 全链路——REST 提交、自动消费、拒绝原因区分、停机后重启恢复。
 * 使用隔离的临时存储目录，不依赖任何外部服务。
 */
@BusinessScenario("REST接口与应用级重启恢复")
@TestMethodOrder(MethodOrderer.DisplayName.class)
@ExtendWith(ScenarioLoggingExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "task-queue.storage-dir=target/integration-task-queue",
                "task-queue.backoff=50ms",
                "task-queue.max-attempts=3",
                "task-queue.concurrency=2"
        })
class ApplicationIntegrationTest {

    @LocalServerPort
    int port;

    private final TestRestTemplate restTemplate = new TestRestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeAll
    static void cleanStorage() throws java.io.IOException {
        java.nio.file.Path dir = java.nio.file.Path.of("target/integration-task-queue");
        if (java.nio.file.Files.exists(dir)) {
            try (var files = java.nio.file.Files.walk(dir)) {
                files.sorted(java.util.Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                java.nio.file.Files.deleteIfExists(path);
                            } catch (java.io.IOException ignored) {
                                // best effort
                            }
                        });
            }
        }
    }

    private String base() {
        return "http://localhost:" + port + "/api";
    }

    private HttpEntity<Map<String, String>> json(Map<String, String> body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return new HttpEntity<>(body, headers);
    }

    @Test
    @DisplayName("1-REST: 提交 demo.echo 后自动消费成功，可按ID与幂等键查询")
    void submitAndAutoConsume() throws Exception {
        ResponseEntity<String> response = restTemplate.postForEntity(base() + "/tasks",
                json(Map.of("idempotencyKey", "int-echo-1", "type", "demo.echo",
                        "payload", "hello")), String.class);
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        JsonNode created = objectMapper.readTree(response.getBody());
        String taskId = created.get("id").asText();

        JsonNode byId = waitForStatus(taskId, "SUCCEEDED", 10_000);
        assertEquals("SUCCEEDED", byId.get("status").asText());

        ResponseEntity<String> byKey = restTemplate.getForEntity(
                base() + "/tasks?idempotencyKey=int-echo-1", String.class);
        assertEquals(HttpStatus.OK, byKey.getStatusCode());
        assertTrue(byKey.getBody().contains("SUCCEEDED"));
    }

    @Test
    @DisplayName("2-REST: 并发/连续重复提交返回 409 DUPLICATE_ACTIVE，已成功键返回 409 ALREADY_SUCCEEDED")
    void duplicateSubmissionsRejectedWithDistinctReasons() {
        restTemplate.postForEntity(base() + "/tasks",
                json(Map.of("idempotencyKey", "int-dup", "type", "demo.echo")),
                String.class);
        ResponseEntity<String> duplicate = restTemplate.postForEntity(base() + "/tasks",
                json(Map.of("idempotencyKey", "int-dup", "type", "demo.echo")),
                String.class);
        assertEquals(HttpStatus.CONFLICT, duplicate.getStatusCode());
        assertTrue(duplicate.getBody().contains("DUPLICATE_ACTIVE"));

        waitForStatusByIdempotencyKey("int-dup", "SUCCEEDED", 10_000);
        ResponseEntity<String> replay = restTemplate.postForEntity(base() + "/tasks",
                json(Map.of("idempotencyKey", "int-dup", "type", "demo.echo")),
                String.class);
        assertEquals(HttpStatus.CONFLICT, replay.getStatusCode());
        assertTrue(replay.getBody().contains("ALREADY_SUCCEEDED"));
    }

    @Test
    @DisplayName("3-REST: 未知任务类型不可重试直接死信，可在死信列表查询到原因")
    void unknownTypeGoesDead() {
        restTemplate.postForEntity(base() + "/tasks",
                json(Map.of("idempotencyKey", "int-unknown", "type", "does.not.exist")),
                String.class);
        JsonNode dead = waitForDeadLetter("int-unknown", 10_000);
        assertEquals("DEAD", dead.get("status").asText());
        assertTrue(dead.get("lastError").asText().contains("No handler registered"));
    }

    @Test
    @DisplayName("4-REST: 非法请求返回 400 INVALID_REQUEST")
    void invalidRequest() {
        ResponseEntity<String> response = restTemplate.postForEntity(base() + "/tasks",
                json(Map.of("idempotencyKey", " ", "type", "demo.echo")), String.class);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertTrue(response.getBody().contains("INVALID_REQUEST"));
    }

    private JsonNode waitForStatus(String taskId, String expected, long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            ResponseEntity<String> response = restTemplate.getForEntity(
                    base() + "/tasks/" + taskId, String.class);
            JsonNode body = uncheckedRead(response.getBody());
            if (expected.equals(body.get("status").asText())) {
                return body;
            }
            sleep();
        }
        throw new AssertionError("Task " + taskId + " did not reach " + expected);
    }

    private void waitForStatusByIdempotencyKey(String key, String expected, long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            ResponseEntity<String> response = restTemplate.getForEntity(
                    base() + "/tasks?idempotencyKey=" + key, String.class);
            if (response.getBody() != null && response.getBody().contains("\"status\":\""
                    + expected + "\"")) {
                return;
            }
            sleep();
        }
        throw new AssertionError("Task key " + key + " did not reach " + expected);
    }

    private JsonNode waitForDeadLetter(String key, long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            ResponseEntity<String> response = restTemplate.getForEntity(
                    base() + "/dead-letters", String.class);
            JsonNode body = uncheckedRead(response.getBody());
            for (JsonNode node : body) {
                if (key.equals(node.get("idempotencyKey").asText())) {
                    return node;
                }
            }
            sleep();
        }
        throw new AssertionError("Dead letter " + key + " not found");
    }

    private JsonNode uncheckedRead(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void sleep() {
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
