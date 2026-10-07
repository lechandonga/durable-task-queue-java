package com.github.lechandonga.durabletaskqueuejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lechandonga.durabletaskqueuejava.core.TaskHandler;
import com.github.lechandonga.durabletaskqueuejava.core.TaskHandlerRegistry;
import com.github.lechandonga.durabletaskqueuejava.core.TaskQueue;
import com.github.lechandonga.durabletaskqueuejava.core.TaskStatus;
import com.github.lechandonga.durabletaskqueuejava.tests.support.BusinessScenario;
import com.github.lechandonga.durabletaskqueuejava.tests.support.ScenarioLoggingExtension;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * 业务：应用级重启恢复。第一次运行提交任务、保留一个在途任务并优雅关闭，
 * 第二次用同一本地存储目录启动，在途任务被恢复并重新投递，状态前后一致。
 */
@BusinessScenario("应用停机重启的状态一致性")
@ExtendWith(ScenarioLoggingExtension.class)
class ApplicationRestartIntegrationTest {

    private static final String STORAGE = "target/restart-it-task-queue";

    /** 阻塞在闩上的处理器，用于制造停机时仍在途的任务。 */
    static class BlockingInFlightHandler implements TaskHandler {
        static final String TYPE = "test.blocking-inflight";
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        @Override
        public void handle(String payload) throws Exception {
            entered.countDown();
            release.await(30, TimeUnit.SECONDS);
        }

        @Override
        public String type() {
            return TYPE;
        }
    }

    private ConfigurableApplicationContext startApplication(BlockingInFlightHandler handler) {
        return new SpringApplicationBuilder(
                com.github.lechandonga.durabletaskqueuejava.Application.class)
                .properties("server.port=0",
                        "spring.main.web-application-type=none",
                        "task-queue.storage-dir=" + STORAGE,
                        "task-queue.backoff=50ms",
                        "task-queue.max-attempts=3",
                        "spring.lifecycle.timeout-per-shutdown-phase=4s",
                        "task-queue.shutdown-await=1s")
                .initializers(context -> context.getBeanFactory()
                        .registerSingleton("blockingInFlightHandler", handler))
                .run();
    }

    @Test
    @DisplayName("优雅停机后用同一存储重启：成功任务保持成功，在途任务恢复后重新投递并完成")
    void stateSurvivesFullApplicationRestart() throws Exception {
        cleanStorage();

        String succeededId;
        String runId = String.valueOf(System.nanoTime());
        String succeededKey = "restart-done-" + runId;
        String inFlightKey = "restart-inflight-" + runId;
        BlockingInFlightHandler firstHandler = new BlockingInFlightHandler();
        try (ConfigurableApplicationContext first = startApplication(firstHandler)) {
            TaskQueue queue = first.getBean(TaskQueue.class);
            succeededId = queue.submit(succeededKey, "demo.echo", null).getId();
            queue.claim().ifPresent(r -> queue.succeed(r.getId(), r.getLeasedBy()));
            queue.submit(inFlightKey, BlockingInFlightHandler.TYPE, "after-restart");

            assertTrue(firstHandler.entered.await(10, TimeUnit.SECONDS));
            waitForStatus(queue, inFlightKey, TaskStatus.RUNNING);
        }

        BlockingInFlightHandler secondHandler = new BlockingInFlightHandler();
        try (ConfigurableApplicationContext second = startApplication(secondHandler)) {
            TaskQueue queue = second.getBean(TaskQueue.class);
            assertEquals(TaskStatus.SUCCEEDED,
                    queue.findById(succeededId).orElseThrow().getStatus());

            secondHandler.release.countDown();
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline
                    && queue.findByIdempotencyKey(inFlightKey).orElseThrow().getStatus()
                    != TaskStatus.SUCCEEDED) {
                Thread.sleep(50);
            }
            assertEquals(TaskStatus.SUCCEEDED,
                    queue.findByIdempotencyKey(inFlightKey).orElseThrow().getStatus(),
                    "停机时在途的任务重启后必须被重新投递并得到确定结果");
            assertTrue(secondHandler.entered.await(1, TimeUnit.SECONDS),
                    "在途任务必须在重启后被重新执行");
            assertTrue(queue.claim().isEmpty(), "成功任务在重启后不得重复投递");
        }
    }

    private static void waitForStatus(TaskQueue queue, String key, TaskStatus expected)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (queue.findByIdempotencyKey(key).orElseThrow().getStatus() == expected) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("Task " + key + " did not reach " + expected);
    }

    private static void cleanStorage() throws IOException {
        Path storagePath = Path.of(STORAGE);
        if (!Files.exists(storagePath)) {
            return;
        }
        try (Stream<Path> files = Files.walk(storagePath)) {
            files.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException ignored) {
                    // best effort
                }
            });
        }
    }
}
