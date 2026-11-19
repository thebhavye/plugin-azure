package io.kestra.plugin.azure.batch.job;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.azure.batch.FakeBatchEndpoint;
import io.kestra.plugin.azure.shared.batch.models.Job;
import io.kestra.plugin.azure.shared.batch.models.Task;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class CreateFakeBatchTest {
    private static final String ACCESS_KEY = "dGVzdC1rZXk=";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void runsTasksAndCollectsOutputs() throws Exception {
        var jobId = "job-" + IdUtils.create().toLowerCase();
        try (var endpoint = new FakeBatchEndpoint()) {
            endpoint
                .reply("GET", "/jobs", 200, "{\"value\":[]}")
                .reply("POST", "/jobs", 201, null)
                .reply("POST", "/jobs/" + jobId + "/addtaskcollection", 200, "{\"value\":[{\"status\":\"success\",\"taskId\":\"echo\"}]}")
                .reply("GET", "/jobs/" + jobId + "/tasks", 200, "{\"value\":[" + completedTask("echo", 0, null) + "]}")
                .reply("GET", "/jobs/" + jobId + "/tasks/echo/files/stdout.txt", 200, "hello\n::{\"outputs\":{\"extract\":\"value\"}}::\n")
                .reply("GET", "/jobs/" + jobId + "/tasks/echo/files/stderr.txt", 200, "")
                .reply("GET", "/jobs/" + jobId + "/tasks/echo/files/wd/out.txt", 200, "file content")
                .reply("DELETE", "/jobs/" + jobId, 202, null);

            var task = create(endpoint, jobId, Task.builder()
                .id("echo")
                .commands(Property.ofValue(List.of("echo hello", "echo 'file content' > out.txt")))
                .outputFiles(Property.ofValue(List.of("out.txt")))
                .build());
            var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

            var output = task.run(runContext);

            assertThat(output.getVars().get("extract"), is("value"));
            assertThat(output.getOutputFiles(), hasKey("out.txt"));
            try (var stream = runContext.storage().getFile(output.getOutputFiles().get("out.txt"))) {
                assertThat(new String(stream.readAllBytes()), is("file content"));
            }

            var createdJob = endpoint.requests("POST", "/jobs").getFirst().body();
            assertThat(createdJob, containsString("\"id\":\"" + jobId + "\""));
            assertThat(createdJob, containsString("\"poolId\":\"my-pool\""));
            var createdTasks = endpoint.requests("POST", "/jobs/" + jobId + "/addtaskcollection");
            assertThat(createdTasks, hasSize(1));
            assertThat(createdTasks.getFirst().body(), containsString("\"id\":\"echo\""));
            assertThat(endpoint.requests("DELETE", "/jobs/" + jobId), hasSize(1));
        }
    }

    @Test
    void failsWhenATaskFails() throws Exception {
        var jobId = "job-" + IdUtils.create().toLowerCase();
        try (var endpoint = new FakeBatchEndpoint()) {
            endpoint
                .reply("GET", "/jobs", 200, "{\"value\":[]}")
                .reply("POST", "/jobs", 201, null)
                .reply("POST", "/jobs/" + jobId + "/addtaskcollection", 200, "{\"value\":[{\"status\":\"success\",\"taskId\":\"boom\"}]}")
                .reply("GET", "/jobs/" + jobId + "/tasks", 200, "{\"value\":[" + completedTask("boom", 1, "Task exited with code 1") + "]}")
                .reply("GET", "/jobs/" + jobId + "/tasks/boom/files/stdout.txt", 200, "")
                .reply("GET", "/jobs/" + jobId + "/tasks/boom/files/stderr.txt", 200, "error\n")
                .reply("DELETE", "/jobs/" + jobId, 202, null);

            var task = create(endpoint, jobId, Task.builder()
                .id("boom")
                .commands(Property.ofValue(List.of("exit 1")))
                .build());

            var error = assertThrows(Exception.class, () -> task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of())));

            assertThat(error.getMessage(), is("1/1 task(s) failed, terminating!"));
            assertThat(endpoint.requests("DELETE", "/jobs/" + jobId), hasSize(1));
        }
    }

    @Test
    void reportsBatchErrors() throws Exception {
        var jobId = "job-" + IdUtils.create().toLowerCase();
        try (var endpoint = new FakeBatchEndpoint()) {
            endpoint
                .reply("GET", "/jobs", 200, "{\"value\":[]}")
                .reply("POST", "/jobs", 409, "{\"code\":\"JobExists\",\"message\":{\"lang\":\"en-US\",\"value\":\"The specified job already exists.\"}}");

            var task = create(endpoint, jobId, Task.builder()
                .id("echo")
                .commands(Property.ofValue(List.of("echo hello")))
                .build());

            var error = assertThrows(Exception.class, () -> task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of())));

            assertThat(error.getMessage(), containsString("JobExists"));
        }
    }

    private static Create create(FakeBatchEndpoint endpoint, String jobId, Task task) {
        return Create.builder()
            .id("create-" + IdUtils.create())
            .type(Create.class.getName())
            .endpoint(Property.ofValue(endpoint.url()))
            .account(Property.ofValue("test-account"))
            .accessKey(Property.ofValue(ACCESS_KEY))
            .poolId(Property.ofValue("my-pool"))
            .job(Job.builder().id(jobId).build())
            .tasks(List.of(task))
            .completionCheckInterval(Property.ofValue(Duration.ofMillis(10)))
            .build();
    }

    private static String completedTask(String id, int exitCode, String failure) {
        var failureInfo = failure == null ? "" :
            ",\"failureInfo\":{\"category\":\"usererror\",\"code\":\"FailureExitCode\",\"message\":\"" + failure + "\"}";
        return """
            {"id": "%s", "state": "completed",
             "executionInfo": {"startTime": "2026-01-01T00:00:00Z", "endTime": "2026-01-01T00:00:02Z", "exitCode": %d,
                               "retryCount": 0, "requeueCount": 0%s},
             "stats": {"url": "https://batch/stats", "startTime": "2026-01-01T00:00:00Z", "lastUpdateTime": "2026-01-01T00:00:02Z",
                       "userCPUTime": "PT1S", "kernelCPUTime": "PT0.5S", "wallClockTime": "PT2S", "waitTime": "PT0S",
                       "readIOps": 3, "writeIOps": 4, "readIOGiB": 0.1, "writeIOGiB": 0.2}}
            """.formatted(id, exitCode, failureInfo);
    }
}
