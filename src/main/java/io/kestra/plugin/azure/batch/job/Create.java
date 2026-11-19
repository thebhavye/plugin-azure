package io.kestra.plugin.azure.batch.job;

import java.io.File;
import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;

import com.azure.compute.batch.BatchClient;
import com.azure.compute.batch.models.BatchErrorException;
import com.azure.compute.batch.models.BatchTask;
import com.azure.compute.batch.models.BatchTaskCreateParameters;
import com.azure.compute.batch.models.BatchTaskFilesListOptions;
import com.azure.core.exception.AzureException;
import com.fasterxml.jackson.annotation.JsonIgnore;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.runners.AbstractLogConsumer;
import io.kestra.core.models.tasks.runners.DefaultLogConsumer;
import io.kestra.core.models.tasks.runners.RemoteRunnerInterface;
import io.kestra.core.models.tasks.runners.ScriptService;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.shared.batch.AbstractBatch;
import io.kestra.plugin.azure.shared.batch.BatchService;
import io.kestra.plugin.azure.shared.batch.job.TaskService;
import io.kestra.plugin.azure.shared.batch.models.Job;
import io.kestra.plugin.azure.shared.batch.models.Task;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import static io.kestra.core.utils.Rethrow.throwRunnable;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: azure_batch_job_create
                namespace: company.team

                tasks:
                  - id: create
                    type: io.kestra.plugin.azure.batch.job.Create
                    endpoint: https://my.francecentral.batch.azure.com
                    account: "{{ secret('AZURE_ACCOUNT') }}"
                    accessKey: "{{ secret('AZURE_ACCESS_KEY') }}"
                    poolId: "{{ secret('AZURE_POOL_ID') }}"
                    job:
                      id: myjob
                    tasks:
                      - id: env
                        commands:
                          - 'echo t1=$ENV_STRING'
                        environments:
                          ENV_STRING: "{{ inputs.first }}"

                      - id: echo
                        commands:
                          - 'echo t2={{ inputs.second }} 1>&2'

                      - id: for
                        commands:
                          -  'for i in $(seq 10); do echo t3=$i; done'

                      - id: vars
                        commands:
                          - echo '::{"outputs":{"extract":"'$(cat files/in/in.txt)'"}::'
                        resourceFiles:
                          - httpUrl: https://unittestkt.blob.core.windows.net/tasks/***?sv=***&se=***&sr=***&sp=***&sig=***
                            filePath: files/in/in.txt

                      - id: output
                        commands:
                          - 'mkdir -p outs/child/sub'
                          - 'echo 1 > outs/1.txt'
                          - 'echo 2 > outs/child/2.txt'
                          - 'echo 3 > outs/child/sub/3.txt'
                        outputFiles:
                          - outs/1.txt
                        outputDirs:
                          - outs/child
                """
        ),
        @Example(
            title = "Use a container to start the task, the pool must use a `microsoft-azure-batch` publisher.",
            full = true,
            code = """
                id: azure_batch_job_create
                namespace: company.team

                tasks:
                  - id: create
                    type: io.kestra.plugin.azure.batch.job.Create
                    endpoint: https://***.francecentral.batch.azure.com
                    account: "{{ secret('AZURE_ACCOUNT') }}"
                    accessKey: "{{ secret('AZURE_ACCESS_KEY') }}"
                    poolId: <pool-id>
                    job:
                      id: <job-name>
                    tasks:
                      - id: echo
                        commands:
                          - 'python --version'
                        containerSettings:
                          imageName: python
                """
        )
    },
    metrics = {
        @Metric(name = "io.read.ops.count", type = Counter.TYPE, description = "The number of read I/O operations performed by the task."),
        @Metric(name = "io.read.gib.count", type = Counter.TYPE, description = "The number of gibibytes read from I/O by the task."),
        @Metric(name = "io.write.ops.count", type = Counter.TYPE, description = "The number of write I/O operations performed by the task."),
        @Metric(name = "io.write.gib.count", type = Counter.TYPE, description = "The number of gibibytes written to I/O by the task."),
        @Metric(name = "cpu.kernel.duration", type = Timer.TYPE, description = "The CPU kernel time."),
        @Metric(name = "cpu.user.duration", type = Timer.TYPE, description = "The CPU user time."),
        @Metric(name = "wall.clock.duration", type = Timer.TYPE, description = "The wall clock time.")
    }
)
@Schema(
    title = "Run tasks in an Azure Batch job",
    description = "Creates or resumes a job on a target pool, uploads task definitions, waits for completion, and optionally deletes the job afterwards. Defaults: resume=true, delete=true, completionCheckInterval=PT1S; set maxDuration to cap wait time."
)
public class Create extends AbstractBatch implements RunnableTask<Create.Output>, RemoteRunnerInterface {
    public static final String DIRECTORY_MARKER = ".kestradirectory";
    @Schema(
        title = "Pool ID",
        description = "Existing Batch pool where the job runs; must be active"
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> poolId;

    @Schema(
        title = "Job definition",
        description = "Job metadata and scheduling limits passed to Azure"
    )
    @PluginProperty(dynamic = true, group = "main")
    @NotNull
    private Job job;

    @Schema(
        title = "Tasks to run",
        description = "Ordered list of task definitions executed within the job"
    )
    @PluginProperty(group = "main")
    @NotNull
    private List<Task> tasks;

    @Schema(
        title = "Maximum wait duration",
        description = "Timeout while waiting for all tasks to finish; null means wait indefinitely"
    )
    @PluginProperty(group = "execution")
    private Property<Duration> maxDuration;

    @Builder.Default
    @PluginProperty(group = "destination")
    private Property<Boolean> syncWorkingDirectory = Property.ofValue(false);

    @Schema(title = "Completion check interval", description = "Poll interval for job status while waiting; defaults to PT1S")
    @Builder.Default
    @PluginProperty(group = "advanced")
    private final Property<Duration> completionCheckInterval = Property.ofValue(Duration.ofSeconds(1));

    @Schema(
        title = "Delete job on completion",
        description = "If true (default), the job is deleted after tasks finish; keep false to inspect failures but retries may reuse the old job"
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "advanced")
    private final Property<Boolean> delete = Property.ofValue(true);

    @Schema(
        title = "Resume existing job if found",
        description = "When true (default), reuses a matching in-progress job instead of creating a new one"
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "advanced")
    private final Property<Boolean> resume = Property.ofValue(true);

    @JsonIgnore
    private AbstractLogConsumer logConsumer;

    @JsonIgnore
    @Builder.Default
    private Boolean pushOutputFilesToInternalStorage = true;

    @JsonIgnore
    @Builder.Default
    @Getter(AccessLevel.NONE)
    private AtomicReference<Runnable> killable = new AtomicReference<>();

    @JsonIgnore
    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final AtomicBoolean isKilled = new AtomicBoolean(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        if (logConsumer == null) {
            logConsumer = new DefaultLogConsumer(runContext);
        }

        Logger logger = runContext.logger();
        BatchClient client = BatchService.batchClient(this.endpoint, this.account, this.accessKey, runContext);

        Map<String, Integer> tasksMapping = new HashMap<>();
        String jobId = null;
        if (Boolean.TRUE.equals(runContext.render(resume).as(Boolean.class).orElse(true))) {
            String baseJobName = ScriptService.jobName(runContext);
            baseJobName = baseJobName.substring(0, baseJobName.lastIndexOf('-') + 1);
            var existingJob = BatchService.getExistingBatchJob(runContext, client, baseJobName);
            if (existingJob.isPresent()) {
                jobId = existingJob.get().getId();
                logger.info("Job is resumed from an already running job {} ", jobId);

                // re-create the task mapping
                int index = 0;
                for (BatchTask current : client.listTasks(jobId)) {
                    tasksMapping.put(current.getId(), index);
                    index++;
                }
            }
        }

        try {
            if (jobId == null) {
                // create job
                var rPoolId = runContext.render(poolId).as(String.class)
                    .orElseThrow(() -> new IllegalArgumentException("'poolId' is required, set it to the ID of an existing Batch pool"));

                var job = this.job.toBatch(runContext, rPoolId);
                jobId = job.getId();
                client.createJob(job);

                logger.info("Job '{}' created on pool '{}'", jobId, rPoolId);

                // create tasks
                List<BatchTaskCreateParameters> tasks = new ArrayList<>();
                int index = 0;
                for (Task current : this.tasks) {
                    var taskCreateParameters = current.toBatch(runContext);

                    tasksMapping.put(taskCreateParameters.getId(), index);
                    tasks.add(taskCreateParameters);

                    index++;
                }

                if (logger.isDebugEnabled()) {
                    tasks
                        .forEach(task -> logger.debug("Starting task '{}' with command: {}", task.getId(), task.getCommandLine()));
                }

                client.createTasks(jobId, tasks);
            }

            String finalJobId = jobId;
            this.killable.set(throwRunnable(() -> safelyKillJobTask(runContext, client, finalJobId)));

            TaskService.waitForTasksToComplete(
                runContext,
                client,
                jobId,
                runContext.render(maxDuration).as(Duration.class).orElse(null),
                runContext.render(completionCheckInterval).as(Duration.class).orElseThrow()
            );

            // get tasks result
            var results = client.listTasks(jobId).stream().toList();

            // failed ?
            var failed = results.stream()
                .filter(Create::hasFailed)
                .toList();

            // populate results
            Map<String, Object> outputs = new ConcurrentHashMap<>();
            Map<String, URI> outputFiles = new ConcurrentHashMap<>();

            for (BatchTask task : results) {
                var executionInfo = task.getExecutionInfo();
                if (hasFailed(task)) {
                    logger.warn("Task '{}' failed with exit code '{}': {}", task.getId(), executionInfo.getExitCode(), executionInfo.getFailureInfo().getMessage());
                } else {
                    logger.info("Task ended '{}' with exit code '{}'", task.getId(), executionInfo == null ? null : executionInfo.getExitCode());
                }

                // metrics
                var stats = task.getTaskStatistics();
                if (stats != null) {
                    runContext.metric(Counter.of("io.read.ops.count", stats.getReadIops()));
                    runContext.metric(Counter.of("io.read.gib.count", stats.getReadIoGiB()));
                    runContext.metric(Counter.of("io.write.ops.count", stats.getWriteIops()));
                    runContext.metric(Counter.of("io.write.gib.count", stats.getWriteIoGiB()));
                    runContext.metric(Timer.of("cpu.kernel.duration", stats.getKernelCpuTime()));
                    runContext.metric(Timer.of("cpu.user.duration", stats.getUserCpuTime()));
                    runContext.metric(Timer.of("wall.clock.duration", stats.getWallClockTime()));
                }

                // log
                TaskService.readRemoteLog(runContext, client, jobId, task, "stdout.txt", msg ->
                {
                    logConsumer.accept(msg, false);
                    outputs.putAll(logConsumer.getOutputs());
                });

                TaskService.readRemoteLog(runContext, client, jobId, task, "stderr.txt", msg ->
                {
                    logConsumer.accept(msg, true);
                    outputs.putAll(logConsumer.getOutputs());
                });

                if (hasFailed(task)) {
                    continue;
                }

                // outputsFiles
                Task currentTask = this.tasks.get(tasksMapping.get(task.getId()));
                String remoteWorkingDir = "wd/";
                if (currentTask.getOutputFiles() != null) {
                    for (String s : runContext.render(currentTask.getOutputFiles()).asList(String.class)) {
                        File file = TaskService.readRemoteFile(runContext, client, jobId, task, remoteWorkingDir + s, s);

                        // As this task is used in the task runner, this processing is already done by the CommandsWrapper so we need some guard
                        if (pushOutputFilesToInternalStorage) {
                            outputFiles.put(s, runContext.storage().putFile(file));
                        }
                    }
                }

                List<String> renderedOutputDirs = runContext.render(currentTask.getOutputDirs()).asList(String.class);
                boolean renderedSyncWorkingDirectory = runContext.render(syncWorkingDirectory).as(Boolean.class).orElseThrow();
                // If we need to sync working directory or have output directories, we need to list all remote files to know which files we are interested in
                if (renderedSyncWorkingDirectory || !renderedOutputDirs.isEmpty()) {
                    var nodeFiles = client.listTaskFiles(jobId, task.getId(), new BatchTaskFilesListOptions().setRecursive(true));

                    for (var nodeFile : nodeFiles) {
                        var nodeFileName = nodeFile.getName();
                        // Filter out directories and files not in the working directory
                        if (!nodeFileName.startsWith(remoteWorkingDir) || Boolean.TRUE.equals(nodeFile.isDirectory()) || nodeFileName.endsWith("/" + DIRECTORY_MARKER)) {
                            continue;
                        }

                        // We check if the file is in one of the output directories to know if we should push it to outputFiles later on
                        Optional<String> parentOutputDirectory = renderedOutputDirs.stream()
                            .filter(s -> nodeFileName.startsWith(remoteWorkingDir + s + "/"))
                            .findFirst();
                        if (parentOutputDirectory.isPresent() || renderedSyncWorkingDirectory) {
                            String relativeFileName = nodeFileName.substring(remoteWorkingDir.length());

                            // This will download the file to runContext wdir so it will already perform the syncWorkingDirectory
                            File file = TaskService.readRemoteFile(runContext, client, jobId, task, nodeFileName, relativeFileName);

                            // We only push the file to outputFiles if it's in one of the output directories
                            if (parentOutputDirectory.isPresent() && pushOutputFilesToInternalStorage) {
                                outputFiles.put(relativeFileName.substring((parentOutputDirectory.get() + "/").length()), runContext.storage().putFile(file));
                            }
                        }
                    }
                }
            }

            if (!failed.isEmpty()) {
                throw new Exception(failed.size() + "/" + this.tasks.size() + " task(s) failed, terminating!");
            }

            return Output
                .builder()
                .vars(outputs)
                .outputFiles(outputFiles)
                .build();
        } catch (BatchErrorException e) {
            var error = e.getValue();
            if (error != null) {
                logger.error("Code '{}', Message '{}'", error.getCode(), error.getMessage() == null ? null : error.getMessage().getValue());
                if (error.getValues() != null) {
                    for (var detail : error.getValues()) {
                        logger.warn("Detail '{}'='{}'", detail.getKey(), detail.getValue());
                    }
                }
            }

            throw new Exception(e.toString(), e);
        } finally {
            kill();
        }
    }

    private static boolean hasFailed(BatchTask task) {
        return task.getExecutionInfo() != null && task.getExecutionInfo().getFailureInfo() != null;
    }

    private void safelyKillJobTask(final RunContext runContext,
        final BatchClient client,
        final String jobId) throws IllegalVariableEvaluationException {
        if (runContext.render(delete).as(Boolean.class).orElse(true)) {
            try {
                // Only starts the deletion: Batch removes the job and its tasks in the background.
                client.beginDeleteJob(jobId);
                runContext.logger().debug("Job deletion started: {}", jobId);
            } catch (AzureException e) {
                runContext.logger().warn("Failed to delete job: {}.", jobId, e);
            }
        }
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void kill() {
        if (isKilled.compareAndSet(false, true)) {
            Runnable runnable = killable.get();
            if (runnable != null) {
                runnable.run();
            }
        }
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Captured outputs from task logs"
        )
        private Map<String, Object> vars;

        @Schema(
            title = "Output files stored in internal storage"
        )
        @PluginProperty(additionalProperties = URI.class)
        private Map<String, URI> outputFiles;
    }
}
