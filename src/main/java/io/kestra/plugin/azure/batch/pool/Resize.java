package io.kestra.plugin.azure.batch.pool;

import com.azure.compute.batch.models.BatchPoolResizeParameters;
import com.azure.compute.batch.models.BatchPoolState;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.shared.batch.AbstractBatch;
import io.kestra.plugin.azure.shared.batch.BatchService;
import io.kestra.plugin.azure.shared.batch.models.Pool;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

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
                id: azure_batch_pool_resize
                namespace: company.team

                tasks:
                  - id: resize
                    type: io.kestra.plugin.azure.batch.pool.Resize
                    endpoint: https://my.francecentral.batch.azure.com
                    account: "{{ secret('AZURE_ACCOUNT') }}"
                    accessKey: "{{ secret('AZURE_ACCESS_KEY') }}"
                    poolId: "<your-pool-id>"
                    targetDedicatedNodes: 12
                """
        )
    }
)
@Schema(
    title = "Resize an Azure Batch pool",
    description = "Adjusts dedicated and low-priority node counts for an active pool. Fails if pool is not ACTIVE."
)
public class Resize extends AbstractBatch implements RunnableTask<Resize.Output> {
    @Schema(
        title = "Pool ID",
        description = "Existing pool to resize; must be active"
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> poolId;

    @Schema(
        title = "Target dedicated nodes",
        description = "Desired dedicated node count; defaults to 0"
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "destination")
    private Property<Integer> targetDedicatedNodes = Property.ofValue(0);

    @Schema(
        title = "Target low-priority nodes",
        description = "Desired spot/low-priority node count; defaults to 0"
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "destination")
    private Property<Integer> targetLowPriorityNodes = Property.ofValue(0);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var client = BatchService.batchClient(this.endpoint, this.account, this.accessKey, runContext);

        var rPoolId = runContext.render(this.poolId).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("'poolId' is required, set it to the ID of an existing Batch pool"));

        if (!client.poolExists(rPoolId)) {
            throw new IllegalArgumentException("Pool '" + rPoolId + "' doesn't exists");
        }

        var pool = client.getPool(rPoolId);

        if (!BatchPoolState.ACTIVE.equals(pool.getState())) {
            throw new IllegalArgumentException("Pool '" + rPoolId + "' is not active, state: '" + pool.getState() + "'");
        }

        // Starts the resize; PoolService waits for the steady state and an idle node.
        client.beginResizePool(
            rPoolId,
            new BatchPoolResizeParameters()
                .setTargetDedicatedNodes(runContext.render(this.targetDedicatedNodes).as(Integer.class).orElse(0))
                .setTargetLowPriorityNodes(runContext.render(this.targetLowPriorityNodes).as(Integer.class).orElse(0))
        );

        var resizedPool = PoolService.waitForReady(runContext, client, pool);

        return Output
            .builder()
            .pool(
                Pool.builder()
                    .id(resizedPool.getId())
                    .targetDedicatedNodes(resizedPool.getTargetDedicatedNodes())
                    .targetLowPriorityNodes(resizedPool.getTargetLowPriorityNodes())
                    .build()
            )
            .build();
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Resized pool state"
        )
        private Pool pool;
    }
}
