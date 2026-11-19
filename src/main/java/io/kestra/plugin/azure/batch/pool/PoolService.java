package io.kestra.plugin.azure.batch.pool;

import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.azure.compute.batch.BatchClient;
import com.azure.compute.batch.models.AllocationState;
import com.azure.compute.batch.models.BatchNodesListOptions;
import com.azure.compute.batch.models.BatchPool;

import io.kestra.core.runners.RunContext;

public class PoolService {
    public static BatchPool waitForReady(RunContext runContext, BatchClient client, BatchPool pool) throws TimeoutException, InterruptedException {
        var poolSteadyTimeout = Duration.ofMinutes(5);
        var vmReadyTimeout = Duration.ofMinutes(20);
        var startTime = System.currentTimeMillis();
        var elapsedTime = 0L;
        var steady = false;

        // Wait for the VM to be allocated
        runContext.logger().trace("Waiting for pool to resize.");
        while (elapsedTime < poolSteadyTimeout.toMillis()) {
            pool = client.getPool(pool.getId());

            if (AllocationState.STEADY.equals(pool.getAllocationState())) {
                steady = true;
                break;
            }
            runContext.logger().debug(".");
            TimeUnit.SECONDS.sleep(10);
            elapsedTime = (new Date()).getTime() - startTime;
        }

        if (!steady) {
            throw new TimeoutException("The pool did not reach a steady state in the allotted time");
        }

        runContext.logger().info("The vm were allocated");

        // The VMs in the pool don't need to be in and IDLE state in order to submit a job.
        // The following code is just an example of how to poll for the VM state
        startTime = System.currentTimeMillis();
        elapsedTime = 0L;
        var hasIdleVM = false;
        var idleNodes = new BatchNodesListOptions()
            .setSelect(List.of("id", "state"))
            .setFilter("state eq 'idle'");

        // Wait for at least 1 VM to reach the IDLE state
        runContext.logger().trace("Waiting for VMs to start.");
        while (elapsedTime < vmReadyTimeout.toMillis()) {
            if (client.listNodes(pool.getId(), idleNodes).stream().findAny().isPresent()) {
                hasIdleVM = true;
                break;
            }

            runContext.logger().debug(".");
            TimeUnit.SECONDS.sleep(10);
            elapsedTime = (new Date()).getTime() - startTime;
        }

        if (!hasIdleVM) {
            throw new TimeoutException("The node did not reach an IDLE state in the allotted time");
        }

        runContext.logger().info("The vm were allocated");

        return pool;
    }
}
