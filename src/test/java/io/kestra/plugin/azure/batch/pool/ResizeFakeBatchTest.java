package io.kestra.plugin.azure.batch.pool;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.azure.batch.FakeBatchEndpoint;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class ResizeFakeBatchTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void resizesAnActivePool() throws Exception {
        try (var endpoint = new FakeBatchEndpoint()) {
            endpoint
                .reply("HEAD", "/pools/my-pool", 200, null)
                .reply("GET", "/pools/my-pool", 200, pool("active", 3, 1))
                .reply("POST", "/pools/my-pool/resize", 202, null)
                .reply("GET", "/pools/my-pool/nodes", 200, "{\"value\":[{\"id\":\"node-1\",\"state\":\"idle\"}]}");

            var task = resize(endpoint, 3, 1);

            var output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

            assertThat(output.getPool().getId(), is("my-pool"));
            assertThat(output.getPool().getTargetDedicatedNodes(), is(3));
            assertThat(output.getPool().getTargetLowPriorityNodes(), is(1));

            var resizeRequests = endpoint.requests("POST", "/pools/my-pool/resize");
            assertThat(resizeRequests, hasSize(1));
            assertThat(resizeRequests.getFirst().body(), containsString("\"targetDedicatedNodes\":3"));
            assertThat(resizeRequests.getFirst().body(), containsString("\"targetLowPriorityNodes\":1"));
        }
    }

    @Test
    void failsWhenThePoolIsNotActive() throws Exception {
        try (var endpoint = new FakeBatchEndpoint()) {
            endpoint
                .reply("HEAD", "/pools/my-pool", 200, null)
                .reply("GET", "/pools/my-pool", 200, pool("deleting", 0, 0));

            var task = resize(endpoint, 3, 0);

            var error = assertThrows(IllegalArgumentException.class, () -> task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of())));

            assertThat(error.getMessage(), is("Pool 'my-pool' is not active, state: 'deleting'"));
            assertThat(endpoint.requests("POST", "/pools/my-pool/resize"), hasSize(0));
        }
    }

    @Test
    void failsWhenThePoolDoesNotExist() throws Exception {
        try (var endpoint = new FakeBatchEndpoint()) {
            var task = resize(endpoint, 3, 0);

            var error = assertThrows(IllegalArgumentException.class, () -> task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of())));

            assertThat(error.getMessage(), is("Pool 'my-pool' doesn't exists"));
        }
    }

    private static Resize resize(FakeBatchEndpoint endpoint, int dedicated, int lowPriority) {
        return Resize.builder()
            .id("resize-" + IdUtils.create())
            .type(Resize.class.getName())
            .endpoint(Property.ofValue(endpoint.url()))
            .account(Property.ofValue("test-account"))
            .accessKey(Property.ofValue("dGVzdC1rZXk="))
            .poolId(Property.ofValue("my-pool"))
            .targetDedicatedNodes(Property.ofValue(dedicated))
            .targetLowPriorityNodes(Property.ofValue(lowPriority))
            .build();
    }

    private static String pool(String state, int dedicated, int lowPriority) {
        return """
            {"id": "my-pool", "state": "%s", "allocationState": "steady",
             "targetDedicatedNodes": %d, "targetLowPriorityNodes": %d}
            """.formatted(state, dedicated, lowPriority);
    }
}
