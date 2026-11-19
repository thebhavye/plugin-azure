package io.kestra.plugin.azure.batch;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.sun.net.httpserver.HttpServer;

/**
 * Minimal Azure Batch REST endpoint for tests: replies are registered per "METHOD /path" and every request is recorded.
 */
public final class FakeBatchEndpoint implements AutoCloseable {
    public record Reply(int status, String body) {
    }

    public record Request(String method, String path, String query, String body) {
    }

    private final HttpServer server;
    private final Map<String, Reply> replies = new ConcurrentHashMap<>();
    private final List<Request> requests = new CopyOnWriteArrayList<>();

    public FakeBatchEndpoint() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            var path = exchange.getRequestURI().getPath();
            var rawQuery = exchange.getRequestURI().getRawQuery();
            requests.add(new Request(
                exchange.getRequestMethod(),
                path,
                rawQuery == null ? "" : URLDecoder.decode(rawQuery, StandardCharsets.UTF_8),
                new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)
            ));

            var reply = replies.getOrDefault(
                exchange.getRequestMethod() + " " + path,
                new Reply(404, "{\"code\":\"NotFound\",\"message\":{\"lang\":\"en-US\",\"value\":\"Not found: " + path + "\"}}")
            );
            var body = reply.body() == null ? new byte[0] : reply.body().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            if ("HEAD".equals(exchange.getRequestMethod()) || body.length == 0) {
                exchange.sendResponseHeaders(reply.status(), -1);
            } else {
                exchange.sendResponseHeaders(reply.status(), body.length);
                try (var response = exchange.getResponseBody()) {
                    response.write(body);
                }
            }
            exchange.close();
        });
        server.start();
    }

    public FakeBatchEndpoint reply(String method, String path, int status, String body) {
        replies.put(method + " " + path, new Reply(status, body));
        return this;
    }

    public String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    public List<Request> requests() {
        return requests;
    }

    public List<Request> requests(String method, String path) {
        return requests.stream().filter(r -> r.method().equals(method) && r.path().equals(path)).toList();
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
