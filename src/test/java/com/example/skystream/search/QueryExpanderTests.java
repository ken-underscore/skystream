package com.example.skystream.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.anthropic.client.AnthropicClientAsync;
import com.anthropic.client.okhttp.AnthropicOkHttpClientAsync;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import reactor.core.publisher.Mono;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/** Runs the real SDK against a local stand-in for the Messages API, so no request leaves the machine. */
class QueryExpanderTests {
    private static final String REPLY = """
            {"id": "msg_test", "type": "message", "role": "assistant", "model": "claude-haiku-5-5",
             "content": [{"type": "text", "text": "{\\"posts\\": [\\"Did Rattlesnake Ledge today\\", \\"Lake 22 was \
            gorgeous\\", \\" \\", \\"Mount Si again\\", \\"one too many\\"]}"}],
             "stop_reason": "end_turn", "stop_sequence": null, "usage": {"input_tokens": 90, "output_tokens": 60}}""";

    private final List<JsonNode> requests = new CopyOnWriteArrayList<>();
    private final Duration timeout = Duration.ofSeconds(5);
    private volatile int status = 200;
    private volatile Duration delay = Duration.ZERO;
    private HttpServer server;
    private AnthropicClientAsync anthropic;

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/v1/messages", this::answer);
        server.start();
        anthropic = AnthropicOkHttpClientAsync.builder().apiKey("test-key").maxRetries(0)
                .baseUrl("http://localhost:" + server.getAddress().getPort()).build();
    }

    @AfterEach
    void stop() {
        anthropic.close();
        server.stop(0);
    }

    private void answer(HttpExchange exchange) throws IOException {
        requests.add(new ObjectMapper().readTree(exchange.getRequestBody()));
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        byte[] body = (status == 200 ? REPLY : "{\"type\": \"error\", \"error\": {\"type\": \"invalid_request_error\", "
                + "\"message\": \"bad\"}}").getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
    }

    private List<String> expand(QueryExpander expander, String query) {
        return expander.expand(query).block(Duration.ofSeconds(10));
    }

    @Test
    void asksHaikuWithThinkingOffForPostsMatchingASchema() {
        var posts = expand(new QueryExpander(anthropic, timeout), "washington state hikes");
        // Blank posts are dropped, and only the first three kept.
        assertEquals(List.of("Did Rattlesnake Ledge today", "Lake 22 was gorgeous", "Mount Si again"), posts);
        JsonNode request = requests.getFirst();
        assertEquals("claude-haiku-5-5", request.get("model").asString());
        assertEquals("disabled", request.get("thinking").get("type").asString());
        JsonNode format = request.get("output_config").get("format");
        assertEquals("json_schema", format.get("type").asString());
        assertEquals("array", format.get("schema").get("properties").get("posts").get("type").asString());
        assertTrue(request.get("messages").get(0).get("content").asString().contains("\"washington state hikes\""));
    }

    @Test
    void reusesTheExpansionForTheSameNormalizedQuery() {
        var expander = new QueryExpander(anthropic, timeout);
        var first = expand(expander, "Washington  State hikes ");
        assertEquals(first, expand(expander, "washington state hikes"));
        assertEquals(1, requests.size());
    }

    @Test
    void fallsBackToNoPostsOnAnErrorAndAsksAgainNextTime() {
        status = 400;
        var expander = new QueryExpander(anthropic, timeout);
        assertEquals(List.of(), expand(expander, "hikes"));
        status = 200;
        assertEquals(3, expand(expander, "hikes").size());
        assertEquals(2, requests.size());
    }

    @Test
    void fallsBackToNoPostsAfterTheTimeoutAndCachesTheLateReply() {
        delay = Duration.ofMillis(800);
        var expander = new QueryExpander(anthropic, Duration.ofMillis(200));
        long start = System.nanoTime();
        assertEquals(List.of(), expand(expander, "hikes"));
        assertTrue(Duration.ofNanos(System.nanoTime() - start).compareTo(Duration.ofMillis(700)) < 0);
        // The call kept running after the timeout; once it's answered, the next search uses its posts.
        Mono.delay(Duration.ofSeconds(1)).block();
        delay = Duration.ZERO;
        assertEquals(3, expand(expander, "hikes").size());
        assertEquals(1, requests.size());
    }
}
