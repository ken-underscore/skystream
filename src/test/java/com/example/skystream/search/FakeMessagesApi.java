package com.example.skystream.search;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.anthropic.client.AnthropicClientAsync;
import com.anthropic.client.okhttp.AnthropicOkHttpClientAsync;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * A local stand-in for the Messages API, so tests run the real SDK without any request leaving the machine. It records
 * each request and answers with the configured status, content type and body, after the configured delay.
 */
class FakeMessagesApi implements AutoCloseable {
    static final String ERROR = """
            {"type": "error", "error": {"type": "invalid_request_error", "message": "bad request"}}""";

    final List<JsonNode> requests = new CopyOnWriteArrayList<>();
    volatile int status = 200;
    volatile String contentType = "application/json";
    volatile String body = "";
    volatile Duration delay = Duration.ZERO;
    final AnthropicClientAsync client;
    private final HttpServer server;

    FakeMessagesApi() throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/v1/messages", this::answer);
        server.start();
        client = AnthropicOkHttpClientAsync.builder().apiKey("test-key").maxRetries(0)
                .baseUrl("http://localhost:" + server.getAddress().getPort()).build();
    }

    /** A streamed reply whose text arrives in the given pieces, ending for the given stop reason. */
    static String streamed(String stopReason, String... pieces) {
        var events = new StringBuilder(event("message_start", """
                {"type": "message_start", "message": {"id": "msg_test", "type": "message", "role": "assistant", \
                "model": "claude-haiku-5-5", "content": [], "stop_reason": null, "stop_sequence": null, \
                "usage": {"input_tokens": 100, "output_tokens": 1}}}"""));
        events.append(event("content_block_start", """
                {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}"""));
        for (String piece : pieces) {
            events.append(event("content_block_delta", """
                    {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": %s}}"""
                    .formatted(new ObjectMapper().writeValueAsString(piece))));
        }
        events.append(event("content_block_stop", "{\"type\": \"content_block_stop\", \"index\": 0}"));
        events.append(event("message_delta", """
                {"type": "message_delta", "delta": {"stop_reason": "%s", "stop_sequence": null}, \
                "usage": {"output_tokens": 20}}""".formatted(stopReason)));
        return events.append(event("message_stop", "{\"type\": \"message_stop\"}")).toString();
    }

    private static String event(String name, String data) {
        return "event: " + name + "\ndata: " + data + "\n\n";
    }

    private void answer(HttpExchange exchange) throws IOException {
        requests.add(new ObjectMapper().readTree(exchange.getRequestBody()));
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        byte[] reply = (status == 200 ? body : ERROR).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", status == 200 ? contentType : "application/json");
        exchange.sendResponseHeaders(status, reply.length);
        exchange.getResponseBody().write(reply);
        exchange.close();
    }

    @Override
    public void close() {
        client.close();
        server.stop(0);
    }
}
