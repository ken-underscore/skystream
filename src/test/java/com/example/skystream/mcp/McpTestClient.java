package com.example.skystream.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;

import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.modelcontextprotocol.server.McpStatelessServerFeatures.AsyncToolSpecification;
import tools.jackson.databind.json.JsonMapper;

/** An MCP server with the given tools, called over its HTTP routes without a running web server. */
public final class McpTestClient {
    private final WebTestClient client;

    public McpTestClient(AsyncToolSpecification... tools) {
        this(new SimpleMeterRegistry(), tools);
    }

    /** Records the calls' metrics in {@code registry}. */
    public McpTestClient(MeterRegistry registry, AsyncToolSpecification... tools) {
        var configuration = new McpConfiguration();
        var transport = configuration.mcpTransport();
        configuration.mcpServer(transport, List.of(tools), registry);
        client = WebTestClient.bindToRouterFunction(transport.getRouterFunction()).build();
    }

    /** Calls a tool without arguments, which MCP allows, and returns the result. */
    public Map<?, ?> call(String tool) {
        return rpc("tools/call", Map.of("name", tool));
    }

    public Map<?, ?> call(String tool, Map<String, ?> arguments) {
        return rpc("tools/call", Map.of("name", tool, "arguments", arguments));
    }

    /** Sends one JSON-RPC request to /mcp and returns its result, failing on a JSON-RPC error. */
    public Map<?, ?> rpc(String method, Map<String, ?> params) {
        var body = client.post().uri("/mcp")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON, MediaType.TEXT_EVENT_STREAM)
                .bodyValue(Map.of("jsonrpc", "2.0", "id", 1, "method", method, "params", params))
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class).returnResult().getResponseBody();
        var response = JsonMapper.shared().readValue(body, Map.class);
        assertEquals(null, response.get("error"), body);
        return (Map<?, ?>) response.get("result");
    }
}
