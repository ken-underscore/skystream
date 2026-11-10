package com.example.skystream.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import com.example.skystream.model.WordSnapshot;
import com.example.skystream.repository.WordBucketRepository;
import com.example.skystream.service.WordSnapshotService;

import reactor.core.publisher.Flux;
import tools.jackson.databind.json.JsonMapper;

class TrendingWordsToolTests {
    private final WordSnapshotService snapshots = mock(WordSnapshotService.class);
    private WebTestClient client;

    @BeforeEach
    void setUp() {
        var configuration = new McpConfiguration();
        var transport = configuration.mcpTransport();
        configuration.mcpServer(transport, List.of(new TrendingWordsTool().trendingWords(snapshots)));
        client = WebTestClient.bindToRouterFunction(transport.getRouterFunction()).build();
    }

    @Test
    void listsTheToolWithEveryWindow() {
        var tool = (Map<?, ?>) ((List<?>) rpc("tools/list", Map.of()).get("tools")).getFirst();
        assertEquals("trending_words", tool.get("name"));
        var window = (Map<?, ?>) ((Map<?, ?>) ((Map<?, ?>) tool.get("inputSchema")).get("properties")).get("windowSeconds");
        assertEquals(WordBucketRepository.SNAPSHOT_WINDOW_SECONDS, window.get("enum"));
    }

    @Test
    void returnsTheWindowsWordsMostUsedFirstBreakingTiesAlphabetically() {
        when(snapshots.updates(300, Duration.ZERO))
                .thenReturn(Flux.just(new WordSnapshot(Map.of("beta", 2L, "alpha", 2L, "gamma", 5L), 240)));
        var result = call(Map.of("name", "trending_words", "arguments", Map.of("windowSeconds", 300)));
        assertEquals(false, result.get("isError"));
        assertEquals(Map.of("words", List.of(Map.of("word", "gamma", "count", 5), Map.of("word", "alpha", "count", 2),
                Map.of("word", "beta", "count", 2)), "collectedSeconds", 240), result.get("structuredContent"));
    }

    @Test
    void withoutArgumentsCountsTheLastHour() {
        when(snapshots.updates(3600, Duration.ZERO)).thenReturn(Flux.just(new WordSnapshot(Map.of("hello", 1L), 3600)));
        var result = call(Map.of("name", "trending_words"));
        assertEquals(Map.of("words", List.of(Map.of("word", "hello", "count", 1)), "collectedSeconds", 3600),
                result.get("structuredContent"));
    }

    @Test
    void refusesAWindowItDoesNotHave() {
        var result = call(Map.of("name", "trending_words", "arguments", Map.of("windowSeconds", 45)));
        assertEquals(true, result.get("isError"));
    }

    @Test
    void saysThereAreNoWordsYetBeforeTheFirstSnapshot() {
        when(snapshots.updates(3600, Duration.ZERO)).thenReturn(Flux.never());
        var result = call(Map.of("name", "trending_words"));
        assertEquals(true, result.get("isError"));
        assertEquals(List.of(Map.of("type", "text",
                "text", "Skystream has no words yet: it has just started. Try again in a few seconds.")), result.get("content"));
    }

    private Map<?, ?> call(Map<String, Object> params) {
        return rpc("tools/call", params);
    }

    /** Sends one JSON-RPC request to /mcp and returns its result, failing on a JSON-RPC error. */
    private Map<?, ?> rpc(String method, Map<String, Object> params) {
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
