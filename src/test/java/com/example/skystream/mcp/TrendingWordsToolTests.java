package com.example.skystream.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.example.skystream.model.WordSnapshot;
import com.example.skystream.repository.WordBucketRepository;
import com.example.skystream.service.WordSnapshotService;

import reactor.core.publisher.Flux;

class TrendingWordsToolTests {
    private final WordSnapshotService snapshots = mock(WordSnapshotService.class);
    private final McpTestClient client = new McpTestClient(new TrendingWordsTool().trendingWords(snapshots));

    @Test
    void listsTheToolWithEveryWindow() {
        var tool = (Map<?, ?>) ((List<?>) client.rpc("tools/list", Map.of()).get("tools")).getFirst();
        assertEquals("trending_words", tool.get("name"));
        var window = (Map<?, ?>) ((Map<?, ?>) ((Map<?, ?>) tool.get("inputSchema")).get("properties")).get("windowSeconds");
        assertEquals(WordBucketRepository.SNAPSHOT_WINDOW_SECONDS, window.get("enum"));
    }

    @Test
    void returnsTheWindowsWordsMostUsedFirstBreakingTiesAlphabetically() {
        when(snapshots.updates(300, Duration.ZERO))
                .thenReturn(Flux.just(new WordSnapshot(Map.of("beta", 2L, "alpha", 2L, "gamma", 5L), 240)));
        var result = client.call("trending_words", Map.of("windowSeconds", 300));
        assertEquals(false, result.get("isError"));
        assertEquals(Map.of("words", List.of(Map.of("word", "gamma", "count", 5), Map.of("word", "alpha", "count", 2),
                Map.of("word", "beta", "count", 2)), "collectedSeconds", 240), result.get("structuredContent"));
    }

    @Test
    void withoutArgumentsCountsTheLastHour() {
        when(snapshots.updates(3600, Duration.ZERO)).thenReturn(Flux.just(new WordSnapshot(Map.of("hello", 1L), 3600)));
        var result = client.call("trending_words");
        assertEquals(Map.of("words", List.of(Map.of("word", "hello", "count", 1)), "collectedSeconds", 3600),
                result.get("structuredContent"));
    }

    @Test
    void refusesAWindowItDoesNotHave() {
        var result = client.call("trending_words", Map.of("windowSeconds", 45));
        assertEquals(true, result.get("isError"));
    }

    @Test
    void saysThereAreNoWordsYetBeforeTheFirstSnapshot() {
        when(snapshots.updates(3600, Duration.ZERO)).thenReturn(Flux.never());
        var result = client.call("trending_words");
        assertEquals(true, result.get("isError"));
        assertEquals(List.of(Map.of("type", "text",
                "text", "Skystream has no words yet: it has just started. Try again in a few seconds.")), result.get("content"));
    }
}
