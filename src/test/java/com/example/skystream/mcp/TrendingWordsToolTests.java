package com.example.skystream.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.example.skystream.mcp.TrendingWordsTool.Trending;
import com.example.skystream.mcp.TrendingWordsTool.WordCount;
import com.example.skystream.model.WordSnapshot;
import com.example.skystream.repository.WordBucketRepository;
import com.example.skystream.service.WordSnapshotService;

import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.TextContent;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class TrendingWordsToolTests {
    private final WordSnapshotService snapshots = mock(WordSnapshotService.class);
    private final McpTestClient client = new McpTestClient(new TrendingWordsTool().trendingWords(snapshots));

    @Test
    void listsTheToolWithEveryWindow() {
        var tool = (Map<?, ?>) ((List<?>) client.rpc("tools/list", Map.of()).get("tools")).getFirst();
        assertEquals("trending_words", tool.get("name"));
        assertEquals(true, ((Map<?, ?>) tool.get("annotations")).get("readOnlyHint"));
        var window = (Map<?, ?>) ((Map<?, ?>) ((Map<?, ?>) tool.get("inputSchema")).get("properties")).get("windowSeconds");
        assertEquals(WordBucketRepository.SNAPSHOT_WINDOW_SECONDS, window.get("enum"));
    }

    @Test
    void returnsTheWindowsWordsAndHowMuchOfItWasCollected() {
        when(snapshots.updates(300, Duration.ZERO))
                .thenReturn(Flux.just(new WordSnapshot(Map.of("alpha", 2L, "gamma", 5L), 240)));
        var result = client.call("trending_words", Map.of("windowSeconds", 300));
        assertEquals(false, result.get("isError"));
        assertEquals(Map.of("words", List.of(Map.of("word", "gamma", "count", 5), Map.of("word", "alpha", "count", 2)),
                "collectedSeconds", 240), result.get("structuredContent"));
    }

    @Test
    void withoutArgumentsCountsTheLastHour() {
        when(snapshots.updates(3600, Duration.ZERO)).thenReturn(Flux.just(new WordSnapshot(Map.of("hello", 1L), 3600)));
        assertEquals(Map.of("words", List.of(Map.of("word", "hello", "count", 1)), "collectedSeconds", 3600),
                client.call("trending_words").get("structuredContent"));
    }

    @Test
    void refusesAWindowItDoesNotHave() {
        assertEquals(true, client.call("trending_words", Map.of("windowSeconds", 45)).get("isError"));
    }

    @Test
    void aFailureIsAToolErrorThatKeepsItsCauseToItself() {
        when(snapshots.updates(anyInt(), any())).thenThrow(new IllegalStateException("internal detail"));
        var result = client.call("trending_words");
        assertEquals(true, result.get("isError"));
        assertEquals(List.of(Map.of("type", "text", "text", "Could not read the word counts. Try again shortly.")),
                result.get("content"));
    }

    @Test
    void sortsMostUsedFirstThenAlphabetically() {
        // Six tied words, so map order would rarely put them in alphabetical order by chance.
        var snapshot = new WordSnapshot(Map.of("fig", 2L, "cherry", 2L, "apple", 2L, "elder", 2L, "banana", 2L,
                "date", 2L, "kiwi", 7L), 60);
        assertEquals(new Trending(List.of(new WordCount("kiwi", 7), new WordCount("apple", 2),
                new WordCount("banana", 2), new WordCount("cherry", 2), new WordCount("date", 2), new WordCount("elder", 2),
                new WordCount("fig", 2)), 60), TrendingWordsTool.trending(snapshot));
    }

    @Test
    void waitsForAFirstSnapshotThatArrivesSoonAfterStartup() {
        StepVerifier.withVirtualTime(() -> {
                    when(snapshots.updates(3600, Duration.ZERO)).thenReturn(Flux.just(new WordSnapshot(Map.of("hi", 1L), 5))
                            .delayElements(TrendingWordsTool.FIRST_SNAPSHOT.minusSeconds(1)));
                    return TrendingWordsTool.trending(snapshots, 3600);
                })
                .thenAwait(TrendingWordsTool.FIRST_SNAPSHOT)
                .assertNext(result -> assertEquals(false, result.isError()))
                .verifyComplete();
    }

    @Test
    void saysThereAreNoWordCountsYetIfNoneArrive() {
        when(snapshots.updates(3600, Duration.ZERO)).thenReturn(Flux.never());
        StepVerifier.withVirtualTime(() -> TrendingWordsTool.trending(snapshots, 3600))
                .thenAwait(TrendingWordsTool.FIRST_SNAPSHOT)
                .assertNext(result -> assertEquals(TrendingWordsTool.NO_WORDS_YET, text(result)))
                .verifyComplete();
    }

    private static String text(CallToolResult result) {
        return ((TextContent) result.content().getFirst()).text();
    }
}
