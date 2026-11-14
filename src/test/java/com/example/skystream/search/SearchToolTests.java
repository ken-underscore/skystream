package com.example.skystream.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.junit.jupiter.api.Test;

import com.example.skystream.mcp.McpTestClient;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import reactor.core.publisher.Flux;

class SearchToolTests {
    private final SearchService search = mock(SearchService.class);
    private final TestClock clock = new TestClock(Instant.parse("2026-11-14T12:00:00Z"));
    private final RateLimiter limiter = new RateLimiter(clock, List.of(new RateLimiter.Limit(2, Duration.ofMinutes(1))));
    private final Semaphore embeddings = new Semaphore(4);
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final McpTestClient client = new McpTestClient(registry, SearchTool.tool(search, limiter, embeddings, registry));

    @Test
    void listsTheToolWithItsLimits() {
        var tool = (Map<?, ?>) ((List<?>) client.rpc("tools/list", Map.of()).get("tools")).getFirst();
        assertEquals("search_posts", tool.get("name"));
        assertEquals(true, ((Map<?, ?>) tool.get("annotations")).get("readOnlyHint"));
        var properties = (Map<?, ?>) ((Map<?, ?>) tool.get("inputSchema")).get("properties");
        assertEquals(SearchController.MAX_QUERY_LENGTH, ((Map<?, ?>) properties.get("query")).get("maxLength"));
        var examples = (Map<?, ?>) properties.get("examples");
        assertEquals(SearchTool.MAX_EXAMPLES, examples.get("maxItems"));
        assertEquals(SearchTool.MAX_EXAMPLE_LENGTH, ((Map<?, ?>) examples.get("items")).get("maxLength"));
    }

    @Test
    void returnsThePostsWithTheirLinksTimesAndSimilarity() {
        var post = new SearchPost("at://did:plc:abc/app.bsky.feed.post/3m1", "Fall colors are exploding up here",
                Instant.parse("2026-11-10T19:04:11Z"));
        when(search.search(eq("autumn hikes"), eq(List.of("Hiked Sahale Arm today, the larches are gold")), eq(embeddings)))
                .thenReturn(Flux.just(new PostRepository.Hit(post, 0.75)));
        var result = client.call("search_posts", Map.of("query", " autumn hikes ",
                "examples", List.of("Hiked Sahale Arm today, the larches are gold ", "", "  ")));
        assertEquals(false, result.get("isError"));
        assertEquals(Map.of("posts", List.of(Map.of("text", "Fall colors are exploding up here",
                "seenAt", "2026-11-10T19:04:11Z", "url", "https://bsky.app/profile/did:plc:abc/post/3m1",
                "similarity", 0.75))), result.get("structuredContent"));
    }

    @Test
    void searchesWithoutExamplesWhenThereAreNone() {
        when(search.search(eq("autumn hikes"), eq(List.of()), any())).thenReturn(Flux.empty());
        assertEquals(Map.of("posts", List.of()), client.call("search_posts", Map.of("query", "autumn hikes"))
                .get("structuredContent"));
    }

    @Test
    void refusesAMissingOrBlankQuery() {
        assertEquals(true, client.call("search_posts").get("isError"));
        assertEquals(true, client.call("search_posts", Map.of("query", "   ")).get("isError"));
        verifyNoInteractions(search);
    }

    @Test
    void refusesTooManyTooLongOrNonTextExamples() {
        for (List<?> examples : List.of(List.of("one", "two", "three", "four"),
                List.of("x".repeat(SearchTool.MAX_EXAMPLE_LENGTH + 1)), List.of(5))) {
            assertEquals(true, client.call("search_posts", Map.of("query", "hikes", "examples", examples)).get("isError"));
        }
        verifyNoInteractions(search);
    }

    @Test
    void aFailedSearchIsAToolError() {
        when(search.search(any(), anyList(), any())).thenReturn(Flux.error(new IllegalStateException("embedding failed")));
        var result = client.call("search_posts", Map.of("query", "autumn hikes"));
        assertEquals(true, result.get("isError"));
        assertEquals(List.of(Map.of("type", "text", "text", "The search failed. Try again shortly.")), result.get("content"));
    }

    @Test
    void timesEachCallByOutcome() {
        when(search.search(any(), anyList(), any())).thenReturn(Flux.empty(), Flux.error(new IllegalStateException()));
        client.call("search_posts", Map.of("query", "autumn hikes"));
        client.call("search_posts", Map.of("query", "autumn hikes"));
        for (String outcome : List.of("success", "error")) {
            assertEquals(1, registry.timer("skystream.mcp.call", "tool", "search_posts", "outcome", outcome).count());
        }
    }

    @Test
    void aClientOverItsLimitIsToldWhenToRetry() {
        when(search.search(any(), anyList(), any())).thenReturn(Flux.empty());
        client.call("search_posts", Map.of("query", "autumn hikes"));
        clock.advance(Duration.ofMillis(20_500));
        client.call("search_posts", Map.of("query", "autumn hikes"));
        var result = client.call("search_posts", Map.of("query", "autumn hikes"));
        assertEquals(true, result.get("isError"));
        assertEquals(List.of(Map.of("type", "text", "text", "Skystream's search limit is reached. Try again in 40 seconds.")),
                result.get("content"));
        assertEquals(1, registry.counter("skystream.mcp.limited", "tool", "search_posts", "reason", "rate").count());
        assertEquals("Skystream's search limit is reached. Try again in 1 second.",
                SearchTool.retryIn(Duration.ofMillis(300)));
    }

    @Test
    void aSearchWithNoPermitToEmbedIsToldSkystreamIsBusy() {
        when(search.search(any(), anyList(), any())).thenReturn(Flux.error(new SearchService.BusyException()));
        var result = client.call("search_posts", Map.of("query", "autumn hikes"));
        assertEquals(true, result.get("isError"));
        assertEquals(List.of(Map.of("type", "text", "text", SearchTool.BUSY)), result.get("content"));
        assertEquals(1, registry.counter("skystream.mcp.limited", "tool", "search_posts", "reason", "busy").count());
    }
}
