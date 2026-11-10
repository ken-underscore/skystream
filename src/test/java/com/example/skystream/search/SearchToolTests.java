package com.example.skystream.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.example.skystream.mcp.McpTestClient;

import reactor.core.publisher.Flux;

class SearchToolTests {
    private final SearchService search = mock(SearchService.class);
    private final McpTestClient client = new McpTestClient(new SearchTool().searchPosts(search));

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
        when(search.search("autumn hikes", List.of("Hiked Sahale Arm today, the larches are gold")))
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
        when(search.search("autumn hikes", List.of())).thenReturn(Flux.empty());
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
        when(search.search(any(), anyList())).thenReturn(Flux.error(new IllegalStateException("embedding failed")));
        var result = client.call("search_posts", Map.of("query", "autumn hikes"));
        assertEquals(true, result.get("isError"));
        assertEquals(List.of(Map.of("type", "text", "text", "The search failed. Try again shortly.")), result.get("content"));
    }
}
