package com.example.skystream.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import tools.jackson.databind.JsonNode;

class QueryExpanderTests {
    private static final String REPLY = """
            {"id": "msg_test", "type": "message", "role": "assistant", "model": "claude-haiku-5-5",
             "content": [{"type": "text", "text": "{\\"posts\\": [\\"Did Rattlesnake Ledge today\\", \\"Lake 22 was \
            gorgeous\\", \\" \\", \\"Mount Si again\\", \\"one too many\\"]}"}],
             "stop_reason": "end_turn", "stop_sequence": null, "usage": {"input_tokens": 90, "output_tokens": 60}}""";

    private final Duration timeout = Duration.ofSeconds(5);
    private FakeMessagesApi api;

    @BeforeEach
    void start() throws IOException {
        api = new FakeMessagesApi();
        api.body = REPLY;
    }

    @AfterEach
    void stop() {
        api.close();
    }

    private List<String> expand(QueryExpander expander, String query) {
        return expander.expand(query).block(Duration.ofSeconds(10));
    }

    @Test
    void asksHaikuWithThinkingOffForPostsMatchingASchema() {
        var posts = expand(new QueryExpander(api.client, timeout, DailyBudgetTests.unlimited(), new SimpleMeterRegistry()),
                "washington state hikes");
        // Blank posts are dropped, and only the first three kept.
        assertEquals(List.of("Did Rattlesnake Ledge today", "Lake 22 was gorgeous", "Mount Si again"), posts);
        JsonNode request = api.requests.getFirst();
        assertEquals("claude-haiku-5-5", request.get("model").asString());
        assertEquals("disabled", request.get("thinking").get("type").asString());
        JsonNode format = request.get("output_config").get("format");
        assertEquals("json_schema", format.get("type").asString());
        assertEquals("array", format.get("schema").get("properties").get("posts").get("type").asString());
        assertTrue(request.get("messages").get(0).get("content").asString().contains("\"washington state hikes\""));
    }

    @Test
    void reusesTheExpansionForTheSameNormalizedQuery() {
        var expander = new QueryExpander(api.client, timeout, DailyBudgetTests.unlimited(), new SimpleMeterRegistry());
        var first = expand(expander, "Washington  State hikes ");
        assertEquals(first, expand(expander, "washington state hikes"));
        assertEquals(1, api.requests.size());
    }

    @Test
    void fallsBackToNoPostsWithoutCallingHaikuOnceTheBudgetIsSpentAndAsksAgainTheNextDay() {
        var clock = new TestClock(Instant.parse("2026-10-29T12:00:00Z"));
        var expander = new QueryExpander(api.client, timeout, new DailyBudget(1, clock, new SimpleMeterRegistry()),
                new SimpleMeterRegistry());
        assertEquals(3, expand(expander, "hikes").size());
        assertEquals(List.of(), expand(expander, "trails"));
        // Cached expansions don't need the budget.
        assertEquals(3, expand(expander, "hikes").size());
        assertEquals(1, api.requests.size());

        clock.advance(Duration.ofDays(1));
        assertEquals(3, expand(expander, "trails").size());
        assertEquals(2, api.requests.size());
    }

    @Test
    void fallsBackToNoPostsOnAnErrorAndAsksAgainNextTime() {
        api.status = 400;
        var registry = new SimpleMeterRegistry();
        var expander = new QueryExpander(api.client, timeout, DailyBudgetTests.unlimited(), registry);
        assertEquals(List.of(), expand(expander, "hikes"));
        api.status = 200;
        assertEquals(3, expand(expander, "hikes").size());
        assertEquals(2, api.requests.size());
        // A fallback is timed as an error.
        assertEquals(1, registry.get("skystream.search.expansion").tag("outcome", "error").timer().count());
        assertEquals(1, registry.get("skystream.search.expansion").tag("outcome", "success").timer().count());
    }

    @Test
    void fallsBackToNoPostsAfterTheTimeoutAndCachesTheLateReply() {
        api.delay = Duration.ofMillis(800);
        var expander = new QueryExpander(api.client, Duration.ofMillis(200), DailyBudgetTests.unlimited(), new SimpleMeterRegistry());
        long start = System.nanoTime();
        assertEquals(List.of(), expand(expander, "hikes"));
        assertTrue(Duration.ofNanos(System.nanoTime() - start).compareTo(Duration.ofMillis(700)) < 0);
        // The call kept running after the timeout. Searches wait on it until it's answered, then use its posts.
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        var posts = expand(expander, "hikes");
        while (posts.isEmpty() && System.nanoTime() < deadline) posts = expand(expander, "hikes");
        assertEquals(3, posts.size());
        assertEquals(1, api.requests.size());
    }
}
