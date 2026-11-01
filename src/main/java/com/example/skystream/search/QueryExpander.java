package com.example.skystream.search;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.anthropic.client.AnthropicClientAsync;
import com.anthropic.models.messages.MessageCreateParams;
import com.anthropic.models.messages.ThinkingConfigDisabled;
import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import reactor.core.publisher.Mono;

/**
 * Asks Haiku for hypothetical posts that would answer a query, so the search looks for posts like them as well as like
 * the query (HyDE). Expansions are cached by normalized query. A failure, a timeout or a spent daily budget gives no
 * posts, and the search falls back to the plain query.
 */
class QueryExpander {
    private static final int POSTS = 3;
    private static final String PROMPT = """
            You are the query-expansion step of a semantic search engine over recent Bluesky posts. Write %d short, \
            realistic Bluesky posts (each under 300 characters) that a real person might have posted and that would be \
            ideal search results for the query: "%s". Write the way people actually post: casual, first person, specific \
            details, no hashtags required. Vary the posts so they cover different ways the query could be answered.""";
    private static final Logger log = LoggerFactory.getLogger(QueryExpander.class);

    /** The reply's JSON, which structured output constrains to this record's schema. */
    record Expansion(List<String> posts) {}

    private final AnthropicClientAsync anthropic;
    private final Duration timeout;
    private final DailyBudget budget;
    private final MeterRegistry registry;
    private final AsyncCache<String, List<String>> cache = Caffeine.newBuilder()
            .maximumSize(10_000).expireAfterWrite(Duration.ofDays(1)).buildAsync();

    QueryExpander(AnthropicClientAsync anthropic, Duration timeout, DailyBudget budget, MeterRegistry registry) {
        this.anthropic = anthropic;
        this.timeout = timeout;
        this.budget = budget;
        this.registry = registry;
    }

    /**
     * Up to three hypothetical posts for the query, or none if Haiku fails, takes longer than the timeout, or today's
     * budget is spent.
     */
    Mono<List<String>> expand(String query) {
        // Concurrent searches for one query share a call. Failed calls aren't cached, and a call that outlasts the
        // timeout keeps running, so its posts are cached for the next search.
        return Mono.fromFuture(() -> cache.get(normalize(query), (key, executor) -> write(query)), true)
                .timeout(timeout)
                .onErrorResume(error -> {
                    // DailyBudget logs once when it's spent.
                    if (!(error instanceof DailyBudget.SpentException)) {
                        log.warn("Could not expand a query, so searching with it alone: {}", error.toString());
                    }
                    return Mono.just(List.of());
                });
    }

    private CompletableFuture<List<String>> write(String query) {
        // A failed load isn't cached, so the query is tried again once the budget refills.
        if (!budget.tryTake()) return CompletableFuture.failedFuture(new DailyBudget.SpentException());
        var params = MessageCreateParams.builder()
                .model(SearchConfiguration.HAIKU)
                .maxTokens(2048)
                // Thinking adds latency without improving the posts (log entry 14).
                .thinking(ThinkingConfigDisabled.builder().build())
                .outputConfig(Expansion.class)
                .addUserMessage(PROMPT.formatted(POSTS, query))
                .build();
        // Times Haiku's call to its end, even past the search's timeout. Cached expansions aren't timed.
        var sample = Timer.start(registry);
        return anthropic.messages().create(params).thenApply(message -> {
            Expansion expansion = message.content().stream().flatMap(block -> block.text().stream()).findFirst()
                    .orElseThrow(() -> new IllegalStateException("No text in a " + message.stopReason() + " reply"))
                    .text();
            return expansion.posts().stream().filter(post -> !post.isBlank()).limit(POSTS).toList();
        }).whenComplete((posts, error) -> sample.stop(
                registry.timer("skystream.search.expansion", "outcome", error == null ? "success" : "error")));
    }

    /** The query as cached: trimmed, lowercase, and with each run of whitespace a single space. */
    static String normalize(String query) {
        return query.strip().toLowerCase(Locale.ROOT).replaceAll("\\s+", " ");
    }
}
