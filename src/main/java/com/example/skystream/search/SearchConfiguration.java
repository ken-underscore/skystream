package com.example.skystream.search;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.anthropic.client.AnthropicClientAsync;
import com.anthropic.client.okhttp.AnthropicOkHttpClientAsync;

import io.micrometer.core.instrument.MeterRegistry;

/** Semantic search is off unless enabled, so the app runs without the embedding model. */
@Configuration
@ConditionalOnProperty(name = "skystream.search.enabled", havingValue = "true")
public class SearchConfiguration {
    /** The model for query expansion and answers: fast and cheap, and good enough at both (log entry 14). */
    static final String HAIKU = "claude-haiku-5-5";

    @Bean
    GemmaEmbedder embedder(@Value("${skystream.search.model-dir:models/embeddinggemma-300m}") Path modelDir,
            @Value("${skystream.search.threads:2}") int threads) {
        return new GemmaEmbedder(modelDir, threads);
    }

    /**
     * Every search calls Claude, so search doesn't start without a key. Surrounding whitespace, such as the {@code \r} a
     * CRLF {@code .env} leaves, is removed, and a key with a space or control character inside is refused: the HTTP
     * client would put it in the error it logs.
     */
    @Bean
    AnthropicClientAsync anthropic(@Value("${ANTHROPIC_API_KEY:}") String apiKey) {
        String key = apiKey.strip();
        if (!key.matches("\\p{Graph}+")) {
            throw new IllegalStateException(
                    "Search needs ANTHROPIC_API_KEY set to a key, with no spaces or control characters");
        }
        // Bounds each call, retries included, well below the SDK's 10 minutes: a pending expansion holds its cache entry.
        return AnthropicOkHttpClientAsync.builder().apiKey(key).timeout(Duration.ofSeconds(30)).build();
    }

    /**
     * An uncached search makes two calls, an expansion and an answer, and costs about $0.00025 (log entry 14), so the
     * default of 8,000 calls is about 4,000 searches, or $1, a day. Days are UTC.
     */
    @Bean
    DailyBudget dailyBudget(@Value("${skystream.search.daily-haiku-calls:8000}") int calls, MeterRegistry registry) {
        return new DailyBudget(calls, Clock.systemUTC(), registry);
    }

    @Bean
    RateLimiter rateLimiter(@Value("${skystream.search.searches-per-minute:10}") int perMinute,
            @Value("${skystream.search.searches-per-day:100}") int perDay) {
        return new RateLimiter(Clock.systemUTC(), List.of(new RateLimiter.Limit(perMinute, Duration.ofMinutes(1)),
                new RateLimiter.Limit(perDay, Duration.ofDays(1))));
    }

    @Bean
    QueryExpander queryExpander(AnthropicClientAsync anthropic, DailyBudget budget, MeterRegistry registry) {
        return new QueryExpander(anthropic, Duration.ofSeconds(5), budget, registry);
    }

    @Bean
    AnswerService answerService(AnthropicClientAsync anthropic, DailyBudget budget, MeterRegistry registry) {
        return new AnswerService(anthropic, Duration.ofSeconds(10), budget, registry);
    }

    @Bean
    SearchService searchService(Embedder embedder, PostRepository posts, QueryExpander expander,
            MeterRegistry registry) {
        return new SearchService(embedder, posts, expander, registry);
    }

    @Bean
    EmbeddingWorker embeddingWorker(PostRepository posts, Embedder embedder, MeterRegistry registry) {
        return new EmbeddingWorker(posts, embedder, 256, Duration.ofSeconds(1), registry);
    }
}
