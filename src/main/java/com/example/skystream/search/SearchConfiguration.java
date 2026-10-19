package com.example.skystream.search;

import java.nio.file.Path;
import java.time.Duration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.anthropic.client.AnthropicClientAsync;
import com.anthropic.client.okhttp.AnthropicOkHttpClientAsync;

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

    @Bean
    PostExpiry postExpiry(PostRepository posts) {
        return new PostExpiry(posts);
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

    @Bean
    QueryExpander queryExpander(AnthropicClientAsync anthropic) {
        return new QueryExpander(anthropic, Duration.ofSeconds(5));
    }

    @Bean
    AnswerService answerService(AnthropicClientAsync anthropic) {
        return new AnswerService(anthropic, Duration.ofSeconds(10));
    }

    @Bean
    SearchService searchService(Embedder embedder, PostRepository posts, QueryExpander expander) {
        return new SearchService(embedder, posts, expander);
    }

    @Bean
    EmbeddingWorker embeddingWorker(PostRepository posts, Embedder embedder) {
        return new EmbeddingWorker(posts, embedder, 256, Duration.ofSeconds(1));
    }
}
