package com.example.skystream.search;

import java.nio.file.Path;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Semantic search is off unless enabled, so the app runs without the embedding model. */
@Configuration
@ConditionalOnProperty(name = "skystream.search.enabled", havingValue = "true")
public class SearchConfiguration {
    @Bean
    GemmaEmbedder embedder(@Value("${skystream.search.model-dir:models/embeddinggemma-300m}") Path modelDir,
            @Value("${skystream.search.threads:2}") int threads) {
        return new GemmaEmbedder(modelDir, threads);
    }

    @Bean
    PostExpiry postExpiry(PostRepository posts) {
        return new PostExpiry(posts);
    }
}
