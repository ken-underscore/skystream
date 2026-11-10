package com.example.skystream.mcp;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.example.skystream.service.WordSnapshotService;

import io.modelcontextprotocol.server.McpStatelessServerFeatures.AsyncToolSpecification;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.Tool;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

/** Spike: the most used words in a window, from the snapshots already cached for the website. */
@Configuration
class TrendingWordsTool {
    record WordCount(String word, long count) {}

    record Trending(List<WordCount> words, long collectedSeconds) {}

    // Spring AI collects beans that are lists of tools, not single tools.
    @Bean
    List<AsyncToolSpecification> trendingWords(WordSnapshotService snapshots, JsonMapper json) {
        var tool = Tool.builder()
                .name("trending_words")
                .description("The words English Bluesky posts used most in a recent window, with counts.")
                .inputSchema(Map.of("type", "object", "properties", Map.of("windowSeconds", Map.of(
                        "type", "integer", "enum", List.of(30, 60, 300, 600, 1800, 3600, 10800, 28800, 86400),
                        "description", "The window in seconds. Defaults to 3600, the last hour."))))
                .build();
        return List.of(new AsyncToolSpecification(tool, (context, request) -> {
            int window = request.arguments().get("windowSeconds") instanceof Number seconds ? seconds.intValue() : 3600;
            return snapshots.updates(window, Duration.ZERO).next()
                    .map(snapshot -> new Trending(snapshot.words().entrySet().stream()
                            .sorted(Map.Entry.<String, Long>comparingByValue().reversed().thenComparing(Map.Entry.comparingByKey()))
                            .map(entry -> new WordCount(entry.getKey(), entry.getValue())).toList(), snapshot.collectedSeconds()))
                    .timeout(Duration.ofSeconds(2), Mono.just(new Trending(List.of(), 0)))
                    .map(trending -> CallToolResult.builder()
                            .structuredContent(trending)
                            .addTextContent(json.writeValueAsString(trending))
                            .build());
        }));
    }
}
