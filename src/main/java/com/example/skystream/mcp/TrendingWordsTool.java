package com.example.skystream.mcp;

import static java.util.Comparator.comparingLong;

import java.time.Duration;
import java.util.List;

import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.example.skystream.repository.WordBucketRepository;
import com.example.skystream.service.WordSnapshotService;

import io.modelcontextprotocol.json.McpJsonDefaults;
import io.modelcontextprotocol.server.McpStatelessServerFeatures.AsyncToolSpecification;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.Tool;
import reactor.core.publisher.Mono;

/** The trending_words tool: the words used most in a window, read from the snapshots cached for the website. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnBooleanProperty(name = "skystream.mcp.enabled", matchIfMissing = true)
class TrendingWordsTool {
    record WordCount(String word, long count) {}

    record Trending(List<WordCount> words, long collectedSeconds) {}

    static final int DEFAULT_WINDOW_SECONDS = 3600;
    /** How long a call waits for the first snapshot after startup before saying there are no words yet. */
    static final Duration FIRST_SNAPSHOT = Duration.ofSeconds(2);

    @Bean
    AsyncToolSpecification trendingWords(WordSnapshotService snapshots) {
        var tool = Tool.builder()
                .name("trending_words")
                .description("The " + WordBucketRepository.TOP_WORDS + " words English Bluesky posts used most in a "
                        + "recent window, most used first, with how often each was used. Common words such as \"the\" "
                        + "are left out. collectedSeconds says how much of the window Skystream has collected: less than "
                        + "the window after a restart or downtime.")
                .inputSchema(McpJsonDefaults.getMapper(), """
                        {
                          "type": "object",
                          "properties": {
                            "windowSeconds": {
                              "type": "integer", "enum": %s,
                              "description": "How far back to count, in seconds. Defaults to %d, the last hour."
                            }
                          }
                        }""".formatted(WordBucketRepository.SNAPSHOT_WINDOW_SECONDS, DEFAULT_WINDOW_SECONDS))
                .annotations(ToolResults.READ_ONLY)
                .build();
        // Deferred, so anything thrown becomes a failed call rather than a malformed response.
        return new AsyncToolSpecification(tool, (context, request) -> Mono.defer(() ->
                trending(snapshots, windowSeconds(request))));
    }

    /** The window asked for, if any. Arguments are optional in MCP, and the input schema has checked any window given. */
    static int windowSeconds(CallToolRequest request) {
        return request.arguments() != null && request.arguments().get("windowSeconds") instanceof Number seconds
                ? seconds.intValue() : DEFAULT_WINDOW_SECONDS;
    }

    static Mono<CallToolResult> trending(WordSnapshotService snapshots, int windowSeconds) {
        return snapshots.updates(windowSeconds, Duration.ZERO).next()
                // The cache replays its latest snapshot at once, and has none only just after startup.
                .timeout(FIRST_SNAPSHOT, Mono.empty())
                .map(snapshot -> ToolResults.of(new Trending(snapshot.words().entrySet().stream()
                        .map(entry -> new WordCount(entry.getKey(), entry.getValue()))
                        .sorted(comparingLong(WordCount::count).reversed().thenComparing(WordCount::word))
                        .toList(), snapshot.collectedSeconds())))
                .defaultIfEmpty(ToolResults.error("Skystream has no words yet: it has just started. Try again in a few seconds."));
    }
}
