package com.example.skystream.mcp;

import static java.util.Comparator.comparingLong;

import java.time.Duration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.example.skystream.model.WordSnapshot;
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
    /** How long a call waits for the first snapshot before saying there are no word counts yet. */
    static final Duration FIRST_SNAPSHOT = Duration.ofSeconds(2);
    static final String NO_WORDS_YET =
            "Skystream has no word counts yet: it has just started, or isn't receiving posts. Try again in a minute.";
    private static final Logger log = LoggerFactory.getLogger(TrendingWordsTool.class);

    static final Tool TOOL = Tool.builder()
            .name("trending_words")
            .description("""
                    The %d words used most in a recent window by Bluesky posts, replies included, that their author's \
                    app tagged as English, most used first. A word's count is how many times it appeared, so one post \
                    can add more than 1: it isn't a number of posts. Words are lowercased. Words with accented letters \
                    are left out, as are about 850 common English words, including some like "new" and "world". \
                    collectedSeconds says how much of the window Skystream has collected, less than the window after \
                    a restart or downtime; count divided by it is a rate that compares across windows. These are the \
                    most used words, not the fastest rising: compare the last hour with the last 24 hours to see what's \
                    rising.""".formatted(WordBucketRepository.TOP_WORDS))
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

    @Bean
    AsyncToolSpecification trendingWords(WordSnapshotService snapshots) {
        return new AsyncToolSpecification(TOOL, (context, request) -> trending(snapshots, windowSeconds(request)));
    }

    /** The window asked for, if any. Arguments are optional in MCP, and the input schema has checked any window given. */
    static int windowSeconds(CallToolRequest request) {
        return request.arguments() != null && request.arguments().get("windowSeconds") instanceof Number seconds
                ? seconds.intValue() : DEFAULT_WINDOW_SECONDS;
    }

    static Mono<CallToolResult> trending(WordSnapshotService snapshots, int windowSeconds) {
        // Deferred, so a failure to start reading is handled below like any other.
        return Mono.defer(() -> snapshots.updates(windowSeconds, Duration.ZERO).next())
                // The cache replays its latest snapshot at once, and has none until its first refresh.
                .timeout(FIRST_SNAPSHOT, Mono.empty())
                .map(snapshot -> ToolResults.of(trending(snapshot)))
                .defaultIfEmpty(ToolResults.error(NO_WORDS_YET))
                .onErrorResume(error -> {
                    log.warn("Could not read the word counts for an agent", error);
                    return Mono.just(ToolResults.error("Could not read the word counts. Try again shortly."));
                });
    }

    /** The snapshot's words, most used first, breaking ties alphabetically as the database does at the cutoff. */
    static Trending trending(WordSnapshot snapshot) {
        return new Trending(snapshot.words().entrySet().stream()
                .map(entry -> new WordCount(entry.getKey(), entry.getValue()))
                .sorted(comparingLong(WordCount::count).reversed().thenComparing(WordCount::word))
                .toList(), snapshot.collectedSeconds());
    }
}
