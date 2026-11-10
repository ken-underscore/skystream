package com.example.skystream.search;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.example.skystream.mcp.ToolResults;

import io.modelcontextprotocol.server.McpStatelessServerFeatures.AsyncToolSpecification;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.Tool;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

/**
 * The search_posts tool, for agents over MCP. It searches as the search page does, but the agent writes the example posts
 * that sharpen the search, and reads the results itself, so it calls Haiku for neither. Present only when search and MCP
 * are both enabled.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "skystream.search.enabled", havingValue = "true")
@ConditionalOnBooleanProperty(name = "skystream.mcp.enabled", matchIfMissing = true)
class SearchTool {
    static final int MAX_EXAMPLES = 3;
    static final int MAX_EXAMPLE_LENGTH = 300;
    private static final Logger log = LoggerFactory.getLogger(SearchTool.class);

    record Results(List<SearchController.Result> posts) {}

    static final Tool TOOL = Tool.builder()
            .name("search_posts")
            .description("""
                    Searches the last 24 hours of English Bluesky posts by meaning rather than keywords, and returns the \
                    10 closest, best match first: each post's text, its bsky.app link, when Skystream saw it (UTC), and \
                    its similarity to the search, from 0 to 1. Similarity is most useful for comparing results: if even \
                    the top ones are low, nothing matched closely. Only top-level posts of at least five words are \
                    searchable, not replies. Results improve a lot with examples.""")
            .inputSchema(JsonMapper.shared().readValue("""
                    {
                      "type": "object",
                      "properties": {
                        "query": {
                          "type": "string", "minLength": 1, "maxLength": %d,
                          "description": "What to look for, in plain words, such as \\"reactions to the eclipse\\"."
                        },
                        "examples": {
                          "type": "array", "maxItems": %d,
                          "items": {"type": "string", "minLength": 1, "maxLength": %d},
                          "description": "Up to %2$d short made-up posts that would match, written the way people post on Bluesky: casual, first person and specific. They sharpen the search."
                        }
                      },
                      "required": ["query"]
                    }""".formatted(SearchController.MAX_QUERY_LENGTH, MAX_EXAMPLES, MAX_EXAMPLE_LENGTH), Map.class))
            .build();

    @Bean
    AsyncToolSpecification searchPosts(SearchService search) {
        // Deferred, so anything thrown becomes a failed call rather than a malformed response.
        return new AsyncToolSpecification(TOOL, (context, request) -> Mono.defer(() ->
                searchPosts(search, Objects.requireNonNullElse(request.arguments(), Map.of()))));
    }

    /** The input schema has checked the arguments' types and lengths. */
    static Mono<CallToolResult> searchPosts(SearchService search, Map<String, Object> arguments) {
        String query = arguments.get("query") instanceof String text ? text.strip() : "";
        if (query.isEmpty()) return Mono.just(ToolResults.error("Give a query with some words in it."));
        List<String> examples = arguments.get("examples") instanceof List<?> list
                ? list.stream().map(example -> ((String) example).strip()).filter(example -> !example.isEmpty()).toList()
                : List.of();
        return search.search(query, examples).map(SearchController.Result::of).collectList()
                .map(posts -> ToolResults.of(new Results(posts)))
                .onErrorResume(error -> {
                    log.warn("Could not search for an agent: {}", error.toString());
                    return Mono.just(ToolResults.error("The search failed. Try again shortly."));
                });
    }
}
