package com.example.skystream.search;

import java.time.Instant;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import reactor.core.publisher.Flux;

/** Semantic search over the captured posts. Like SearchConfiguration, present only when search is enabled. */
@RestController
@ConditionalOnProperty(name = "skystream.search.enabled", havingValue = "true")
class SearchController {
    static final int MAX_QUERY_LENGTH = 200;

    record Result(String text, Instant seenAt, String url, double similarity) {}

    private final SearchService search;

    SearchController(SearchService search) {
        this.search = search;
    }

    @GetMapping("/api/search")
    Flux<Result> search(@RequestParam String q) {
        String query = q.strip();
        if (query.isEmpty() || query.length() > MAX_QUERY_LENGTH) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "q must be 1 to " + MAX_QUERY_LENGTH + " characters, ignoring surrounding whitespace");
        }
        return search.search(query).map(hit -> new Result(hit.post().text(), hit.post().seenAt(),
                hit.post().blueskyUrl(), hit.similarity()));
    }
}
