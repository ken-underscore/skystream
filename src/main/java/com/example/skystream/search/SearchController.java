package com.example.skystream.search;

import java.time.Instant;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Semantic search over the captured posts. Like SearchConfiguration, present only when search is enabled. */
@RestController
@ConditionalOnProperty(name = "skystream.search.enabled", havingValue = "true")
class SearchController {
    static final int MAX_QUERY_LENGTH = 200;
    private static final Logger log = LoggerFactory.getLogger(SearchController.class);

    record Result(String text, Instant seenAt, String url, double similarity) {
        static Result of(PostRepository.Hit hit) {
            return new Result(hit.post().text(), hit.post().seenAt(), hit.post().blueskyUrl(), hit.similarity());
        }
    }

    record AnswerText(String text) {}

    private final SearchService search;
    private final AnswerService answers;

    SearchController(SearchService search, AnswerService answers) {
        this.search = search;
        this.answers = answers;
    }

    @GetMapping("/api/search")
    Flux<Result> search(@RequestParam String q) {
        return search.search(validated(q)).map(Result::of);
    }

    /**
     * The results as one {@code results} event, then the answer as {@code answer} events as it's written, ending with
     * {@code done}, or with {@code failed} if no answer could be written.
     */
    @GetMapping(value = "/api/search/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    Flux<ServerSentEvent<Object>> searchAndAnswer(@RequestParam String q) {
        String query = validated(q);
        return search.search(query).collectList().flatMapMany(hits -> Flux.concat(
                Mono.just(event("results", hits.stream().map(Result::of).toList())),
                answers.answer(query, hits.stream().map(PostRepository.Hit::post).toList())
                        .map(text -> event("answer", new AnswerText(text)))
                        .concatWith(Mono.just(event("done", Map.of())))
                        .onErrorResume(error -> {
                            log.warn("Could not answer a search: {}", error.toString());
                            return Mono.just(event("failed", Map.of()));
                        })));
    }

    private static String validated(String q) {
        String query = q.strip();
        if (query.isEmpty() || query.length() > MAX_QUERY_LENGTH) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "q must be 1 to " + MAX_QUERY_LENGTH + " characters, ignoring surrounding whitespace");
        }
        return query;
    }

    // Every event carries data: browsers don't dispatch an event without any.
    private static ServerSentEvent<Object> event(String name, Object data) {
        return ServerSentEvent.builder(data).event(name).build();
    }
}
