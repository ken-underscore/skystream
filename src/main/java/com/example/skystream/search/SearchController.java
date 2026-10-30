package com.example.skystream.search;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.http.server.reactive.ServerHttpRequest;
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

    record Limited(long retryAfterSeconds) {}

    private final SearchService search;
    private final AnswerService answers;
    private final RateLimiter limiter;

    SearchController(SearchService search, AnswerService answers, RateLimiter limiter) {
        this.search = search;
        this.answers = answers;
        this.limiter = limiter;
    }

    /** The results, or HTTP 429 with {@code Retry-After} when the client has searched too often. */
    @GetMapping("/api/search")
    ResponseEntity<Flux<Result>> search(@RequestParam String q, ServerHttpRequest request) {
        String query = validated(q);
        return limited(request)
                .<ResponseEntity<Flux<Result>>>map(seconds -> ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                        .header(HttpHeaders.RETRY_AFTER, String.valueOf(seconds)).build())
                .orElseGet(() -> ResponseEntity.ok(search.search(query).map(Result::of)));
    }

    /**
     * The results as one {@code results} event, then the answer as {@code answer} events as it's written, ending with
     * {@code done}. It ends with {@code paused} instead when today's budget is spent, or {@code failed} if no answer
     * could be written. A client that has searched too often gets only a {@code limited} event, since a browser's
     * {@code EventSource} can't read an HTTP status.
     */
    @GetMapping(value = "/api/search/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    Flux<ServerSentEvent<Object>> searchAndAnswer(@RequestParam String q, ServerHttpRequest request) {
        String query = validated(q);
        var wait = limited(request);
        if (wait.isPresent()) return Flux.just(event("limited", new Limited(wait.get())));
        return search.search(query).collectList().flatMapMany(hits -> Flux.concat(
                Mono.just(event("results", hits.stream().map(Result::of).toList())),
                answers.answer(query, hits.stream().map(PostRepository.Hit::post).toList())
                        .map(text -> event("answer", new AnswerText(text)))
                        .concatWith(Mono.just(event("done", Map.of())))
                        .onErrorResume(error -> {
                            if (error instanceof DailyBudget.SpentException) return Mono.just(event("paused", Map.of()));
                            log.warn("Could not answer a search: {}", error.toString());
                            return Mono.just(event("failed", Map.of()));
                        })));
    }

    /**
     * Counts a search by the request's client, or returns the seconds, rounded up, until it may search again. The
     * client is the address the request came from: behind a proxy, the one in its {@code X-Forwarded-For}.
     */
    private Optional<Long> limited(ServerHttpRequest request) {
        String client = Optional.ofNullable(request.getRemoteAddress()).map(InetSocketAddress::getHostString).orElse("unknown");
        return limiter.tryAcquire(client).map(wait -> Math.ceilDiv(wait.toMillis(), 1000));
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
