package com.example.skystream.service;

import java.time.Duration;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import com.example.skystream.repository.WordBucketRepository;
import com.example.skystream.search.PostRepository;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Deletes what the app no longer shows, every ten minutes while ingesting: word counts past their longest window, and
 * search posts older than {@link PostRepository#RETENTION}.
 */
@Service
@ConditionalOnProperty(name = "skystream.jetstream.enabled", havingValue = "true")
public class ExpiryService implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ExpiryService.class);
    private final Disposable schedule;

    public ExpiryService(WordBucketRepository words, PostRepository posts) {
        // Dropping ticks while an expiry runs keeps a slow one from overflowing the interval.
        schedule = Flux.interval(Duration.ZERO, Duration.ofMinutes(10)).onBackpressureDrop()
                .concatMap(tick -> Flux.concat(
                        logFailure("word counts", words.expire()),
                        logFailure("search posts", posts.expire(Instant.now().minus(PostRepository.RETENTION)))), 0)
                .subscribe();
    }

    private static Mono<Void> logFailure(String data, Mono<?> expiry) {
        return expiry.then().onErrorResume(error -> {
            log.warn("Could not expire {}", data, error);
            return Mono.empty();
        });
    }

    @Override
    public void close() {
        schedule.dispose();
    }
}
