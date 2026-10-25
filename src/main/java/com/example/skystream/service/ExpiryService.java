package com.example.skystream.service;

import java.time.Duration;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import com.example.skystream.Timing;
import com.example.skystream.repository.WordBucketRepository;
import com.example.skystream.search.PostRepository;

import io.micrometer.core.instrument.MeterRegistry;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Deletes what the app no longer shows, every ten minutes while ingesting: word counts past their longest window, and
 * search posts older than {@link PostRepository#RETENTION}.
 */
@Service
// Ingestion is on unless disabled, so expiry is too.
@ConditionalOnProperty(name = "skystream.jetstream.enabled", havingValue = "true", matchIfMissing = true)
public class ExpiryService implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ExpiryService.class);
    private final MeterRegistry registry;
    private final Disposable schedule;

    public ExpiryService(WordBucketRepository words, PostRepository posts, MeterRegistry registry) {
        this.registry = registry;
        // Dropping ticks while an expiry runs keeps a slow one from overflowing the interval.
        schedule = Flux.interval(Duration.ZERO, Duration.ofMinutes(10)).onBackpressureDrop()
                .concatMap(tick -> Flux.concat(
                        expire("word_buckets", words.expire()),
                        expire("search_posts", posts.expire(Instant.now().minus(PostRepository.RETENTION)))), 0)
                .subscribe();
    }

    /** Runs one expiry, counting the rows it deletes, and logs a failure rather than ending the schedule. */
    private Mono<Void> expire(String data, Mono<Long> expiry) {
        return Timing.timed(expiry, registry, "skystream.expiry", "data", data)
                .doOnNext(deleted -> registry.counter("skystream.expiry.deleted", "data", data).increment(deleted))
                .then()
                .onErrorResume(error -> {
                    log.warn("Could not expire {}", data, error);
                    return Mono.empty();
                });
    }

    @Override
    public void close() {
        schedule.dispose();
    }
}
