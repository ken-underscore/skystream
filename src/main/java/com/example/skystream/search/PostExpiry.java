package com.example.skystream.search;

import java.time.Duration;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Deletes posts older than {@link PostRepository#RETENTION}, every ten minutes while search is enabled. */
class PostExpiry implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(PostExpiry.class);
    private final Disposable schedule;

    PostExpiry(PostRepository posts) {
        // Dropping ticks while an expiry runs keeps a slow one from overflowing the interval.
        schedule = Flux.interval(Duration.ZERO, Duration.ofMinutes(10)).onBackpressureDrop()
                .concatMap(tick -> posts.expire(Instant.now().minus(PostRepository.RETENTION))
                        .onErrorResume(error -> {
                            log.warn("Could not expire search posts", error);
                            return Mono.empty();
                        }), 0)
                .subscribe();
    }

    @Override
    public void close() {
        schedule.dispose();
    }
}
