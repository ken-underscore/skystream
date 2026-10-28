package com.example.skystream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class TimingTests {
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    private long count(String outcome) {
        var timer = registry.find("work").tags("kind", "test", "outcome", outcome).timer();
        return timer == null ? 0 : timer.count();
    }

    private <T> Mono<T> timed(Mono<T> mono) {
        return Timing.timed(mono, registry, "work", "kind", "test");
    }

    @Test
    void timesAMonoWithHowItEnded() {
        timed(Mono.just(1)).block();
        timed(Mono.empty()).block();
        assertThrows(IllegalStateException.class, () -> timed(Mono.error(new IllegalStateException())).block());
        timed(Mono.never()).timeout(Duration.ofMillis(10), Mono.just(0)).block();
        assertEquals(2, count("success"));
        assertEquals(1, count("error"));
        assertEquals(1, count("cancel"));
    }

    @Test
    void recordsOnceWhenCancelledAfterSuccess() {
        timed(Mono.just(1)).flux().take(1).blockLast();
        assertEquals(1, count("success"));
        assertNull(registry.find("work").tag("outcome", "cancel").timer());
    }

    @Test
    void timesEachRetryOnItsOwn() {
        var attempts = new AtomicInteger();
        timed(Mono.fromCallable(() -> {
            if (attempts.incrementAndGet() == 1) throw new IllegalStateException("first attempt fails");
            return 1;
        })).retry(1).block();
        assertEquals(1, count("error"));
        assertEquals(1, count("success"));
    }

    @Test
    void timesAFluxUntilItEndsOrIsCancelled() {
        Timing.timed(Flux.range(1, 10), registry, "work", "kind", "test").blockLast();
        Timing.timed(Flux.range(1, 10), registry, "work", "kind", "test").take(3).blockLast();
        assertEquals(1, count("success"));
        assertEquals(1, count("cancel"));
    }
}
