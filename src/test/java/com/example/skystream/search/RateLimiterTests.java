package com.example.skystream.search;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

class RateLimiterTests {
    private final TestClock clock = new TestClock(Instant.parse("2026-10-29T12:00:00Z"));
    private final RateLimiter limiter = new RateLimiter(clock, List.of(
            new RateLimiter.Limit(2, Duration.ofMinutes(1)), new RateLimiter.Limit(3, Duration.ofDays(1))));

    @Test
    void allowsEachClientItsSearchesPerWindowThenSaysWhenItResets() {
        assertEquals(Optional.empty(), limiter.tryAcquire("a"));
        clock.advance(Duration.ofSeconds(20));
        assertEquals(Optional.empty(), limiter.tryAcquire("a"));
        assertEquals(Optional.of(Duration.ofSeconds(40)), limiter.tryAcquire("a"));
        assertEquals(Optional.empty(), limiter.tryAcquire("b"));

        clock.advance(Duration.ofSeconds(40));
        assertEquals(Optional.empty(), limiter.tryAcquire("a"));
    }

    @Test
    void refusedSearchesDontCountAndTheLongestWaitIsReported() {
        limiter.tryAcquire("a");
        limiter.tryAcquire("a");
        // Refused by the minute limit, so the day's count stays at 2.
        assertTrue(limiter.tryAcquire("a").isPresent());
        assertTrue(limiter.tryAcquire("a").isPresent());

        clock.advance(Duration.ofMinutes(1));
        assertEquals(Optional.empty(), limiter.tryAcquire("a"));
        assertEquals(Optional.of(Duration.ofDays(1).minusMinutes(1)), limiter.tryAcquire("a"));
    }
}
