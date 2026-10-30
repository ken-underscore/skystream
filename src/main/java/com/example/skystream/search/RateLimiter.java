package com.example.skystream.search;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Limits how often each client searches, so one script can't spend the day's budget. Each limit counts in a fixed window
 * that starts at the client's first search in it. Windows are kept in memory, so a restart resets them.
 */
class RateLimiter {
    /** At most {@code searches} in each {@code window}. */
    record Limit(int searches, Duration window) {}

    private static final class Window {
        final Instant end;
        int used;

        Window(Instant end) {
            this.end = end;
        }
    }

    // Bounds the memory clients spread over many addresses can take; past it, the daily budget still bounds the spend.
    private static final int MAX_CLIENTS = 100_000;

    private final Clock clock;
    private final List<Limit> limits;
    // For each limit, each client's current window. Caffeine drops a window once it has gone that long unchanged.
    private final List<Cache<String, Window>> windows;

    RateLimiter(Clock clock, List<Limit> limits) {
        this.clock = clock;
        this.limits = List.copyOf(limits);
        this.windows = limits.stream().map(limit -> Caffeine.newBuilder()
                .maximumSize(MAX_CLIENTS).expireAfterWrite(limit.window()).<String, Window>build()).toList();
    }

    /** Counts a search by {@code client} against every limit, or, if one is reached, returns how long until it resets. */
    synchronized Optional<Duration> tryAcquire(String client) {
        Instant now = clock.instant();
        var current = new ArrayList<Window>(limits.size());
        Duration wait = Duration.ZERO;
        for (int i = 0; i < limits.size(); i++) {
            var limit = limits.get(i);
            Window window = windows.get(i).asMap().compute(client,
                    (key, existing) -> existing == null || !now.isBefore(existing.end) ? new Window(now.plus(limit.window())) : existing);
            if (window.used >= limit.searches()) {
                Duration untilReset = Duration.between(now, window.end);
                if (untilReset.compareTo(wait) > 0) wait = untilReset;
            }
            current.add(window);
        }
        if (!wait.isZero()) return Optional.of(wait);
        current.forEach(window -> window.used++);
        return Optional.empty();
    }
}
