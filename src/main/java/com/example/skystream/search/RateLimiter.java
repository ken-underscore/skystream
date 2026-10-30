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

    /** A limit, and each client's current window for it. Caffeine drops a window once it goes that long untouched. */
    private record Counter(Limit limit, Cache<String, Window> windows) {}

    private final Clock clock;
    private final List<Counter> counters;

    RateLimiter(Clock clock, List<Limit> limits) {
        this.clock = clock;
        this.counters = limits.stream().map(limit -> new Counter(limit, Caffeine.newBuilder()
                .maximumSize(MAX_CLIENTS).expireAfterWrite(limit.window()).<String, Window>build())).toList();
    }

    /** Counts a search by {@code client} against every limit, or, if one is reached, returns how long until it resets. */
    synchronized Optional<Duration> tryAcquire(String client) {
        Instant now = clock.instant();
        var current = new ArrayList<Window>(counters.size());
        Duration wait = null;
        for (var counter : counters) {
            Window window = counter.windows().asMap().compute(client, (key, existing) -> existing == null
                    || !now.isBefore(existing.end) ? new Window(now.plus(counter.limit().window())) : existing);
            if (window.used >= counter.limit().searches()) {
                Duration untilReset = Duration.between(now, window.end);
                if (wait == null || untilReset.compareTo(wait) > 0) wait = untilReset;
            }
            current.add(window);
        }
        if (wait != null) return Optional.of(wait);
        current.forEach(window -> window.used++);
        return Optional.empty();
    }
}
