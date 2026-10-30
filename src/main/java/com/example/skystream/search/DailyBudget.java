package com.example.skystream.search;

import java.time.Clock;
import java.time.LocalDate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * The Haiku calls allowed each UTC day, so however many people search, the bill stays bounded. Calls are counted rather
 * than dollars: each is bounded by its {@code max_tokens}. The count is kept in memory, so a restart refills the day's
 * budget, and the Anthropic console's spending limit is the backstop.
 */
class DailyBudget {
    /** Today's budget is spent. */
    static final class SpentException extends RuntimeException {
        SpentException() {
            super("Today's Haiku budget is spent", null, false, false);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(DailyBudget.class);

    private final int calls;
    private final Clock clock;
    private LocalDate day;
    private int used;

    /** {@code clock}'s zone decides when a day starts. */
    DailyBudget(int calls, Clock clock, MeterRegistry registry) {
        this.calls = calls;
        this.clock = clock;
        Gauge.builder("skystream.search.haiku.budget.remaining", this, DailyBudget::remaining).register(registry);
    }

    /** Takes a call from today's budget, or returns false when it's spent. */
    synchronized boolean tryTake() {
        startDay();
        if (used == calls) return false;
        if (++used == calls) {
            log.warn("Today's budget of {} Haiku calls is spent: searches go without expansion or answers until the day ends",
                    calls);
        }
        return true;
    }

    synchronized int remaining() {
        startDay();
        return calls - used;
    }

    private void startDay() {
        var today = LocalDate.now(clock);
        if (!today.equals(day)) {
            day = today;
            used = 0;
        }
    }
}
