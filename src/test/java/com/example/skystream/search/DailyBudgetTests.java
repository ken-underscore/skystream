package com.example.skystream.search;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

class DailyBudgetTests {
    /** A budget no test reaches. */
    static DailyBudget unlimited() {
        return new DailyBudget(Integer.MAX_VALUE, Clock.systemUTC(), new SimpleMeterRegistry());
    }

    @Test
    void givesItsCallsEachDayAndRefillsWhenTheNextUtcDayStarts() {
        var clock = new TestClock(Instant.parse("2026-10-29T23:59:58Z"));
        var registry = new SimpleMeterRegistry();
        var budget = new DailyBudget(2, clock, registry);
        assertTrue(budget.tryTake());
        assertTrue(budget.tryTake());
        assertFalse(budget.tryTake());
        assertEquals(0, registry.get("skystream.search.haiku.budget.remaining").gauge().value());

        clock.advance(Duration.ofSeconds(1));
        assertFalse(budget.tryTake());
        clock.advance(Duration.ofSeconds(1));
        assertEquals(2, registry.get("skystream.search.haiku.budget.remaining").gauge().value());
        assertTrue(budget.tryTake());
        assertEquals(1, budget.remaining());
    }

    @Test
    void anEmptyBudgetRefusesEveryCallAndANegativeOneIsRejected() {
        assertFalse(new DailyBudget(0, Clock.systemUTC(), new SimpleMeterRegistry()).tryTake());
        assertThrows(IllegalArgumentException.class, () -> new DailyBudget(-1, Clock.systemUTC(), new SimpleMeterRegistry()));
    }
}
