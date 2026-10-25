package com.example.skystream.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.example.skystream.repository.WordBucketRepository;
import com.example.skystream.search.PostRepository;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import reactor.core.publisher.Mono;
import reactor.test.scheduler.VirtualTimeScheduler;

class ExpiryServiceTests {
    @Test
    void expiresSearchPostsADayOldEvenWhenWordsFailAndKeepsRunning() {
        var words = mock(WordBucketRepository.class);
        var posts = mock(PostRepository.class);
        var wordRuns = new AtomicInteger();
        var postRuns = new AtomicInteger();
        when(words.expire()).thenReturn(Mono.defer(() -> {
            wordRuns.incrementAndGet();
            return Mono.error(new IllegalStateException("database down"));
        }));
        when(posts.expire(any())).thenReturn(Mono.fromCallable(() -> (long) postRuns.incrementAndGet()));
        var time = VirtualTimeScheduler.getOrSet();
        Instant before = Instant.now();
        var registry = new SimpleMeterRegistry();
        try (var expiry = new ExpiryService(words, posts, registry)) {
            time.advanceTime();
            assertEquals(1, wordRuns.get());
            assertEquals(1, postRuns.get());
            time.advanceTimeBy(Duration.ofMinutes(10));
            assertEquals(2, wordRuns.get());
            assertEquals(2, postRuns.get());
        } finally {
            VirtualTimeScheduler.reset();
        }
        // The fake post expiries delete 1 row, then 2.
        assertEquals(3, registry.get("skystream.expiry.deleted").tag("data", "search_posts").counter().count());
        assertEquals(2, registry.get("skystream.expiry").tags("data", "word_buckets", "outcome", "error").timer().count());
        verify(posts, times(2)).expire(argThat(cutoff -> !cutoff.isBefore(before.minus(Duration.ofHours(24)))
                && !cutoff.isAfter(Instant.now().minus(Duration.ofHours(24)))));
    }
}
