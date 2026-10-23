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
        try (var expiry = new ExpiryService(words, posts)) {
            time.advanceTime();
            assertEquals(1, wordRuns.get());
            assertEquals(1, postRuns.get());
            time.advanceTimeBy(Duration.ofMinutes(10));
            assertEquals(2, wordRuns.get());
            assertEquals(2, postRuns.get());
        } finally {
            VirtualTimeScheduler.reset();
        }
        verify(posts, times(2)).expire(argThat(cutoff -> !cutoff.isBefore(before.minus(Duration.ofHours(24)))
                && !cutoff.isAfter(Instant.now().minus(Duration.ofHours(24)))));
    }
}
