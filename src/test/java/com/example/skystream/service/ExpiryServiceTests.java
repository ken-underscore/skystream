package com.example.skystream.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import com.example.skystream.repository.WordBucketRepository;
import com.example.skystream.search.PostRepository;

import reactor.core.publisher.Mono;

class ExpiryServiceTests {
    @Test
    void expiresSearchPostsOlderThanADayEvenWhenWordsFail() {
        var words = mock(WordBucketRepository.class);
        var posts = mock(PostRepository.class);
        when(words.expire()).thenReturn(Mono.error(new IllegalStateException("database down")));
        when(posts.expire(any())).thenReturn(Mono.just(0L));
        Instant before = Instant.now();
        try (var expiry = new ExpiryService(words, posts)) {
            verify(posts, timeout(5000)).expire(argThat(cutoff -> !cutoff.isBefore(before.minus(Duration.ofHours(24)))
                    && !cutoff.isAfter(Instant.now().minus(Duration.ofHours(24)))));
        }
        verify(words).expire();
    }
}
