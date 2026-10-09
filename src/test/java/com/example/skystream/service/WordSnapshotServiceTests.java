package com.example.skystream.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.TransientDataAccessResourceException;

import com.example.skystream.model.BucketBatch;
import com.example.skystream.model.ConsumerCheckpoint;
import com.example.skystream.model.WordBucket;
import com.example.skystream.repository.WordBucketRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

class WordSnapshotServiceTests {
    private final WordBucketRepository repository = mock(WordBucketRepository.class);
    private final WordSnapshotService service = new WordSnapshotService(repository);
    private final Instant start = Instant.parse("2026-10-04T12:00:00Z");
    private final Duration timeout = Duration.ofSeconds(5);

    @AfterEach
    void close() { service.close(); }

    private BucketBatch batch(int i) {
        return new BucketBatch(i, i + 1, start.plusSeconds((i + 1) * 5), List.of(new WordBucket(start, Map.of())));
    }

    private Map<Integer, Map<String, Long>> windows(long value) {
        return IntStream.of(30, 60, 300, 600, 1800, 3600).boxed()
                .collect(java.util.stream.Collectors.toUnmodifiableMap(seconds -> seconds, seconds -> Map.of("hello", value)));
    }

    private void successfulSaves() {
        when(repository.saveBatch(any())).thenAnswer(invocation -> {
            BucketBatch batch = invocation.getArgument(0);
            return Mono.just(new ConsumerCheckpoint(batch.latestSeq(), batch.maxBucketStart(), batch.latestSeq()));
        });
    }

    private void awaitSnapshot(long value) {
        assertEquals(Map.of("hello", value), service.wordBuckets(30, 0)
                .filter(Map.of("hello", value)::equals).next().block(timeout));
    }

    @Test
    void startsEmptyAndQueriesOnlyAfterCommit() {
        successfulSaves();
        when(repository.snapshotCounts(any())).thenReturn(Mono.just(windows(12)));
        Sinks.One<ConsumerCheckpoint> commit = Sinks.one();
        when(repository.saveBatch(batch(0))).thenReturn(commit.asMono());
        StepVerifier.create(service.wordBuckets(30, 0).take(1))
                .then(() -> {
                    service.persistBatches(Flux.just(batch(0))).subscribe();
                    verify(repository, never()).snapshotCounts(any());
                    commit.tryEmitValue(new ConsumerCheckpoint(1, start.plusSeconds(5), 1));
                })
                .expectNext(Map.of("hello", 12L)).verifyComplete();
        verify(repository).snapshotCounts(start.plusSeconds(5));
    }

    @Test
    void allVisitorsAndDurationsShareOneQueryPerRefresh() {
        successfulSaves();
        when(repository.snapshotCounts(any())).thenReturn(Mono.just(windows(12)));
        service.persistBatches(Flux.just(batch(0))).blockLast(timeout);
        awaitSnapshot(12);
        var visitors = IntStream.range(0, 100)
                .mapToObj(i -> service.wordBuckets(new int[]{30, 60, 300, 600, 1800, 3600}[i % 6], 60000).next()).toList();
        StepVerifier.create(Flux.merge(visitors)).expectNextCount(100).verifyComplete();
        verify(repository, times(1)).snapshotCounts(any());
    }

    @Test
    void replacesAllWindowsOnlyAfterTheWholeQueryFinishes() {
        successfulSaves();
        var pending = Sinks.<Map<Integer, Map<String, Long>>>one();
        when(repository.snapshotCounts(start.plusSeconds(5))).thenReturn(Mono.just(windows(1)));
        when(repository.snapshotCounts(start.plusSeconds(10))).thenReturn(pending.asMono());
        service.persistBatches(Flux.just(batch(0))).blockLast(timeout);
        awaitSnapshot(1);
        service.persistBatches(Flux.just(batch(1))).blockLast(timeout);
        assertEquals(Map.of("hello", 1L), service.wordBuckets(3600, 0).next().block(timeout));
        pending.tryEmitValue(windows(2));
        awaitSnapshot(2);
        assertEquals(Map.of("hello", 2L), service.wordBuckets(3600, 0).next().block(timeout));
    }

    @Test
    void slowQueriesDoNotDelayPersistenceAndEventuallyCacheLatestProgress() {
        successfulSaves();
        var pending = Sinks.<Map<Integer, Map<String, Long>>>one();
        when(repository.snapshotCounts(any())).thenAnswer(invocation -> {
            Instant end = invocation.getArgument(0);
            return end.equals(start.plusSeconds(5)) ? pending.asMono()
                    : Mono.just(windows(Duration.between(start, end).getSeconds()));
        });
        for (int i = 0; i < 3; i++) {
            assertEquals(i + 1, service.persistBatches(Flux.just(batch(i))).blockLast(timeout).latestSeq());
        }
        pending.tryEmitValue(windows(5));
        awaitSnapshot(15);
        verify(repository, times(3)).saveBatch(any());
    }

    @Test
    void failedCacheQueryPreservesPreviousResultsAndDoesNotStopIngestion() {
        successfulSaves();
        var failed = Sinks.<Boolean>one();
        when(repository.snapshotCounts(start.plusSeconds(5))).thenReturn(Mono.just(windows(1)));
        when(repository.snapshotCounts(start.plusSeconds(10))).thenReturn(
                Mono.<Map<Integer, Map<String, Long>>>error(new IllegalStateException("query failed"))
                        .doOnError(error -> failed.tryEmitValue(true)));
        when(repository.snapshotCounts(start.plusSeconds(15))).thenReturn(Mono.just(windows(3)));
        service.persistBatches(Flux.just(batch(0))).blockLast(timeout);
        awaitSnapshot(1);
        assertEquals(2, service.persistBatches(Flux.just(batch(1))).blockLast(timeout).latestSeq());
        assertTrue(failed.asMono().block(timeout));
        awaitSnapshot(1);
        assertEquals(3, service.persistBatches(Flux.just(batch(2))).blockLast(timeout).latestSeq());
        awaitSnapshot(3);
    }

    @Test
    void failedWriteDoesNotRefreshCacheOrStartAnotherSave() {
        successfulSaves();
        when(repository.saveBatch(batch(0))).thenReturn(Mono.error(new IllegalStateException("write failed")));
        StepVerifier.create(service.persistBatches(Flux.just(batch(0), batch(1))))
                .expectErrorMessage("write failed").verify();
        verify(repository, never()).snapshotCounts(any());
        verify(repository, never()).saveBatch(batch(1));
    }

    @Test
    void retriesTransientWriteFailureAndRefreshesAfterCommit() {
        var attempts = new java.util.concurrent.atomic.AtomicInteger();
        when(repository.saveBatch(any())).thenReturn(Mono.defer(() -> attempts.getAndIncrement() == 0
                ? Mono.error(new TransientDataAccessResourceException("temporary"))
                : Mono.just(new ConsumerCheckpoint(1, start.plusSeconds(5), 1))));
        when(repository.snapshotCounts(any())).thenReturn(Mono.just(windows(1)));
        StepVerifier.withVirtualTime(() -> service.persistBatches(Flux.just(batch(0))))
                .thenAwait(Duration.ofSeconds(2))
                .expectNext(new ConsumerCheckpoint(1, start.plusSeconds(5), 1)).verifyComplete();
        awaitSnapshot(1);
        assertEquals(2, attempts.get());
    }

    @Test
    void firstResultIsImmediateThenTimerRepeatsCacheAndPicksUpUpdates() {
        successfulSaves();
        when(repository.snapshotCounts(any())).thenAnswer(invocation -> {
            Instant end = invocation.getArgument(0);
            return Mono.just(windows(Duration.between(start, end).getSeconds()));
        });
        var timed = new java.util.concurrent.atomic.AtomicReference<WordSnapshotService>();
        StepVerifier.withVirtualTime(() -> {
            timed.set(new WordSnapshotService(repository));
            return timed.get().wordBuckets(30, 30000);
        })
                .then(() -> timed.get().persistBatches(Flux.just(batch(0))).subscribe())
                .thenAwait(Duration.ZERO)
                .expectNext(Map.of("hello", 5L))
                .expectNoEvent(Duration.ofMillis(29999))
                .thenAwait(Duration.ofMillis(1)).expectNext(Map.of("hello", 5L))
                .then(() -> timed.get().persistBatches(Flux.just(batch(1))).subscribe())
                .expectNoEvent(Duration.ofMillis(29999))
                .thenAwait(Duration.ofMillis(1)).expectNext(Map.of("hello", 10L))
                .then(() -> timed.get().close())
                .verifyComplete();
    }
}
