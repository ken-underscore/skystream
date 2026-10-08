package com.example.skystream.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

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

    private BucketBatch batch(int i) {
        return new BucketBatch(i, i + 1, start.plusSeconds((i + 1) * 5), List.of(new WordBucket(start, Map.of())));
    }

    private void successfulSaves() {
        when(repository.saveBatch(any())).thenAnswer(invocation -> {
            BucketBatch batch = invocation.getArgument(0);
            return Mono.just(new ConsumerCheckpoint(batch.latestSeq(), batch.maxBucketStart(), batch.latestSeq()));
        });
    }

    private Flux<Map<String, Long>> snapshots(Flux<BucketBatch> batches) {
        return service.createSnapshots(service.persistBatches(batches), Duration.ofSeconds(30));
    }

    @Test
    void publishesEveryCommittedFlushUsingTheLastThirtySeconds() {
        successfulSaves();
        when(repository.combinedCounts(any(), any())).thenReturn(Mono.just(Map.of("hello", 12L)));
        StepVerifier.create(snapshots(
                Flux.fromIterable(IntStream.range(0, 13).mapToObj(this::batch).toList())))
                .expectNextCount(13).verifyComplete();
        verify(repository, times(13)).saveBatch(any());
        for (int i = 0; i < 13; i++) {
            Instant end = batch(i).maxBucketStart();
            verify(repository).combinedCounts(end.minusSeconds(30), end);
        }
        verify(repository, times(13)).combinedCounts(any(), any());
    }

    @Test
    void waitsForCommitBeforePublishingAndDoesNotPrefetchTheNextFlush() {
        successfulSaves();
        var batches = IntStream.range(0, 2).mapToObj(this::batch).toList();
        Sinks.One<ConsumerCheckpoint> commit = Sinks.one();
        when(repository.saveBatch(batches.get(0))).thenReturn(commit.asMono());
        var checkpoint = new ConsumerCheckpoint(1, start.plusSeconds(5), 1);
        StepVerifier.create(service.persistBatches(Flux.fromIterable(batches)))
                .then(() -> {
                    verify(repository, never()).saveBatch(batches.get(1));
                    commit.tryEmitValue(checkpoint);
                })
                .expectNext(checkpoint, new ConsumerCheckpoint(2, start.plusSeconds(10), 2)).verifyComplete();
        // Ingestion completes without a browser or a snapshot query.
        verify(repository, never()).combinedCounts(any(), any());
    }

    @Test
    void slowSnapshotQueryUsesNewestPendingCheckpoint() {
        var checkpoints = Sinks.many().unicast().<ConsumerCheckpoint>onBackpressureBuffer();
        var firstQuery = Sinks.<Map<String, Long>>one();
        when(repository.combinedCounts(any(), any())).thenAnswer(invocation -> {
            Instant end = invocation.getArgument(1);
            return end.equals(start.plusSeconds(5)) ? firstQuery.asMono()
                    : Mono.just(Map.of("progress", Duration.between(start, end).getSeconds()));
        });
        StepVerifier.create(service.createSnapshots(checkpoints.asFlux(), Duration.ofSeconds(30)))
                .then(() -> {
                    checkpoints.tryEmitNext(new ConsumerCheckpoint(1, start.plusSeconds(5), 1));
                    checkpoints.tryEmitNext(new ConsumerCheckpoint(2, start.plusSeconds(10), 2));
                    checkpoints.tryEmitNext(new ConsumerCheckpoint(3, start.plusSeconds(15), 3));
                    checkpoints.tryEmitComplete();
                    firstQuery.tryEmitValue(Map.of("progress", 5L));
                })
                .expectNext(Map.of("progress", 5L), Map.of("progress", 15L)).verifyComplete();
    }

    @Test
    void failedWriteDoesNotProduceSnapshotOrStartAnotherSave() {
        successfulSaves();
        var batches = IntStream.range(0, 2).mapToObj(this::batch).toList();
        when(repository.saveBatch(batches.get(0))).thenReturn(Mono.error(new IllegalStateException("write failed")));
        StepVerifier.create(snapshots(Flux.fromIterable(batches)))
                .expectErrorMessage("write failed").verify();
        verify(repository, never()).combinedCounts(any(), any());
        verify(repository, never()).saveBatch(batches.get(1));
    }

    @Test
    void retriesTransientFailureAndPublishesAfterCommit() {
        var attempts = new java.util.concurrent.atomic.AtomicInteger();
        when(repository.saveBatch(any())).thenReturn(Mono.defer(() -> attempts.getAndIncrement() == 0
                ? Mono.error(new TransientDataAccessResourceException("temporary"))
                : Mono.just(new ConsumerCheckpoint(1, start.plusSeconds(5), 1))));
        when(repository.combinedCounts(any(), any())).thenReturn(Mono.just(Map.of()));
        StepVerifier.withVirtualTime(() -> snapshots(Flux.just(batch(0))))
                .thenAwait(Duration.ofSeconds(2)).expectNext(Map.of()).verifyComplete();
        verify(repository).combinedCounts(start.minusSeconds(25), start.plusSeconds(5));
        org.junit.jupiter.api.Assertions.assertEquals(2, attempts.get());
    }
}
