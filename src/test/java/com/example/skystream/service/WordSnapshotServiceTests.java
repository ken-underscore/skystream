package com.example.skystream.service;

import static com.example.skystream.repository.WordBucketRepository.Tier.FIVE_MINUTES;
import static com.example.skystream.repository.WordBucketRepository.Tier.FIVE_SECONDS;
import static com.example.skystream.repository.WordBucketRepository.Tier.ONE_MINUTE;
import static com.example.skystream.repository.WordBucketRepository.SNAPSHOT_WINDOW_SECONDS;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.TransientDataAccessResourceException;

import com.example.skystream.model.BucketBatch;
import com.example.skystream.model.ConsumerCheckpoint;
import com.example.skystream.model.WordBucket;
import com.example.skystream.model.WordSnapshot;
import com.example.skystream.repository.WordBucketRepository;
import com.example.skystream.repository.WordBucketRepository.Tier;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;

class WordSnapshotServiceTests {
    private final WordBucketRepository repository = mock(WordBucketRepository.class);
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final WordSnapshotService service = new WordSnapshotService(repository, registry);
    private final Instant start = Instant.parse("2026-10-04T12:00:00Z");
    private final Duration timeout = Duration.ofSeconds(5);

    @AfterEach
    void close() { service.close(); }

    private BucketBatch batch(int i) {
        return new BucketBatch(i, i + 1, start.plusSeconds((i + 1) * 5), List.of(new WordBucket(start, Map.of())),
                List.of(), List.of());
    }

    /** Every window of the tier holds "hello" with the given count. */
    private static Map<Integer, WordSnapshot> windows(Tier tier, long value) {
        return tier.windowSeconds().stream().collect(Collectors.toUnmodifiableMap(seconds -> seconds,
                seconds -> new WordSnapshot(Map.of("hello", value), seconds)));
    }

    private Map<Integer, WordSnapshot> windows(long value) {
        return windows(FIVE_SECONDS, value);
    }

    /** Answers each query with the seconds from the start to its newest bucket, for every tier. */
    private void countsSecondsSinceStart() {
        when(repository.snapshots(any(), any())).thenAnswer(invocation -> {
            Instant end = invocation.getArgument(1);
            return Mono.just(windows(invocation.getArgument(0), Duration.between(start, end).getSeconds()));
        });
    }

    @BeforeEach
    void longWindowsAnswerZero() {
        for (Tier tier : List.of(ONE_MINUTE, FIVE_MINUTES)) {
            when(repository.snapshots(eq(tier), any())).thenAnswer(invocation -> Mono.just(windows(tier, 0)));
        }
    }

    private void successfulSaves() {
        when(repository.saveBatch(any())).thenAnswer(invocation -> {
            BucketBatch batch = invocation.getArgument(0);
            return Mono.just(new ConsumerCheckpoint(batch.latestSeq(), batch.maxBucketStart()));
        });
    }

    private void awaitSnapshot(long value) {
        assertEquals(Map.of("hello", value), service.updates(30, Duration.ZERO).map(WordSnapshot::words)
                .filter(Map.of("hello", value)::equals).next().block(timeout));
    }

    @Test
    void startsEmptyAndQueriesOnlyAfterCommit() {
        successfulSaves();
        when(repository.snapshots(eq(FIVE_SECONDS), any())).thenReturn(Mono.just(windows(12)));
        Sinks.One<ConsumerCheckpoint> commit = Sinks.one();
        when(repository.saveBatch(batch(0))).thenReturn(commit.asMono());
        StepVerifier.create(service.updates(30, Duration.ZERO).map(WordSnapshot::words).take(1))
                .then(() -> {
                    service.persistBatches(Flux.just(batch(0))).subscribe();
                    verify(repository, never()).snapshots(any(), any());
                    commit.tryEmitValue(new ConsumerCheckpoint(1, start.plusSeconds(5)));
                })
                .expectNext(Map.of("hello", 12L)).verifyComplete();
        verify(repository).snapshots(FIVE_SECONDS, start.plusSeconds(5));
    }

    @Test
    void timesSavesAndRefreshesAndReportsHowFarBehindTheNewestSavedBucketIs() {
        successfulSaves();
        countsSecondsSinceStart();
        assertEquals(0, registry.get("skystream.ingestion.lag").timeGauge().value(TimeUnit.SECONDS), 2);
        service.persistBatches(Flux.just(batch(0))).blockLast(timeout);
        awaitSnapshot(5);
        service.persistBatches(Flux.just(batch(1))).blockLast(timeout);
        awaitSnapshot(10);
        assertEquals(2, registry.get("skystream.ingestion.save").tag("outcome", "success").timer().count());
        assertEquals(2, registry.get("skystream.snapshots.refresh").tag("tier", "five_seconds").timer().count());
        // The 5-minute tier refreshes on a worker of its own, which nothing above waited for.
        Mono.fromSupplier(() -> registry.find("skystream.snapshots.refresh").tag("tier", "five_minutes").timers().stream()
                        .mapToLong(Timer::count).sum())
                .filter(count -> count > 0)
                .repeatWhenEmpty(repeat -> repeat.delayElements(Duration.ofMillis(20)))
                .block(timeout);
        assertEquals(1, registry.get("skystream.snapshots.refresh").tag("tier", "five_minutes").timer().count());
        double lag = registry.get("skystream.ingestion.lag").timeGauge().value(TimeUnit.SECONDS);
        double expected = Duration.between(start.plusSeconds(10), Instant.now()).toSeconds();
        assertEquals(expected, lag, 2);
    }

    @Test
    void allVisitorsAndDurationsShareOneQueryPerRefresh() {
        successfulSaves();
        when(repository.snapshots(eq(FIVE_SECONDS), any())).thenReturn(Mono.just(windows(12)));
        service.persistBatches(Flux.just(batch(0))).blockLast(timeout);
        awaitSnapshot(12);
        var visitors = IntStream.range(0, 100)
                .mapToObj(i -> service.updates(SNAPSHOT_WINDOW_SECONDS.get(i % SNAPSHOT_WINDOW_SECONDS.size()), Duration.ofMinutes(1)).next()).toList();
        StepVerifier.create(Flux.merge(visitors)).expectNextCount(100).verifyComplete();
        verify(repository, times(1)).snapshots(eq(FIVE_SECONDS), any());
        verify(repository, times(1)).snapshots(eq(ONE_MINUTE), any());
        verify(repository, times(1)).snapshots(eq(FIVE_MINUTES), any());
    }

    @Test
    void longWindowsRefreshOnlyWhenANewCoarseBucketStarts() {
        successfulSaves();
        countsSecondsSinceStart();
        // Batches end at :05, :10, 1:00 and 5:00. The first two share the minute starting at :00; the first three share
        // the five minutes starting at :00.
        for (int i : new int[]{0, 1, 11, 59}) service.persistBatches(Flux.just(batch(i))).blockLast(timeout);
        awaitSnapshot(300);
        for (int window : new int[]{3600, 86400}) {
            assertEquals(Map.of("hello", 300L), service.updates(window, Duration.ZERO).map(WordSnapshot::words)
                    .filter(Map.of("hello", 300L)::equals).next().block(timeout));
        }
        verify(repository).snapshots(ONE_MINUTE, start);
        verify(repository).snapshots(ONE_MINUTE, start.plusSeconds(60));
        verify(repository).snapshots(ONE_MINUTE, start.plusSeconds(300));
        verify(repository, times(3)).snapshots(eq(ONE_MINUTE), any());
        verify(repository).snapshots(FIVE_MINUTES, start);
        verify(repository).snapshots(FIVE_MINUTES, start.plusSeconds(300));
        verify(repository, times(2)).snapshots(eq(FIVE_MINUTES), any());
        verify(repository, times(4)).snapshots(eq(FIVE_SECONDS), any());
    }

    @Test
    void slowVisitorReceivesOnlyTheNewestPendingResult() {
        successfulSaves();
        countsSecondsSinceStart();
        StepVerifier.create(service.updates(30, Duration.ZERO).map(WordSnapshot::words), 0)
                .then(() -> {
                    for (int i = 0; i < 5; i++) service.persistBatches(Flux.just(batch(i))).blockLast(timeout);
                    awaitSnapshot(25);
                })
                .thenRequest(Long.MAX_VALUE)
                .expectNext(Map.of("hello", 25L))
                .expectNoEvent(Duration.ofMillis(100))
                .thenCancel().verify(timeout);
    }

    @Test
    void replacesAllWindowsOnlyAfterTheWholeQueryFinishes() {
        successfulSaves();
        var pending = Sinks.<Map<Integer, WordSnapshot>>one();
        when(repository.snapshots(FIVE_SECONDS, start.plusSeconds(5))).thenReturn(Mono.just(windows(1)));
        when(repository.snapshots(FIVE_SECONDS, start.plusSeconds(10))).thenReturn(pending.asMono());
        service.persistBatches(Flux.just(batch(0))).blockLast(timeout);
        awaitSnapshot(1);
        service.persistBatches(Flux.just(batch(1))).blockLast(timeout);
        assertEquals(Map.of("hello", 1L), service.updates(600, Duration.ZERO).next().block(timeout).words());
        pending.tryEmitValue(windows(2));
        awaitSnapshot(2);
        assertEquals(Map.of("hello", 2L), service.updates(600, Duration.ZERO).next().block(timeout).words());
    }

    @Test
    void slowQueriesDoNotDelayPersistenceAndEventuallyCacheLatestProgress() {
        successfulSaves();
        var pending = Sinks.<Map<Integer, WordSnapshot>>one();
        countsSecondsSinceStart();
        when(repository.snapshots(FIVE_SECONDS, start.plusSeconds(5))).thenReturn(pending.asMono());
        // Virtual time hands each checkpoint to the query worker synchronously, so both later checkpoints are
        // waiting when the slow query finishes. A real worker thread can lag behind this test and fetch :10.
        VirtualTimeScheduler.getOrSet();
        try {
            timed.set(new WordSnapshotService(repository, new SimpleMeterRegistry()));
            for (int i = 0; i < 3; i++) {
                assertEquals(i + 1, timed.get().persistBatches(Flux.just(batch(i))).blockLast(timeout).latestSeq());
            }
            pending.tryEmitValue(windows(5));
            assertEquals(Map.of("hello", 15L), timed.get().updates(30, Duration.ZERO).next().block(timeout).words());
        } finally {
            timed.get().close();
            VirtualTimeScheduler.reset();
        }
        verify(repository, never()).snapshots(FIVE_SECONDS, start.plusSeconds(10));
        verify(repository, times(3)).saveBatch(any());
    }

    @Test
    void failedCacheQueryPreservesPreviousResultsAndDoesNotStopIngestion() {
        successfulSaves();
        var failed = Sinks.<Boolean>one();
        when(repository.snapshots(FIVE_SECONDS, start.plusSeconds(5))).thenReturn(Mono.just(windows(1)));
        when(repository.snapshots(FIVE_SECONDS, start.plusSeconds(10))).thenReturn(
                Mono.<Map<Integer, WordSnapshot>>error(new IllegalStateException("query failed"))
                        .doOnError(error -> failed.tryEmitValue(true)));
        when(repository.snapshots(FIVE_SECONDS, start.plusSeconds(15))).thenReturn(Mono.just(windows(3)));
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
        verify(repository, never()).snapshots(any(), any());
        verify(repository, never()).saveBatch(batch(1));
    }

    @Test
    void retriesTransientWriteFailureAndRefreshesAfterCommit() {
        var attempts = new AtomicInteger();
        when(repository.saveBatch(any())).thenReturn(Mono.defer(() -> attempts.getAndIncrement() == 0
                ? Mono.error(new TransientDataAccessResourceException("temporary"))
                : Mono.just(new ConsumerCheckpoint(1, start.plusSeconds(5)))));
        when(repository.snapshots(eq(FIVE_SECONDS), any())).thenReturn(Mono.just(windows(1)));
        StepVerifier.withVirtualTime(() -> service.persistBatches(Flux.just(batch(0))))
                .thenAwait(Duration.ofSeconds(2))
                .expectNext(new ConsumerCheckpoint(1, start.plusSeconds(5))).verifyComplete();
        awaitSnapshot(1);
        assertEquals(2, attempts.get());
    }

    // Created while virtual time is installed, so the service's schedulers use it.
    private final AtomicReference<WordSnapshotService> timed = new AtomicReference<>();

    private StepVerifier.FirstStep<Map<String, Long>> verifyEvery30Seconds(long initialRequest) {
        successfulSaves();
        countsSecondsSinceStart();
        return StepVerifier.withVirtualTime(() -> {
            timed.set(new WordSnapshotService(repository, new SimpleMeterRegistry()));
            return timed.get().updates(30, Duration.ofSeconds(30)).map(WordSnapshot::words);
        }, initialRequest);
    }

    private void persistTimed(int i) {
        timed.get().persistBatches(Flux.just(batch(i))).subscribe();
    }

    @Test
    void firstResultIsImmediateThenOnlyTheNewestChangePerInterval() {
        verifyEvery30Seconds(Long.MAX_VALUE)
                .then(() -> persistTimed(0))
                .thenAwait(Duration.ZERO)
                .expectNext(Map.of("hello", 5L))
                // An unchanged cache is not sent again.
                .expectNoEvent(Duration.ofSeconds(60))
                .then(() -> { persistTimed(1); persistTimed(2); })
                .expectNoEvent(Duration.ofMillis(29999))
                .thenAwait(Duration.ofMillis(1)).expectNext(Map.of("hello", 15L))
                .then(() -> timed.get().close())
                .expectComplete().verify(timeout);
    }

    @Test
    void slowTimedVisitorDoesNotOverflowAndReceivesTheNewestResult() {
        verifyEvery30Seconds(1)
                .then(() -> persistTimed(0))
                .thenAwait(Duration.ZERO)
                .expectNext(Map.of("hello", 5L))
                .then(() -> { persistTimed(1); persistTimed(2); })
                .thenAwait(Duration.ofSeconds(90))
                .thenRequest(1).expectNext(Map.of("hello", 15L))
                .then(() -> timed.get().close())
                .expectComplete().verify(timeout);
    }
}
