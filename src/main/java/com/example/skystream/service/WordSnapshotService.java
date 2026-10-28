package com.example.skystream.service;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;

import com.example.skystream.Timing;
import com.example.skystream.model.BucketBatch;
import com.example.skystream.model.ConsumerCheckpoint;
import com.example.skystream.model.WordSnapshot;
import com.example.skystream.repository.WordBucketRepository;
import com.example.skystream.repository.WordBucketRepository.Tier;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.TimeGauge;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

@Service
public class WordSnapshotService {
    private static final Logger log = LoggerFactory.getLogger(WordSnapshotService.class);
    private final WordBucketRepository repository;
    private final MeterRegistry registry;
    private final Sinks.Many<Instant> refreshRequests = Sinks.many().multicast().onBackpressureBuffer();
    private final Map<Tier, Sinks.Many<Map<Integer, WordSnapshot>>> caches = new EnumMap<>(Tier.class);
    private final Disposable cacheRefresh;
    // Startup until the first save, so a stall right after startup still shows as growing lag.
    private volatile Instant newestSaved = Instant.now();

    public WordSnapshotService(WordBucketRepository repository, MeterRegistry registry) {
        this.repository = repository;
        this.registry = registry;
        TimeGauge.builder("skystream.ingestion.lag", this, TimeUnit.MILLISECONDS, WordSnapshotService::lagMillis)
                .description("How far behind now the newest saved 5-second bucket starts, or since startup before the first save").register(registry);
        for (Tier tier : Tier.values()) caches.put(tier, Sinks.many().replay().latest());
        // Each tier has its own worker, so a slow 24-hour query doesn't hold up the 30-second window.
        cacheRefresh = Disposables.composite(Arrays.stream(Tier.values()).map(this::refresh).toList());
    }

    /**
     * Refreshes the tier's windows each time its newest bucket changes: after every batch for 5-second buckets, every 5
     * minutes for 5-minute ones. Query work runs separately from ingestion; if it falls behind, it keeps the newest.
     */
    private Disposable refresh(Tier tier) {
        String tierTag = tier.name().toLowerCase(Locale.ROOT);
        return refreshRequests.asFlux()
                .map(tier::bucketStart)
                .distinctUntilChanged()
                .publishOn(Schedulers.parallel())
                .onBackpressureLatest()
                .concatMap(newest -> Timing.timed(Mono.defer(() -> repository.snapshots(tier, newest)), registry,
                                "skystream.snapshots.refresh", "tier", tierTag)
                        .retryWhen(databaseRetry())
                        .doOnNext(windows -> caches.get(tier).tryEmitNext(windows))
                        .onErrorResume(error -> {
                            log.warn("Could not refresh the word snapshots ending {}; keeping previous results", newest,
                                    error);
                            return Mono.empty();
                        }), 0)
                .subscribe();
    }

    public Flux<ConsumerCheckpoint> persistBatches(Flux<BucketBatch> batches) {
        // No prefetch: don't accumulate another batch while its predecessor is still committing.
        return batches.concatMap(batch -> Timing.timed(repository.saveBatch(batch), registry, "skystream.ingestion.save")
                .retryWhen(databaseRetry())
                .filter(checkpoint -> checkpoint.latestSeq() == batch.latestSeq()), 0)
                .doOnNext(checkpoint -> {
                    newestSaved = checkpoint.maxBucketStart();
                    refreshRequests.tryEmitNext(newestSaved);
                });
    }

    /** Up to 5 seconds plus Jetstream's own delay while keeping up; how far behind ingestion is while catching up. */
    private double lagMillis() {
        return Duration.between(newestSaved, Instant.now()).toMillis();
    }

    public Flux<WordSnapshot> updates(int windowSeconds, Duration refreshInterval) {
        var tier = Tier.serving(windowSeconds)
                .orElseThrow(() -> new IllegalArgumentException("No snapshot window of " + windowSeconds + " seconds"));
        var results = caches.get(tier).asFlux();
        if (!refreshInterval.isZero()) {
            // First result immediately, then at most one new result per interval. Each refresh is a new
            // map, so identity skips the replayed initial result without comparing contents.
            results = results.take(1).concatWith(results.sample(refreshInterval))
                    .distinctUntilChanged(windows -> windows, (a, b) -> a == b);
        }
        // A slow client keeps only the newest pending result, rather than pinning every refresh.
        return results.onBackpressureLatest().map(windows -> windows.get(windowSeconds));
    }

    // Runs before the web server's graceful shutdown, which waits for open SSE streams to complete.
    @EventListener(ContextClosedEvent.class)
    public void close() {
        cacheRefresh.dispose();
        caches.values().forEach(Sinks.Many::tryEmitComplete);
    }

    private Retry databaseRetry() {
        return Retry.backoff(3, Duration.ofMillis(200))
                .filter(TransientDataAccessException.class::isInstance)
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
    }
}
