package com.example.skystream.service;

import java.time.Duration;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;

import com.example.skystream.model.BucketBatch;
import com.example.skystream.model.ConsumerCheckpoint;
import com.example.skystream.repository.WordBucketRepository;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

@Service
public class WordSnapshotService {
    private static final Logger log = LoggerFactory.getLogger(WordSnapshotService.class);
    private final WordBucketRepository repository;
    private final Sinks.Many<ConsumerCheckpoint> refreshRequests = Sinks.many().unicast().onBackpressureBuffer();
    private final Sinks.Many<Map<Integer, Map<String, Long>>> cache = Sinks.many().replay().latest();
    private final Disposable cacheRefresh;

    public WordSnapshotService(WordBucketRepository repository) {
        this.repository = repository;
        // Query work runs separately from ingestion. If it falls behind, keep the newest pending checkpoint.
        cacheRefresh = refreshRequests.asFlux()
                .publishOn(Schedulers.parallel())
                .onBackpressureLatest()
                .concatMap(checkpoint -> Mono.defer(() -> repository.snapshotCounts(checkpoint.maxBucketStart()))
                        .retryWhen(databaseRetry())
                        .doOnNext(windows -> cache.tryEmitNext(windows))
                        .onErrorResume(error -> {
                            log.warn("Could not refresh snapshot cache after seq={}; keeping previous results",
                                    checkpoint.latestSeq(), error);
                            return Mono.empty();
                        }), 0)
                .subscribe();
    }

    public Flux<ConsumerCheckpoint> persistBatches(Flux<BucketBatch> batches) {
        // No prefetch: don't accumulate another batch while its predecessor is still committing.
        return batches.concatMap(batch -> repository.saveBatch(batch)
                .retryWhen(databaseRetry())
                .filter(checkpoint -> checkpoint.latestSeq() == batch.latestSeq()), 0)
                .doOnNext(checkpoint -> refreshRequests.tryEmitNext(checkpoint));
    }

    public Flux<Map<String, Long>> updates(int windowSeconds, Duration refreshInterval) {
        var results = cache.asFlux();
        if (!refreshInterval.isZero()) {
            // First result immediately, then at most one new result per interval. Each refresh is a new
            // map, so identity skips the replayed initial result without comparing contents.
            results = results.take(1).concatWith(results.sample(refreshInterval))
                    .distinctUntilChanged(windows -> windows, (a, b) -> a == b);
        }
        // A slow client keeps only the newest pending result, rather than pinning every refresh.
        return results.onBackpressureLatest().map(windows -> windows.getOrDefault(windowSeconds, Map.of()));
    }

    // Runs before the web server's graceful shutdown, which waits for open SSE streams to complete.
    @EventListener(ContextClosedEvent.class)
    public void close() {
        cacheRefresh.dispose();
        cache.tryEmitComplete();
    }

    private Retry databaseRetry() {
        return Retry.backoff(3, Duration.ofMillis(200))
                .filter(TransientDataAccessException.class::isInstance)
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
    }
}
