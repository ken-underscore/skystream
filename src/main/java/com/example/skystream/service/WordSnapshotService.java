package com.example.skystream.service;

import java.time.Duration;
import java.util.Map;

import org.springframework.dao.TransientDataAccessException;
import org.springframework.stereotype.Service;

import com.example.skystream.model.BucketBatch;
import com.example.skystream.model.ConsumerCheckpoint;
import com.example.skystream.repository.WordBucketRepository;

import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;

@Service
public class WordSnapshotService {
    private final WordBucketRepository repository;

    public WordSnapshotService(WordBucketRepository repository) {
        this.repository = repository;
    }

    public Flux<ConsumerCheckpoint> persistBatches(Flux<BucketBatch> batches) {
        // No prefetch: don't accumulate another batch while its predecessor is still committing.
        return batches.concatMap(batch -> repository.saveBatch(batch)
                .retryWhen(databaseRetry())
                .filter(checkpoint -> checkpoint.latestSeq() == batch.latestSeq()), 0);
    }

    public Flux<Map<String, Long>> createSnapshots(Flux<ConsumerCheckpoint> checkpoints, Duration window) {
        // A slow query keeps only the newest pending checkpoint, rather than queuing old windows.
        return checkpoints.onBackpressureLatest().concatMap(checkpoint -> repository.combinedCounts(
                checkpoint.maxBucketStart().minus(window), checkpoint.maxBucketStart())
                .retryWhen(databaseRetry()), 0);
    }

    private Retry databaseRetry() {
        return Retry.backoff(3, Duration.ofMillis(200))
                .filter(TransientDataAccessException.class::isInstance)
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
    }
}
