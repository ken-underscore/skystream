package com.example.skystream.repository;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;

import com.example.skystream.model.BucketBatch;
import com.example.skystream.model.ConsumerCheckpoint;
import com.example.skystream.model.WordBucket;

import io.r2dbc.spi.Row;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
@DependsOnDatabaseInitialization
public class WordBucketRepository {
    private final DatabaseClient database;
    private final TransactionalOperator transactions;

    public WordBucketRepository(DatabaseClient database, ReactiveTransactionManager transactionManager) {
        this.database = database;
        this.transactions = TransactionalOperator.create(transactionManager);
    }

    public Mono<ConsumerCheckpoint> checkpoint() {
        return readCheckpoint(false);
    }

    private Mono<ConsumerCheckpoint> readCheckpoint(boolean lock) {
        return database.sql("SELECT latest_seq, max_bucket_started_at, flush_count FROM consumer_checkpoint WHERE id = 1"
                + (lock ? " FOR UPDATE" : ""))
                .map((row, metadata) -> checkpointFrom(row)).one();
    }

    private ConsumerCheckpoint checkpointFrom(Row row) {
        OffsetDateTime maxStart = row.get("max_bucket_started_at", OffsetDateTime.class);
        return new ConsumerCheckpoint(row.get("latest_seq", Long.class),
                maxStart == null ? null : maxStart.toInstant(), row.get("flush_count", Long.class));
    }

    /** Atomically adds all contributions and advances the sequence, returning only after commit. */
    public Mono<ConsumerCheckpoint> saveBatch(BucketBatch batch) {
        return Mono.defer(() -> {
            Mono<ConsumerCheckpoint> writes = readCheckpoint(true).flatMap(current -> {
                // A retry after a successful but unacknowledged commit must not add words again.
                if (batch.latestSeq() <= current.latestSeq()) {
                    return Mono.just(current);
                }
                if (batch.previousSeq() != current.latestSeq()) {
                    return Mono.error(new IllegalStateException("Batch does not continue the persisted sequence; reload checkpoint"));
                }
                return Flux.fromIterable(batch.buckets()).concatMap(this::addBucket).then(
                        database.sql("""
                                UPDATE consumer_checkpoint
                                SET latest_seq = :seq, max_bucket_started_at = :maxStart, flush_count = flush_count + 1
                                WHERE id = 1
                                RETURNING latest_seq, max_bucket_started_at, flush_count
                                """)
                                .bind("seq", batch.latestSeq())
                                .bind("maxStart", batch.maxBucketStart().atOffset(ZoneOffset.UTC))
                                .map((row, metadata) -> checkpointFrom(row)).one());
            });
            // single() waits for transaction completion, not just its result's onNext.
            return transactions.transactional(writes.flux()).single();
        });
    }

    private Mono<Void> addBucket(WordBucket bucket) {
        return database.sql("""
                INSERT INTO word_bucket (started_at) VALUES (:start)
                ON CONFLICT (started_at) DO NOTHING
                """)
                .bind("start", bucket.start().atOffset(ZoneOffset.UTC)).fetch().rowsUpdated()
                .thenMany(Flux.fromIterable(bucket.words().entrySet()).buffer(500)
                        .concatMap(entries -> addWords(bucket.start(), entries)))
                .then();
    }

    private Mono<Void> addWords(Instant start, List<Map.Entry<String, Integer>> entries) {
        String[] words = new String[entries.size()];
        Long[] counts = new Long[entries.size()];
        for (int i = 0; i < entries.size(); i++) {
            var entry = entries.get(i);
            words[i] = entry.getKey();
            counts[i] = entry.getValue().longValue();
        }
        return database.sql("""
                INSERT INTO bucket_word (bucket_started_at, word, count)
                SELECT :start, words.word, words.count
                FROM UNNEST(CAST(:words AS text[]), CAST(:counts AS bigint[])) AS words(word, count)
                ON CONFLICT (bucket_started_at, word)
                DO UPDATE SET count = bucket_word.count + EXCLUDED.count
                """)
                .bind("start", start.atOffset(ZoneOffset.UTC))
                .bind("words", words)
                .bind("counts", counts)
                .fetch().rowsUpdated().then();
    }

    public Mono<Map<String, Long>> combinedCounts(Instant start, Instant end) {
        return database.sql("""
                SELECT word, SUM(count)::BIGINT AS total
                FROM bucket_word
                WHERE bucket_started_at >= :start AND bucket_started_at < :end
                GROUP BY word
                """)
                .bind("start", start.atOffset(ZoneOffset.UTC)).bind("end", end.atOffset(ZoneOffset.UTC))
                .map((row, metadata) -> Map.entry(row.get("word", String.class), row.get("total", Long.class)))
                .all().collectMap(Map.Entry::getKey, Map.Entry::getValue);
    }
}
