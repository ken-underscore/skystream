package com.example.skystream.repository;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    /** Snapshot durations, in seconds, that {@link #snapshotCounts} calculates. */
    public static final List<Integer> SNAPSHOT_WINDOW_SECONDS = List.of(30, 60, 300, 600, 1800, 3600);
    /** Words kept per window: the browser draws at most this many. */
    public static final int TOP_WORDS = 100;

    private record WindowCount(int seconds, String word, long count) {}
    // Same order as the browser's, so both keep the same words when counts tie at the cutoff.
    private static final Comparator<WindowCount> MOST_FREQUENT =
            Comparator.comparingLong(WindowCount::count).reversed().thenComparing(WindowCount::word);

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
        return database.sql("SELECT latest_seq, max_bucket_started_at FROM consumer_checkpoint WHERE id = 1"
                + (lock ? " FOR UPDATE" : ""))
                .map((row, metadata) -> checkpointFrom(row)).one();
    }

    private ConsumerCheckpoint checkpointFrom(Row row) {
        OffsetDateTime maxStart = row.get("max_bucket_started_at", OffsetDateTime.class);
        return new ConsumerCheckpoint(row.get("latest_seq", Long.class),
                maxStart == null ? null : maxStart.toInstant());
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
                                SET latest_seq = :seq, max_bucket_started_at = :maxStart
                                WHERE id = 1
                                RETURNING latest_seq, max_bucket_started_at
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

    /** Calculates all supported windows in one statement, excluding the current incomplete bucket. Each window keeps its {@value #TOP_WORDS} most frequent words; windows without words are omitted. */
    public Mono<Map<Integer, Map<String, Long>>> snapshotCounts(Instant end) {
        // Columns must match SNAPSHOT_WINDOW_SECONDS.
        return database.sql("""
                SELECT word,
                    SUM(count) FILTER (WHERE bucket_started_at >= :end - INTERVAL '30 seconds')::BIGINT AS seconds_30,
                    SUM(count) FILTER (WHERE bucket_started_at >= :end - INTERVAL '60 seconds')::BIGINT AS seconds_60,
                    SUM(count) FILTER (WHERE bucket_started_at >= :end - INTERVAL '300 seconds')::BIGINT AS seconds_300,
                    SUM(count) FILTER (WHERE bucket_started_at >= :end - INTERVAL '600 seconds')::BIGINT AS seconds_600,
                    SUM(count) FILTER (WHERE bucket_started_at >= :end - INTERVAL '1800 seconds')::BIGINT AS seconds_1800,
                    SUM(count)::BIGINT AS seconds_3600
                FROM bucket_word
                WHERE bucket_started_at >= :end - INTERVAL '3600 seconds' AND bucket_started_at < :end
                GROUP BY word
                """)
                .bind("end", end.atOffset(ZoneOffset.UTC))
                .map((row, metadata) -> {
                    String word = row.get("word", String.class);
                    var counts = new ArrayList<WindowCount>();
                    for (int seconds : SNAPSHOT_WINDOW_SECONDS) {
                        Long count = row.get("seconds_" + seconds, Long.class);
                        if (count != null) counts.add(new WindowCount(seconds, word, count));
                    }
                    return counts;
                })
                .all()
                .flatMapIterable(counts -> counts)
                .collect(Collectors.groupingBy(WindowCount::seconds, Collectors.collectingAndThen(Collectors.toList(),
                        counts -> counts.stream().sorted(MOST_FREQUENT).limit(TOP_WORDS)
                                .collect(Collectors.toUnmodifiableMap(WindowCount::word, WindowCount::count)))));
    }
}
