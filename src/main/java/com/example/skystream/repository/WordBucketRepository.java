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
import com.example.skystream.model.WordSnapshot;
import com.example.skystream.search.PostRepository;

import io.r2dbc.spi.Row;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
@DependsOnDatabaseInitialization
public class WordBucketRepository {
    /**
     * Word counts kept at one bucket size, and the windows read from them. Each batch adds its counts to every tier, so a
     * long window sums a few hundred coarse buckets rather than thousands of 5-second ones.
     */
    public record Tier(String buckets, String words, int bucketSeconds, List<Integer> windowSeconds) {
        /** The start of the bucket holding {@code time}. */
        public Instant bucketStart(Instant time) {
            return Instant.ofEpochSecond(Math.floorDiv(time.getEpochSecond(), bucketSeconds) * bucketSeconds);
        }

        int longestWindow() {
            return windowSeconds.getLast();
        }
    }

    public static final Tier FIVE_SECONDS = new Tier("word_bucket", "bucket_word", 5, List.of(30, 60, 300, 600, 1800, 3600));
    public static final Tier FIVE_MINUTES = new Tier("word_bucket_5m", "bucket_word_5m", 300, List.of(10800, 28800, 86400));
    public static final List<Tier> TIERS = List.of(FIVE_SECONDS, FIVE_MINUTES);
    /** Every snapshot duration, in seconds, shortest first. */
    public static final List<Integer> SNAPSHOT_WINDOW_SECONDS =
            TIERS.stream().flatMap(tier -> tier.windowSeconds().stream()).toList();
    /** Words kept per window: the browser draws at most this many. */
    public static final int TOP_WORDS = 100;

    private record WindowCount(int seconds, String word, long count) {}
    // Same order as the browser's, so both keep the same words when counts tie at the cutoff.
    private static final Comparator<WindowCount> MOST_FREQUENT =
            Comparator.comparingLong(WindowCount::count).reversed().thenComparing(WindowCount::word);

    private final DatabaseClient database;
    private final PostRepository posts;
    private final TransactionalOperator transactions;

    public WordBucketRepository(DatabaseClient database, PostRepository posts, ReactiveTransactionManager transactionManager) {
        this.database = database;
        this.posts = posts;
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

    /** Atomically adds all contributions and captured search posts and advances the sequence, returning only after commit. */
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
                return Flux.fromIterable(batch.buckets()).concatMap(this::addBucket)
                        .then(posts.capture(batch.posts(), batch.deletedUris())).then(
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
        return Flux.fromIterable(TIERS).concatMap(tier -> addBucket(tier, tier.bucketStart(bucket.start()), bucket.words()))
                .then();
    }

    private Mono<Void> addBucket(Tier tier, Instant start, Map<String, Integer> counts) {
        return database.sql("INSERT INTO " + tier.buckets() + " (started_at) VALUES (:start) ON CONFLICT (started_at) DO NOTHING")
                .bind("start", start.atOffset(ZoneOffset.UTC)).fetch().rowsUpdated()
                .thenMany(Flux.fromIterable(counts.entrySet()).buffer(500)
                        .concatMap(entries -> addWords(tier, start, entries)))
                .then();
    }

    private Mono<Void> addWords(Tier tier, Instant start, List<Map.Entry<String, Integer>> entries) {
        String[] words = new String[entries.size()];
        Long[] counts = new Long[entries.size()];
        for (int i = 0; i < entries.size(); i++) {
            var entry = entries.get(i);
            words[i] = entry.getKey();
            counts[i] = entry.getValue().longValue();
        }
        return database.sql("""
                INSERT INTO %1$s (bucket_started_at, word, count)
                SELECT :start, words.word, words.count
                FROM UNNEST(CAST(:words AS text[]), CAST(:counts AS bigint[])) AS words(word, count)
                ON CONFLICT (bucket_started_at, word)
                DO UPDATE SET count = %1$s.count + EXCLUDED.count
                """.formatted(tier.words()))
                .bind("start", start.atOffset(ZoneOffset.UTC))
                .bind("words", words)
                .bind("counts", counts)
                .fetch().rowsUpdated().then();
    }

    /**
     * Deletes the buckets each tier's longest window no longer reaches, with their words. Like the windows, this is
     * measured from the newest bucket rather than the clock, so catching up after downtime keeps what the windows show.
     */
    public Mono<Void> expire() {
        return checkpoint().mapNotNull(ConsumerCheckpoint::maxBucketStart)
                .flatMapMany(newest -> Flux.fromIterable(TIERS).concatMap(tier -> database
                        .sql("DELETE FROM " + tier.buckets() + " WHERE started_at < :cutoff")
                        .bind("cutoff", tier.bucketStart(newest).minusSeconds(tier.longestWindow()).atOffset(ZoneOffset.UTC))
                        .fetch().rowsUpdated()))
                .then();
    }

    /**
     * Each of the tier's windows, ending at the start of the bucket holding {@code newest}, which is incomplete though its
     * first counts are saved. Each window keeps its {@value #TOP_WORDS} most frequent words, and says how much of it has
     * been collected, which is less than the window until the app has run that long.
     */
    public Mono<Map<Integer, WordSnapshot>> snapshots(Tier tier, Instant newest) {
        Instant end = tier.bucketStart(newest);
        return Mono.zip(topWords(tier, end), collectedSeconds(tier, end)).map(results -> tier.windowSeconds().stream()
                .collect(Collectors.toUnmodifiableMap(seconds -> seconds, seconds -> new WordSnapshot(
                        results.getT1().getOrDefault(seconds, Map.of()), results.getT2().get(seconds)))));
    }

    private Mono<Map<Integer, Map<String, Long>>> topWords(Tier tier, Instant end) {
        return database.sql("""
                SELECT word, %s
                FROM %s
                WHERE bucket_started_at >= :end - INTERVAL '%d seconds' AND bucket_started_at < :end
                GROUP BY word
                """.formatted(windowColumns(tier, "SUM(count)", "bucket_started_at"), tier.words(), tier.longestWindow()))
                .bind("end", end.atOffset(ZoneOffset.UTC))
                .map((row, metadata) -> {
                    String word = row.get("word", String.class);
                    var counts = new ArrayList<WindowCount>();
                    for (int seconds : tier.windowSeconds()) {
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

    /** How much of each window has buckets, gaps from downtime excluded. */
    private Mono<Map<Integer, Long>> collectedSeconds(Tier tier, Instant end) {
        return database.sql("""
                SELECT %s
                FROM %s
                WHERE started_at >= :end - INTERVAL '%d seconds' AND started_at < :end
                """.formatted(windowColumns(tier, "COUNT(*)", "started_at"), tier.buckets(), tier.longestWindow()))
                .bind("end", end.atOffset(ZoneOffset.UTC))
                .map((row, metadata) -> tier.windowSeconds().stream().collect(Collectors.toUnmodifiableMap(seconds -> seconds,
                        seconds -> row.get("seconds_" + seconds, Long.class) * tier.bucketSeconds())))
                .one();
    }

    /** One column per window, {@code seconds_<window>}, aggregating the rows whose {@code time} falls in it. */
    private static String windowColumns(Tier tier, String aggregate, String time) {
        return tier.windowSeconds().stream()
                .map(seconds -> "%s FILTER (WHERE %s >= :end - INTERVAL '%d seconds')::BIGINT AS seconds_%d"
                        .formatted(aggregate, time, seconds, seconds))
                .collect(Collectors.joining(", "));
    }
}
