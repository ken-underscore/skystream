package com.example.skystream.repository;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
     * long window sums a few hundred coarse buckets rather than thousands of 5-second ones, and refreshes only when a
     * new coarse bucket starts.
     */
    public enum Tier {
        FIVE_SECONDS("word_bucket", "bucket_word", 5, List.of(30, 60, 300, 600)),
        ONE_MINUTE("word_bucket_1m", "bucket_word_1m", 60, List.of(1800, 3600)),
        FIVE_MINUTES("word_bucket_5m", "bucket_word_5m", 300, List.of(10800, 28800, 86400));

        private final String buckets;
        private final String words;
        private final int bucketSeconds;
        private final List<Integer> windowSeconds;

        Tier(String buckets, String words, int bucketSeconds, List<Integer> windowSeconds) {
            this.buckets = buckets;
            this.words = words;
            this.bucketSeconds = bucketSeconds;
            this.windowSeconds = windowSeconds;
        }

        /** The tier serving a snapshot duration, if any does. */
        public static Optional<Tier> serving(int windowSeconds) {
            return Arrays.stream(values()).filter(tier -> tier.windowSeconds.contains(windowSeconds)).findFirst();
        }

        /** The start of the bucket holding {@code time}. */
        public Instant bucketStart(Instant time) {
            return Instant.ofEpochSecond(Math.floorDiv(time.getEpochSecond(), bucketSeconds) * bucketSeconds);
        }

        public int bucketSeconds() {
            return bucketSeconds;
        }

        public List<Integer> windowSeconds() {
            return windowSeconds;
        }

        public String buckets() {
            return buckets;
        }

        public String words() {
            return words;
        }

        private int longestWindow() {
            return windowSeconds.getLast();
        }
    }

    /** Every snapshot duration, in seconds, shortest first. */
    public static final List<Integer> SNAPSHOT_WINDOW_SECONDS =
            Arrays.stream(Tier.values()).flatMap(tier -> tier.windowSeconds().stream()).toList();
    /** Words kept per window: the browser draws at most this many. */
    public static final int TOP_WORDS = 100;
    /**
     * A word's count in one bucket is far below int's limit. Summed as int, each total is a plain 64-bit number; summed
     * as the column's bigint, it's a 128-bit value allocated per word and window, and the 24-hour query took 3.1 s
     * rather than 1.9 s.
     */
    private static final String BUCKET_COUNT_SUM = "SUM(count::int)";

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
        return Flux.just(Tier.values()).concatMap(tier -> addBucket(tier, tier.bucketStart(bucket.start()), bucket.words()))
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
     * Returns how many buckets were deleted.
     */
    public Mono<Long> expire() {
        return checkpoint().mapNotNull(ConsumerCheckpoint::maxBucketStart)
                .flatMapMany(newest -> Flux.just(Tier.values()).concatMap(tier -> database
                        .sql("DELETE FROM " + tier.buckets() + " WHERE started_at < :cutoff")
                        .bind("cutoff", tier.bucketStart(newest).minusSeconds(tier.longestWindow()).atOffset(ZoneOffset.UTC))
                        .fetch().rowsUpdated()))
                .reduce(0L, Long::sum);
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

    /**
     * Sums every window in one pass, then keeps each window's top words in the database, so only those rows reach the
     * app. Ties at the cutoff keep the alphabetically first words, as the browser does.
     */
    private Mono<Map<Integer, Map<String, Long>>> topWords(Tier tier, Instant end) {
        String tops = tier.windowSeconds().stream().map(seconds -> """
                (SELECT %1$d AS seconds, word, seconds_%1$d AS count FROM totals
                WHERE seconds_%1$d IS NOT NULL ORDER BY seconds_%1$d DESC, word LIMIT %2$d)""".formatted(seconds, TOP_WORDS))
                .collect(Collectors.joining(" UNION ALL "));
        return database.sql("""
                WITH totals AS MATERIALIZED (
                    SELECT word, %s
                    FROM %s
                    WHERE bucket_started_at >= :end - INTERVAL '%d seconds' AND bucket_started_at < :end
                    GROUP BY word
                )
                %s
                """.formatted(windowColumns(tier, BUCKET_COUNT_SUM, "bucket_started_at"), tier.words(), tier.longestWindow(), tops))
                .bind("end", end.atOffset(ZoneOffset.UTC))
                .map((row, metadata) -> new WindowCount(row.get("seconds", Integer.class), row.get("word", String.class),
                        row.get("count", Long.class)))
                .all()
                .collect(Collectors.groupingBy(WindowCount::seconds,
                        Collectors.toUnmodifiableMap(WindowCount::word, WindowCount::count)));
    }

    private record WindowCount(int seconds, String word, long count) {}

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
                .map(seconds -> "%s FILTER (WHERE %s >= :end - INTERVAL '%d seconds') AS seconds_%d"
                        .formatted(aggregate, time, seconds, seconds))
                .collect(Collectors.joining(", "));
    }
}
