package com.example.skystream.service;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import com.example.skystream.model.BucketBatch;
import com.example.skystream.model.ConsumerCheckpoint;
import com.example.skystream.model.WordBucket;
import com.example.skystream.repository.WordBucketRepository;
import com.example.skystream.SkystreamApplication;
import dto.JetstreamCommit;
import dto.PostRecord;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"spring.docker.compose.enabled=false", "skystream.jetstream.enabled=false"})
class WordBucketDatabaseTests {
    @Container
    static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry properties) {
        properties.add("spring.r2dbc.url", () -> "r2dbc:postgresql://" + postgres.getHost() + ":"
                + postgres.getMappedPort(5432) + "/" + postgres.getDatabaseName());
        properties.add("spring.r2dbc.username", postgres::getUsername);
        properties.add("spring.r2dbc.password", postgres::getPassword);
    }

    @Autowired WordBucketRepository repository;
    @Autowired WordSnapshotService snapshots;
    @Autowired DatabaseClient database;
    @Autowired JetstreamConsumer consumer;
    @LocalServerPort int port;
    private final Instant origin = Instant.parse("2026-10-04T12:00:00Z");
    private final Duration timeout = Duration.ofSeconds(10);

    private BucketBatch batch(long previous, long seq, int maxSecond, WordBucket... buckets) {
        return new BucketBatch(previous, seq, origin.plusSeconds(maxSecond), List.of(buckets));
    }

    private WordBucket bucket(int second, Map<String, Integer> counts) {
        return new WordBucket(origin.plusSeconds(second), counts);
    }

    private JetstreamCommit commit(long seq, int second, String text) {
        return new JetstreamCommit("network.bsky.jetstream.subscribeEvents#commit", seq, "did:test",
                "2020-01-01T00:00:00Z", origin.plusSeconds(second).toString(), "rev", "create",
                "app.bsky.feed.post", "key" + seq, "cid" + seq,
                new PostRecord("app.bsky.feed.post", text, origin.toString(), new String[]{"en"}));
    }

    private Map<String, Long> awaitSnapshot(Map<String, Long> expected) {
        return snapshots.updates(30, Duration.ZERO).filter(expected::equals).next().block(timeout);
    }

    /** Reads stored counts directly, independent of the production snapshot query. */
    private Map<String, Long> combinedCounts(Instant start, Instant end) {
        return database.sql("""
                SELECT word, SUM(count)::BIGINT AS total
                FROM bucket_word
                WHERE bucket_started_at >= :start AND bucket_started_at < :end
                GROUP BY word
                """)
                .bind("start", start.atOffset(ZoneOffset.UTC)).bind("end", end.atOffset(ZoneOffset.UTC))
                .map((row, metadata) -> Map.entry(row.get("word", String.class), row.get("total", Long.class)))
                .all().collectMap(Map.Entry::getKey, Map.Entry::getValue).block(timeout);
    }

    @BeforeEach
    void clearDatabase() {
        database.sql("TRUNCATE word_bucket CASCADE").fetch().rowsUpdated()
                .then(database.sql("UPDATE consumer_checkpoint SET latest_seq = 0, max_bucket_started_at = NULL, flush_count = 0 WHERE id = 1")
                        .fetch().rowsUpdated()).block(timeout);
    }

    @Test
    void cleanStartProfileDropsExistingTablesAndRecreatesTheCurrentSchema() {
        repository.saveBatch(batch(0, 10, 5, bucket(0, Map.of("galaxy", 3)))).block(timeout);
        database.sql("ALTER TABLE word_bucket ADD COLUMN obsolete_column TEXT").fetch().rowsUpdated().block(timeout);
        String url = "r2dbc:postgresql://" + postgres.getHost() + ":"
                + postgres.getMappedPort(5432) + "/" + postgres.getDatabaseName();
        try (var restarted = new SpringApplicationBuilder(SkystreamApplication.class)
                .web(WebApplicationType.NONE).profiles("clean-start")
                .run("--spring.docker.compose.enabled=false", "--skystream.jetstream.enabled=false",
                        "--spring.r2dbc.url=" + url, "--spring.r2dbc.username=" + postgres.getUsername(),
                        "--spring.r2dbc.password=" + postgres.getPassword())) {
            var freshRepository = restarted.getBean(WordBucketRepository.class);
            assertEquals(ConsumerCheckpoint.empty(), freshRepository.checkpoint().block(timeout));
            assertEquals(Map.of(), combinedCounts(origin, origin.plusSeconds(5)));
            Long obsoleteColumns = database.sql("""
                    SELECT COUNT(*) AS total FROM information_schema.columns
                    WHERE table_schema = current_schema() AND table_name = 'word_bucket'
                        AND column_name = 'obsolete_column'
                    """).map((row, metadata) -> row.get("total", Long.class)).one().block(timeout);
            assertEquals(0L, obsoleteColumns);
            freshRepository.saveBatch(batch(0, 1, 5, bucket(0, Map.of("nebula", 2)))).block(timeout);
            assertEquals(Map.of("nebula", 2L), combinedCounts(origin, origin.plusSeconds(5)));
        }
    }

    @Test
    void persistsTriggeringEventAndPublishesLastThirtySecondsAsUnnamedSse() {
        var commits = java.util.stream.IntStream.rangeClosed(0, 6)
                .mapToObj(i -> commit(i + 1, i * 5, "galaxy nebula")).toList();
        consumer.consumeCommits(Flux.fromIterable(commits)).block(timeout);
        assertEquals(Map.of("galaxy", 6L, "nebula", 6L), awaitSnapshot(Map.of("galaxy", 6L, "nebula", 6L)));
        var checkpoint = repository.checkpoint().block(timeout);
        assertEquals(7, checkpoint.latestSeq());
        assertEquals(6, checkpoint.flushCount());
        // The event at :30 is persisted too, but this snapshot covers [:00, :30).
        assertEquals(Map.of("galaxy", 1L, "nebula", 1L), combinedCounts(origin.plusSeconds(30), origin.plusSeconds(35)));
        var event = WebClient.create("http://localhost:" + port).get().uri("/api/word-buckets")
                .accept(MediaType.TEXT_EVENT_STREAM).retrieve()
                .bodyToFlux(new ParameterizedTypeReference<ServerSentEvent<Map<String, Long>>>() {})
                .next().block(timeout);
        assertNotNull(event);
        assertNull(event.event());
        assertEquals(Map.of("galaxy", 6L, "nebula", 6L), event.data());
    }

    @Test
    void servesIndependentSnapshotDurationsWithInclusiveStartAndExclusiveEnd() {
        repository.saveBatch(batch(0, 10, 3595,
                bucket(-5, Map.of("expired", 9)), bucket(0, Map.of("hour", 1)), bucket(1800, Map.of("half-hour", 2)),
                bucket(3000, Map.of("ten-minutes", 3)), bucket(3300, Map.of("five-minutes", 4)),
                bucket(3540, Map.of("minute", 5)), bucket(3570, Map.of("thirty-seconds", 6)),
                bucket(3595, Map.of("recent", 7)))).block(timeout);
        consumer.consumeCommits(Flux.just(commit(11, 3600, "partial"))).block(timeout);
        assertEquals(Map.of("thirty-seconds", 6L, "recent", 7L), awaitSnapshot(Map.of("thirty-seconds", 6L, "recent", 7L)));
        var client = WebClient.create("http://localhost:" + port);
        var eventType = new ParameterizedTypeReference<ServerSentEvent<Map<String, Long>>>() {};
        int[] durations = {30, 60, 300, 600, 1800, 3600};
        String[] words = {"thirty-seconds", "minute", "five-minutes", "ten-minutes", "half-hour", "hour"};
        long[] counts = {6, 5, 4, 3, 2, 1};
        var expected = new java.util.HashMap<String, Long>();
        expected.put("recent", 7L);
        for (int i = 0; i < durations.length; i++) {
            expected.put(words[i], counts[i]);
            var event = client.get().uri("/api/word-buckets?windowSeconds=" + durations[i] + "&refreshIntervalMs=60000")
                    .accept(MediaType.TEXT_EVENT_STREAM).retrieve().bodyToFlux(eventType).next().block(timeout);
            assertEquals(expected, event.data(), "Window of " + durations[i] + " seconds");
        }
        var defaultEvent = client.get().uri("/api/word-buckets")
                .accept(MediaType.TEXT_EVENT_STREAM).retrieve().bodyToFlux(eventType).next().block(timeout);
        assertEquals(Map.of("thirty-seconds", 6L, "recent", 7L), defaultEvent.data());
        assertEquals(11, repository.checkpoint().block(timeout).latestSeq());
        for (String invalid : List.of("0", "-30", "31", "3601", "invalid")) {
            var status = client.get().uri("/api/word-buckets?windowSeconds=" + invalid)
                    .exchangeToMono(response -> reactor.core.publisher.Mono.just(response.statusCode().value())).block(timeout);
            assertEquals(400, status);
        }
        for (String invalid : List.of("-1", "1", "60001", "invalid")) {
            var status = client.get().uri("/api/word-buckets?refreshIntervalMs=" + invalid)
                    .exchangeToMono(response -> reactor.core.publisher.Mono.just(response.statusCode().value())).block(timeout);
            assertEquals(400, status);
        }
    }

    @Test
    void publishesEachBatchWhenQueriesKeepUpAndDropsWordsOutsideTheRollingWindow() {
        var batches = java.util.stream.IntStream.range(0, 8)
                .mapToObj(i -> batch(i, i + 1, (i + 1) * 5,
                        bucket(i * 5, Map.of("word-" + i, i + 1)))).toList();
        var expected = java.util.stream.IntStream.range(0, 8)
                .mapToObj(i -> java.util.stream.IntStream.rangeClosed(Math.max(0, i - 5), i).boxed()
                        .collect(java.util.stream.Collectors.toMap(j -> "word-" + j, j -> j + 1L))).toList();
        for (int i = 0; i < batches.size(); i++) {
            snapshots.persistBatches(Flux.just(batches.get(i))).blockLast(timeout);
            assertEquals(expected.get(i), awaitSnapshot(expected.get(i)));
        }
    }

    @Test
    void filteredAggregatesReturnEmptyWindowsAndReflectCorrectionsToOlderBuckets() {
        assertEquals(Map.of(), repository.snapshotCounts(origin).block(timeout));
        repository.saveBatch(batch(0, 10, 5, bucket(0, Map.of("hello", 3)))).block(timeout);
        var first = repository.snapshotCounts(origin.plusSeconds(5)).block(timeout);
        assertEquals(Set.copyOf(WordBucketRepository.SNAPSHOT_WINDOW_SECONDS), first.keySet());
        assertTrue(first.values().stream().allMatch(words -> words.equals(Map.of("hello", 3L))));
        repository.saveBatch(batch(10, 12, 10, bucket(0, Map.of("hello", 2, "late", 1)))).block(timeout);
        var corrected = repository.snapshotCounts(origin.plusSeconds(10)).block(timeout);
        assertTrue(corrected.values().stream().allMatch(words -> words.equals(Map.of("hello", 5L, "late", 1L))));
        assertTrue(first.values().stream().allMatch(words -> words.equals(Map.of("hello", 3L))));
    }

    @Test
    void repeatedOrConcurrentBatchDoesNotAddCountsOrAdvanceFlushCountTwice() {
        var first = batch(0, 10, 5, bucket(0, Map.of("hello", 3)), bucket(5, Map.of()));
        Flux.merge(repository.saveBatch(first), repository.saveBatch(first)).blockLast(timeout);
        assertEquals(Map.of("hello", 3L), combinedCounts(origin, origin.plusSeconds(5)));
        assertEquals(10, repository.checkpoint().block(timeout).latestSeq());
        assertEquals(1, repository.checkpoint().block(timeout).flushCount());
    }

    @Test
    void laterFlushAddsCorrectionsToAnOlderBucket() {
        repository.saveBatch(batch(0, 10, 5, bucket(0, Map.of("hello", 3)), bucket(5, Map.of()))).block(timeout);
        repository.saveBatch(batch(10, 12, 10, bucket(0, Map.of("hello", 2, "late", 1)), bucket(10, Map.of()))).block(timeout);
        assertEquals(Map.of("hello", 5L, "late", 1L), combinedCounts(origin, origin.plusSeconds(5)));
        assertEquals(12, repository.checkpoint().block(timeout).latestSeq());
    }

    @Test
    void preservesWordCountPairsAcrossChunksAndAddsCorrections() {
        var counts = java.util.stream.IntStream.range(0, 501).boxed()
                .collect(java.util.stream.Collectors.toMap(i -> "word-" + i, i -> i + 1));
        repository.saveBatch(batch(0, 10, 5, bucket(0, counts))).block(timeout);
        repository.saveBatch(batch(10, 20, 10, bucket(0, counts))).block(timeout);
        var expected = counts.entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue() * 2L));
        assertEquals(expected, combinedCounts(origin, origin.plusSeconds(5)));
        assertEquals(20, repository.checkpoint().block(timeout).latestSeq());
        assertEquals(2, repository.checkpoint().block(timeout).flushCount());
    }

    @Test
    void invalidWordRollsBackContributionsAndCheckpointTogether() {
        StepVerifier.create(repository.saveBatch(batch(0, 10, 5, bucket(0, Map.of("hello", 3, "bad", -1)))))
                .expectError().verify(timeout);
        Long headers = database.sql("SELECT COUNT(*) AS total FROM word_bucket")
                .map((row, meta) -> row.get("total", Long.class)).one().block(timeout);
        assertEquals(0L, headers);
        assertEquals(0, repository.checkpoint().block(timeout).latestSeq());
        assertEquals(0, repository.checkpoint().block(timeout).flushCount());
    }

    @Test
    void rejectsBatchThatSkipsUnpersistedEvents() {
        StepVerifier.create(repository.saveBatch(batch(99, 100, 5, bucket(0, Map.of("hello", 3)))))
                .expectErrorMessage("Batch does not continue the persisted sequence; reload checkpoint").verify(timeout);
        assertEquals(0, repository.checkpoint().block(timeout).latestSeq());
    }

    @Test
    void reconnectSkipsPersistedEventsReplaysPendingWordsAndPublishesUpdates() {
        var firstSession = java.util.stream.IntStream.rangeClosed(0, 4)
                .mapToObj(i -> commit(i + 1, i * 5, "galaxy")).toList();
        consumer.consumeCommits(Flux.concat(Flux.fromIterable(firstSession), Flux.just(commit(6, 22, "pending")))).block(timeout);
        assertEquals(5, repository.checkpoint().block(timeout).latestSeq());
        assertEquals(4, repository.checkpoint().block(timeout).flushCount());
        assertEquals(Map.of("galaxy", 4L), awaitSnapshot(Map.of("galaxy", 4L)));
        consumer.consumeCommits(Flux.just(
                commit(4, 15, "duplicate"), commit(5, 20, "duplicate"),
                commit(6, 22, "pending"), commit(7, 25, "galaxy"), commit(8, 30, "galaxy"))).block(timeout);
        assertEquals(Map.of("galaxy", 6L, "pending", 1L), combinedCounts(origin, origin.plusSeconds(30)));
        assertEquals(8, repository.checkpoint().block(timeout).latestSeq());
        assertEquals(6, repository.checkpoint().block(timeout).flushCount());
        assertEquals(Map.of("galaxy", 6L, "pending", 1L), awaitSnapshot(Map.of("galaxy", 6L, "pending", 1L)));
    }
}
