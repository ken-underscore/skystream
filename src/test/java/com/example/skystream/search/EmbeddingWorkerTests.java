package com.example.skystream.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {"spring.docker.compose.enabled=false", "skystream.jetstream.enabled=false"})
class EmbeddingWorkerTests {
    @Container
    static final PostgreSQLContainer postgres = new PostgreSQLContainer(
            DockerImageName.parse("pgvector/pgvector:pg17").asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry properties) {
        properties.add("spring.r2dbc.url", () -> "r2dbc:postgresql://" + postgres.getHost() + ":"
                + postgres.getMappedPort(5432) + "/" + postgres.getDatabaseName());
        properties.add("spring.r2dbc.username", postgres::getUsername);
        properties.add("spring.r2dbc.password", postgres::getPassword);
    }

    @Autowired PostRepository repository;
    @Autowired DatabaseClient database;
    private final Instant now = Instant.now();
    private final Duration timeout = Duration.ofSeconds(10);

    @BeforeEach
    void clear() {
        database.sql("TRUNCATE search_post").fetch().rowsUpdated().block(timeout);
    }

    /**
     * Records each batch it embeds, spending {@code busy} on each. It fails the first {@code failures} batches, and
     * every batch with a text containing "poison".
     */
    private static final class FakeEmbedder implements Embedder {
        final List<List<String>> batches = new CopyOnWriteArrayList<>();
        final CountDownLatch started = new CountDownLatch(1);
        final AtomicInteger finished = new AtomicInteger();
        private final AtomicInteger failures;
        private final Duration busy;

        FakeEmbedder(int failures, Duration busy) {
            this.failures = new AtomicInteger(failures);
            this.busy = busy;
        }

        @Override
        public float[][] embedDocuments(List<String> texts) {
            batches.add(texts);
            started.countDown();
            // Spins rather than sleeping: like the model's native code, it ignores interrupts.
            long end = System.nanoTime() + busy.toNanos();
            while (System.nanoTime() < end) Thread.onSpinWait();
            if (failures.getAndDecrement() > 0 || texts.stream().anyMatch(text -> text.contains("poison"))) {
                throw new IllegalStateException("embedding failed");
            }
            finished.incrementAndGet();
            return texts.stream().map(text -> Arrays.copyOf(new float[]{1}, 768)).toArray(float[][]::new);
        }

        @Override
        public float[] embedQuery(String query) {
            throw new UnsupportedOperationException();
        }
    }

    private void capture(String... uris) {
        // Each URI is seen a second before the one listed before it, so they're listed newest first.
        var posts = IntStream.range(0, uris.length)
                .mapToObj(i -> new SearchPost(uris[i], "text of " + uris[i], now.minusSeconds(1 + i))).toList();
        repository.capture(posts, List.of()).block(timeout);
    }

    private void awaitEmbedded(long count) {
        Mono.defer(() -> database.sql("SELECT COUNT(*) AS total FROM search_post WHERE embedding IS NOT NULL")
                        .map((row, meta) -> row.get("total", Long.class)).one())
                .filter(total -> total >= count)
                .repeatWhenEmpty(repeat -> repeat.delayElements(Duration.ofMillis(50)))
                .block(timeout);
    }

    private void awaitBatches(FakeEmbedder embedder, int count) {
        Mono.fromSupplier(embedder.batches::size).filter(batches -> batches >= count)
                .repeatWhenEmpty(repeat -> repeat.delayElements(Duration.ofMillis(50)))
                .block(timeout);
    }

    private List<String> failedUris() {
        return database.sql("SELECT uri FROM search_post WHERE embedding_failed ORDER BY uri")
                .map((row, meta) -> row.get("uri", String.class)).all().collectList().block(timeout);
    }

    private List<String> unembeddedUris() {
        return database.sql("SELECT uri FROM search_post WHERE embedding IS NULL ORDER BY uri")
                .map((row, meta) -> row.get("uri", String.class)).all().collectList().block(timeout);
    }

    @Test
    void embedsTheNewestPostsFirstAndNeverThosePastRetention() {
        capture("1", "2", "3", "4", "5");
        repository.capture(List.of(new SearchPost("expired", "text of expired", now.minus(Duration.ofHours(25)))),
                List.of()).block(timeout);
        var embedder = new FakeEmbedder(0, Duration.ZERO);
        try (var worker = new EmbeddingWorker(repository, embedder, 2, Duration.ofMillis(50))) {
            awaitEmbedded(5);
        }
        assertEquals(List.of(List.of("text of 1", "text of 2"), List.of("text of 3", "text of 4"), List.of("text of 5")),
                embedder.batches);
        assertEquals(List.of("expired"), unembeddedUris());
    }

    @Test
    void waitsOnlyAfterABatchThatIsntFull() {
        capture("1", "2", "3", "4");
        var fetches = new AtomicInteger();
        var counted = new PostRepository(database) {
            @Override
            public Flux<SearchPost> newestUnembedded(Instant since, int limit) {
                fetches.incrementAndGet();
                return super.newestUnembedded(since, limit);
            }
        };
        try (var worker = new EmbeddingWorker(counted, new FakeEmbedder(0, Duration.ZERO), 2, Duration.ofMinutes(1))) {
            awaitEmbedded(4);
            // Long enough for a fetch that didn't wait to happen.
            Mono.delay(Duration.ofMillis(200)).block();
        }
        // Two full batches fetched back to back, then an empty fetch that starts the minute's wait.
        assertEquals(3, fetches.get());
    }

    @Test
    void retriesAFailedBatchAndKeepsWorking() {
        capture("1");
        // Fails the batch and then the post on its own, so nothing embeds and the batch itself is retried.
        var embedder = new FakeEmbedder(2, Duration.ZERO);
        try (var worker = new EmbeddingWorker(repository, embedder, 2, Duration.ofMillis(50))) {
            awaitEmbedded(1);
            capture("2");
            awaitEmbedded(2);
        }
        assertEquals(List.of(), failedUris());
        assertEquals(List.of(List.of("text of 1"), List.of("text of 1"), List.of("text of 1"), List.of("text of 2")),
                embedder.batches);
    }

    @Test
    void marksAPostThatFailsOnItsOwnAndEmbedsTheRest() {
        capture("1", "poison", "3");
        var embedder = new FakeEmbedder(0, Duration.ZERO);
        try (var worker = new EmbeddingWorker(repository, embedder, 3, Duration.ofMillis(50))) {
            awaitEmbedded(2);
            capture("4");
            awaitEmbedded(3);
        }
        assertEquals(List.of("poison"), failedUris());
        assertEquals(List.of(List.of("text of 1", "text of poison", "text of 3"),
                List.of("text of 1"), List.of("text of poison"), List.of("text of 3"),
                List.of("text of 4")), embedder.batches);
    }

    @Test
    void marksNothingWhenNoPostEmbedsOnItsOwn() {
        capture("1", "2");
        var embedder = new FakeEmbedder(Integer.MAX_VALUE, Duration.ZERO);
        try (var worker = new EmbeddingWorker(repository, embedder, 2, Duration.ofMillis(50))) {
            // Each attempt is the batch and then each post on its own: two attempts, so the batch was retried.
            awaitBatches(embedder, 6);
        }
        assertEquals(List.of(), failedUris());
        assertEquals(List.of("1", "2"), unembeddedUris());
    }

    @Test
    void closeWaitsForTheBatchInProgress() throws InterruptedException {
        capture("1");
        var embedder = new FakeEmbedder(0, Duration.ofMillis(500));
        var worker = new EmbeddingWorker(repository, embedder, 2, Duration.ofMillis(50));
        assertTrue(embedder.started.await(timeout.toSeconds(), TimeUnit.SECONDS));
        worker.close();
        assertEquals(1, embedder.finished.get());
    }

    @Test
    void closeStopsEmbeddingPostsOneAtATime() throws InterruptedException {
        capture("1", "2", "3");
        var embedder = new FakeEmbedder(Integer.MAX_VALUE, Duration.ofMillis(500));
        var worker = new EmbeddingWorker(repository, embedder, 3, Duration.ofMillis(50));
        assertTrue(embedder.started.await(timeout.toSeconds(), TimeUnit.SECONDS));
        worker.close();
        // Closed while the batch ran, so once it failed, no post was embedded on its own.
        assertEquals(1, embedder.batches.size());
    }
}
