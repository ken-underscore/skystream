package com.example.skystream.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {"spring.docker.compose.enabled=false", "skystream.jetstream.enabled=false"})
class PostRepositoryTests {
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
    @Autowired ReactiveTransactionManager transactionManager;
    private final Instant now = Instant.parse("2026-10-15T12:00:00Z");
    private final Duration timeout = Duration.ofSeconds(10);

    @BeforeEach
    void clear() {
        database.sql("TRUNCATE search_post").fetch().rowsUpdated().block(timeout);
    }

    private SearchPost post(String uri, Duration age) {
        return new SearchPost(uri, "text of " + uri, now.minus(age));
    }

    /** A 768-dimension vector starting with the given values, zero elsewhere. */
    private static float[] vector(float... head) {
        return Arrays.copyOf(head, 768);
    }

    private List<String> uris() {
        return database.sql("SELECT uri FROM search_post ORDER BY uri").map((row, meta) -> row.get("uri", String.class))
                .all().collectList().block(timeout);
    }

    @Test
    void captureIgnoresRepeatedUrisAndAppliesDeletes() {
        repository.capture(List.of(post("a", Duration.ZERO), post("b", Duration.ZERO)), List.of()).block(timeout);
        repository.capture(List.of(new SearchPost("a", "replayed text", now)), List.of("b", "never-captured")).block(timeout);
        assertEquals(List.of("a"), uris());
        assertEquals("text of a", repository.newestUnembedded(now.minusSeconds(60), 10).blockFirst(timeout).text());
    }

    @Test
    void captureRollsBackWithTheCallersTransaction() {
        repository.capture(List.of(post("b", Duration.ZERO)), List.of()).block(timeout);
        var transactions = TransactionalOperator.create(transactionManager);
        StepVerifier.create(transactions.transactional(repository.capture(List.of(post("a", Duration.ZERO)), List.of("b"))
                        .then(Mono.error(new IllegalStateException("word-cloud write failed")))))
                .expectErrorMessage("word-cloud write failed").verify(timeout);
        assertEquals(List.of("b"), uris());
    }

    @Test
    void captureStripsNulCharactersInsteadOfFailing() {
        repository.capture(List.of(new SearchPost("a", "before\u0000after", now)), List.of()).block(timeout);
        assertEquals("beforeafter", repository.newestUnembedded(now.minusSeconds(60), 1).blockFirst(timeout).text());
    }

    @Test
    void newestUnembeddedSkipsEmbeddedPostsAndThoseOutsideTheWindow() {
        repository.capture(List.of(post("old", Duration.ofHours(30)), post("hour", Duration.ofHours(1)),
                post("minute", Duration.ofMinutes(1)), post("embedded", Duration.ZERO)), List.of()).block(timeout);
        repository.saveEmbeddings(Map.of("embedded", vector(1))).block(timeout);
        var pending = repository.newestUnembedded(now.minus(Duration.ofHours(24)), 10).map(SearchPost::uri)
                .collectList().block(timeout);
        assertEquals(List.of("minute", "hour"), pending);
        var backlog = repository.backlog().block(timeout);
        assertEquals(3, backlog.posts());
        assertEquals(now.minus(Duration.ofHours(30)), backlog.oldestSeenAt());
    }

    @Test
    void nearestReturnsOnlyEmbeddedPostsMostSimilarFirst() {
        repository.capture(List.of(post("same", Duration.ZERO), post("close", Duration.ZERO), post("far", Duration.ZERO),
                post("waiting", Duration.ZERO)), List.of()).block(timeout);
        repository.saveEmbeddings(Map.of("same", vector(1, 0), "close", vector(0.8f, 0.6f), "far", vector(0, 1)))
                .block(timeout);
        var hits = repository.nearest(vector(1, 0), 10).collectList().block(timeout);
        assertEquals(List.of("same", "close", "far"), hits.stream().map(hit -> hit.post().uri()).toList());
        assertEquals(1.0, hits.get(0).similarity(), 0.001);
        assertEquals(0.8, hits.get(1).similarity(), 0.001);
        assertEquals(0.0, hits.get(2).similarity(), 0.001);
    }

    @Test
    void expireRemovesPostsSeenBeforeTheCutoffEmbeddedOrNot() {
        repository.capture(List.of(post("old", Duration.ofHours(25)), post("old-embedded", Duration.ofHours(26)),
                post("recent", Duration.ofHours(1))), List.of()).block(timeout);
        repository.saveEmbeddings(Map.of("old-embedded", vector(1))).block(timeout);
        assertEquals(2L, repository.expire(now.minus(Duration.ofHours(24))).block(timeout));
        assertEquals(List.of("recent"), uris());
        assertNull(repository.nearest(vector(1), 10).blockFirst(timeout));
        assertEquals(1, repository.backlog().block(timeout).posts());
    }
}
