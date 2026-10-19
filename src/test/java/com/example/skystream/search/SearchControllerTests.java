package com.example.skystream.search;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {"spring.docker.compose.enabled=false", "skystream.jetstream.enabled=false"})
class SearchControllerTests {
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
    private final Instant seenAt = Instant.parse("2026-10-18T12:00:00Z");
    private final Duration timeout = Duration.ofSeconds(10);
    private final List<String> queries = new CopyOnWriteArrayList<>();
    private WebTestClient client;

    @BeforeEach
    void setUp() {
        database.sql("TRUNCATE search_post").fetch().rowsUpdated().block(timeout);
        // Every query embeds to the same vector, so the posts' vectors alone decide the order.
        var embedder = new Embedder() {
            @Override
            public float[][] embedDocuments(List<String> texts) {
                throw new UnsupportedOperationException();
            }

            @Override
            public float[] embedQuery(String query) {
                queries.add(query);
                return Arrays.copyOf(new float[]{1, 0}, 768);
            }
        };
        client = WebTestClient.bindToController(new SearchController(new SearchService(embedder, repository))).build();
    }

    private static String uri(String rkey) {
        return "at://did:plc:author/app.bsky.feed.post/" + rkey;
    }

    @Test
    void returnsTheTenPostsNearestTheQueryMostSimilarFirst() {
        // Post i points 5i degrees away from the query. Their lengths vary, so ordering by distance or inner product
        // instead of cosine would differ, and they're saved in reverse, so storage order isn't the answer.
        var posts = new ArrayList<SearchPost>();
        var embeddings = new HashMap<String, float[]>();
        for (int i = 11; i >= 0; i--) {
            posts.add(new SearchPost(uri("post" + i), "post " + i, seenAt));
            double angle = Math.toRadians(5 * i);
            float length = 1 + i % 3;
            embeddings.put(uri("post" + i),
                    Arrays.copyOf(new float[]{length * (float) Math.cos(angle), length * (float) Math.sin(angle)}, 768));
        }
        posts.add(new SearchPost(uri("waiting"), "waiting", seenAt));
        repository.capture(posts, List.of()).block(timeout);
        repository.saveEmbeddings(embeddings).block(timeout);

        client.get().uri("/api/search?q={q}", "  sore throat  ").exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$[*].text").isEqualTo(IntStream.range(0, 10).mapToObj(i -> "post " + i).toList())
                .jsonPath("$[0].url").isEqualTo("https://bsky.app/profile/did:plc:author/post/post0")
                .jsonPath("$[0].seenAt").isEqualTo("2026-10-18T12:00:00Z")
                .jsonPath("$[0].similarity").value(Double.class, similarity -> assertEquals(1.0, similarity, 0.001));
        assertEquals(List.of("sore throat"), queries);
    }

    @Test
    void returnsAnEmptyListWhenNothingIsEmbedded() {
        repository.capture(List.of(new SearchPost(uri("waiting"), "waiting", seenAt)), List.of()).block(timeout);
        client.get().uri("/api/search?q={q}", "anything").exchange()
                .expectStatus().isOk()
                .expectBody().json("[]");
    }

    @Test
    void rejectsAMissingBlankOrOversizedQueryWithoutEmbeddingIt() {
        client.get().uri("/api/search").exchange().expectStatus().isBadRequest();
        client.get().uri("/api/search?q={q}", " \t ").exchange().expectStatus().isBadRequest();
        client.get().uri("/api/search?q={q}", "x".repeat(SearchController.MAX_QUERY_LENGTH + 1)).exchange()
                .expectStatus().isBadRequest();
        assertEquals(List.of(), queries);
        // Surrounding whitespace doesn't count towards the limit.
        client.get().uri("/api/search?q={q}", " " + "x".repeat(SearchController.MAX_QUERY_LENGTH) + " ").exchange()
                .expectStatus().isOk();
    }
}
