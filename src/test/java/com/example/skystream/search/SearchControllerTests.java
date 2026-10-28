package com.example.skystream.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
    private final List<String> documents = new CopyOnWriteArrayList<>();
    private volatile List<String> hypothetical = List.of();
    private volatile Flux<String> answer = Flux.empty();
    private final List<List<SearchPost>> answered = new CopyOnWriteArrayList<>();
    private WebTestClient client;
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    @BeforeEach
    void setUp() {
        database.sql("TRUNCATE search_post").fetch().rowsUpdated().block(timeout);
        // Every query embeds to the same vector, and every hypothetical post to one at right angles to it.
        var embedder = new Embedder() {
            @Override
            public float[][] embedDocuments(List<String> texts) {
                documents.addAll(texts);
                return texts.stream().map(text -> Arrays.copyOf(new float[]{0, 1}, 768)).toArray(float[][]::new);
            }

            @Override
            public float[] embedQuery(String query) {
                if (query.equals("unembeddable")) throw new IllegalStateException("embedding failed");
                queries.add(query);
                return Arrays.copyOf(new float[]{1, 0}, 768);
            }
        };
        var expander = new QueryExpander(null, Duration.ZERO, new SimpleMeterRegistry()) {
            @Override
            Mono<List<String>> expand(String query) {
                return Mono.just(hypothetical);
            }
        };
        var answers = new AnswerService(null, Duration.ZERO, new SimpleMeterRegistry()) {
            @Override
            Flux<String> answer(String query, List<SearchPost> posts) {
                answered.add(posts);
                return answer;
            }
        };
        client = WebTestClient.bindToController(
                new SearchController(new SearchService(embedder, repository, expander, registry), answers)).build();
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
        assertEquals(1, searchTimes("success"));
    }

    private long searchTimes(String outcome) {
        return registry.get("skystream.search.query").tag("outcome", outcome).timer().count();
    }

    @Test
    void searchesByTheMeanOfTheQueryAndItsHypotheticalPosts() {
        // The query points at 0 degrees and its hypothetical post at 90, so their normalized mean points at 45. Alone,
        // the query would rank the post at 0 first.
        var embeddings = new HashMap<String, float[]>();
        var posts = new ArrayList<SearchPost>();
        for (int degrees : new int[]{0, 45, 80}) {
            posts.add(new SearchPost(uri("at" + degrees), "at " + degrees, seenAt));
            double angle = Math.toRadians(degrees);
            embeddings.put(uri("at" + degrees), Arrays.copyOf(new float[]{(float) Math.cos(angle), (float) Math.sin(angle)}, 768));
        }
        repository.capture(posts, List.of()).block(timeout);
        repository.saveEmbeddings(embeddings).block(timeout);
        hypothetical = List.of("Did Rattlesnake Ledge today");

        client.get().uri("/api/search?q={q}", "washington state hikes").exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$[*].text").isEqualTo(List.of("at 45", "at 80", "at 0"))
                .jsonPath("$[0].similarity").value(Double.class, similarity -> assertEquals(1.0, similarity, 0.001));
        assertEquals(hypothetical, documents);
    }

    private List<ServerSentEvent<String>> stream(String query) {
        return client.get().uri("/api/search/stream?q={q}", query).accept(MediaType.TEXT_EVENT_STREAM).exchange()
                .expectStatus().isOk()
                .returnResult(new ParameterizedTypeReference<ServerSentEvent<String>>() {})
                .getResponseBody().collectList().block(timeout);
    }

    private void captureEmbedded(String... rkeys) {
        repository.capture(Arrays.stream(rkeys).map(rkey -> new SearchPost(uri(rkey), rkey, seenAt)).toList(), List.of())
                .block(timeout);
        var embeddings = new HashMap<String, float[]>();
        for (int i = 0; i < rkeys.length; i++) {
            double angle = Math.toRadians(10 * i);
            embeddings.put(uri(rkeys[i]), Arrays.copyOf(new float[]{(float) Math.cos(angle), (float) Math.sin(angle)}, 768));
        }
        repository.saveEmbeddings(embeddings).block(timeout);
    }

    @Test
    void streamsTheResultsThenTheAnswerFromThemThenDone() {
        captureEmbedded("first", "second");
        answer = Flux.just("Hikers loved ", "Sahale Arm [1].");
        var events = stream("washington state hikes");
        assertEquals(List.of("results", "answer", "answer", "done"), events.stream().map(ServerSentEvent::event).toList());
        assertTrue(events.get(0).data().startsWith("[{\"text\":\"first\""));
        assertEquals("{\"text\":\"Hikers loved \"}", events.get(1).data());
        assertEquals(List.of(List.of("first", "second")),
                answered.stream().map(posts -> posts.stream().map(SearchPost::text).toList()).toList());
    }

    @Test
    void keepsTheResultsAndEndsWithFailedWhenTheAnswerFails() {
        captureEmbedded("first");
        answer = Flux.just("Hikers ").concatWith(Flux.error(new IllegalStateException("Haiku declined to answer")));
        assertEquals(List.of("results", "answer", "failed"),
                stream("washington state hikes").stream().map(ServerSentEvent::event).toList());
    }

    @Test
    void streamRejectsABadQueryAndFailsWithoutEventsWhenTheSearchFails() {
        client.get().uri("/api/search/stream?q={q}", " ").accept(MediaType.TEXT_EVENT_STREAM).exchange()
                .expectStatus().isBadRequest();
        client.get().uri("/api/search/stream?q={q}", "unembeddable").accept(MediaType.TEXT_EVENT_STREAM).exchange()
                .expectStatus().is5xxServerError();
        assertEquals(List.of(), answered);
        assertEquals(1, searchTimes("error"));
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
