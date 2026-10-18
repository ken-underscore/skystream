package com.example.skystream.search;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;

import io.r2dbc.spi.Row;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Stores captured posts and their embeddings. Posts are inserted without an embedding as part of each ingestion
 * batch, so the table is also the embedding backlog: rows with a null embedding are waiting to be embedded.
 */
@Repository
@DependsOnDatabaseInitialization
public class PostRepository {
    /** How long posts stay searchable. */
    public static final Duration RETENTION = Duration.ofHours(24);

    public record Hit(SearchPost post, double similarity) {}

    public record Backlog(long posts, Instant oldestSeenAt) {}

    private final DatabaseClient database;

    public PostRepository(DatabaseClient database) {
        this.database = database;
    }

    /**
     * Deletes removed posts, then inserts new ones, in the caller's transaction. Deleting first lets a URI deleted
     * and created again keep its new post; otherwise existing URIs are left unchanged. NUL characters are stripped:
     * PostgreSQL rejects them in text, and one post must not fail the caller's batch.
     */
    public Mono<Void> capture(List<SearchPost> posts, List<String> deletedUris) {
        Mono<Long> deleted = deletedUris.isEmpty() ? Mono.empty() : database.sql(
                        "DELETE FROM search_post WHERE uri = ANY(CAST(:uris AS text[]))")
                .bind("uris", deletedUris.toArray(String[]::new))
                .fetch().rowsUpdated();
        Mono<Long> inserted = posts.isEmpty() ? Mono.empty() : database.sql("""
                INSERT INTO search_post (uri, text, seen_at)
                SELECT uri, text, to_timestamp(seen_us / 1e6)
                FROM UNNEST(CAST(:uris AS text[]), CAST(:texts AS text[]), CAST(:seen AS bigint[])) AS post(uri, text, seen_us)
                ON CONFLICT (uri) DO NOTHING
                """)
                .bind("uris", posts.stream().map(SearchPost::uri).toArray(String[]::new))
                .bind("texts", posts.stream().map(post -> post.text().replace("\u0000", "")).toArray(String[]::new))
                .bind("seen", posts.stream().map(post -> ChronoUnit.MICROS.between(Instant.EPOCH, post.seenAt()))
                        .toArray(Long[]::new))
                .fetch().rowsUpdated();
        return deleted.then(inserted).then();
    }

    /** Deletes posts seen before the cutoff, embedded or not. */
    public Mono<Long> expire(Instant cutoff) {
        return database.sql("DELETE FROM search_post WHERE seen_at < :cutoff")
                .bind("cutoff", cutoff.atOffset(ZoneOffset.UTC))
                .fetch().rowsUpdated();
    }

    /** The newest posts still waiting for an embedding, seen at or after {@code since}. */
    public Flux<SearchPost> newestUnembedded(Instant since, int limit) {
        return database.sql("""
                SELECT uri, text, seen_at FROM search_post
                WHERE embedding IS NULL AND seen_at >= :since
                ORDER BY seen_at DESC LIMIT :limit
                """)
                .bind("since", since.atOffset(ZoneOffset.UTC))
                .bind("limit", limit)
                .map((row, metadata) -> postFrom(row))
                .all();
    }

    /** Stores each post's embedding, keyed by URI. Posts deleted in the meantime are skipped. */
    public Mono<Void> saveEmbeddings(Map<String, float[]> embeddings) {
        if (embeddings.isEmpty()) return Mono.empty();
        var uris = embeddings.keySet().toArray(String[]::new);
        return database.sql("""
                UPDATE search_post AS post SET embedding = CAST(saved.embedding AS halfvec)
                FROM UNNEST(CAST(:uris AS text[]), CAST(:embeddings AS text[])) AS saved(uri, embedding)
                WHERE post.uri = saved.uri
                """)
                .bind("uris", uris)
                .bind("embeddings", Arrays.stream(uris).map(uri -> literal(embeddings.get(uri))).toArray(String[]::new))
                .fetch().rowsUpdated().then();
    }

    /** The embedded posts nearest to {@code vector} by cosine similarity, most similar first. */
    public Flux<Hit> nearest(float[] vector, int limit) {
        return database.sql("""
                SELECT uri, text, seen_at, 1 - (embedding <=> CAST(:vector AS halfvec)) AS similarity
                FROM search_post WHERE embedding IS NOT NULL
                ORDER BY embedding <=> CAST(:vector AS halfvec) LIMIT :limit
                """)
                .bind("vector", literal(vector))
                .bind("limit", limit)
                .map((row, metadata) -> new Hit(postFrom(row), row.get("similarity", Double.class)))
                .all();
    }

    /** How many posts seen at or after {@code since} are waiting for an embedding, and when the oldest was seen. */
    public Mono<Backlog> backlog(Instant since) {
        return database.sql("""
                SELECT COUNT(*) AS posts, MIN(seen_at) AS oldest FROM search_post
                WHERE embedding IS NULL AND seen_at >= :since
                """)
                .bind("since", since.atOffset(ZoneOffset.UTC))
                .map((row, metadata) -> {
                    OffsetDateTime oldest = row.get("oldest", OffsetDateTime.class);
                    return new Backlog(row.get("posts", Long.class), oldest == null ? null : oldest.toInstant());
                })
                .one();
    }

    private static SearchPost postFrom(Row row) {
        return new SearchPost(row.get("uri", String.class), row.get("text", String.class),
                row.get("seen_at", OffsetDateTime.class).toInstant());
    }

    /** pgvector's text form, e.g. {@code [0.1,0.2]}: R2DBC has no pgvector type, so vectors are cast from text. */
    private static String literal(float[] vector) {
        var text = new StringBuilder("[");
        for (int i = 0; i < vector.length; i++) {
            if (i > 0) text.append(',');
            text.append(vector[i]);
        }
        return text.append(']').toString();
    }
}
