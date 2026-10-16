package com.example.skystream.service;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.example.skystream.model.ConsumerCheckpoint;
import com.example.skystream.search.SearchPost;
import dto.JetstreamCommit;
import dto.PostRecord;
import reactor.core.publisher.Flux;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.JsonNodeFactory;

class AggregatorServiceTests {
    private final AggregatorService aggregator = new AggregatorService(true);
    private final Instant origin = Instant.parse("2026-10-04T12:00:00Z");

    private JetstreamCommit commit(long seq, double seconds, String text, String language) {
        return post(seq, seconds, text, language, null);
    }

    private JetstreamCommit post(long seq, double seconds, String text, String language, JsonNode reply) {
        return new JetstreamCommit("network.bsky.jetstream.subscribeEvents#commit", seq, "did:test",
                "2020-01-01T00:00:00Z", origin.plusNanos((long) (seconds * 1_000_000_000)).toString(),
                "rev", "create", "app.bsky.feed.post", "key" + seq, "cid",
                new PostRecord("app.bsky.feed.post", text, "2020-01-01T00:00:00Z", new String[]{language}, reply));
    }

    private JetstreamCommit delete(long seq, double seconds, long deletedSeq) {
        return new JetstreamCommit("network.bsky.jetstream.subscribeEvents#commit", seq, "did:test",
                "2020-01-01T00:00:00Z", origin.plusNanos((long) (seconds * 1_000_000_000)).toString(),
                "rev", "delete", "app.bsky.feed.post", "key" + deletedSeq, null, null);
    }

    private static String uri(long seq) {
        return "at://did:test/app.bsky.feed.post/key" + seq;
    }

    @Test
    void flushIncludesTriggeringEventAndItsSequence() {
        var batches = aggregator.createWordBuckets(Flux.just(
                commit(10, 0, "Galaxy the NEBULA https://example.com", "en"),
                commit(11, 4.999999, "galaxy", "en"),
                commit(12, 5, "comet", "en")), ConsumerCheckpoint.empty())
                .collectList().block(Duration.ofSeconds(5));
        assertEquals(1, batches.size());
        var batch = batches.getFirst();
        assertEquals(0, batch.previousSeq());
        assertEquals(12, batch.latestSeq());
        assertEquals(origin.plusSeconds(5), batch.maxBucketStart());
        assertEquals(Map.of("galaxy", 2, "nebula", 1), batch.buckets().get(0).words());
        assertEquals(Map.of("comet", 1), batch.buckets().get(1).words());
    }

    @Test
    void olderBucketsRemainUpdatableAndDuplicatesWithinBatchAreSkipped() {
        var batches = aggregator.createWordBuckets(Flux.just(
                commit(1, 0, "galaxy", "en"),
                commit(2, 5, "nebula", "en"),
                commit(3, 1, "late", "en"),
                commit(3, 1, "late", "en"),
                commit(4, 10, "progress", "fr")), ConsumerCheckpoint.empty())
                .collectList().block(Duration.ofSeconds(5));
        assertEquals(2, batches.size());
        assertEquals(2, batches.get(1).previousSeq());
        assertEquals(4, batches.get(1).latestSeq());
        assertEquals(origin, batches.get(1).buckets().getFirst().start());
        assertEquals(Map.of("late", 1), batches.get(1).buckets().getFirst().words());
        assertTrue(batches.get(1).buckets().getLast().words().isEmpty());
    }

    @Test
    void resumesCheckpointAndRebuildsUnpersistedContributions() {
        var checkpoint = new ConsumerCheckpoint(12, origin.plusSeconds(5));
        var batches = aggregator.createWordBuckets(Flux.just(
                commit(11, 0, "duplicate", "en"),
                commit(12, 5, "duplicate", "en"),
                commit(13, 6, "pending", "en"),
                commit(14, 10, "trigger", "en")), checkpoint)
                .collectList().block(Duration.ofSeconds(5));
        assertEquals(1, batches.size());
        assertEquals(12, batches.getFirst().previousSeq());
        assertEquals(14, batches.getFirst().latestSeq());
        assertEquals(Map.of("pending", 1), batches.getFirst().buckets().getFirst().words());
        assertEquals(Map.of("trigger", 1), batches.getFirst().buckets().getLast().words());
    }

    @Test
    void filtersPublishedStopWordsAndStandaloneDigitsButKeepsMultiDigitNumbers() {
        var batches = aggregator.createWordBuckets(Flux.just(
                commit(1, 0, "the COMPUTER HELLO about 0 1 2 3 4 5 6 7 8 9 10 42 2026 GALAXY", "en"),
                commit(2, 5, "nebula", "en")), ConsumerCheckpoint.empty())
                .collectList().block(Duration.ofSeconds(5));
        assertEquals(Map.of("10", 1, "42", 1, "2026", 1, "galaxy", 1),
                batches.getFirst().buckets().getFirst().words());
        assertEquals(Map.of("nebula", 1), batches.getFirst().buckets().getLast().words());
    }

    @Test
    void filtersContractionsWithStraightAndCurlyApostrophes() {
        var batches = aggregator.createWordBuckets(Flux.just(
                commit(1, 0, "Don't don't DON’T can't can’t I'm I’m isn't isn’t you're you’re wouldn't wouldn’t galaxy", "en"),
                commit(2, 5, "nebula", "en")), ConsumerCheckpoint.empty())
                .collectList().block(Duration.ofSeconds(5));
        assertEquals(Map.of("galaxy", 1), batches.getFirst().buckets().getFirst().words());
        assertEquals(Map.of("nebula", 1), batches.getFirst().buckets().getLast().words());
    }

    @Test
    void preservesInternalApostrophesWhileIgnoringQuotesAndOtherPunctuation() {
        var batches = aggregator.createWordBuckets(Flux.just(
                commit(1, 0, "friend's friend’s O'NEILL 'galaxy' ‘nebula’ (comet), galaxy! café naïve 東京 0 9 10 2026", "en"),
                commit(2, 5, "nebula", "en")), ConsumerCheckpoint.empty())
                .collectList().block(Duration.ofSeconds(5));
        assertEquals(Map.of("friend's", 2, "o'neill", 1, "galaxy", 2, "nebula", 1,
                "comet", 1, "10", 1, "2026", 1), batches.getFirst().buckets().getFirst().words());
    }

    @Test
    void streamCompletionDoesNotFlushBeforeANewerBucket() {
        var batches = aggregator.createWordBuckets(Flux.just(commit(1, 1, "pending", "en")), ConsumerCheckpoint.empty())
                .collectList().block(Duration.ofSeconds(5));
        assertTrue(batches.isEmpty());
    }

    @Test
    void capturesEnglishTopLevelPostsWithFiveWordsOnceLinksAreRemoved() {
        var batch = aggregator.createWordBuckets(Flux.just(
                commit(1, 0, "Finally   did Sahale Arm yesterday https://example.com/photo", "en"),
                commit(2, 1, "four words only here https://example.com", "en"),
                commit(3, 2, "cinq mots en français ici", "fr"),
                post(4, 3, "a reply with five words", "en", JsonNodeFactory.instance.objectNode()),
                commit(5, 5, "next bucket flushes the batch", "en-US")), ConsumerCheckpoint.empty())
                .blockFirst(Duration.ofSeconds(5));
        assertEquals(List.of(new SearchPost(uri(1), "Finally did Sahale Arm yesterday", origin),
                new SearchPost(uri(5), "next bucket flushes the batch", origin.plusSeconds(5))), batch.posts());
        assertEquals(List.of(), batch.deletedUris());
    }

    @Test
    void deleteCancelsACreateInTheSameBatchAndIsSavedOtherwise() {
        var batch = aggregator.createWordBuckets(Flux.just(
                commit(10, 0, "a post that is deleted right away", "en"),
                delete(11, 1, 10),
                delete(12, 2, 3),
                commit(13, 5, "short", "en")), ConsumerCheckpoint.empty())
                .blockFirst(Duration.ofSeconds(5));
        assertEquals(List.of(), batch.posts());
        assertEquals(List.of(uri(3)), batch.deletedUris());
    }

    @Test
    void capturesNothingWhileSearchIsDisabled() {
        var batch = new AggregatorService(false).createWordBuckets(Flux.just(
                commit(1, 0, "a post with five whole words", "en"),
                delete(2, 1, 0),
                commit(3, 5, "another post with five words", "en")), ConsumerCheckpoint.empty())
                .blockFirst(Duration.ofSeconds(5));
        assertEquals(List.of(), batch.posts());
        assertEquals(List.of(), batch.deletedUris());
    }

    @Test
    void sameInstanceResumeUriUsesInclusiveSavedCursor() {
        assertFalse(JetstreamConsumer.resumeUri(0).toString().contains("cursor="));
        assertTrue(JetstreamConsumer.resumeUri(123).toString().endsWith("&cursor=123"));
    }
}
