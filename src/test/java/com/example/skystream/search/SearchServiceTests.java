package com.example.skystream.search;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class SearchServiceTests {
    // The query embeds along the first axis, and each example along the second.
    private final Embedder embedder = new Embedder() {
        @Override
        public float[][] embedDocuments(List<String> texts) {
            return texts.stream().map(text -> Arrays.copyOf(new float[]{0, 1}, 3)).toArray(float[][]::new);
        }

        @Override
        public float[] embedQuery(String query) {
            return Arrays.copyOf(new float[]{1, 0}, 3);
        }
    };
    private final PostRepository posts = mock(PostRepository.class);
    private final QueryExpander expander = mock(QueryExpander.class);

    @Test
    void normalizedMeanPointsBetweenTheVectorsWithLengthOne() {
        float half = (float) Math.sqrt(0.5);
        assertArrayEquals(new float[]{half, half, 0}, SearchService.normalizedMean(
                List.of(new float[]{1, 0, 0}, new float[]{0, 1, 0})), 1e-6f);
    }

    @Test
    void searchesWithTheCallersExamplesWithoutAskingHaiku() {
        float x = (float) (1 / Math.sqrt(10));
        assertArrayEquals(new float[]{x, 3 * x, 0}, searchVector(List.of("one", "two", "three")), 1e-6f);
        verifyNoInteractions(expander);
    }

    @Test
    void searchesWithTheQueryAloneWithoutExamples() {
        assertArrayEquals(new float[]{1, 0, 0}, searchVector(List.of()), 1e-6f);
        verifyNoInteractions(expander);
    }

    @Test
    void aCallersSearchWithoutAPermitFailsAsBusyWithoutEmbedding() {
        var service = new SearchService(embedder, posts, expander, new SimpleMeterRegistry());
        StepVerifier.create(service.search("hikes", List.of(), new Semaphore(0)))
                .expectError(SearchService.BusyException.class).verify(Duration.ofSeconds(5));
        verifyNoInteractions(posts);
    }

    @Test
    void aCancelledSearchKeepsItsPermitUntilItsEmbeddingEnds() throws InterruptedException {
        var embedding = new CountDownLatch(1);
        var finish = new CountDownLatch(1);
        var slow = new Embedder() {
            @Override
            public float[][] embedDocuments(List<String> texts) {
                return embedder.embedDocuments(texts);
            }

            @Override
            public float[] embedQuery(String query) {
                embedding.countDown();
                // The model can't be interrupted, so neither is this.
                while (true) {
                    try {
                        if (finish.await(5, TimeUnit.SECONDS)) return embedder.embedQuery(query);
                    } catch (InterruptedException ignored) {
                        // Carries on, as a model run does.
                    }
                }
            }
        };
        when(posts.nearest(any(), anyInt())).thenReturn(Flux.empty());
        var permits = new Semaphore(1);
        var search = new SearchService(slow, posts, expander, new SimpleMeterRegistry())
                .search("hikes", List.of(), permits).subscribe();
        try {
            assertTrue(embedding.await(5, TimeUnit.SECONDS));
            search.dispose();
            assertEquals(0, permits.availablePermits());
        } finally {
            finish.countDown();
        }
        // Released on the embedding thread once the model returns.
        for (int i = 0; i < 500 && permits.availablePermits() == 0; i++) Thread.sleep(10);
        assertEquals(1, permits.availablePermits());
    }

    /** The vector a search with these examples looks up. */
    private float[] searchVector(List<String> examples) {
        when(posts.nearest(any(), anyInt())).thenReturn(Flux.empty());
        var permits = new Semaphore(1);
        new SearchService(embedder, posts, expander, new SimpleMeterRegistry()).search("hikes", examples, permits)
                .blockLast();
        assertEquals(1, permits.availablePermits());
        var vector = ArgumentCaptor.forClass(float[].class);
        verify(posts).nearest(vector.capture(), eq(SearchService.RESULTS));
        return vector.getValue();
    }
}
