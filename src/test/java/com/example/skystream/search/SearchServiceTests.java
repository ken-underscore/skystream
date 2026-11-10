package com.example.skystream.search;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import reactor.core.publisher.Flux;

class SearchServiceTests {
    @Test
    void normalizedMeanPointsBetweenTheVectorsWithLengthOne() {
        float half = (float) Math.sqrt(0.5);
        assertArrayEquals(new float[]{half, half, 0}, SearchService.normalizedMean(
                List.of(new float[]{1, 0, 0}, new float[]{0, 1, 0})), 1e-6f);
    }

    @Test
    void searchesWithTheCallersExamplesWithoutAskingHaiku() {
        // The query embeds along the first axis, and each example along the second.
        var embedder = new Embedder() {
            @Override
            public float[][] embedDocuments(List<String> texts) {
                return texts.stream().map(text -> Arrays.copyOf(new float[]{0, 1}, 3)).toArray(float[][]::new);
            }

            @Override
            public float[] embedQuery(String query) {
                return Arrays.copyOf(new float[]{1, 0}, 3);
            }
        };
        var posts = mock(PostRepository.class);
        var expander = mock(QueryExpander.class);
        when(posts.nearest(any(), anyInt())).thenReturn(Flux.empty());
        new SearchService(embedder, posts, expander, new SimpleMeterRegistry())
                .search("hikes", List.of("one", "two", "three")).blockLast();
        var vector = ArgumentCaptor.forClass(float[].class);
        verify(posts).nearest(vector.capture(), eq(SearchService.RESULTS));
        float x = (float) (1 / Math.sqrt(10));
        assertArrayEquals(new float[]{x, 3 * x, 0}, vector.getValue(), 1e-6f);
        verifyNoInteractions(expander);
    }
}
