package com.example.skystream.search;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

class SearchServiceTests {
    @Test
    void normalizedMeanPointsBetweenTheVectorsWithLengthOne() {
        float half = (float) Math.sqrt(0.5);
        assertArrayEquals(new float[]{half, half, 0}, SearchService.normalizedMean(
                List.of(new float[]{1, 0, 0}, new float[]{0, 1, 0})), 1e-6f);
    }
}
