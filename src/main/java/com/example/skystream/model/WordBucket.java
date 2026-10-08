package com.example.skystream.model;

import java.time.Instant;
import java.util.Map;
/** Word-count increments for a canonical five-second interval. */
public record WordBucket(Instant start, Map<String, Integer> words) {
    public WordBucket {
        words = Map.copyOf(words);
    }
}
