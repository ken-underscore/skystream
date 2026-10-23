package com.example.skystream.model;

import java.util.Map;

/** A window's most frequent words, and how many of its seconds the app has collected. */
public record WordSnapshot(Map<String, Long> words, long collectedSeconds) {
    public WordSnapshot {
        words = Map.copyOf(words);
    }
}
