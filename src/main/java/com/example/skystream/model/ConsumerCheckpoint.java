package com.example.skystream.model;

import java.time.Instant;

/** Progress committed atomically with word counts for the single upstream consumer. */
public record ConsumerCheckpoint(long latestSeq, Instant maxBucketStart) {
    public static ConsumerCheckpoint empty() {
        return new ConsumerCheckpoint(0, null);
    }
}
