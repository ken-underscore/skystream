package com.example.skystream.model;

import java.time.Instant;
import java.util.List;

/** All contributions after previousSeq, through latestSeq, including the flush-triggering event. */
public record BucketBatch(long previousSeq, long latestSeq, Instant maxBucketStart, List<WordBucket> buckets) {
    public BucketBatch {
        if (latestSeq <= previousSeq) throw new IllegalArgumentException("A batch must advance the sequence");
        buckets = List.copyOf(buckets);
    }
}
