package com.example.skystream.model;

import java.time.Instant;
import java.util.List;

import com.example.skystream.search.SearchPost;

/**
 * All contributions after previousSeq, through latestSeq, including the flush-triggering event: word counts, and the
 * search posts captured and deleted in the same range, which are saved with them.
 */
public record BucketBatch(long previousSeq, long latestSeq, Instant maxBucketStart, List<WordBucket> buckets,
        List<SearchPost> posts, List<String> deletedUris) {
    public BucketBatch {
        if (latestSeq <= previousSeq) throw new IllegalArgumentException("A batch must advance the sequence");
        buckets = List.copyOf(buckets);
        posts = List.copyOf(posts);
        deletedUris = List.copyOf(deletedUris);
    }
}
