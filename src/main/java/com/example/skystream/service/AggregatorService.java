package com.example.skystream.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import com.example.skystream.model.WordBucket;
import com.example.skystream.model.BucketBatch;
import com.example.skystream.model.ConsumerCheckpoint;
import com.example.skystream.search.SearchPost;

import dto.JetstreamCommit;
import reactor.core.publisher.Flux;

@Service
public class AggregatorService {
    private static final Pattern TOKENS = Pattern.compile("\\p{L}+(?:'\\p{L}+)*|\\p{Nd}+");
    private static final Pattern VALID_WORD = Pattern.compile("[a-z]+(?:'[a-z]+)*|[0-9]+");
    private static final Set<String> STOP_WORDS = loadStopWords();

    // Search posts are captured only when search is enabled; otherwise nothing would expire or embed them.
    private final boolean captureSearchPosts;

    // Matches SearchConfiguration's @ConditionalOnProperty, which enables search only for "true".
    public AggregatorService(@Value("${skystream.search.enabled:false}") String searchEnabled) {
        this.captureSearchPosts = "true".equalsIgnoreCase(searchEnabled);
    }

    private static Set<String> loadStopWords() {
        var resource = new ClassPathResource("stopwords/english.txt");
        try (var reader = new BufferedReader(new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8))) {
            return reader.lines()
                    .map(String::trim)
                    .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                    .map(AggregatorService::normalize)
                    .collect(Collectors.toUnmodifiableSet());
        } catch (IOException e) {
            throw new IllegalStateException("Could not load English stop words", e);
        }
    }

    private static String normalize(String text) {
        return text.replace('\u2019', '\'').toLowerCase(Locale.ROOT);
    }

    public Flux<BucketBatch> createWordBuckets(Flux<JetstreamCommit> commits, ConsumerCheckpoint checkpoint) {
        return Flux.defer(() -> {
            BucketAccumulator accumulator = new BucketAccumulator(checkpoint);
            return commits.handle((commit, sink) -> {
                BucketBatch batch = accumulator.accept(commit);
                if (batch != null) sink.next(batch);
            });
        });
    }

    private final class BucketAccumulator {
        private final TreeMap<Instant, Map<String, Integer>> pending = new TreeMap<>();
        private final Map<String, SearchPost> posts = new LinkedHashMap<>();
        private final Set<String> deletedUris = new LinkedHashSet<>();
        private long latestProcessedSeq;
        private long latestFlushedSeq;
        private Instant maxBucketStart;

        BucketAccumulator(ConsumerCheckpoint checkpoint) {
            latestProcessedSeq = checkpoint.latestSeq();
            latestFlushedSeq = checkpoint.latestSeq();
            maxBucketStart = checkpoint.maxBucketStart();
        }

        BucketBatch accept(JetstreamCommit commit) {
            // The same instance delivers in seq order. This also suppresses duplicates
            // within the pending batch, not just events covered by the durable checkpoint.
            if (commit.seq() <= latestProcessedSeq) {
                return null;
            }
            if (commit.witnessedAt() == null) {
                throw new IllegalArgumentException("Jetstream commit is missing witnessedAt");
            }
            Instant witnessedAt = Instant.parse(commit.witnessedAt());
            Instant start = Instant.ofEpochSecond(Math.floorDiv(witnessedAt.getEpochSecond(), 5) * 5);
            addWords(commit, pending.computeIfAbsent(start, key -> new HashMap<>()));
            if (captureSearchPosts) capture(commit, witnessedAt);
            latestProcessedSeq = commit.seq();

            if (maxBucketStart == null) {
                maxBucketStart = start;
                return null;
            }
            if (!start.isAfter(maxBucketStart)) {
                return null;
            }
            maxBucketStart = start;
            // Include the triggering event: nothing through latestSeq is left only in memory.
            var buckets = pending.entrySet().stream()
                    .map(entry -> new WordBucket(entry.getKey(), entry.getValue())).toList();
            BucketBatch batch = new BucketBatch(latestFlushedSeq, latestProcessedSeq, maxBucketStart, buckets,
                    List.copyOf(posts.values()), List.copyOf(deletedUris));
            pending.clear();
            posts.clear();
            deletedUris.clear();
            latestFlushedSeq = latestProcessedSeq;
            return batch;
        }

        /**
         * Keeps the posts whose latest event in the batch is a create, and every deleted URI: deletes are saved
         * before creates, so the stored posts match applying the events in order.
         */
        private void capture(JetstreamCommit commit, Instant witnessedAt) {
            if ("delete".equals(commit.operation())) {
                String uri = SearchPost.uri(commit);
                posts.remove(uri);
                deletedUris.add(uri);
            } else {
                SearchPost.from(commit, witnessedAt).ifPresent(post -> posts.put(post.uri(), post));
            }
        }
    }

    private void addWords(JetstreamCommit commit, Map<String, Integer> words) {
        var record = commit.record();
        if (!"create".equals(commit.operation()) || record == null || record.text() == null || !record.isEnglish()) {
            return;
        }
        String noURLs = record.text().replaceAll("https?://\\S+", "");
        var tokens = TOKENS.matcher(normalize(noURLs));
        while (tokens.find()) {
            String word = tokens.group();
            if (VALID_WORD.matcher(word).matches() && !STOP_WORDS.contains(word)) {
                words.merge(word, 1, Integer::sum);
            }
        }
    }
}
