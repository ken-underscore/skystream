package com.example.skystream.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import com.example.skystream.model.WordBucket;
import com.example.skystream.model.BucketBatch;
import com.example.skystream.model.ConsumerCheckpoint;

import dto.JetstreamCommit;
import reactor.core.publisher.Flux;

@Service
public class AggregatorService {
    private static final Pattern TOKENS = Pattern.compile("\\p{L}+(?:'\\p{L}+)*|\\p{Nd}+");
    private static final Pattern VALID_WORD = Pattern.compile("[a-z]+(?:'[a-z]+)*|[0-9]+");
    private static final Set<String> STOP_WORDS = loadStopWords();

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
            BucketBatch batch = new BucketBatch(latestFlushedSeq, latestProcessedSeq, maxBucketStart, buckets);
            pending.clear();
            latestFlushedSeq = latestProcessedSeq;
            return batch;
        }
    }

    private void addWords(JetstreamCommit commit, Map<String, Integer> words) {
        var record = commit.record();
        if (!"create".equals(commit.operation()) || record == null || record.text() == null
                || record.langs() == null || Arrays.stream(record.langs()).noneMatch(
                        lang -> lang != null && lang.toLowerCase(Locale.ROOT).startsWith("en"))) {
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
