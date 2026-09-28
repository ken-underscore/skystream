package com.example.skystream.service;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.springframework.stereotype.Service;

import dto.JetstreamCommit;
import opennlp.tools.tokenize.SimpleTokenizer;
import reactor.core.publisher.Flux;

@Service 
public class AggregatorService {
    SimpleTokenizer tokenizer = SimpleTokenizer.INSTANCE;

    private static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
        "the", "a", "an", "and", "or", "but", "is", "are", "was", "to", "of", "in", "on", "at", "it", "this"
    ));

    public Flux<Map<String, Integer>> createWordBuckets(Flux<JetstreamCommit> flux) {
        return flux
            // Delete commits have null records
            .filter(commit -> commit.operation().equals("create"))
            .filter(commit -> commit.record() != null)
            .map(JetstreamCommit::record)
            .filter(record -> {
                if (record.langs() != null) {
                    return Arrays.stream(record.langs())
                        .anyMatch(lang -> lang != null && lang.toLowerCase().startsWith("en"));
                } else {
                    return false;
                }
            })
            .window(Duration.ofSeconds(5))
            .flatMapSequential(innerFlux -> 
                innerFlux.reduce(new HashMap<String, Integer>(), (wordBucket, record) -> {
                    String noURLS = record.text().replaceAll("https?://\\S+", "");
                    String[] tokens = tokenizer.tokenize(noURLS);

                    for (String token : tokens) {
                        String normalized = token.toLowerCase(Locale.ROOT);

                        if (!normalized.matches("^[a-zA-Z0-9]+$")) {
                            continue;
                        }

                        if (STOP_WORDS.contains(normalized)) {
                            continue;
                        }

                        wordBucket.merge(normalized, 1, Integer::sum);
                    }

                    return wordBucket;
                }));
            
    }
}
