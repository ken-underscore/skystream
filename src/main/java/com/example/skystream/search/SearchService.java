package com.example.skystream.search;

import java.util.ArrayList;
import java.util.List;

import com.example.skystream.Timing;

import io.micrometer.core.instrument.MeterRegistry;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/** Finds the captured posts nearest in meaning to a query and the hypothetical posts written for it. */
class SearchService {
    static final int RESULTS = 10;
    private final Embedder embedder;
    private final PostRepository posts;
    private final QueryExpander expander;
    private final MeterRegistry registry;

    SearchService(Embedder embedder, PostRepository posts, QueryExpander expander, MeterRegistry registry) {
        this.embedder = embedder;
        this.posts = posts;
        this.expander = expander;
        this.registry = registry;
    }

    /** The posts most similar to the query, most similar first. */
    Flux<PostRepository.Hit> search(String query) {
        // Embedding blocks on the model, so it runs off the request threads.
        return Timing.timed(expander.expand(query)
                .flatMap(hypothetical -> Mono.fromCallable(() -> searchVector(query, hypothetical))
                        .subscribeOn(Schedulers.boundedElastic()))
                .flatMapMany(vector -> posts.nearest(vector, RESULTS)), registry, "skystream.search.query");
    }

    private float[] searchVector(String query, List<String> hypothetical) {
        var vectors = new ArrayList<float[]>(List.of(embedder.embedQuery(query)));
        if (!hypothetical.isEmpty()) vectors.addAll(List.of(embedder.embedDocuments(hypothetical)));
        return normalizedMean(vectors);
    }

    /** The vectors' mean scaled to length 1: dividing by their count first would make no difference. */
    static float[] normalizedMean(List<float[]> vectors) {
        var sum = new float[vectors.getFirst().length];
        for (float[] vector : vectors) {
            for (int i = 0; i < sum.length; i++) sum[i] += vector[i];
        }
        double squares = 0;
        for (float value : sum) squares += value * value;
        float length = (float) Math.sqrt(squares);
        for (int i = 0; i < sum.length; i++) sum[i] /= length;
        return sum;
    }
}
