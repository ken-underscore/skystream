package com.example.skystream.search;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import com.example.skystream.Timing;

import io.micrometer.core.instrument.MeterRegistry;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/** Finds the captured posts nearest in meaning to a query and the hypothetical posts written for it. */
class SearchService {
    static final int RESULTS = 10;
    /** The page's searches aren't capped: its rate limits, and the seconds Haiku takes to expand, bound them. */
    private static final Semaphore UNCAPPED = new Semaphore(Integer.MAX_VALUE);
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

    /** Thrown when a capped search finds every permit taken. */
    static final class BusyException extends RuntimeException {
        BusyException() {
            super("Every permit to embed is taken", null, false, false);
        }
    }

    /** The posts most similar to the query, most similar first, sharpened by hypothetical posts Haiku writes. */
    Flux<PostRepository.Hit> search(String query) {
        return search(query, expander.expand(query), UNCAPPED, "haiku");
    }

    /**
     * The same, sharpened by example posts the caller wrote, as an agent does through MCP: no call to Haiku. It embeds
     * only with a permit from {@code embeddings}, failing with {@link BusyException} if there's none.
     */
    Flux<PostRepository.Hit> search(String query, List<String> examples, Semaphore embeddings) {
        return search(query, Mono.just(examples), embeddings, "caller");
    }

    /** {@code expansion} tags the timer: Haiku takes seconds to write its posts, and a caller's arrive with the query. */
    private Flux<PostRepository.Hit> search(String query, Mono<List<String>> hypotheticalPosts, Semaphore embeddings,
            String expansion) {
        // Embedding blocks on the model, so it runs off the request threads.
        return Timing.timed(hypotheticalPosts
                .flatMap(hypothetical -> Mono.fromCallable(() -> searchVector(query, hypothetical, embeddings))
                        .subscribeOn(Schedulers.boundedElastic()))
                .flatMapMany(vector -> posts.nearest(vector, RESULTS)), registry, "skystream.search.query", "expansion", expansion);
    }

    /**
     * The permit is held on the embedding thread until the model is done: a cancelled search's embedding can't be
     * stopped, so it keeps its permit until it finishes.
     */
    private float[] searchVector(String query, List<String> hypothetical, Semaphore embeddings) {
        if (!embeddings.tryAcquire()) throw new BusyException();
        try {
            var vectors = new ArrayList<float[]>(List.of(embedder.embedQuery(query)));
            if (!hypothetical.isEmpty()) vectors.addAll(List.of(embedder.embedDocuments(hypothetical)));
            return normalizedMean(vectors);
        } finally {
            embeddings.release();
        }
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
