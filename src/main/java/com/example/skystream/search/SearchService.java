package com.example.skystream.search;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/** Finds the captured posts nearest in meaning to a query. */
class SearchService {
    static final int RESULTS = 10;
    private final Embedder embedder;
    private final PostRepository posts;

    SearchService(Embedder embedder, PostRepository posts) {
        this.embedder = embedder;
        this.posts = posts;
    }

    /** The posts most similar to the query, most similar first. */
    Flux<PostRepository.Hit> search(String query) {
        // Embedding blocks on the model, so it runs off the request threads.
        return Mono.fromCallable(() -> embedder.embedQuery(query)).subscribeOn(Schedulers.boundedElastic())
                .flatMapMany(vector -> posts.nearest(vector, RESULTS));
    }
}
