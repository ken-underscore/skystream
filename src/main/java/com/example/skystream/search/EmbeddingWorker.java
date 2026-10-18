package com.example.skystream.search;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

/**
 * Embeds captured posts newest first, so in a burst fresh posts become searchable first and the older backlog fills
 * in later or expires untouched. Logs the backlog every minute as the measure of how far embedding is behind.
 */
class EmbeddingWorker implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(EmbeddingWorker.class);
    private final PostRepository posts;
    private final Embedder embedder;
    private final int batchSize;
    // The embedder blocks for up to seconds per batch, so it gets a thread of its own.
    private final Scheduler embedding = Schedulers.newSingle("search-embedding", true);
    private final Disposable work;
    private final Disposable backlogLog;

    /** Fetches up to {@code batchSize} posts at a time, and waits {@code idle} after a batch that wasn't full. */
    EmbeddingWorker(PostRepository posts, Embedder embedder, int batchSize, Duration idle) {
        this.posts = posts;
        this.embedder = embedder;
        this.batchSize = batchSize;
        work = Mono.defer(this::embedNewest)
                .flatMap(embedded -> embedded < batchSize ? Mono.delay(idle).thenReturn(embedded) : Mono.just(embedded))
                .repeat()
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1)).maxBackoff(Duration.ofMinutes(1))
                        .transientErrors(true)
                        .doBeforeRetry(signal -> log.warn("Could not embed search posts; retrying", signal.failure())))
                .subscribe();
        backlogLog = Flux.interval(Duration.ofMinutes(1))
                .concatMap(tick -> posts.backlog(Instant.now().minus(PostRepository.RETENTION))
                        .doOnNext(EmbeddingWorker::logBacklog)
                        .onErrorResume(error -> {
                            log.warn("Could not read the embedding backlog", error);
                            return Mono.empty();
                        }), 0)
                .subscribe();
    }

    /** Embeds and saves the newest waiting posts, returning how many there were. */
    private Mono<Integer> embedNewest() {
        return posts.newestUnembedded(Instant.now().minus(PostRepository.RETENTION), batchSize).collectList()
                .filter(batch -> !batch.isEmpty())
                .flatMap(batch -> Mono.fromCallable(() -> embed(batch)).subscribeOn(embedding))
                .flatMap(embeddings -> posts.saveEmbeddings(embeddings).thenReturn(embeddings.size()))
                .defaultIfEmpty(0);
    }

    private Map<String, float[]> embed(List<SearchPost> batch) {
        float[][] vectors = embedder.embedDocuments(batch.stream().map(SearchPost::text).toList());
        var embeddings = new HashMap<String, float[]>();
        for (int i = 0; i < vectors.length; i++) embeddings.put(batch.get(i).uri(), vectors[i]);
        return embeddings;
    }

    private static void logBacklog(PostRepository.Backlog backlog) {
        if (backlog.oldestSeenAt() == null) {
            log.info("Embedding backlog: 0 posts");
        } else {
            log.info("Embedding backlog: {} posts, oldest seen {}s ago", backlog.posts(),
                    Duration.between(backlog.oldestSeenAt(), Instant.now()).toSeconds());
        }
    }

    /** Stops embedding, waiting for a batch in progress: the embedder must not be closed while it runs. */
    @Override
    public void close() {
        work.dispose();
        backlogLog.dispose();
        embedding.disposeGracefully().block(Duration.ofMinutes(1));
    }
}
