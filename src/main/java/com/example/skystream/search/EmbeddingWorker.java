package com.example.skystream.search;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.TimeGauge;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

/**
 * Embeds captured posts newest first, so in a burst fresh posts become searchable first and the older backlog fills
 * in later or expires untouched. Reads the backlog every minute, the measure of how far embedding is behind, to log it
 * and report it as metrics.
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
    private final Counter embedded;
    private final Counter failed;
    private volatile PostRepository.Backlog backlog;

    /** Fetches up to {@code batchSize} posts at a time, and waits {@code idle} after a batch that wasn't full. */
    EmbeddingWorker(PostRepository posts, Embedder embedder, int batchSize, Duration idle, MeterRegistry registry) {
        this.posts = posts;
        this.embedder = embedder;
        this.batchSize = batchSize;
        embedded = registry.counter("skystream.search.embeddings", "outcome", "embedded");
        failed = registry.counter("skystream.search.embeddings", "outcome", "failed");
        Gauge.builder("skystream.search.backlog", this, worker -> worker.backlog == null ? Double.NaN : worker.backlog.posts())
                .description("Posts waiting for an embedding").register(registry);
        TimeGauge.builder("skystream.search.backlog.age", this, TimeUnit.MILLISECONDS, EmbeddingWorker::backlogAgeMillis)
                .description("How long ago the oldest post waiting for an embedding was seen").register(registry);
        work = Mono.defer(this::embedNewest)
                .flatMap(fetched -> fetched < batchSize ? Mono.delay(idle).thenReturn(fetched) : Mono.just(fetched))
                .repeat()
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1)).maxBackoff(Duration.ofMinutes(1))
                        .transientErrors(true)
                        .doBeforeRetry(signal -> log.warn("Could not embed search posts; retrying", signal.failure())))
                .subscribe();
        // A query slower than a minute skips the ticks it overlaps, rather than failing the interval.
        backlogLog = Flux.interval(Duration.ZERO, Duration.ofMinutes(1)).onBackpressureDrop()
                .concatMap(tick -> posts.backlog(Instant.now().minus(PostRepository.RETENTION))
                        .doOnNext(backlog -> {
                            this.backlog = backlog;
                            logBacklog(backlog);
                        })
                        .onErrorResume(error -> {
                            log.warn("Could not read the embedding backlog", error);
                            return Mono.empty();
                        }), 0)
                .subscribe();
    }

    private record Embedded(Map<String, float[]> embeddings, List<String> failedUris) {}

    /** Embeds and saves the newest waiting posts, returning how many there were. */
    private Mono<Integer> embedNewest() {
        return posts.newestUnembedded(Instant.now().minus(PostRepository.RETENTION), batchSize).collectList()
                .filter(batch -> !batch.isEmpty())
                .flatMap(batch -> Mono.fromCallable(() -> embed(batch)).subscribeOn(embedding)
                        .flatMap(result -> posts.saveEmbeddings(result.embeddings())
                                .then(posts.markEmbeddingFailed(result.failedUris()))
                                .then(Mono.fromRunnable(() -> {
                                    embedded.increment(result.embeddings().size());
                                    failed.increment(result.failedUris().size());
                                })))
                        .thenReturn(batch.size()))
                .defaultIfEmpty(0);
    }

    /**
     * Embeds the batch, or if that fails, each post on its own, so one post that can't be embedded is marked failed
     * instead of failing every batch it's in. If no post embeds on its own, the embedder is at fault, not the posts,
     * so the failure is rethrown and the batch retried.
     */
    private Embedded embed(List<SearchPost> batch) {
        try {
            float[][] vectors = embedder.embedDocuments(batch.stream().map(SearchPost::text).toList());
            var embeddings = new HashMap<String, float[]>();
            for (int i = 0; i < vectors.length; i++) embeddings.put(batch.get(i).uri(), vectors[i]);
            return new Embedded(embeddings, List.of());
        } catch (RuntimeException batchFailure) {
            var embeddings = new HashMap<String, float[]>();
            var failures = new LinkedHashMap<String, RuntimeException>();
            for (var post : batch) {
                // Closing interrupts this thread: stop, rather than embed the rest only for them to be discarded.
                if (Thread.currentThread().isInterrupted()) throw batchFailure;
                try {
                    embeddings.put(post.uri(), embedder.embedDocuments(List.of(post.text()))[0]);
                } catch (RuntimeException failure) {
                    failures.put(post.uri(), failure);
                }
            }
            if (embeddings.isEmpty()) throw batchFailure;
            // Attached only here: a broken embedder would otherwise log every post's failure on every retry.
            failures.values().forEach(batchFailure::addSuppressed);
            log.warn("Could not embed a batch of {} posts, so embedded them one at a time; marked {} failed: {}",
                    batch.size(), failures.size(), failures.keySet(), batchFailure);
            return new Embedded(embeddings, List.copyOf(failures.keySet()));
        }
    }

    private double backlogAgeMillis() {
        var current = backlog;
        if (current == null) return Double.NaN;
        return current.oldestSeenAt() == null ? 0 : Duration.between(current.oldestSeenAt(), Instant.now()).toMillis();
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
