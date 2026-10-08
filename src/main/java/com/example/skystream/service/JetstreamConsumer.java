package com.example.skystream.service;

import java.net.URI;
import java.time.Duration;
import java.util.Map;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.beans.factory.annotation.Value;
import jakarta.annotation.PreDestroy;
import reactor.core.Disposable;
import org.springframework.context.event.EventListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;

import dto.JetstreamCommit;
import dto.JetstreamFrame;
import com.example.skystream.repository.WordBucketRepository;
import com.example.skystream.model.ConsumerCheckpoint;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.WebsocketClientSpec;
import reactor.util.retry.Retry;
import tools.jackson.databind.ObjectMapper;

@Service 
public class JetstreamConsumer {
    private static final Logger log = LoggerFactory.getLogger(JetstreamConsumer.class);
    private final WebSocketClient client = createWebSocketClient();
    private final ObjectMapper objectMapper;
    private final AggregatorService aggregatorService;
    private final WordSnapshotService snapshotService;
    private final WordBucketRepository repository;
    private Disposable connection;

    @Value("${skystream.jetstream.enabled:true}")
    private boolean enabled;

    public JetstreamConsumer(ObjectMapper objectMapper, AggregatorService aggregatorService,
            WordSnapshotService snapshotService, WordBucketRepository repository) {
        this.objectMapper = objectMapper;
        this.aggregatorService = aggregatorService;
        this.snapshotService = snapshotService;
        this.repository = repository;
    }

    static ReactorNettyWebSocketClient createWebSocketClient() {
        return new ReactorNettyWebSocketClient(HttpClient.create(),
                () -> WebsocketClientSpec.builder().maxFramePayloadLength(1024 * 1024));
    }

    private final Sinks.Many<ConsumerCheckpoint> checkpoints =
        Sinks.many().replay().latest();

    private static final String COMMIT_TYPE = 
        "network.bsky.jetstream.subscribeEvents#commit";

    private static final URI URL = URI.create(
        "wss://jetstream.us-west.bsky.network" +
        "/xrpc/network.bsky.jetstream.subscribeEvents" +
        "?kinds=commit&collections=app.bsky.feed.post"
    );

    public Flux<Map<String, Long>> wordBuckets(int windowSeconds) {
        return snapshotService.createSnapshots(checkpoints.asFlux(), Duration.ofSeconds(windowSeconds));
    }

    @EventListener(ApplicationReadyEvent.class)
    public void connectToApi() {
        if (!enabled || (connection != null && !connection.isDisposed())) return;
        connection = Mono.defer(() -> repository.checkpoint().flatMap(this::connectFrom))
                .repeatWhen(completed -> completed.delayElements(Duration.ofSeconds(1)))
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1)).maxBackoff(Duration.ofSeconds(30))
                        .filter(error -> !(error instanceof IllegalArgumentException))
                        .doBeforeRetry(signal -> log.warn("Jetstream disconnected; resuming from database checkpoint", signal.failure())))
                .subscribe(ignored -> {}, error -> log.error("Jetstream ingestion stopped", error));
    }

    static URI resumeUri(long latestSeq) {
        return latestSeq == 0 ? URL : URI.create(URL + "&cursor=" + latestSeq);
    }

    private Mono<Void> connectFrom(ConsumerCheckpoint checkpoint) {
        return client.execute(resumeUri(checkpoint.latestSeq()), session -> {
            log.info("Jetstream connected; resuming after seq={}", checkpoint.latestSeq());

            Flux<JetstreamCommit> commits = session.receive()
                    .map(WebSocketMessage::getPayloadAsText)
                    .mapNotNull(this::decodeFrame)
                    .map(frame -> {
                        if ("error".equals(frame.type())) {
                            throw new IllegalStateException(
                                    "Jetstream error: "
                                            + frame.error()
                                            + " — "
                                            + frame.message()
                            );
                        }

                        if (!"message".equals(frame.type())) {
                            throw new IllegalStateException(
                                    "Unexpected frame type: " + frame.type()
                            );
                        }

                        JetstreamCommit event = frame.payload();

                        if (event == null) {
                            throw new IllegalStateException(
                                    "Message frame contained no payload"
                            );
                        }

                        if (!COMMIT_TYPE.equals(event.type())) {
                            throw new IllegalStateException(
                                    "Expected commit payload but received: " + event.type()
                            );
                        }

                        return event;
                    });
            return consumeCommits(commits, checkpoint);
        });
    }

    Mono<Void> consumeCommits(Flux<JetstreamCommit> commits) {
        return repository.checkpoint().flatMap(checkpoint -> consumeCommits(commits, checkpoint));
    }

    private Mono<Void> consumeCommits(Flux<JetstreamCommit> commits, ConsumerCheckpoint checkpoint) {
        return aggregatorService.createWordBuckets(commits, checkpoint)
                .transform(snapshotService::persistBatches)
                .doOnNext(checkpointAfterCommit -> {
                    var result = checkpoints.tryEmitNext(checkpointAfterCommit);
                    if (result.isFailure()) throw new IllegalStateException("Failed to publish checkpoint: " + result);
                })
                .then();
    }

    @PreDestroy
    @EventListener(ContextClosedEvent.class)
    public void disconnect() {
        if (connection != null) connection.dispose();
        checkpoints.tryEmitComplete();
    }

    private JetstreamFrame decodeFrame(String json) {
        try {
            var frame = objectMapper.readTree(json);
            var payload = frame.path("payload");
            if ("message".equals(frame.path("$type").asString())
                    && "network.bsky.jetstream.subscribeEvents#info".equals(payload.path("$type").asString())) {
                log.info("Jetstream advisory: {}", payload);
                // Advisories are logged but do not enter commit processing.
                return null;
            }
            return objectMapper.treeToValue(frame, JetstreamFrame.class);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Could not decode Jetstream frame: " + json, e);
        }
    }
}
