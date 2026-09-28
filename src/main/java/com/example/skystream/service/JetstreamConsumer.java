package com.example.skystream.service;

import java.net.URI;
import java.util.List;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;

import dto.JetstreamCommit;
import dto.JetstreamFrame;
import dto.PostRecord;
import tools.jackson.databind.ObjectMapper;

@Service 
public class JetstreamConsumer {
    private final WebSocketClient client = new ReactorNettyWebSocketClient();
    private final ObjectMapper objectMapper;
    private final AggregatorService aggregatorService;

    public JetstreamConsumer(ObjectMapper objectMapper, AggregatorService aggregatorService) {
        this.objectMapper = objectMapper;
        this.aggregatorService = aggregatorService;
    }

    private static final String COMMIT_TYPE = 
        "network.bsky.jetstream.subscribeEvents#commit";

    private static final URI URL = URI.create(
        "wss://jetstream.us-west.bsky.network" +
        "/xrpc/network.bsky.jetstream.subscribeEvents" +
        "?kinds=commit&collections=app.bsky.feed.post"
    );

    @EventListener(ApplicationReadyEvent.class)
    public void connectToApi() {

        client.execute(URL, session -> {
            System.out.println("✅ V2 WebSocket connected: " + session.getId());

            return session.receive()
                    .map(WebSocketMessage::getPayloadAsText)
                    .doOnNext(json -> System.out.println("RAW: " + json))
                    .map(this::decodeFrame)
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
                    })
                    .transform(aggregatorService::createWordBuckets)
                    .doOnNext(bucket -> System.out.println(bucket))
                    .doOnComplete(() ->
                        System.err.println("⚠️ V2 receive stream completed"))
                    .doOnError(error -> {
                        System.err.println("🚨 V2 stream failed");
                        error.printStackTrace();
                    })
                    .then();
        })
        .doOnError(error -> {
            System.err.println("🚨 Connection failed");
            error.printStackTrace();
        })
        .doFinally(signal ->
            System.err.println("WebSocket ended: " + signal))
        .subscribe(
            ignored -> {},
            error -> System.err.println("Terminal error: " + error)
        );


    }

    private JetstreamFrame decodeFrame(String json) {
        try {
            return objectMapper.readValue(json, JetstreamFrame.class);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Could not decode Jetstream frame: " + json, e);
        }
    }
}
