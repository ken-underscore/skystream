package com.example.skystream.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import reactor.core.publisher.Mono;
import reactor.netty.http.server.HttpServer;
import reactor.test.StepVerifier;

class JetstreamConsumerWebSocketTests {
    @Test
    void receivesFramesLargerThanSixtyFourKiB() {
        String payload = "x".repeat(128 * 1024);
        Duration timeout = Duration.ofSeconds(10);
        var server = HttpServer.create().host("127.0.0.1").port(0)
                .handle((request, response) -> response.sendWebsocket(
                        (inbound, outbound) -> outbound.sendString(Mono.just(payload))))
                .bindNow(timeout);
        try {
            var received = new AtomicReference<String>();
            var client = JetstreamConsumer.createWebSocketClient();
            StepVerifier.create(client.execute(URI.create("ws://127.0.0.1:" + server.port()),
                    session -> session.receive().next()
                            .doOnNext(message -> received.set(message.getPayloadAsText())).then()))
                    .expectComplete().verify(timeout);
            assertEquals(payload, received.get());
        } finally {
            server.disposeNow(timeout);
        }
    }
}
