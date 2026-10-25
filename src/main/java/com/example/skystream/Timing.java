package com.example.skystream;

import java.util.concurrent.atomic.AtomicBoolean;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Times each subscription, so each retry is timed on its own, and tags it with how it ended: {@code outcome} is
 * {@code success}, {@code error} or {@code cancel}. The time is recorded before the result is passed on, so it excludes
 * what subscribers do with it.
 */
public final class Timing {
    private Timing() {}

    public static <T> Mono<T> timed(Mono<T> mono, MeterRegistry registry, String name, String... tags) {
        return Mono.defer(() -> {
            var timing = new Run(registry, name, tags);
            return mono.doOnSuccess(value -> timing.end("success"))
                    .doOnError(error -> timing.end("error"))
                    .doOnCancel(() -> timing.end("cancel"));
        });
    }

    public static <T> Flux<T> timed(Flux<T> flux, MeterRegistry registry, String name, String... tags) {
        return Flux.defer(() -> {
            var timing = new Run(registry, name, tags);
            return flux.doOnComplete(() -> timing.end("success"))
                    .doOnError(error -> timing.end("error"))
                    .doOnCancel(() -> timing.end("cancel"));
        });
    }

    /** One timed subscription, recorded once: a cancel after success isn't a second run. */
    private static final class Run {
        private final MeterRegistry registry;
        private final String name;
        private final Tags tags;
        private final Timer.Sample sample;
        private final AtomicBoolean ended = new AtomicBoolean();

        Run(MeterRegistry registry, String name, String[] tags) {
            this.registry = registry;
            this.name = name;
            this.tags = Tags.of(tags);
            this.sample = Timer.start(registry);
        }

        void end(String outcome) {
            if (ended.compareAndSet(false, true)) sample.stop(registry.timer(name, tags.and("outcome", outcome)));
        }
    }
}
