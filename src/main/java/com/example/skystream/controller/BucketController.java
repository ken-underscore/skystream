package com.example.skystream.controller;

import java.util.Map;
import java.util.Set;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import com.example.skystream.service.JetstreamConsumer;

import reactor.core.publisher.Flux;

@RestController 
public class BucketController {
    private static final Set<Integer> WINDOW_SECONDS = Set.of(30, 60, 300, 600, 1800, 3600);
    private final JetstreamConsumer jetstreamConsumer;
    public BucketController(JetstreamConsumer jetstreamConsumer) {
        this.jetstreamConsumer = jetstreamConsumer;
    }
    @GetMapping(
        value = "/api/word-buckets",
        produces = MediaType.TEXT_EVENT_STREAM_VALUE
    )
    public Flux<Map<String, Long>> wordBuckets(@RequestParam(defaultValue = "30") int windowSeconds) {
        if (!WINDOW_SECONDS.contains(windowSeconds)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "windowSeconds must be 30, 60, 300, 600, 1800, or 3600");
        }
        return jetstreamConsumer.wordBuckets(windowSeconds);
    }
}
