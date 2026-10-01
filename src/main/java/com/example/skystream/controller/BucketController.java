package com.example.skystream.controller;

import java.util.Map;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.skystream.service.JetstreamConsumer;

import reactor.core.publisher.Flux;

@RestController 
public class BucketController {
    private final JetstreamConsumer jetstreamConsumer;
    public BucketController(JetstreamConsumer jetstreamConsumer) {
        this.jetstreamConsumer = jetstreamConsumer;
    }
    @GetMapping(
        value = "/api/word-buckets",
        produces = MediaType.TEXT_EVENT_STREAM_VALUE
    )
    public Flux<Map<String, Integer>> wordBuckets() {
        return jetstreamConsumer.wordBuckets();
    }
}
