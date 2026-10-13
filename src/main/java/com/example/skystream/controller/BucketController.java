package com.example.skystream.controller;

import static com.example.skystream.repository.WordBucketRepository.SNAPSHOT_WINDOW_SECONDS;

import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import com.example.skystream.service.WordSnapshotService;

import reactor.core.publisher.Flux;

@RestController 
public class BucketController {
    private static final List<Integer> REFRESH_INTERVAL_MS = List.of(0, 5000, 10000, 30000, 60000);
    private final WordSnapshotService snapshots;
    public BucketController(WordSnapshotService snapshots) {
        this.snapshots = snapshots;
    }
    @GetMapping(
        value = "/api/word-buckets",
        produces = MediaType.TEXT_EVENT_STREAM_VALUE
    )
    public Flux<Map<String, Long>> wordBuckets(@RequestParam(defaultValue = "30") int windowSeconds,
            @RequestParam(defaultValue = "0") int refreshIntervalMs) {
        if (!SNAPSHOT_WINDOW_SECONDS.contains(windowSeconds)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "windowSeconds must be one of " + SNAPSHOT_WINDOW_SECONDS);
        }
        if (!REFRESH_INTERVAL_MS.contains(refreshIntervalMs)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "refreshIntervalMs must be one of " + REFRESH_INTERVAL_MS);
        }
        return snapshots.wordBuckets(windowSeconds, refreshIntervalMs);
    }
}
