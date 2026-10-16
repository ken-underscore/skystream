package com.example.skystream.search;

import java.time.Instant;

/** A captured top-level post. {@code seenAt} is Jetstream's time for the event, not the author's createdAt. */
public record SearchPost(String uri, String text, Instant seenAt) {}
