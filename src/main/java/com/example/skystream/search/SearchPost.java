package com.example.skystream.search;

import java.time.Instant;
import java.util.Optional;
import java.util.regex.Pattern;

import dto.JetstreamCommit;

/** A captured top-level post. {@code seenAt} is Jetstream's time for the event, not the author's createdAt. */
public record SearchPost(String uri, String text, Instant seenAt) {
    private static final Pattern URLS = Pattern.compile("https?://\\S+");
    private static final Pattern WHITESPACE = Pattern.compile("\\s+", Pattern.UNICODE_CHARACTER_CLASS);
    private static final int MIN_WORDS = 5;

    /**
     * The searchable post in a commit: an English top-level create with at least five words once links are removed.
     * Its text has links removed and whitespace collapsed, as the search evaluation embedded it.
     */
    public static Optional<SearchPost> from(JetstreamCommit commit) {
        var record = commit.record();
        if (!"create".equals(commit.operation()) || record == null || record.text() == null || record.reply() != null
                || !record.isEnglish()) {
            return Optional.empty();
        }
        String[] words = WHITESPACE.split(URLS.matcher(record.text()).replaceAll("").strip());
        return words.length < MIN_WORDS ? Optional.empty()
                : Optional.of(new SearchPost(uri(commit), String.join(" ", words), Instant.parse(commit.witnessedAt())));
    }

    /** The post's AT URI, which identifies it across creates and deletes. */
    public static String uri(JetstreamCommit commit) {
        return "at://" + commit.did() + "/" + commit.collection() + "/" + commit.rkey();
    }
}
