package com.example.skystream.search;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;

import dto.JetstreamCommit;

/** A captured top-level post. {@code seenAt} is Jetstream's time for the event, not the author's createdAt. */
public record SearchPost(String uri, String text, Instant seenAt) {
    // Unicode-aware, as the evaluation's Python was: a non-breaking space ends a link and separates words.
    private static final Pattern URLS = Pattern.compile("https?://\\S+", Pattern.UNICODE_CHARACTER_CLASS);
    private static final Pattern WORDS = Pattern.compile("\\S+", Pattern.UNICODE_CHARACTER_CLASS);
    private static final int MIN_WORDS = 5;

    /**
     * The searchable post in a commit seen at {@code seenAt}: an English top-level create with at least five words
     * once links are removed. Its text has links removed and whitespace collapsed, as the search evaluation embedded it.
     */
    public static Optional<SearchPost> from(JetstreamCommit commit, Instant seenAt) {
        var record = commit.record();
        if (!"create".equals(commit.operation()) || record == null || record.text() == null || record.reply() != null
                || !record.isEnglish()) {
            return Optional.empty();
        }
        List<String> words = WORDS.matcher(URLS.matcher(record.text()).replaceAll("")).results()
                .map(MatchResult::group).toList();
        return words.size() < MIN_WORDS ? Optional.empty()
                : Optional.of(new SearchPost(uri(commit), String.join(" ", words), seenAt));
    }

    /** The post's page on bsky.app, from its AT URI, {@code at://<did>/app.bsky.feed.post/<rkey>}. */
    public String blueskyUrl() {
        String[] parts = uri.substring("at://".length()).split("/");
        return "https://bsky.app/profile/" + parts[0] + "/post/" + parts[2];
    }

    /** The post's AT URI, which identifies it across creates and deletes. */
    public static String uri(JetstreamCommit commit) {
        return "at://" + commit.did() + "/" + commit.collection() + "/" + commit.rkey();
    }
}
