package dto;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import tools.jackson.databind.JsonNode;

@JsonIgnoreProperties(ignoreUnknown = true)
public record PostRecord(
        @JsonProperty("$type") String type,
        String text,
        String createdAt,
        String[] langs,
        // Present only on replies.
        JsonNode reply,
        // The author's own labels, as com.atproto.label.defs#selfLabels. Read as a tree: a frame that fails to decode
        // stops ingestion, and a label's shape isn't worth that.
        JsonNode labels
) {
    // The labels Bluesky hides or puts behind a warning by default.
    private static final Set<String> ADULT_LABELS = Set.of("porn", "sexual", "nudity", "graphic-media");

    /** Whether the post declares an English language tag, such as "en" or "en-US". */
    public boolean isEnglish() {
        return langs != null && Arrays.stream(langs).anyMatch(lang -> lang != null && lang.toLowerCase(Locale.ROOT).startsWith("en"));
    }

    /** Whether the author labeled the post as adult content or graphic media. */
    public boolean isLabeledAdult() {
        return labels != null && labels.path("values").valueStream()
                .anyMatch(label -> ADULT_LABELS.contains(label.path("val").stringValue("")));
    }

    /** Whether createdAt is before {@code time}; false when createdAt is missing or not a valid timestamp. */
    public boolean createdBefore(Instant time) {
        if (createdAt == null) return false;
        try {
            return Instant.parse(createdAt).isBefore(time);
        } catch (DateTimeParseException e) {
            return false;
        }
    }
}
