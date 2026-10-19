package dto;

import java.util.Arrays;
import java.util.Locale;

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
        JsonNode reply
) {
    /** Whether the post declares an English language tag, such as "en" or "en-US". */
    public boolean isEnglish() {
        return langs != null && Arrays.stream(langs).anyMatch(lang -> lang != null && lang.toLowerCase(Locale.ROOT).startsWith("en"));
    }
}
