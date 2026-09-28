package dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record PostRecord(
        @JsonProperty("$type") String type,
        String text,
        String createdAt,
        String[] langs
) {}
