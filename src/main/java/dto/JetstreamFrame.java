package dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record JetstreamFrame(
        @JsonProperty("$type") String type,
        JetstreamCommit payload,

        // Present on terminal error frames instead of payload
        String error,
        String message
) {}
