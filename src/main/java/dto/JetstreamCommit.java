package dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record JetstreamCommit(
        @JsonProperty("$type") String type,
        long seq,
        String did,
        String time,
        String witnessedAt,
        String rev,
        String operation,
        String collection,
        String rkey,
        String cid,
        PostRecord record
) {}
