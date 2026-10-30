package dto;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import tools.jackson.databind.ObjectMapper;

class PostRecordTests {
    private final ObjectMapper mapper = new ObjectMapper();

    private PostRecord withLabels(String labels) {
        return mapper.readValue("{\"text\": \"a post\", \"labels\": " + labels + "}", PostRecord.class);
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "{\"$type\": \"com.atproto.label.defs#selfLabels\", \"values\": [{\"val\": \"porn\"}]}             | true",
            "{\"values\": [{\"val\": \"spoiler\"}, {\"val\": \"nudity\"}]}                                    | true",
            "{\"values\": [{\"val\": \"graphic-media\"}]}                                                     | true",
            "{\"values\": [{\"val\": \"spoiler\"}]}                                                           | false",
            "{\"values\": []}                                                                                 | false",
            // Read leniently: an object of labels counts as well as an array.
            "{\"values\": {\"a\": {\"val\": \"porn\"}}}                                                         | true",
            "null                                                                                             | false",
            // Malformed labels decode, and don't count.
            "\"porn\"                                                                                         | false",
            "{\"values\": \"porn\"}                                                                           | false",
            "{\"values\": [\"porn\", {\"val\": 1}, {\"val\": [\"porn\"]}, null]}                              | false",
            "{\"values\": [{\"val\": null}, {}]}                                                                | false",
    })
    void recognizesAdultSelfLabelsAndToleratesMalformedOnes(String labels, boolean adult) {
        assertEquals(adult, withLabels(labels).isLabeledAdult());
    }

    @Test
    void aPostWithoutLabelsIsNotAdult() {
        assertFalse(mapper.readValue("{\"text\": \"a post\"}", PostRecord.class).isLabeledAdult());
    }
}
