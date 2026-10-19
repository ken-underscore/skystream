package com.example.skystream.search;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class SearchConfigurationTests {
    @Test
    void refusesAMissingKeyOrOneWithStrayCharactersWithoutEchoingIt() {
        var configuration = new SearchConfiguration();
        for (String key : new String[]{"", "  ", "sk-ant-sec\rret", "sk-ant secret"}) {
            var error = assertThrows(IllegalStateException.class, () -> configuration.anthropic(key));
            assertFalse(error.getMessage().contains("secret"));
        }
        // Surrounding whitespace, such as the \r a CRLF .env leaves, is ignored.
        configuration.anthropic(" sk-ant-test\r\n").close();
    }
}
