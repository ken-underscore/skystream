package com.example.skystream.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.json.JsonMapper;

/** server.json, which describes the server to the MCP Registry, must agree with the server. */
class ServerJsonTests {
    @Test
    void hasTheServersVersion() throws IOException {
        // Tests run from the project root, where server.json is.
        var server = JsonMapper.shared().readValue(Files.readString(Path.of("server.json")), Map.class);
        assertEquals(McpConfiguration.VERSION, server.get("version"));
    }
}
