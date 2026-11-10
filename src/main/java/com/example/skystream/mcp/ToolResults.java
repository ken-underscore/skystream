package com.example.skystream.mcp;

import java.util.Map;

import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import tools.jackson.databind.json.JsonMapper;

/** Tool results as MCP asks for them: structured content, and the same JSON as text for clients that read only text. */
public final class ToolResults {
    private static final JsonMapper JSON = JsonMapper.shared();

    private ToolResults() {}

    public static CallToolResult of(Object value) {
        String text = JSON.writeValueAsString(value);
        return CallToolResult.builder().structuredContent(JSON.readValue(text, Map.class)).addTextContent(text).build();
    }

    /** A failure the calling model reads and can act on, rather than a protocol error. */
    public static CallToolResult error(String message) {
        return CallToolResult.builder().isError(true).addTextContent(message).build();
    }
}
