package com.example.skystream.mcp;

import java.util.Map;

import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.ToolAnnotations;
import tools.jackson.databind.json.JsonMapper;

/** Tool results as MCP asks for them: structured content, and the same JSON as text for clients that read only text. */
public final class ToolResults {
    private static final JsonMapper JSON = JsonMapper.shared();

    /** Every Skystream tool only reads, and reads what the public posted: Bluesky is an open world. */
    public static final ToolAnnotations READ_ONLY = new ToolAnnotations(null, true, false, true, true, null);

    private ToolResults() {}

    public static CallToolResult of(Object value) {
        return CallToolResult.builder()
                .structuredContent(JSON.convertValue(value, Map.class))
                .addTextContent(JSON.writeValueAsString(value))
                .build();
    }

    /** A failure the calling model reads and can act on, rather than a protocol error. */
    public static CallToolResult error(String message) {
        return CallToolResult.builder().isError(true).addTextContent(message).build();
    }
}
