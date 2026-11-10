package com.example.skystream.mcp;

import java.util.List;

import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;

import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpStatelessAsyncServer;
import io.modelcontextprotocol.server.McpStatelessServerFeatures.AsyncToolSpecification;
import io.modelcontextprotocol.spec.McpSchema.ServerCapabilities;
import org.springframework.ai.mcp.server.webflux.transport.WebFluxStatelessServerTransport;

/**
 * Agents call Skystream's tools at {@code /mcp}, over MCP's Streamable HTTP transport. Stateless: each call stands alone,
 * so there are no sessions to keep. Each tool is an {@link AsyncToolSpecification} bean.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnBooleanProperty(name = "skystream.mcp.enabled", matchIfMissing = true)
public class McpConfiguration {
    @Bean
    WebFluxStatelessServerTransport mcpTransport() {
        return WebFluxStatelessServerTransport.builder().messageEndpoint("/mcp").build();
    }

    @Bean
    RouterFunction<?> mcpRoutes(WebFluxStatelessServerTransport transport) {
        return transport.getRouterFunction();
    }

    @Bean
    McpStatelessAsyncServer mcpServer(WebFluxStatelessServerTransport transport, List<AsyncToolSpecification> tools) {
        return McpServer.async(transport)
                .serverInfo("skystream", "1.0.0")
                .capabilities(ServerCapabilities.builder().tools(false).build())
                .tools(tools)
                .build();
    }
}
