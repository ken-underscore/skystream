package com.example.skystream.mcp;

import java.util.List;
import java.util.Map;

import org.springframework.ai.mcp.server.webflux.transport.WebFluxStatelessServerTransport;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.reactive.function.server.RouterFunction;

import com.example.skystream.Timing;

import io.micrometer.core.instrument.MeterRegistry;
import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpStatelessAsyncServer;
import io.modelcontextprotocol.server.McpStatelessServerFeatures.AsyncToolSpecification;
import io.modelcontextprotocol.spec.McpSchema.ServerCapabilities;
import reactor.core.publisher.Mono;

/**
 * Agents call Skystream's tools at {@code /mcp}, over MCP's Streamable HTTP transport. Stateless: each call stands alone,
 * so there are no sessions to keep. Each tool is an {@link AsyncToolSpecification} bean.
 * <p>
 * The transport's Origin check is left off: the tools are public and read-only, and need no login or cookie a page in a
 * visitor's browser could borrow.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnBooleanProperty(name = "skystream.mcp.enabled", matchIfMissing = true)
public class McpConfiguration {
    /** The key of the {@link ServerHttpRequest} a call came in, in each call's context, for telling callers apart. */
    public static final String HTTP_REQUEST = "httpRequest";

    @Bean
    WebFluxStatelessServerTransport mcpTransport() {
        return WebFluxStatelessServerTransport.builder()
                .messageEndpoint("/mcp")
                .contextExtractor(request -> McpTransportContext.create(
                        Map.of(HTTP_REQUEST, request.exchange().getRequest())))
                .build();
    }

    @Bean
    RouterFunction<?> mcpRoutes(WebFluxStatelessServerTransport transport) {
        return transport.getRouterFunction();
    }

    @Bean
    McpStatelessAsyncServer mcpServer(WebFluxStatelessServerTransport transport, List<AsyncToolSpecification> tools,
            MeterRegistry registry) {
        return McpServer.async(transport)
                // The version the MCP Registry lists, from server.json: change both together.
                .serverInfo("skystream", "1.0.0")
                .capabilities(ServerCapabilities.builder().tools(false).build())
                .tools(tools.stream().map(tool -> timed(tool, registry)).toList())
                .build();
    }

    /**
     * Times each call that reaches the tool, tagged with the tool's name and an {@code outcome} of {@code success} or
     * {@code error}. Calls the input schema refuses don't reach it, and a limited call is an error.
     */
    static AsyncToolSpecification timed(AsyncToolSpecification tool, MeterRegistry registry) {
        return new AsyncToolSpecification(tool.tool(), (context, request) -> Timing.timed(
                Mono.defer(() -> tool.callHandler().apply(context, request)),
                result -> Boolean.TRUE.equals(result.isError()) ? "error" : "success",
                registry, "skystream.mcp.call", "tool", tool.tool().name()));
    }
}
