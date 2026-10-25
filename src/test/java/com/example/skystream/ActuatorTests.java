package com.example.skystream;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/** Metrics are served on the management port only, and nothing but health and metrics is exposed there. */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"spring.docker.compose.enabled=false", "skystream.jetstream.enabled=false",
                // Tests turn metrics export off by default.
                "management.defaults.metrics.export.enabled=true"})
class ActuatorTests {
    @Container
    static final PostgreSQLContainer postgres = new PostgreSQLContainer(
            DockerImageName.parse("pgvector/pgvector:pg17").asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry properties) {
        properties.add("spring.r2dbc.url", () -> "r2dbc:postgresql://" + postgres.getHost() + ":"
                + postgres.getMappedPort(5432) + "/" + postgres.getDatabaseName());
        properties.add("spring.r2dbc.username", postgres::getUsername);
        properties.add("spring.r2dbc.password", postgres::getPassword);
    }

    @LocalServerPort int port;
    @LocalManagementPort int managementPort;

    private WebTestClient client(int port) {
        return WebTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
    }

    @Test
    void servesPrometheusMetricsOnTheManagementPortOnly() {
        String metrics = client(managementPort).get().uri("/actuator/prometheus").exchange()
                .expectStatus().isOk()
                .expectBody(String.class).returnResult().getResponseBody();
        assertTrue(metrics.contains("jvm_memory_used_bytes"), "built-in JVM metrics");
        assertTrue(metrics.contains("skystream_ingestion_lag_seconds"), "the app's own metrics");
        client(managementPort).get().uri("/actuator/health").exchange().expectStatus().isOk();
        client(managementPort).get().uri("/actuator/env").exchange().expectStatus().isNotFound();
        client(port).get().uri("/actuator/prometheus").exchange().expectStatus().isNotFound();
    }
}
