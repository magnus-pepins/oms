package com.balh.oms;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Base class for integration tests that need a real Postgres instance.
 *
 * <p>Uses Testcontainers to start an <strong>ephemeral</strong> Postgres — separate
 * from any {@code docker compose up postgres} you run for local app development.
 * If Docker is not available, tests extending this class are <strong>skipped</strong>
 * ({@code disabledWithoutDocker = true}).
 *
 * <p>To run the OMS against your Compose Postgres instead, use {@code ./gradlew bootRun}
 * with {@code OMS_PG_URL} / {@code OMS_PG_USER} / {@code OMS_PG_PASSWORD}; see README.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
public abstract class AbstractPostgresIntegrationTest {

    /**
     * Hikari waits for a TCP connection from the Testcontainers-mapped port; GitHub-hosted runners
     * occasionally expose the mapped port before Postgres accepts connections.
     */
    private static final String HIKARI_CONNECTION_TIMEOUT_MS = "60000";

    /**
     * Clears the order graph and slice-6 settlement tables in one statement.
     *
     * <p>{@code TRUNCATE orders CASCADE} alone does not remove {@code positions} (no FK from
     * positions to orders), which can leak state across integration tests that reuse the same
     * container and default custody account.
     */
    public static final String SQL_TRUNCATE_ORDERS_AND_SETTLEMENT =
            "TRUNCATE TABLE corporate_action_event, manual_settlement_actions, ledger_settlement_outbox, broker_settlement_confirm, settlement_file_import_batch, position_history, positions, orders CASCADE";

    /** Docker / runner flakes: retry container start before failing the JVM test run. */
    private static final int POSTGRES_CONTAINER_STARTUP_ATTEMPTS = 3;

    /**
     * Default Postgres {@code max_connections} is 100. Many cached {@code @SpringBootTest} contexts
     * each open a Hikari pool against this single shared container; without a higher cap, CI hits
     * {@code FATAL: sorry, too many clients already} and HTTP ITs see uncaught JDBC failures as 500.
     */
    private static final int POSTGRES_MAX_CONNECTIONS = 256;

    @Container
    protected static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("oms")
                    .withUsername("oms")
                    .withPassword("oms")
                    .withCommand("postgres", "-c", "max_connections=" + POSTGRES_MAX_CONNECTIONS)
                    .withStartupAttempts(POSTGRES_CONTAINER_STARTUP_ATTEMPTS);

    @DynamicPropertySource
    static void registerPgProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.datasource.hikari.connection-timeout", () -> HIKARI_CONNECTION_TIMEOUT_MS);
    }
}
