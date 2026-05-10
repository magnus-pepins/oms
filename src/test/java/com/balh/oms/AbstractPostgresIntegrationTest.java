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
     * Clears the order graph and slice-6 settlement tables in one statement.
     *
     * <p>{@code TRUNCATE orders CASCADE} alone does not remove {@code positions} (no FK from
     * positions to orders), which can leak state across integration tests that reuse the same
     * container and default custody account.
     */
    public static final String SQL_TRUNCATE_ORDERS_AND_SETTLEMENT =
            "TRUNCATE TABLE corporate_action_event, manual_settlement_actions, ledger_settlement_outbox, broker_settlement_confirm, settlement_file_import_batch, position_history, positions, orders CASCADE";

    @Container
    protected static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("oms")
                    .withUsername("oms")
                    .withPassword("oms");

    @DynamicPropertySource
    static void registerPgProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }
}
