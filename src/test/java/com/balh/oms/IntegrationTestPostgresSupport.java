package com.balh.oms;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Assumptions;
import org.testcontainers.DockerClientFactory;

/**
 * Shared Postgres + Flyway setup for tests that do not boot Spring (so they do not get
 * {@code spring.flyway} from {@link AbstractPostgresIntegrationTest} contexts).
 *
 * <p>Flyway runs at most once per test JVM against the shared integration database
 * ({@link AbstractPostgresIntegrationTest#integrationTestJdbcUrl()}), matching Spring IT
 * behaviour and avoiding repeated full migration passes when multiple postgres-only classes
 * load in the same Gradle worker.
 */
public final class IntegrationTestPostgresSupport {

    private static final AtomicBoolean FLYWAY_MIGRATED = new AtomicBoolean(false);

    private IntegrationTestPostgresSupport() {}

    /**
     * Skips the test when Testcontainers cannot reach Docker and {@code OMS_CI_JDBC_URL} is unset.
     * Prefer {@code OMS_CI_JDBC_URL=jdbc:postgresql://127.0.0.1:5440/oms} locally (compose) to
     * skip container startup entirely — see {@code oms/docs/runbooks/local-multi-jvm-bench.md}.
     */
    public static void assumeIntegrationPostgresAvailable() {
        if (System.getenv(AbstractPostgresIntegrationTest.ENV_OMS_CI_JDBC_URL) != null
                && !System.getenv(AbstractPostgresIntegrationTest.ENV_OMS_CI_JDBC_URL).isBlank()) {
            return;
        }
        Assumptions.assumeTrue(
                DockerClientFactory.instance().isDockerAvailable(),
                "Docker is not available and OMS_CI_JDBC_URL is unset — skipping postgres integration test");
    }

    /** Applies classpath migrations once per JVM (no-op on subsequent calls). */
    public static void ensureFlywayMigratedOnce() {
        assumeIntegrationPostgresAvailable();
        if (!FLYWAY_MIGRATED.compareAndSet(false, true)) {
            return;
        }
        Flyway.configure()
                .dataSource(
                        AbstractPostgresIntegrationTest.integrationTestJdbcUrl(),
                        AbstractPostgresIntegrationTest.integrationTestJdbcUser(),
                        AbstractPostgresIntegrationTest.integrationTestJdbcPassword())
                .locations("classpath:db/migration")
                .configuration(Map.of("flyway.postgresql.transactional.lock", "false"))
                .load()
                .migrate();
    }

    public static HikariDataSource newDataSource(String poolName) {
        assumeIntegrationPostgresAvailable();
        ensureFlywayMigratedOnce();
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(AbstractPostgresIntegrationTest.integrationTestJdbcUrl());
        hc.setUsername(AbstractPostgresIntegrationTest.integrationTestJdbcUser());
        hc.setPassword(AbstractPostgresIntegrationTest.integrationTestJdbcPassword());
        hc.setMaximumPoolSize(2);
        hc.setPoolName(poolName);
        return new HikariDataSource(hc);
    }
}
