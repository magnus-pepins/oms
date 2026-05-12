package com.balh.oms;

import io.aeron.archive.Archive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.agrona.IoUtil;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * Base class for integration tests that need a real Postgres instance and (when applicable) a
 * running Aeron Cluster.
 *
 * <h2>Postgres</h2>
 *
 * Two paths, mutually exclusive:
 *
 * <ul>
 *   <li><strong>Externally-managed Postgres</strong> (preferred when available): set
 *       {@code OMS_CI_JDBC_URL} (and optionally {@code OMS_CI_JDBC_USER} /
 *       {@code OMS_CI_JDBC_PASSWORD}). Used by GitHub Actions {@code services: postgres}, and
 *       equivalently by a developer running OMS Postgres via {@code docker compose} (see
 *       {@code oms/docker-compose.yml}). No Testcontainers / Docker probe is performed.</li>
 *   <li><strong>Testcontainers Postgres</strong> (default for IDE / dev): when
 *       {@code OMS_CI_JDBC_URL} is unset, a singleton {@link PostgreSQLContainer} is started on
 *       demand. Tests are skipped if Docker is not reachable from the JVM.</li>
 * </ul>
 *
 * <h2>Aeron Cluster (Phase 1c slice A)</h2>
 *
 * <p>Phase 1c of {@code system-documentation/plans/oms-aeron-cluster-substrate.md} makes the
 * cluster the only admission path. Slice 1c-A (this class) provides the <strong>infrastructure
 * only</strong>: a JVM-wide singleton in-process Aeron Cluster, started <em>lazily</em> the
 * first time {@link #testClusterAeronDirectory()} is read by an opt-in test, and closed via a
 * JVM shutdown hook. Tests that want the cluster path register their own
 * {@link DynamicPropertySource} pointing at the singleton:
 *
 * <pre>{@code
 * @DynamicPropertySource
 * static void clusterProps(DynamicPropertyRegistry registry) {
 *     registry.add("oms.cluster.client.enabled", () -> "true");
 *     registry.add("oms.cluster.client.aeron-directory",
 *             AbstractPostgresIntegrationTest::testClusterAeronDirectory);
 *     registry.add("oms.cluster.client.ingress-endpoints",
 *             AbstractPostgresIntegrationTest::testClusterIngressEndpoints);
 * }
 * }</pre>
 *
 * <p>Slice 1c-B will move that opt-in into this base class once {@code OrderIngressService} is
 * cluster-only and the matching tests have been adapted; this slice keeps the test suite's
 * existing chronicle-based tests untouched.
 *
 * <p>Cluster shutdown is handled by a JVM shutdown hook so test classes can come and go without
 * tearing the cluster down. Spring closes per-context {@code AeronCluster} clients via
 * {@code @PreDestroy} <em>before</em> the JVM hook fires the cluster node shutdown, so client
 * close always sees a live driver (no {@code DriverTimeoutException} on graceful shutdown).
 *
 * <p>Tests that need their own cluster (cluster-internal smoke tests, or this same class boot
 * coverage like {@code OmsClusterNodeBootstrapSmokeIT}) keep their own boot logic on different
 * ports — see individual classes.
 *
 * <p>To run the OMS app itself against compose Postgres, use {@code ./gradlew bootRun} with
 * {@code OMS_PG_URL} / {@code OMS_PG_USER} / {@code OMS_PG_PASSWORD}; see README.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
public abstract class AbstractPostgresIntegrationTest {

    /** Env var: JDBC URL of an externally-managed Postgres (CI service or local docker compose). */
    static final String ENV_OMS_CI_JDBC_URL = "OMS_CI_JDBC_URL";

    private static final String ENV_OMS_CI_JDBC_USER = "OMS_CI_JDBC_USER";
    private static final String ENV_OMS_CI_JDBC_PASSWORD = "OMS_CI_JDBC_PASSWORD";

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
            "TRUNCATE TABLE corporate_action_event, manual_settlement_actions, ledger_settlement_outbox, broker_settlement_confirm, settlement_file_import_batch, position_history, positions, orders, fx_stub_leg_group CASCADE";

    /** Docker / runner flakes: retry container start before failing the JVM test run (Testcontainers path only). */
    private static final int POSTGRES_CONTAINER_STARTUP_ATTEMPTS = 3;

    /**
     * Singleton Testcontainers Postgres for the externally-unmanaged path. Started lazily on first
     * access; Testcontainers' Ryuk reaper handles cleanup at JVM exit.
     *
     * <p>{@code null} when {@link #useCiManagedPostgres()} is true.
     */
    private static volatile PostgreSQLContainer<?> testcontainersPostgres;

    @DynamicPropertySource
    static void registerPgProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> integrationTestJdbcUrl());
        registry.add("spring.datasource.username", () -> integrationTestJdbcUser());
        registry.add("spring.datasource.password", () -> integrationTestJdbcPassword());
        registry.add("spring.datasource.hikari.connection-timeout", () -> HIKARI_CONNECTION_TIMEOUT_MS);
    }

    /**
     * Lazily starts (if needed) the JVM-wide {@link TestAeronClusterSingleton} and returns its
     * {@code aeron} working directory. Use from a test's
     * {@link DynamicPropertySource @DynamicPropertySource} as the value for
     * {@code oms.cluster.client.aeron-directory}.
     */
    public static String testClusterAeronDirectory() {
        return TestAeronClusterSingleton.startedInstance().aeronDirectory();
    }

    /**
     * Ingress endpoints string for the JVM-wide {@link TestAeronClusterSingleton}. Use from a
     * test's {@link DynamicPropertySource @DynamicPropertySource} as the value for
     * {@code oms.cluster.client.ingress-endpoints}. Pure constant; does <em>not</em> start the
     * cluster.
     */
    public static String testClusterIngressEndpoints() {
        return TestAeronClusterSingleton.INGRESS_ENDPOINTS;
    }

    /** JDBC URL for the shared integration Postgres (env-driven or Testcontainers-driven). */
    public static String integrationTestJdbcUrl() {
        if (useCiManagedPostgres()) {
            return requireEnv(ENV_OMS_CI_JDBC_URL);
        }
        return testcontainersPostgres().getJdbcUrl();
    }

    /** Username for {@link #integrationTestJdbcUrl()}. */
    public static String integrationTestJdbcUser() {
        if (useCiManagedPostgres()) {
            return firstNonBlank(System.getenv(ENV_OMS_CI_JDBC_USER), "oms");
        }
        return testcontainersPostgres().getUsername();
    }

    /** Password for {@link #integrationTestJdbcUrl()}. */
    public static String integrationTestJdbcPassword() {
        if (useCiManagedPostgres()) {
            return firstNonBlank(System.getenv(ENV_OMS_CI_JDBC_PASSWORD), "oms");
        }
        return testcontainersPostgres().getPassword();
    }

    private static boolean useCiManagedPostgres() {
        String url = System.getenv(ENV_OMS_CI_JDBC_URL);
        return url != null && !url.isBlank();
    }

    @SuppressWarnings("resource")
    private static PostgreSQLContainer<?> testcontainersPostgres() {
        PostgreSQLContainer<?> local = testcontainersPostgres;
        if (local != null) {
            return local;
        }
        synchronized (AbstractPostgresIntegrationTest.class) {
            if (testcontainersPostgres == null) {
                PostgreSQLContainer<?> pg = new PostgreSQLContainer<>("postgres:16-alpine")
                        .withDatabaseName("oms")
                        .withUsername("oms")
                        .withPassword("oms")
                        .withStartupAttempts(POSTGRES_CONTAINER_STARTUP_ATTEMPTS);
                pg.start();
                testcontainersPostgres = pg;
            }
            return testcontainersPostgres;
        }
    }

    private static String requireEnv(String name) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException("Missing required environment variable: " + name);
        }
        return v;
    }

    private static String firstNonBlank(String value, String fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return value;
    }

    /**
     * JVM-wide singleton in-process Aeron Cluster used by tests that opt into the cluster path
     * via {@link #testClusterAeronDirectory()} / {@link #testClusterIngressEndpoints()}. Started
     * lazily on first opt-in, closed by a JVM shutdown hook. Ports are dedicated (20510-20550)
     * so they don't collide with the cluster ports used by
     * {@link com.balh.oms.cluster.OmsClusterIngressClientIT} (defaults around 20110-20440)
     * or {@link OmsClusterNodeBootstrapSmokeIT} when those classes run in the same Gradle test
     * JVM.
     */
    private static final class TestAeronClusterSingleton {

        private static final int INGRESS_PORT = 20510;
        private static final int CONSENSUS_PORT = 20520;
        private static final int LOG_PORT = 20530;
        private static final int CATCHUP_PORT = 20540;
        private static final int ARCHIVE_PORT = 20550;

        private static final String CLUSTER_MEMBERS_SINGLE_NODE = "0,localhost:" + INGRESS_PORT
                + ",localhost:" + CONSENSUS_PORT
                + ",localhost:" + LOG_PORT
                + ",localhost:" + CATCHUP_PORT
                + ",localhost:" + ARCHIVE_PORT;

        static final String INGRESS_ENDPOINTS = "0=localhost:" + INGRESS_PORT;

        private static final String ARCHIVE_CONTROL_CHANNEL =
                "aeron:udp?endpoint=localhost:" + ARCHIVE_PORT;

        /**
         * Session timeout we set on the test cluster's {@link ConsensusModule.Context}. Aeron's
         * default is 5s, which is too tight for the test JVM where many Spring contexts come and
         * go: a context that loads {@code OmsClusterIngressClient} but doesn't issue a submit for
         * &gt;5s would otherwise lose the session and the next submit fails as
         * {@code cluster_unavailable}. The production
         * {@link com.balh.oms.cluster.OmsClusterIngressClient} heartbeat thread keeps real
         * sessions warm; the larger test-only timeout is defense in depth so any transient
         * stutter in the heartbeat loop (e.g. context restart, GC pause) doesn't tank the suite.
         */
        private static final long SESSION_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(60L);

        private static volatile TestAeronClusterSingleton INSTANCE;
        private static final Object LOCK = new Object();

        private final OmsClusterNodeBootstrap.ClusterNodePaths paths;
        private final ClusteredMediaDriver clusteredMediaDriver;
        private final ClusteredServiceContainer container;

        private TestAeronClusterSingleton(
                OmsClusterNodeBootstrap.ClusterNodePaths paths,
                ClusteredMediaDriver clusteredMediaDriver,
                ClusteredServiceContainer container) {
            this.paths = paths;
            this.clusteredMediaDriver = clusteredMediaDriver;
            this.container = container;
        }

        static TestAeronClusterSingleton startedInstance() {
            TestAeronClusterSingleton local = INSTANCE;
            if (local != null) {
                return local;
            }
            synchronized (LOCK) {
                if (INSTANCE != null) {
                    return INSTANCE;
                }
                INSTANCE = launch();
                Runtime.getRuntime().addShutdownHook(new Thread(
                        TestAeronClusterSingleton::closeInstance, "oms-test-cluster-shutdown"));
                return INSTANCE;
            }
        }

        private static TestAeronClusterSingleton launch() {
            Path baseDir;
            try {
                baseDir = Files.createTempDirectory("oms-test-cluster-");
            } catch (IOException e) {
                throw new UncheckedIOException("could not allocate Aeron Cluster work dir", e);
            }
            OmsClusterNodeBootstrap.ClusterNodePaths paths = new OmsClusterNodeBootstrap.ClusterNodePaths(
                    baseDir.toString(),
                    baseDir.resolve("media-driver").toString(),
                    baseDir.resolve("archive").toString(),
                    baseDir.resolve("consensus-module").toString(),
                    baseDir.resolve("cluster-services").toString());
            for (String dir : new String[] {
                paths.aeronDirBase(), paths.archiveDir(), paths.clusterDir(), paths.clusterServicesDir()
            }) {
                File f = new File(dir);
                if (!f.exists() && !f.mkdirs()) {
                    throw new IllegalStateException("could not create test cluster dir: " + dir);
                }
            }
            IoUtil.delete(new File(paths.aeronDirectory()), /* ignoreFailures = */ true);

            Archive.Context archiveContext = OmsClusterNodeBootstrap.buildArchiveContext(paths)
                    .controlChannel(ARCHIVE_CONTROL_CHANNEL);

            ConsensusModule.Context consensusContext = OmsClusterNodeBootstrap.buildConsensusModuleContext(
                            paths, /* memberId = */ 0, CLUSTER_MEMBERS_SINGLE_NODE)
                    .sessionTimeoutNs(SESSION_TIMEOUT_NS);

            ClusteredMediaDriver driver = ClusteredMediaDriver.launch(
                    OmsClusterNodeBootstrap.buildMediaDriverContext(paths),
                    archiveContext,
                    consensusContext);
            ClusteredServiceContainer container =
                    ClusteredServiceContainer.launch(OmsClusterNodeBootstrap.buildServiceContainerContext(paths));

            return new TestAeronClusterSingleton(paths, driver, container);
        }

        private static void closeInstance() {
            TestAeronClusterSingleton local = INSTANCE;
            if (local == null) {
                return;
            }
            try {
                local.container.close();
            } catch (RuntimeException ignored) {
                // best-effort during JVM shutdown
            }
            try {
                local.clusteredMediaDriver.close();
            } catch (RuntimeException ignored) {
                // best-effort during JVM shutdown
            }
        }

        String aeronDirectory() {
            return paths.aeronDirectory();
        }
    }
}
