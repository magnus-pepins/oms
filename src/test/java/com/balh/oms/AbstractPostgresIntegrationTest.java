package com.balh.oms;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.ExecutionAppliedEvent;
import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.cluster.OrderAdmittedEvent;
import com.balh.oms.cluster.VenueResolutionAppliedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.persistence.ExecutionsRepository;
import com.balh.oms.persistence.PositionsRepository;
import com.balh.oms.settlement.PredictionMarketLedgerOutboxRepository;
import com.balh.oms.settlement.PredictionMarketResolutionService;
import com.balh.oms.settlement.VenueContractResolutionRepository;
import com.balh.oms.events.DomainEventEnvelopeCodec;
import com.balh.oms.persistence.DomainEventOutboxRepository;
import com.balh.oms.persistence.LedgerInflightOutboxRepository;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.predictionmarket.PredictionMarketContractRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

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
 * <p><strong>macOS Docker Desktop note:</strong> on a Mac with Docker Desktop, Testcontainers
 * may fail with {@code "Could not find a valid Docker environment"} even when the daemon is
 * running and {@code docker ps} works in the shell. The cause is that Docker Desktop's
 * symlink at {@code /var/run/docker.sock} points to a CLI proxy
 * ({@code ~/.docker/run/docker.sock}) that responds with an empty {@code /info} body plus a
 * {@code com.docker.desktop.address} label naming the real daemon socket
 * ({@code ~/Library/Containers/com.docker.docker/Data/docker-cli.sock}). The Docker CLI
 * follows that redirect; the Testcontainers Java client (as of {@code 1.20.x}) does not.
 * Workaround: set {@code OMS_CI_JDBC_URL} to your local OMS Postgres (e.g.
 * {@code jdbc:postgresql://127.0.0.1:5440/oms} matching {@code oms/docker-compose.yml}). The
 * runbook {@code oms/docs/runbooks/local-multi-jvm-bench.md} captures the bring-up details.
 *
 * <h2>Aeron Cluster (Phase 1c slices A + B)</h2>
 *
 * <p>Phase 1c of {@code system-documentation/plans/oms-aeron-cluster-substrate.md} makes the
 * cluster the only admission path. Slice A provided the infrastructure (JVM-wide singleton
 * cluster, heartbeat, max-sessions). Slice B (this revision) moves the cluster opt-in
 * <em>into</em> this base class: every Spring context loaded via
 * {@link SpringBootTest @SpringBootTest} that activates
 * {@link com.balh.oms.config.OmsProfiles#ORDER_ACCEPT_PROFILE} now boots a connected
 * {@code OmsClusterIngressClient} pointing at the singleton. Control-worker / fix-worker
 * profiles deactivate that profile, so they get the cluster as harmless background overhead
 * without loading the client.
 *
 * <p>The singleton starts lazily on the first {@link DynamicPropertySource} resolution and
 * closes via a JVM shutdown hook. Spring closes per-context {@code AeronCluster} clients via
 * {@code @PreDestroy} <em>before</em> the JVM hook fires the cluster node shutdown, so client
 * close always sees a live driver (no {@code DriverTimeoutException} on graceful shutdown).
 *
 * <p><strong>Readiness gate.</strong> Phase 2.3's {@code OmsClusterReadinessFilter} blocks
 * cluster-mutating ingress POSTs until the {@code oms-cluster-ready} Aeron counter is READY.
 * A fresh test cluster has empty replay, which stays NOT_READY unless
 * {@code OMS_READINESS_ALLOW_EMPTY_REPLAY=true} (set in {@code build.gradle.kts}
 * {@code tasks.withType<Test>}). Without it, {@code OrdersControllerIntegrationTest} and any
 * other order-accept IT sees {@code 503 OMS_CLUSTER_NOT_READY} on every {@code POST /orders}.
 *
 * <p>Tests that need their own cluster (cluster-internal smoke tests, or this same class boot
 * coverage like {@code OmsClusterNodeBootstrapSmokeIT}) keep their own boot logic on different
 * ports — see individual classes.
 *
 * <h2>Postgres orders projector daemon (Phase 2 slice 2c)</h2>
 *
 * <p>Phase 2 slice 2c of the cluster substrate plan removes the {@code orders} INSERT from
 * {@link com.balh.oms.ingress.OrderIngressService} — the orders row now arrives via a separate
 * projector JVM ({@code oms-postgres-projector}). In tests we run a JVM-wide
 * {@link TestPostgresProjectorSingleton} that subscribes to the test cluster's events
 * recording and idempotently inserts orders rows into the test Postgres, so HTTP / cluster
 * integration tests still see the row materialise after a brief Awaitility wait. The daemon
 * uses a self-contained code path (no {@code aeron_projector_cursor} write) to avoid racing
 * the Spring-managed projector that runs under
 * {@link com.balh.oms.config.OmsProfiles#POSTGRES_PROJECTOR}.
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
    /**
     * Per-test truncate list. Phase 2 slice 2c (V25) dropped the FK constraints from the
     * control / outbox / domain / execution side-tables back to {@code orders(id)} because the
     * projector now writes those rows asynchronously, so {@code TRUNCATE orders CASCADE} no
     * longer reaches them. Enumerate the side-tables explicitly here so the per-{@code @BeforeEach}
     * clean stays comprehensive — otherwise control_decisions / domain_event_outbox / executions
     * leak across test classes inside the JVM-wide Spring context cache.
     */
    public static final String SQL_TRUNCATE_ORDERS_AND_SETTLEMENT =
            "TRUNCATE TABLE control_decisions, domain_event_outbox,"
                    + " ledger_inflight_outbox, executions, market_context,"
                    + " corporate_action_event, manual_settlement_actions, ledger_settlement_outbox,"
                    + " broker_settlement_confirm, settlement_file_import_batch, position_history,"
                    + " cash_reconciliation_report_row, cash_reconciliation_report,"
                    + " broker_cash_statement_movement, broker_cash_statement_batch,"
                    + " broker_settlement_fail_row, broker_settlement_fail_batch,"
                    + " broker_corporate_action_row, broker_corporate_action_batch,"
                    + " execution_settlement_lot, oms_account_tax_wrapper,"
                    + " corporate_action_entitlement, corporate_action_position_impact,"
                    + " corporate_action_cash_impact, corporate_action_ledger_outbox,"
                    + " corporate_action_record_date_snapshot, corporate_action_election,"
                    + " isk_tax_parameters,"
                    + " settlement_customer_notification_outbox,"
                    + " isk_valuation_snapshot, isk_tax_year_export,"
                    + " position_reconciliation_report_row, position_reconciliation_report,"
                    + " broker_position_snapshot_row, broker_position_snapshot_batch,"
                    + " reconciliation_breaks,"
                    + " broker_trade_confirm_fee, broker_trade_confirm, broker_confirm_batch,"
                    + " positions, orders, fx_stub_leg_group CASCADE";

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
     * Cluster opt-in moved here in Phase 1c slice B: every order-accept Spring context now boots
     * a connected {@code OmsClusterIngressClient} pointing at the JVM-wide singleton cluster.
     * Tests that don't activate {@link com.balh.oms.config.OmsProfiles#ORDER_ACCEPT_PROFILE}
     * still resolve these properties (so they're cheap), but the
     * {@code @ConditionalOnProperty} on {@code OmsClusterIngressClient} only loads the client
     * for order-accept contexts.
     *
     * <p>Phase 2 slice 2c also lazy-starts the {@link TestPostgresProjectorSingleton} alongside
     * the cluster wiring so the orders row appears in Postgres even though the ingress no longer
     * writes it. The daemon is a JVM-wide singleton and runs for the lifetime of the test JVM.
     */
    @DynamicPropertySource
    static void registerClusterClientProperties(DynamicPropertyRegistry registry) {
        registry.add("oms.cluster.client.enabled", () -> "true");
        registry.add("oms.cluster.client.aeron-directory", () -> testClusterAeronDirectory());
        registry.add("oms.cluster.client.ingress-endpoints", () -> testClusterIngressEndpoints());
        // Eagerly start the projector daemon so the first test that depends on the orders row
        // does not pay the projector connect / first-recording cost on its hot path.
        testPostgresProjectorDaemon();
    }

    /**
     * Lazily starts (if needed) the JVM-wide {@link TestAeronClusterSingleton} and returns its
     * {@code aeron} working directory.
     */
    public static String testClusterAeronDirectory() {
        return TestAeronClusterSingleton.startedInstance().aeronDirectory();
    }

    /** Ingress endpoints string for the JVM-wide {@link TestAeronClusterSingleton}. */
    public static String testClusterIngressEndpoints() {
        return TestAeronClusterSingleton.INGRESS_ENDPOINTS;
    }

    /**
     * Lazily starts the JVM-wide {@link TestPostgresProjectorSingleton} (Phase 2 slice 2c). The
     * daemon subscribes to the test cluster's events recording and idempotently writes orders
     * rows so tests that expect read-after-write semantics on the orders table still observe the
     * row (after a small Awaitility wait) even though
     * {@link com.balh.oms.ingress.OrderIngressService} no longer writes it. Idempotent on
     * {@code (account_id, client_idempotency_key)}; safe to start multiple times across a test
     * suite (singleton).
     *
     * <p>Returns the singleton (not {@code null}) so callers can observe lifecycle if needed.
     */
    public static TestPostgresProjectorSingleton testPostgresProjectorDaemon() {
        return TestPostgresProjectorSingleton.startedInstance();
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
     *
     * <p><strong>Stale-JVM recovery.</strong> The JVM shutdown hook does NOT run on
     * {@code SIGKILL} (e.g. {@code kill -9} on the gradle script, IDE force-quit, test
     * timeout). A killed Gradle test worker can therefore survive its parent and keep
     * holding the Aeron UDP sockets, causing the next run to fail at
     * {@link ClusteredMediaDriver#launch} with {@code Address already in use (localhost:20550)}.
     * Recover with {@code ./scripts/clean-stale-test-jvms.sh}, which finds and kills any
     * stale {@code GradleWorkerMain} JVMs and verifies the ports are free.
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
        private final OmsClusterNodeBootstrap.EventsRecordingHandle eventsRecording;

        private TestAeronClusterSingleton(
                OmsClusterNodeBootstrap.ClusterNodePaths paths,
                ClusteredMediaDriver clusteredMediaDriver,
                ClusteredServiceContainer container,
                OmsClusterNodeBootstrap.EventsRecordingHandle eventsRecording) {
            this.paths = paths;
            this.clusteredMediaDriver = clusteredMediaDriver;
            this.container = container;
            this.eventsRecording = eventsRecording;
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
            // Phase 2 slice 2b-1: register events recording before the service container brings the
            // publication up. See OmsClusterNodeBootstrap.startEventsRecording for rationale.
            OmsClusterNodeBootstrap.EventsRecordingHandle eventsRecording =
                    OmsClusterNodeBootstrap.startEventsRecording(paths);
            ClusteredServiceContainer container =
                    ClusteredServiceContainer.launch(OmsClusterNodeBootstrap.buildServiceContainerContext(paths));

            return new TestAeronClusterSingleton(paths, driver, container, eventsRecording);
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
                local.eventsRecording.close();
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

    /**
     * JVM-wide singleton projector daemon for Phase 2 slice 2c integration tests. Subscribes to
     * the test cluster's events recording (via {@link AeronArchive#replay}), decodes
     * {@link OrderAdmittedEvent} fragments, and idempotently inserts orders rows via
     * {@link OrdersRepository#insertFromAdmittedEvent} on a dedicated Hikari pool over the
     * shared test Postgres.
     *
     * <p>Why this is a separate codepath from the production
     * {@link com.balh.oms.projector.OmsPostgresProjector}:
     *
     * <ul>
     *   <li>The production projector is {@code @Profile(POSTGRES_PROJECTOR)} and topology-validated
     *       to be mutually exclusive with {@code ORDER_ACCEPT_PROFILE}. Order-accept contexts
     *       therefore never load it, but they still need orders rows to materialise.</li>
     *   <li>Multiple Spring contexts come and go during the test JVM lifetime; each one would
     *       construct its own projector if we relied on a Spring bean, which would race on the
     *       {@code aeron_projector_cursor} table.</li>
     *   <li>This daemon does NOT touch {@code aeron_projector_cursor} — it always replays from
     *       position 0. Tests truncate {@code orders} between cases (or assert per-id), and the
     *       projector's idempotent {@code INSERT ... ON CONFLICT DO NOTHING} keeps re-replays
     *       safe. The Spring-managed projector that runs under
     *       {@link com.balh.oms.projector.OmsPostgresProjectorIT} uses a different projector
     *       identity / cursor, so this daemon does not interfere with it.</li>
     * </ul>
     *
     * <p>Phase 4 Tier 2.5 phase D-9: the production projector is also the only writer of
     * the BUY-async {@code ledger_inflight_outbox} row (see
     * {@code OmsPostgresProjector#recordLedgerInflightOutboxIfNeeded}). The test daemon
     * mirrors that path here so {@code LedgerInflightOutboxIntegrationTest} sees the row
     * appear after the HTTP response without depending on the Spring-managed projector.
     *
     * <p>Phase 4 Tier 2.5 phase D-3: the production projector now also writes the
     * {@code OrderAccepted} envelope into {@code domain_event_outbox} on a fresh admission
     * (because ingress stopped doing it to drop the per-request Postgres tx). This daemon
     * mirrors that behaviour so integration tests like {@code NatsDomainFanoutIntegrationTest}
     * still observe an OrderAccepted row in {@code domain_event_outbox} after a successful
     * {@code POST /internal/v1/orders}.
     */
    public static final class TestPostgresProjectorSingleton {

        private static final long REPLAY_POLL_PARK_NANOS = 1_000_000L;
        private static final int REPLAY_FRAGMENT_LIMIT = 64;
        private static final long RECORDING_LOOKUP_PARK_MS = 50L;
        /** Stream id reserved for the test-only replay subscription so it never collides with the
         * production replay stream id (4321) used by {@code OmsPostgresProjector}. */
        private static final int REPLAY_STREAM_ID = 4322;

        private static volatile TestPostgresProjectorSingleton INSTANCE;
        private static final Object LOCK = new Object();

        private final HikariDataSource dataSource;
        private final OrdersRepository ordersRepository;
        private final DomainEventOutboxRepository domainEventOutboxRepository;
        private final DomainEventEnvelopeCodec domainEventEnvelopeCodec;
        private final LedgerInflightOutboxRepository ledgerInflightOutboxRepository;
        private final PredictionMarketResolutionService predictionMarketResolutionService;
        private final Aeron aeron;
        private final AeronArchive archive;
        private final Subscription replay;
        private final Thread replayThread;
        private final AtomicBoolean running;

        private TestPostgresProjectorSingleton(
                HikariDataSource dataSource,
                OrdersRepository ordersRepository,
                DomainEventOutboxRepository domainEventOutboxRepository,
                DomainEventEnvelopeCodec domainEventEnvelopeCodec,
                LedgerInflightOutboxRepository ledgerInflightOutboxRepository,
                PredictionMarketResolutionService predictionMarketResolutionService,
                Aeron aeron,
                AeronArchive archive,
                Subscription replay,
                Thread replayThread,
                AtomicBoolean running) {
            this.dataSource = dataSource;
            this.ordersRepository = ordersRepository;
            this.domainEventOutboxRepository = domainEventOutboxRepository;
            this.domainEventEnvelopeCodec = domainEventEnvelopeCodec;
            this.ledgerInflightOutboxRepository = ledgerInflightOutboxRepository;
            this.predictionMarketResolutionService = predictionMarketResolutionService;
            this.aeron = aeron;
            this.archive = archive;
            this.replay = replay;
            this.replayThread = replayThread;
            this.running = running;
        }

        static TestPostgresProjectorSingleton startedInstance() {
            TestPostgresProjectorSingleton local = INSTANCE;
            if (local != null) {
                return local;
            }
            synchronized (LOCK) {
                if (INSTANCE != null) {
                    return INSTANCE;
                }
                // Force the cluster to start first so the events recording exists by the time we
                // poll listRecordingsForUri.
                String aeronDir = testClusterAeronDirectory();
                INSTANCE = launch(aeronDir);
                Runtime.getRuntime().addShutdownHook(new Thread(
                        TestPostgresProjectorSingleton::closeInstance, "oms-test-projector-shutdown"));
                return INSTANCE;
            }
        }

        private static TestPostgresProjectorSingleton launch(String aeronDir) {
            HikariConfig hc = new HikariConfig();
            hc.setJdbcUrl(integrationTestJdbcUrl());
            hc.setUsername(integrationTestJdbcUser());
            hc.setPassword(integrationTestJdbcPassword());
            hc.setMaximumPoolSize(2);
            hc.setMinimumIdle(1);
            hc.setPoolName("oms-test-projector");
            hc.setConnectionTimeout(Long.parseLong(HIKARI_CONNECTION_TIMEOUT_MS));
            HikariDataSource ds = new HikariDataSource(hc);
            NamedParameterJdbcTemplate jdbc = new NamedParameterJdbcTemplate(ds);
            OrdersRepository orders = new OrdersRepository(jdbc);
            DomainEventOutboxRepository domainEventOutbox = new DomainEventOutboxRepository(jdbc);
            ObjectMapper domainEventObjectMapper = new ObjectMapper();
            domainEventObjectMapper.registerModule(new JavaTimeModule());
            DomainEventEnvelopeCodec domainEventCodec = new DomainEventEnvelopeCodec(domainEventObjectMapper);
            LedgerInflightOutboxRepository ledgerInflightOutbox = new LedgerInflightOutboxRepository(jdbc);
            ExecutionsRepository executionsRepository = new ExecutionsRepository(jdbc);
            PositionsRepository positionsRepository = new PositionsRepository(jdbc);
            VenueContractResolutionRepository venueContractResolutionRepository =
                    new VenueContractResolutionRepository(jdbc);
            PredictionMarketLedgerOutboxRepository predictionMarketLedgerOutboxRepository =
                    new PredictionMarketLedgerOutboxRepository(jdbc);
            OmsConfig omsConfig = new OmsConfig();
            TestPostgresProjectorTradeApplier tradeApplier =
                    new TestPostgresProjectorTradeApplier(
                            omsConfig, orders, executionsRepository, positionsRepository);
            ObjectMapper resolutionObjectMapper = new ObjectMapper();
            resolutionObjectMapper.registerModule(new JavaTimeModule());
            PredictionMarketContractRepository predictionMarketContractRepository =
                    new PredictionMarketContractRepository(jdbc);
            PredictionMarketResolutionService predictionMarketResolutionService =
                    new PredictionMarketResolutionService(
                            venueContractResolutionRepository,
                            predictionMarketContractRepository,
                            predictionMarketLedgerOutboxRepository,
                            positionsRepository,
                            domainEventOutbox,
                            jdbc,
                            omsConfig,
                            resolutionObjectMapper);
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
            AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
                    .aeron(aeron)
                    .ownsAeronClient(false)
                    .controlRequestChannel("aeron:ipc?term-length=64k")
                    .controlResponseChannel("aeron:ipc?term-length=64k"));

            long recordingId = waitForRecording(archive);
            Subscription replay = archive.replay(
                    recordingId,
                    /* position = */ 0L,
                    /* length = */ Long.MAX_VALUE,
                    "aeron:ipc?term-length=64k",
                    REPLAY_STREAM_ID);

            AtomicBoolean running = new AtomicBoolean(true);
            Thread thread = new Thread(
                    () -> replayLoop(
                            replay,
                            orders,
                            domainEventOutbox,
                            domainEventCodec,
                            predictionMarketResolutionService,
                            tradeApplier,
                            running),
                    "oms-test-projector-daemon");
            thread.setDaemon(true);
            TestPostgresProjectorSingleton instance = new TestPostgresProjectorSingleton(
                    ds,
                    orders,
                    domainEventOutbox,
                    domainEventCodec,
                    ledgerInflightOutbox,
                    predictionMarketResolutionService,
                    aeron,
                    archive,
                    replay,
                    thread,
                    running);
            thread.start();
            return instance;
        }

        private static long waitForRecording(AeronArchive archive) {
            long[] result = {-1L};
            while (result[0] < 0) {
                archive.listRecordingsForUri(
                        /* fromRecordingId = */ 0L,
                        /* recordCount = */ 1024,
                        OmsClusterWireFormat.EVENTS_CHANNEL,
                        OmsClusterWireFormat.EVENTS_STREAM_ID,
                        (controlSessionId,
                                correlationId,
                                recordingId,
                                startTimestamp,
                                stopTimestamp,
                                startPosition,
                                stopPosition,
                                initialTermId,
                                segmentFileLength,
                                termBufferLength,
                                mtuLength,
                                sessionId,
                                streamId,
                                strippedChannel,
                                originalChannel,
                                sourceIdentity) -> {
                            if (streamId == OmsClusterWireFormat.EVENTS_STREAM_ID && recordingId > result[0]) {
                                result[0] = recordingId;
                            }
                        });
                if (result[0] >= 0) {
                    return result[0];
                }
                try {
                    Thread.sleep(RECORDING_LOOKUP_PARK_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                            "interrupted while waiting for events recording", e);
                }
            }
            return result[0];
        }

        private static void replayLoop(
                Subscription replay,
                OrdersRepository orders,
                DomainEventOutboxRepository domainEventOutbox,
                DomainEventEnvelopeCodec domainEventCodec,
                PredictionMarketResolutionService predictionMarketResolutionService,
                TestPostgresProjectorTradeApplier tradeApplier,
                AtomicBoolean running) {
            FragmentHandler handler = (buffer, offset, length, header) -> {
                int typeId = buffer.getInt(offset + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET);
                try {
                    if (typeId == OmsClusterWireFormat.TYPE_ID_ORDER_ADMITTED) {
                        applyOrderAdmittedFragment(
                                buffer,
                                offset,
                                length,
                                orders,
                                domainEventOutbox,
                                domainEventCodec);
                    } else if (typeId == OmsClusterWireFormat.TYPE_ID_EXECUTION_APPLIED) {
                        ExecutionAppliedEvent ev = ExecutionAppliedEvent.decode(buffer, offset, length);
                        tradeApplier.applyExecutionAppliedEvent(ev);
                    } else if (typeId == OmsClusterWireFormat.TYPE_ID_VENUE_RESOLUTION_APPLIED) {
                        VenueResolutionAppliedEvent ev = VenueResolutionAppliedEvent.decode(buffer, offset, length);
                        predictionMarketResolutionService.apply(ev);
                    }
                } catch (RuntimeException e) {
                    // Swallow per-fragment errors so the daemon doesn't die on a single bad row.
                    // Tests that care will see the missing row and time out; the JVM logs will show
                    // the offending event.
                    e.printStackTrace();
                }
            };
            while (running.get()) {
                int polled = replay.poll(handler, REPLAY_FRAGMENT_LIMIT);
                if (polled == 0) {
                    LockSupport.parkNanos(REPLAY_POLL_PARK_NANOS);
                }
            }
        }

        private static void applyOrderAdmittedFragment(
                org.agrona.DirectBuffer buffer,
                int offset,
                int length,
                OrdersRepository orders,
                DomainEventOutboxRepository domainEventOutbox,
                DomainEventEnvelopeCodec domainEventCodec) {
            OrderAdmittedEvent ev = OrderAdmittedEvent.decode(buffer, offset, length);
            boolean fresh = orders.insertFromAdmittedEvent(ev);
            if (fresh) {
                // Mirror production OmsPostgresProjector D-3: emit the OrderAccepted
                // envelope here so domain-fanout integration tests still see the row.
                try {
                    domainEventOutbox.insert(
                            ev.orderId(), domainEventCodec.orderAcceptedFromAdmitted(ev));
                } catch (JsonProcessingException e) {
                    throw new IllegalStateException(
                            "OrderAccepted envelope serialisation failed for orderId=" + ev.orderId(), e);
                }
            }
        }

        private static void closeInstance() {
            TestPostgresProjectorSingleton local = INSTANCE;
            if (local == null) {
                return;
            }
            local.running.set(false);
            try {
                local.replayThread.join(2_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            CloseHelper.quietClose(local.replay);
            CloseHelper.quietClose(local.archive);
            CloseHelper.quietClose(local.aeron);
            try {
                local.dataSource.close();
            } catch (RuntimeException ignored) {
                // best-effort during JVM shutdown
            }
        }
    }
}
