package com.balh.oms.fixin;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.fixin.it.FixInClientEmbeddedInitiator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.jdbc.Sql;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import org.junit.jupiter.api.parallel.ResourceLock;

/** Mutating soak IT: operator force-logout drops session and writes audited admin action. */
@ResourceLock("oms-fixin-wire-it")
@ActiveProfiles({"test", OmsProfiles.FIX_INGRESS})
@Import(FixInMutatingSoakIT.Beans.class)
@Sql(scripts = "/db/fix-in-uat-seed.sql")
class FixInMutatingSoakIT extends FixInWireItAcceptorSupport {

    private static final UUID LOOPBACK_SESSION_ID = UUID.fromString("00000001-0000-4000-8000-000000000001");
    private static final Duration LOGON_TIMEOUT = Duration.ofSeconds(45);

    private static final int ACCEPT_PORT = allocatePort();
    private static final Path ACCEPTOR_STORE;

    static {
        try {
            ACCEPTOR_STORE = Files.createTempDirectory("oms-fix-in-mut-soak-acc");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @DynamicPropertySource
    static void jdbcStoreProps(DynamicPropertyRegistry registry) {
        registry.add("oms.fix-in.enabled", () -> "true");
        registry.add("oms.fix-in.auto-start", () -> "false");
        registry.add("oms.fix-in.accept-port", () -> String.valueOf(ACCEPT_PORT));
        registry.add("oms.fix-in.bind-host", () -> "127.0.0.1");
        registry.add("oms.fix-in.file-store-path", ACCEPTOR_STORE.toAbsolutePath()::toString);
        registry.add("oms.fix-in.session-store-type", () -> "jdbc");
        registry.add("oms.fix-in.return-publisher.enabled", () -> "false");
        registry.add("oms.cluster.client.enabled", () -> "true");
        registry.add("oms.cluster.client.aeron-directory", AbstractPostgresIntegrationTest::testClusterAeronDirectory);
    }

    @Autowired FixInClientEmbeddedInitiator fixInClient;
    @Autowired FixInSessionAdminService sessionAdminService;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void startClient() {
        if (!fixInClient.isRunning()) {
            fixInClient.start();
        }
    }

    @AfterEach
    void stopClient() {
        fixInClient.stop();
    }

    @Test
    void forceLogout_dropsSessionAndWritesAdminAction() {
        await().atMost(LOGON_TIMEOUT).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            assertThat(fixInClient.isLoggedOn()).isTrue();
            assertThat(sessionAdminService.getEnriched(LOOPBACK_SESSION_ID))
                    .isPresent()
                    .get()
                    .extracting(e -> e.runtime().loggedOn())
                    .isEqualTo(true);
        });

        sessionAdminService.forceLogout(LOOPBACK_SESSION_ID, "mutating-soak-it", "disconnect probe");

        await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            assertThat(fixInClient.isLoggedOn()).isFalse();
            assertThat(sessionAdminService.getEnriched(LOOPBACK_SESSION_ID))
                    .isPresent()
                    .get()
                    .extracting(e -> e.runtime().loggedOn())
                    .isEqualTo(false);
        });

        Integer logoutActions = jdbc.queryForObject(
                """
                SELECT COUNT(*) FROM oms_fix_session_admin_actions
                 WHERE fix_session_id = ? AND action_type = 'FORCE_LOGOUT'
                """,
                Integer.class,
                LOOPBACK_SESSION_ID);
        assertThat(logoutActions).isGreaterThanOrEqualTo(1);

        Integer jdbcSessionRows = jdbc.queryForObject(
                """
                SELECT COUNT(*) FROM oms_fix_sessions
                 WHERE beginstring = 'FIX.4.4'
                   AND sendercompid = 'BALH_OMS'
                   AND targetcompid = 'LOOPBACK_CLIENT'
                """,
                Integer.class);
        assertThat(jdbcSessionRows).isGreaterThanOrEqualTo(1);
    }

    @TestConfiguration
    static class Beans {
        @Bean
        FixInClientEmbeddedInitiator fixInClientEmbeddedInitiator() {
            return new FixInClientEmbeddedInitiator(ACCEPT_PORT, clientStore(), "LOOPBACK_CLIENT", "BALH_OMS");
        }

        private static Path clientStore() {
            try {
                return Files.createTempDirectory("oms-fix-in-mut-soak-client");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static int allocatePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
