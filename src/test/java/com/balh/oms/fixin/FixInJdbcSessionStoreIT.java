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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Verifies FIX-in acceptor persists QuickFIX/J seq state to {@code oms_fix_sessions} when JDBC store is enabled. */
@ActiveProfiles({"test", OmsProfiles.FIX_INGRESS})
@Import(FixInJdbcSessionStoreIT.Beans.class)
@Sql(scripts = "/db/fix-in-uat-seed.sql")
class FixInJdbcSessionStoreIT extends FixInWireItAcceptorSupport {

    private static final int ACCEPT_PORT = allocatePort();
    private static final Path ACCEPTOR_STORE;

    static {
        try {
            ACCEPTOR_STORE = Files.createTempDirectory("oms-fix-in-jdbc-it-acc");
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
    void jdbcStore_persistsSessionSeqAfterClientLogon() {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            assertThat(fixInClient.isLoggedOn()).isTrue();
            Integer rows = jdbc.queryForObject(
                    """
                    SELECT COUNT(*) FROM oms_fix_sessions
                     WHERE beginstring = 'FIX.4.4'
                       AND sendercompid = 'BALH_OMS'
                       AND targetcompid = 'LOOPBACK_CLIENT'
                    """,
                    Integer.class);
            assertThat(rows).isGreaterThanOrEqualTo(1);
        });
    }

    @TestConfiguration
    static class Beans {
        @Bean
        FixInClientEmbeddedInitiator fixInClientEmbeddedInitiator() {
            return new FixInClientEmbeddedInitiator(ACCEPT_PORT, clientStore(), "LOOPBACK_CLIENT", "BALH_OMS");
        }

        private static Path clientStore() {
            try {
                return Files.createTempDirectory("oms-fix-in-jdbc-it-client");
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
