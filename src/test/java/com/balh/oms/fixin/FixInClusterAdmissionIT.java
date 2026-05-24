package com.balh.oms.fixin;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.fixin.persistence.FixInAccountBindingRepository;
import com.balh.oms.fixin.persistence.FixInSessionRepository;
import com.balh.oms.fixin.persistence.FixInSessionRow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.jdbc.Sql;
import quickfix.SessionID;
import quickfix.field.ClOrdID;
import quickfix.field.OrdType;
import quickfix.field.OrderQty;
import quickfix.field.Price;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.TimeInForce;
import quickfix.fix44.NewOrderSingle;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end FIX-in admission: {@code 35=D} → cluster {@code AcceptOrderCommand} with
 * {@code FixInIngressMetadata} → {@code orders} row via test projector.
 */
@ActiveProfiles({"test", OmsProfiles.FIX_INGRESS})
@Sql(scripts = "/db/fix-in-uat-seed.sql")
class FixInClusterAdmissionIT extends AbstractPostgresIntegrationTest {

    private static final Duration ORDERS_VISIBLE_TIMEOUT = Duration.ofSeconds(20);

    private static final UUID SESSION_ID = UUID.fromString("00000001-0000-4000-8000-000000000001");
    private static final UUID ACCOUNT_ID = UUID.fromString("a0000001-0000-4000-8000-000000000002");

    @DynamicPropertySource
    static void fixInTestProps(DynamicPropertyRegistry registry) {
        registry.add("oms.fix-in.auto-start", () -> "false");
        registry.add("oms.fix-in.return-publisher.enabled", () -> "false");
    }

    @Autowired FixInIngressService ingressService;
    @Autowired FixInSessionRegistry sessionRegistry;
    @Autowired FixInSessionRepository sessionRepository;
    @Autowired FixInAccountBindingRepository accountBindingRepository;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void registerWireSession() {
        FixInSessionRow row = sessionRepository.findById(SESSION_ID).orElseThrow();
        SessionID wireId = new SessionID("FIX.4.4", row.targetCompId(), row.senderCompId());
        sessionRegistry.register(wireId, row);
    }

    @Test
    void newOrderSingle_admitsToClusterAndProjectsOrder() throws Exception {
        String clOrdId = "IT-NEW-" + UUID.randomUUID().toString().substring(0, 8);
        NewOrderSingle nos = new NewOrderSingle();
        nos.set(new ClOrdID(clOrdId));
        nos.set(new Symbol("AAPL"));
        nos.set(new Side(Side.BUY));
        nos.set(new OrdType(OrdType.LIMIT));
        nos.set(new OrderQty(5));
        nos.set(new Price(100));
        nos.set(new TimeInForce(TimeInForce.DAY));

        SessionID wireId = new SessionID("FIX.4.4", "BALH_OMS", "LOOPBACK_CLIENT");
        ingressService.handleNewOrderSingle(nos, wireId);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Long count = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM oms_fix_in_order_map WHERE session_id = ? AND client_cl_ord_id = ?",
                    Long.class,
                    SESSION_ID,
                    clOrdId);
            assertThat(count).isEqualTo(1L);
            Long orders = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM orders WHERE account_id = ? AND client_idempotency_key = ?",
                    Long.class,
                    ACCOUNT_ID,
                    "fixin:" + SESSION_ID + ":" + clOrdId);
            assertThat(orders).isEqualTo(1L);
        });
    }
}
