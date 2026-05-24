package com.balh.oms.reconciler;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.corporateaction.CorporateActionCashLedgerBookingService;
import com.balh.oms.corporateaction.CorporateActionCustomerNotificationService;
import com.balh.oms.ledger.LedgerSettlementLegPoster;
import com.balh.oms.ledger.LedgerSettlementPostingClient;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/** Drains {@code corporate_action_ledger_outbox} to Ledger HTTP (§5.9). */
@Component
@ConditionalOnBean(LedgerSettlementLegPoster.class)
public class CorporateActionLedgerOutboxReconciler {

    private static final Logger log = LoggerFactory.getLogger(CorporateActionLedgerOutboxReconciler.class);

    public record OutboxRow(long id, long cashImpactId, String legKind, String payloadJson, int attempts) {}

    private static final String LOCK =
            """
                    SELECT id, cash_impact_id, leg_kind, payload_json::text AS payload_json, attempts
                    FROM corporate_action_ledger_outbox
                    WHERE posted_at IS NULL AND skipped_at IS NULL
                    ORDER BY id
                    LIMIT :lim
                    FOR UPDATE SKIP LOCKED
                    """;

    private static final String MARK_POSTED =
            "UPDATE corporate_action_ledger_outbox SET posted_at = NOW() WHERE id = :id";

    private static final String MARK_FAILED =
            """
                    UPDATE corporate_action_ledger_outbox
                    SET attempts = attempts + 1, last_error = :err
                    WHERE id = :id
                    """;

    private final NamedParameterJdbcTemplate jdbc;
    private final LedgerSettlementLegPoster poster;
    private final CorporateActionCustomerNotificationService customerNotifications;
    private final OmsConfig config;
    private final TransactionTemplate transactionTemplate;

    public CorporateActionLedgerOutboxReconciler(
            NamedParameterJdbcTemplate jdbc,
            LedgerSettlementLegPoster poster,
            CorporateActionCustomerNotificationService customerNotifications,
            OmsConfig config,
            org.springframework.transaction.PlatformTransactionManager transactionManager) {
        this.jdbc = jdbc;
        this.poster = poster;
        this.customerNotifications = customerNotifications;
        this.config = config;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @Scheduled(fixedDelayString = "${oms.corporate-action.ledger-outbox-reconciler-interval-ms:1000}")
    public void runOnce() {
        if (!config.getLedger().isSettlementOutboxReconcilerEnabled()) {
            return;
        }
        transactionTemplate.executeWithoutResult(
                status -> {
                    List<OutboxRow> rows =
                            jdbc.query(
                                    LOCK,
                                    new MapSqlParameterSource(
                                            "lim", config.getCorporateAction().getLedgerOutboxReconcilerBatchSize()),
                                    (rs, rowNum) ->
                                            new OutboxRow(
                                                    rs.getLong("id"),
                                                    rs.getLong("cash_impact_id"),
                                                    rs.getString("leg_kind"),
                                                    rs.getString("payload_json"),
                                                    rs.getInt("attempts")));
                    for (OutboxRow row : rows) {
                        try {
                            poster.postCorporateActionOutbox(row.id(), row.legKind(), row.payloadJson());
                            jdbc.update(MARK_POSTED, new MapSqlParameterSource("id", row.id()));
                            if (CorporateActionCashLedgerBookingService.LEG_DIVIDEND.equals(row.legKind())) {
                                customerNotifications.enqueueDividendPaidIfEnabled(row.payloadJson());
                            }
                        } catch (LedgerSettlementPostingClient.LedgerSettlementPostingException e) {
                            jdbc.update(
                                    MARK_FAILED,
                                    new MapSqlParameterSource("id", row.id()).addValue("err", truncate(e.getMessage())));
                            log.warn(
                                    "corporate_action ledger outbox post failed id={} leg={}: {}",
                                    row.id(),
                                    row.legKind(),
                                    e.getMessage());
                        }
                    }
                });
    }

    private static String truncate(String message) {
        if (message == null) {
            return null;
        }
        return message.length() <= 4000 ? message : message.substring(0, 4000);
    }
}
