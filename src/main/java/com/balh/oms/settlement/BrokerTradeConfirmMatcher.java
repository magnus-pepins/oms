package com.balh.oms.settlement;

import com.balh.oms.persistence.ExecutionsRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * Phase A.3 of the stock-settlement gap plan (§5.2). Resolves each pending
 * {@link BrokerTradeConfirmRepository.MatchableRow} to an OMS {@code executions} row,
 * compares the broker's economic record against the OMS fill, and writes one of three
 * outcomes:
 *
 * <ul>
 *   <li><strong>matched</strong>: broker and OMS agree on side, symbol, quantity, price,
 *       and account. The row's {@code resolved_execution_id} is set and a
 *       {@code broker_settlement_confirm} row is enqueued so the existing v1
 *       {@link SettlementConfirmProcessor} drives the execution through
 *       {@code executed → matched → confirmed → settling → settled} unchanged.</li>
 *   <li><strong>mismatch</strong>: the execution was found but at least one field disagrees.
 *       A {@code reconciliation_breaks} row of type {@code trade_mismatch} is opened with
 *       the per-field diff; settlement does <strong>not</strong> advance until ops resolves
 *       the break (gap plan §5.13).</li>
 *   <li><strong>unresolved</strong>: no execution matches the broker confirm's
 *       {@code (brokerId, venueExecRef)} or {@code (accountId, venueExecRef)}. A
 *       {@code reconciliation_breaks} row of type {@code unresolved_confirm} is opened.</li>
 * </ul>
 *
 * <p>Settlement-date comparison (§5.3 Slice 2a): when an otherwise-matched confirm
 * disagrees with the OMS-computed {@code executions.expected_settlement_date}, a
 * <strong>side break</strong> of type
 * {@link ReconciliationBreakRepository#BREAK_SETTLEMENT_DATE_MISMATCH} (severity
 * {@code medium}) is opened in addition to the matched outcome. The settlement itself is
 * <strong>not</strong> blocked — the broker is authoritative on the actual settlement
 * date and our v1 calendar uses a single configured cycle (placeholder until the
 * instrument-profile lookup lands; see {@link SettlementDateCalculator}). The break exists
 * for calendar / config drift visibility, not as a settle-stop.
 *
 * <p>ISIN/MIC/currency comparisons remain informational: differences are recorded in
 * {@code match_diff_json} but do <strong>not</strong> drive {@code mismatch}.
 */
@Service
public class BrokerTradeConfirmMatcher {

    private static final Logger log = LoggerFactory.getLogger(BrokerTradeConfirmMatcher.class);

    /** Hard cap on a single {@link #processPendingBatch(int)} call to keep transactions bounded. */
    private static final int MATCHER_BATCH_MAX = 200;

    private final BrokerTradeConfirmRepository confirms;
    private final ExecutionsRepository executions;
    private final ReconciliationBreakRepository breaks;
    private final SettlementConfirmProcessor settlementProcessor;
    private final ObjectMapper objectMapper;

    public BrokerTradeConfirmMatcher(
            BrokerTradeConfirmRepository confirms,
            ExecutionsRepository executions,
            ReconciliationBreakRepository breaks,
            SettlementConfirmProcessor settlementProcessor,
            ObjectMapper objectMapper) {
        this.confirms = confirms;
        this.executions = executions;
        this.breaks = breaks;
        this.settlementProcessor = settlementProcessor;
        this.objectMapper = objectMapper;
    }

    public enum Outcome {
        MATCHED,
        MISMATCH,
        UNRESOLVED,
        ALREADY_DECIDED
    }

    public record MatchResult(long confirmId, Long executionId, Outcome outcome, String diffJson) {}

    /** Process up to {@code maxRows} pending confirms in one transaction. */
    @Transactional
    public List<MatchResult> processPendingBatch(int maxRows) {
        int cap = Math.min(Math.max(1, maxRows), MATCHER_BATCH_MAX);
        List<BrokerTradeConfirmRepository.MatchableRow> batch = confirms.lockPendingBatch(cap);
        return batch.stream().map(this::matchOne).toList();
    }

    /** Match a single pending confirm by id; for ops manual trigger or test. */
    @Transactional
    public Optional<MatchResult> matchById(long confirmId) {
        return confirms.lockPendingById(confirmId).map(this::matchOne);
    }

    private MatchResult matchOne(BrokerTradeConfirmRepository.MatchableRow confirm) {
        Optional<Long> executionId = resolveExecution(confirm);
        if (executionId.isEmpty()) {
            String diff = unresolvedDiff(confirm);
            int updated = confirms.markUnresolved(confirm.id(), diff);
            if (updated == 0) {
                return new MatchResult(confirm.id(), null, Outcome.ALREADY_DECIDED, diff);
            }
            breaks.insert(new ReconciliationBreakRepository.InsertCommand(
                    ReconciliationBreakRepository.BREAK_UNRESOLVED_CONFIRM,
                    ReconciliationBreakRepository.SEVERITY_HIGH,
                    ReconciliationBreakRepository.SOURCE_BROKER,
                    confirm.id(),
                    null,
                    confirm.accountId(),
                    confirm.tradeDate(),
                    diff,
                    "system"));
            return new MatchResult(confirm.id(), null, Outcome.UNRESOLVED, diff);
        }
        long execId = executionId.get();
        Optional<SettlementExecutionRow> snapshot = executions.findSettlementRow(execId);
        if (snapshot.isEmpty()) {
            String diff = unresolvedDiff(confirm); // race: row vanished between resolve & read
            confirms.markUnresolved(confirm.id(), diff);
            return new MatchResult(confirm.id(), null, Outcome.UNRESOLVED, diff);
        }
        Optional<LocalDate> omsExpectedSettlementDate = executions.findExpectedSettlementDate(execId);
        DiffPayload diff = compare(confirm, snapshot.get(), omsExpectedSettlementDate);
        String diffJson = serialize(diff.node);
        if (diff.allMatch) {
            int updated = confirms.markMatched(confirm.id(), execId, diffJson);
            if (updated == 0) {
                return new MatchResult(confirm.id(), execId, Outcome.ALREADY_DECIDED, diffJson);
            }
            try {
                settlementProcessor.enqueueBrokerSettlementConfirmForTradeOrThrow(execId);
            } catch (IllegalStateException e) {
                log.warn(
                        "matched confirm but v1 broker_settlement_confirm enqueue failed: confirmId={} executionId={} reason={}",
                        confirm.id(),
                        execId,
                        e.getMessage());
            }
            maybeOpenSettlementDateMismatchBreak(confirm, execId, diff);
            return new MatchResult(confirm.id(), execId, Outcome.MATCHED, diffJson);
        }
        int updated = confirms.markMismatch(confirm.id(), execId, diffJson);
        if (updated == 0) {
            return new MatchResult(confirm.id(), execId, Outcome.ALREADY_DECIDED, diffJson);
        }
        breaks.insert(new ReconciliationBreakRepository.InsertCommand(
                ReconciliationBreakRepository.BREAK_TRADE_MISMATCH,
                ReconciliationBreakRepository.SEVERITY_HIGH,
                ReconciliationBreakRepository.SOURCE_BROKER,
                confirm.id(),
                execId,
                confirm.accountId(),
                confirm.tradeDate(),
                diffJson,
                "system"));
        return new MatchResult(confirm.id(), execId, Outcome.MISMATCH, diffJson);
    }

    private Optional<Long> resolveExecution(BrokerTradeConfirmRepository.MatchableRow confirm) {
        if (confirm.accountId() == null
                || confirm.venueExecRef() == null
                || confirm.venueExecRef().isBlank()) {
            return Optional.empty();
        }
        return executions.findTradeExecutionIdByAccountAndVenueRef(confirm.accountId(), confirm.venueExecRef());
    }

    private static DiffPayload compare(
            BrokerTradeConfirmRepository.MatchableRow confirm,
            SettlementExecutionRow snapshot,
            Optional<LocalDate> omsExpectedSettlementDate) {
        ObjectNode fields = JSON.objectNode();
        boolean allMatch = true;
        allMatch &= addField(fields, "side", confirm.side(), snapshot.side(), Objects::equals);
        allMatch &= addField(
                fields,
                "instrumentSymbol",
                confirm.instrumentSymbol(),
                snapshot.instrumentSymbol(),
                Objects::equals);
        allMatch &= addField(fields, "accountId", str(confirm.accountId()), str(snapshot.accountId()), Objects::equals);
        allMatch &= addField(
                fields,
                "quantity",
                plain(confirm.quantity()),
                plain(snapshot.lastQuantity()),
                BrokerTradeConfirmMatcher::numericEquals);
        allMatch &= addField(
                fields,
                "price",
                plain(confirm.price()),
                plain(snapshot.lastPrice()),
                BrokerTradeConfirmMatcher::numericEquals);
        SettlementDateAxis dateAxis = settlementDateAxis(confirm.settlementDate(), omsExpectedSettlementDate);
        fields.set("settlementDate", dateAxis.node());
        ObjectNode root = JSON.objectNode();
        root.put("resolvedBy", "(accountId,venueExecRef)");
        root.set("fields", fields);
        return new DiffPayload(root, allMatch, dateAxis);
    }

    /**
     * Builds the {@code settlementDate} axis for the diff JSON. We deliberately do NOT
     * include this axis in {@code allMatch}: the broker is authoritative on the actual
     * settlement date, and the v1 OMS calculator uses a configured-default cycle (no
     * holidays, no instrument profile yet — see {@link SettlementDateCalculator}). A
     * disagreement opens a side break ({@code settlement_date_mismatch}) on the matched
     * path but does not block settlement.
     */
    private static SettlementDateAxis settlementDateAxis(
            LocalDate brokerSettlementDate, Optional<LocalDate> omsExpectedSettlementDate) {
        ObjectNode node = JSON.objectNode();
        node.put("broker", brokerSettlementDate == null ? null : brokerSettlementDate.toString());
        node.put("oms", omsExpectedSettlementDate.map(LocalDate::toString).orElse(null));
        Boolean compared; // null = could not compare (one side unknown)
        if (brokerSettlementDate == null || omsExpectedSettlementDate.isEmpty()) {
            compared = null;
            node.put("match", (Boolean) null);
            node.put(
                    "reason",
                    brokerSettlementDate == null
                            ? "broker_settlement_date_missing"
                            : "oms_expected_unknown_pre_v58_or_non_trade");
        } else {
            compared = brokerSettlementDate.equals(omsExpectedSettlementDate.get());
            node.put("match", compared);
        }
        return new SettlementDateAxis(node, compared);
    }

    /**
     * Opens a side break when the settlement-date axis disagreed on an otherwise-matched
     * confirm. NOP when {@link SettlementDateAxis#compared} is {@code null} (one side
     * unknown) or {@code true} (agreement).
     */
    private void maybeOpenSettlementDateMismatchBreak(
            BrokerTradeConfirmRepository.MatchableRow confirm, long executionId, DiffPayload diff) {
        Boolean compared = diff.dateAxis().compared();
        if (compared == null || compared) {
            return;
        }
        // Reuse the per-confirm diff payload so ops sees the full economic context, not just
        // the date drift in isolation. The break_type is the routing key; ops UI filters on
        // it.
        breaks.insert(new ReconciliationBreakRepository.InsertCommand(
                ReconciliationBreakRepository.BREAK_SETTLEMENT_DATE_MISMATCH,
                ReconciliationBreakRepository.SEVERITY_MEDIUM,
                ReconciliationBreakRepository.SOURCE_BROKER,
                confirm.id(),
                executionId,
                confirm.accountId(),
                confirm.tradeDate(),
                serialize(diff.node()),
                "system"));
    }

    /**
     * @return {@code true} when the two normalized strings represent the same numeric value
     *     (so {@code "5.00"} and {@code "5"} match).
     */
    static boolean numericEquals(String left, String right) {
        if (left == null || right == null) {
            return Objects.equals(left, right);
        }
        try {
            return new BigDecimal(left).compareTo(new BigDecimal(right)) == 0;
        } catch (NumberFormatException e) {
            return left.equals(right);
        }
    }

    private static boolean addField(
            ObjectNode fields,
            String name,
            String brokerVal,
            String omsVal,
            java.util.function.BiPredicate<String, String> eq) {
        boolean match = eq.test(brokerVal, omsVal);
        ObjectNode node = JSON.objectNode();
        node.put("broker", brokerVal);
        node.put("oms", omsVal);
        node.put("match", match);
        fields.set(name, node);
        return match;
    }

    private static String str(Object value) {
        return value == null ? null : value.toString();
    }

    private static String plain(BigDecimal value) {
        return value == null ? null : value.toPlainString();
    }

    private String serialize(ObjectNode node) {
        try {
            return objectMapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("match diff serialise failed", e);
        }
    }

    private String unresolvedDiff(BrokerTradeConfirmRepository.MatchableRow confirm) {
        ObjectNode root = JSON.objectNode();
        root.put("reason", "no_execution_found");
        root.put("accountId", confirm.accountId() == null ? null : confirm.accountId().toString());
        root.put("venueExecRef", confirm.venueExecRef());
        root.put("brokerId", confirm.brokerId());
        root.put("brokerTradeId", confirm.brokerTradeId());
        return serialize(root);
    }

    private record DiffPayload(ObjectNode node, boolean allMatch, SettlementDateAxis dateAxis) {}

    /**
     * Result of the settlement-date comparison. {@code compared} is {@code null} when one
     * side was unknown (broker omitted the date, or the OMS execution pre-dates V58), and
     * carries the boolean equality otherwise.
     */
    private record SettlementDateAxis(ObjectNode node, Boolean compared) {}

    /** Shared lazy Jackson factory for tiny diff trees; avoids passing ObjectMapper to static helpers. */
    private static final class JSON {
        private static final ObjectMapper SHARED = new ObjectMapper();

        static ObjectNode objectNode() {
            return SHARED.createObjectNode();
        }

        private JSON() {}
    }

    /** Lower-case helper for ad-hoc string comparison if needed by future fields. */
    @SuppressWarnings("unused")
    private static String lc(String s) {
        return s == null ? null : s.toLowerCase(Locale.ROOT);
    }
}
