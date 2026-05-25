package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Manual hedge submitter for the trading-desk FX console.
 *
 * <p>Flow:
 * <ol>
 *   <li>Validate the inbound action (pair, side, base/quote amounts, nostros).
 *       Recall the quote when a {@code quoteId} is supplied so the desk can
 *       lock-and-hedge against the price the operator saw; if no quoteId,
 *       re-quote at the requested tier.
 *   <li>Insert the row into {@code fx_hedge_actions} with status='pending'.
 *       The {@code action_key} is the idempotency token (UI-generated or
 *       derived from the request body).
 *   <li>Best-effort POST to Ledger {@code /transactions?sync=true} with
 *       source = quote-side nostro, destination = base-side nostro, amount
 *       in the base currency, with the quoted rate. Ledger handles the
 *       multi-currency mechanics via the standard {@code rate} parameter
 *       (same path FX customer trades will use later).
 *   <li>Update the audit row with {@code ledger_transaction_id}/{@code posted_at}
 *       on success, or {@code status='failed'} + {@code failure_reason}
 *       on a Ledger error. The audit row is the source of truth for the
 *       trading-desk UI either way.
 * </ol>
 *
 * <p>This is intentionally narrow for the demo: no netting, no per-pair limits,
 * no multi-leg atomic group, no PB session. See V37 migration header for the
 * deferred list.
 */
@Service
public class FxHedgeService {

    private static final Logger log = LoggerFactory.getLogger(FxHedgeService.class);
    /** Ledger amount field is a {@code number}; use BigDecimal-friendly precision. */
    private static final int LEDGER_AMOUNT_SCALE = 8;

    private final JdbcTemplate jdbc;
    private final FxQuoteService quoteService;
    private final OmsConfig omsConfig;
    private final ObjectMapper objectMapper;
    private final ObjectProvider<RestClient> ledgerRestClient;
    private final ObjectProvider<OmsFxHedgePublisher> hedgePublisher;
    private final Clock clock;
    private final Counter submittedCounter;
    private final Counter postedCounter;
    private final Counter failedCounter;

    public FxHedgeService(
            JdbcTemplate jdbc,
            FxQuoteService quoteService,
            OmsConfig omsConfig,
            ObjectMapper objectMapper,
            ObjectProvider<RestClient> ledgerRestClient,
            ObjectProvider<OmsFxHedgePublisher> hedgePublisher,
            Clock clock,
            MeterRegistry registry) {
        this.jdbc = jdbc;
        this.quoteService = quoteService;
        this.omsConfig = omsConfig;
        this.objectMapper = objectMapper;
        this.ledgerRestClient = ledgerRestClient;
        this.hedgePublisher = hedgePublisher;
        this.clock = clock;
        this.submittedCounter = Counter.builder("oms.fx.hedge.submitted_total")
                .description("Manual FX hedge submissions accepted by the OMS")
                .register(registry);
        this.postedCounter = Counter.builder("oms.fx.hedge.posted_total")
                .description("FX hedges with a posted Ledger transaction")
                .register(registry);
        this.failedCounter = Counter.builder("oms.fx.hedge.failed_total")
                .description("FX hedges where the Ledger post failed")
                .register(registry);
    }

    public record HedgeRequest(
            String actionKey,
            String submittedBy,
            String pair,
            String side,
            String tier,
            String quoteId,
            BigDecimal baseAmount,
            String baseNostroId,
            String quoteNostroId,
            String description
    ) {}

    public Map<String, Object> submit(HedgeRequest req) {
        validate(req);
        String pair = req.pair.toUpperCase();
        String side = req.side.toUpperCase();
        String tier = (req.tier == null || req.tier.isBlank()) ? "default" : req.tier.toLowerCase();
        String submittedBy = req.submittedBy.trim();
        BigDecimal baseAmount = req.baseAmount.setScale(LEDGER_AMOUNT_SCALE, RoundingMode.HALF_UP);
        String baseCcy = pair.substring(0, 3);
        String quoteCcy = pair.substring(3);

        // Resolve rate: prefer recalled quote, fall back to a fresh quote in the same tier.
        FxQuoteService.CachedQuote cached = req.quoteId == null ? null : quoteService.recall(req.quoteId);
        BigDecimal rate;
        String effectiveQuoteId;
        if (cached != null && cached.pair().equals(pair)) {
            rate = "BUY".equals(side) ? cached.ask() : cached.bid();
            effectiveQuoteId = cached.quoteId();
        } else {
            Map<String, Object> fresh = quoteService.quote(pair, tier);
            rate = new BigDecimal((String) fresh.get("BUY".equals(side) ? "ask" : "bid"));
            effectiveQuoteId = (String) fresh.get("quoteId");
        }
        BigDecimal quoteAmount = baseAmount.multiply(rate)
                .setScale(LEDGER_AMOUNT_SCALE, RoundingMode.HALF_UP);

        // For BUY base/quote (buying base, selling quote), the desk debits the
        // quote nostro and credits the base nostro. For SELL, vice versa.
        String source = "BUY".equals(side) ? req.quoteNostroId.trim() : req.baseNostroId.trim();
        String destination = "BUY".equals(side) ? req.baseNostroId.trim() : req.quoteNostroId.trim();

        // Step 1: idempotent insert. If the action_key already exists, return the
        // existing row instead of creating a duplicate. We still emit the event
        // so a desk that missed the original write (subscriber lag, page reload)
        // converges to the same state.
        Long existingId = findByActionKey(req.actionKey);
        if (existingId != null) {
            linkManualExecution(req.actionKey, existingId);
            Map<String, Object> existing = readById(existingId);
            publishHedgeEvent(existing);
            return existing;
        }

        Long actionId = insertPending(req, pair, side, baseCcy, quoteCcy, baseAmount, quoteAmount,
                rate, effectiveQuoteId, source, destination);
        submittedCounter.increment();
        linkManualExecution(req.actionKey, actionId);

        // Step 2: best-effort Ledger post. The audit row is canonical; the
        // ledger txn is the actual money movement.
        try {
            String ledgerTxnId = postLedgerTransaction(req, pair, side, baseCcy, quoteCcy,
                    baseAmount, rate, source, destination);
            markPosted(actionId, ledgerTxnId);
            postedCounter.increment();
        } catch (Exception e) {
            log.warn("[fx-hedge] ledger post failed actionId={} reason={}", actionId, e.getMessage(), e);
            markFailed(actionId, e.getMessage());
            failedCounter.increment();
        }
        // Step 3: publish the terminal-state row to the live event stream so
        // the trading-desk Treasury page can stream the update instead of
        // polling. Best-effort: failures here never throw because the audit
        // row + Ledger txn are already durable.
        Map<String, Object> finalRow = readById(actionId);
        publishHedgeEvent(finalRow);
        return finalRow;
    }

    private void publishHedgeEvent(Map<String, Object> row) {
        if (row == null) return;
        OmsFxHedgePublisher pub = hedgePublisher.getIfAvailable();
        if (pub == null) return;
        try {
            pub.publish(row);
        } catch (RuntimeException e) {
            // publisher is already best-effort; double-guard so a publish bug
            // can never poison the submit() return.
            log.debug("[fx-hedge] event publish skipped: {}", e.getMessage());
        }
    }

    private void validate(HedgeRequest req) {
        if (req == null) throw new IllegalArgumentException("request_required");
        if (req.actionKey == null || req.actionKey.isBlank()) throw new IllegalArgumentException("action_key_required");
        if (req.submittedBy == null || req.submittedBy.isBlank()) throw new IllegalArgumentException("submitted_by_required");
        if (req.pair == null || req.pair.length() != 6) throw new IllegalArgumentException("pair_must_be_6_chars");
        if (req.side == null || !(req.side.equalsIgnoreCase("BUY") || req.side.equalsIgnoreCase("SELL")))
            throw new IllegalArgumentException("side_must_be_BUY_or_SELL");
        if (req.baseAmount == null || req.baseAmount.signum() <= 0)
            throw new IllegalArgumentException("base_amount_must_be_positive");
        if (req.baseNostroId == null || req.baseNostroId.isBlank())
            throw new IllegalArgumentException("base_nostro_required");
        if (req.quoteNostroId == null || req.quoteNostroId.isBlank())
            throw new IllegalArgumentException("quote_nostro_required");
        if (req.baseNostroId.equals(req.quoteNostroId))
            throw new IllegalArgumentException("nostros_must_differ");
    }

    private Long findByActionKey(String actionKey) {
        try {
            List<Long> ids = jdbc.queryForList(
                    "SELECT id FROM fx_hedge_actions WHERE action_key = ? LIMIT 1",
                    Long.class, actionKey);
            return ids.isEmpty() ? null : ids.get(0);
        } catch (DataAccessException e) {
            log.warn("[fx-hedge] findByActionKey failed", e);
            return null;
        }
    }

    /** Parity with {@link FxAutoHedger#linkAutoFire} for desk-initiated advisory executes. */
    private void linkManualExecution(String actionKey, long hedgeActionId) {
        try {
            jdbc.update(
                    "UPDATE fx_hedger_recommendations SET executed_action_id = ? "
                            + "WHERE action_key = ? AND executed_action_id IS NULL",
                    hedgeActionId, actionKey);
        } catch (DataAccessException e) {
            log.warn("[fx-hedge] link manual execution failed actionKey={}: {}", actionKey, e.getMessage());
        }
    }

    private Long insertPending(HedgeRequest req, String pair, String side, String baseCcy, String quoteCcy,
                               BigDecimal baseAmount, BigDecimal quoteAmount, BigDecimal rate,
                               String quoteId, String source, String destination) {
        // Single-shot insert + RETURNING id.
        return jdbc.queryForObject(
                "INSERT INTO fx_hedge_actions ("
                        + "action_key, submitted_by, pair, side, base_currency, quote_currency, "
                        + "base_amount, quote_amount, quoted_rate, quote_id, "
                        + "base_nostro_id, quote_nostro_id, status, payload_json"
                        + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?::jsonb) "
                        + "RETURNING id",
                Long.class,
                req.actionKey, req.submittedBy, pair, side, baseCcy, quoteCcy,
                baseAmount, quoteAmount, rate, quoteId,
                req.baseNostroId.trim(), req.quoteNostroId.trim(),
                buildPayloadJson(req, source, destination));
    }

    private String buildPayloadJson(HedgeRequest req, String source, String destination) {
        try {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("description", req.description == null ? "" : req.description);
            m.put("source", source);
            m.put("destination", destination);
            m.put("tier", req.tier);
            return objectMapper.writeValueAsString(m);
        } catch (Exception e) {
            return "{}";
        }
    }

    /**
     * Posts the hedge atomically as two same-currency Ledger transactions
     * through {@code @FX-Suspense-<CCY>} balances, using
     * {@code POST /transactions/bulk?atomic=true}:
     *
     * <pre>
     *   leg 1 (base)  : @FX-Suspense-&lt;BASE&gt;   -&gt; baseNostro       (+baseAmount BASE)
     *   leg 2 (quote) : quoteNostro            -&gt; @FX-Suspense-&lt;QUOTE&gt; (+quoteAmount QUOTE)
     *
     * BUY side  : the desk acquires BASE (credit base nostro, debit base suspense)
     *             and pays QUOTE (debit quote nostro, credit quote suspense).
     * SELL side : flipped — the desk gives up BASE (debit base nostro,
     *             credit base suspense) and acquires QUOTE (credit quote
     *             nostro, debit quote suspense).
     * </pre>
     *
     * <p>Why split: the Ledger refuses {@code rate != 1} multi-currency posts
     * with HTTP 501 (J-7 deferred). Both legs above are rate=1 single-currency
     * so they take the standard apply path. The two suspense balances net-zero
     * per currency once all hedges close against the PB settlement.
     *
     * <p>Atomicity guarantee: with {@code atomic=true} the ledger's
     * {@code BulkController} stops on first leg failure and issues a
     * compensating {@link com.balh.ledger.cluster.wire.RecordPostedTransactionCommand}
     * with source/destination swapped on every already-applied leg
     * ({@code BulkController.runAtomic} + {@code reverseAppliedLegs}). This
     * closes the previous gap where leg 1 could commit and leg 2 fail,
     * leaving orphaned cash visible only by hand-reconciling the ledger.
     * The atomicity is compensating-reversal, not Raft-native — same
     * caveat the TS Ledger has carried since cutover. Tracked: a future
     * {@code RecordBulkTransactionsCommand} would give true log-level
     * atomicity (see {@code BulkController} class doc).
     *
     * <p>Each leg carries {@code allowOverdraft=true} per row because
     * suspense and nostro balances may both legitimately swing negative
     * pending PB settlement; the soft limit lives in
     * {@code oms.fx.suspense.max-abs-csv} + {@link FxSuspenseLimitMonitor}.
     *
     * @return the ledger {@code batch_id} (one per atomic hedge); leg
     *         transaction ids are recoverable via the read-side projector
     *         by joining on {@code metaData.batch_id}.
     */
    private String postLedgerTransaction(HedgeRequest req, String pair, String side, String baseCcy,
                                         String quoteCcy, BigDecimal baseAmount, BigDecimal rate,
                                         String unusedSource, String unusedDestination) throws Exception {
        var ledger = omsConfig.getLedger();
        if (!ledger.isEnabled()) {
            throw new IllegalStateException("oms.ledger.enabled=false; cannot post hedge");
        }
        RestClient http = ledgerRestClient.getIfAvailable();
        if (http == null) {
            throw new IllegalStateException("oms_ledger_rest_client_unavailable");
        }
        String apiKey = ledger.getApiKey();
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalStateException("oms.ledger.api-key is required");
        }

        BigDecimal quoteAmount = baseAmount.multiply(rate)
                .setScale(LEDGER_AMOUNT_SCALE, RoundingMode.HALF_UP);
        String baseSuspense = "@FX-Suspense-" + baseCcy;
        String quoteSuspense = "@FX-Suspense-" + quoteCcy;

        String baseLegSource, baseLegDest, quoteLegSource, quoteLegDest;
        if ("BUY".equals(side)) {
            baseLegSource = baseSuspense;
            baseLegDest = req.baseNostroId.trim();
            quoteLegSource = req.quoteNostroId.trim();
            quoteLegDest = quoteSuspense;
        } else {
            baseLegSource = req.baseNostroId.trim();
            baseLegDest = baseSuspense;
            quoteLegSource = quoteSuspense;
            quoteLegDest = req.quoteNostroId.trim();
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("inflight", false);
        body.put("atomic", true);
        List<Map<String, Object>> txns = new ArrayList<>(2);
        txns.add(buildLegRow(req, pair, side, "BASE",
                baseLegSource, baseLegDest, baseAmount, baseCcy, rate));
        txns.add(buildLegRow(req, pair, side, "QUOTE",
                quoteLegSource, quoteLegDest, quoteAmount, quoteCcy, rate));
        body.put("transactions", txns);

        try {
            String resp = http.post()
                    .uri("/transactions/bulk")
                    .contentType(MediaType.APPLICATION_JSON)
                    .header("Authorization", "Bearer " + apiKey)
                    .body(body)
                    .retrieve()
                    .body(String.class);
            JsonNode root = objectMapper.readTree(resp == null ? "{}" : resp);
            String status = root.path("status").asText("");
            String batchId = root.path("batch_id").asText(null);
            JsonNode errors = root.path("errors");
            // BulkController returns 201 with status=applied on full success;
            // status=partial cannot happen under atomic=true (atomic clears
            // applied[] on any failure). Treat anything other than applied
            // as a failure with the ledger-provided error string.
            if (!"applied".equals(status)) {
                throw new IllegalStateException(
                        "ledger bulk hedge non-applied: status=" + status
                                + " errors=" + truncate(errors.toString()));
            }
            if (batchId == null || batchId.isBlank()) {
                throw new IllegalStateException("ledger bulk hedge missing batch_id; body=" + resp);
            }
            return batchId;
        } catch (RestClientResponseException e) {
            // 400 "all failed" branch carries an errors[] we want surfaced.
            String b = e.getResponseBodyAsString();
            throw new IllegalStateException(
                    "ledger bulk hedge HTTP " + e.getStatusCode().value() + ": "
                            + truncate(b));
        }
    }

    private Map<String, Object> buildLegRow(HedgeRequest req, String pair, String side, String legLabel,
                                            String src, String dst, BigDecimal amount, String ccy,
                                            BigDecimal rate) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("source", src);
        row.put("destination", dst);
        row.put("amount", amount.doubleValue());
        row.put("currency", ccy);
        row.put("reference", "fx-hedge-" + req.actionKey + "-" + legLabel);
        row.put("description",
                "FX hedge " + side + " " + pair + " leg=" + legLabel + " " + amount.toPlainString() + " " + ccy);
        // Per-row, J-4h.5 enabled. Suspense + nostro may both legitimately
        // swing negative; the soft cap lives in FxSuspenseLimitMonitor.
        row.put("allowOverdraft", true);
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("kind", "fx_hedge_leg");
        meta.put("pair", pair);
        meta.put("side", side);
        meta.put("leg", legLabel);
        meta.put("amount", amount.toPlainString());
        meta.put("currency", ccy);
        meta.put("rate", rate.toPlainString());
        meta.put("submittedBy", req.submittedBy);
        meta.put("actionKey", req.actionKey);
        row.put("metaData", meta);
        return row;
    }

    private static String truncate(String s) {
        if (s == null) return "";
        return s.length() <= 300 ? s : s.substring(0, 300) + "…";
    }

    private void markPosted(Long id, String ledgerTxnId) {
        // Spring JDBC's default Object-binding doesn't know how to map a
        // java.time.Instant to a Postgres TIMESTAMPTZ column without the
        // jdbc-driver type hint dance; convert to java.sql.Timestamp once
        // so we hand the driver a native PreparedStatement.setTimestamp
        // parameter.
        java.sql.Timestamp now = java.sql.Timestamp.from(Instant.now(clock));
        jdbc.update(
                "UPDATE fx_hedge_actions SET status = 'posted', ledger_transaction_id = ?, posted_at = ? WHERE id = ?",
                ledgerTxnId, now, id);
    }

    private void markFailed(Long id, String reason) {
        String safe = reason == null ? "unknown" : (reason.length() > 1000 ? reason.substring(0, 1000) : reason);
        jdbc.update(
                "UPDATE fx_hedge_actions SET status = 'failed', failure_reason = ? WHERE id = ?",
                safe, id);
    }

    public Map<String, Object> readById(Long id) {
        return jdbc.queryForObject(
                "SELECT id, action_key, submitted_by, pair, side, base_currency, quote_currency, "
                        + "base_amount, quote_amount, quoted_rate, quote_id, base_nostro_id, quote_nostro_id, "
                        + "ledger_transaction_id, status, failure_reason, submitted_at, posted_at "
                        + "FROM fx_hedge_actions WHERE id = ?",
                (rs, n) -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("id", rs.getLong("id"));
                    m.put("actionKey", rs.getString("action_key"));
                    m.put("submittedBy", rs.getString("submitted_by"));
                    m.put("pair", rs.getString("pair"));
                    m.put("side", rs.getString("side"));
                    m.put("baseCurrency", rs.getString("base_currency"));
                    m.put("quoteCurrency", rs.getString("quote_currency"));
                    m.put("baseAmount", rs.getBigDecimal("base_amount").toPlainString());
                    m.put("quoteAmount", rs.getBigDecimal("quote_amount").toPlainString());
                    m.put("quotedRate", rs.getBigDecimal("quoted_rate").toPlainString());
                    m.put("quoteId", rs.getString("quote_id"));
                    m.put("baseNostroId", rs.getString("base_nostro_id"));
                    m.put("quoteNostroId", rs.getString("quote_nostro_id"));
                    m.put("ledgerTransactionId", rs.getString("ledger_transaction_id"));
                    m.put("status", rs.getString("status"));
                    m.put("failureReason", rs.getString("failure_reason"));
                    m.put("submittedAt", rs.getTimestamp("submitted_at").toInstant().toString());
                    var p = rs.getTimestamp("posted_at");
                    m.put("postedAt", p == null ? null : p.toInstant().toString());
                    return m;
                },
                id);
    }

    public List<Map<String, Object>> recent(int limit) {
        int lim = Math.max(1, Math.min(500, limit));
        return jdbc.query(
                "SELECT id, action_key, submitted_by, pair, side, base_currency, quote_currency, "
                        + "base_amount, quote_amount, quoted_rate, quote_id, base_nostro_id, quote_nostro_id, "
                        + "ledger_transaction_id, status, failure_reason, submitted_at, posted_at "
                        + "FROM fx_hedge_actions ORDER BY submitted_at DESC LIMIT ?",
                (rs, n) -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("id", rs.getLong("id"));
                    m.put("actionKey", rs.getString("action_key"));
                    m.put("submittedBy", rs.getString("submitted_by"));
                    m.put("pair", rs.getString("pair"));
                    m.put("side", rs.getString("side"));
                    m.put("baseCurrency", rs.getString("base_currency"));
                    m.put("quoteCurrency", rs.getString("quote_currency"));
                    m.put("baseAmount", rs.getBigDecimal("base_amount").toPlainString());
                    m.put("quoteAmount", rs.getBigDecimal("quote_amount").toPlainString());
                    m.put("quotedRate", rs.getBigDecimal("quoted_rate").toPlainString());
                    m.put("quoteId", rs.getString("quote_id"));
                    m.put("baseNostroId", rs.getString("base_nostro_id"));
                    m.put("quoteNostroId", rs.getString("quote_nostro_id"));
                    m.put("ledgerTransactionId", rs.getString("ledger_transaction_id"));
                    m.put("status", rs.getString("status"));
                    m.put("failureReason", rs.getString("failure_reason"));
                    m.put("submittedAt", rs.getTimestamp("submitted_at").toInstant().toString());
                    var p = rs.getTimestamp("posted_at");
                    m.put("postedAt", p == null ? null : p.toInstant().toString());
                    return m;
                },
                lim);
    }
}
