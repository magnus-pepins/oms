package com.balh.oms.persistence;

import com.balh.oms.domain.Order;
import com.balh.oms.domain.Side;
import com.balh.oms.settlement.SettlementExecutionRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;

/**
 * Customer positions and history (slice 6 / §12.2). BUY fills increase total + pending buy settle;
 * SELL fills reduce pending buy first, then settled, then total, and add the fill qty to {@code
 * quantity_pending_sell_settle} until broker settlement clears it via {@link #recordSellSettled}.
 */
@Repository
public class PositionsRepository {

    private static final Logger log = LoggerFactory.getLogger(PositionsRepository.class);

    private static final String UPSERT_BUY_SQL = """
            INSERT INTO positions (
                account_id, instrument_symbol, custody_account_id,
                quantity_total, quantity_pending_buy_settle, updated_at
            ) VALUES (
                :account_id, :symbol, :custody_id,
                :qty, :qty, NOW()
            )
            ON CONFLICT (account_id, instrument_symbol, custody_account_id) DO UPDATE SET
                quantity_total = positions.quantity_total + EXCLUDED.quantity_total,
                quantity_pending_buy_settle = positions.quantity_pending_buy_settle
                    + EXCLUDED.quantity_pending_buy_settle,
                updated_at = NOW()
            """;

    private static final String SELL_UPDATE_SQL = """
            UPDATE positions SET
                quantity_pending_buy_settle = quantity_pending_buy_settle
                    - LEAST(quantity_pending_buy_settle, :qty),
                quantity_settled = quantity_settled - LEAST(
                    GREATEST(0, quantity_settled),
                    :qty - LEAST(quantity_pending_buy_settle, :qty)
                ),
                quantity_total = quantity_total - :qty,
                quantity_pending_sell_settle = quantity_pending_sell_settle + :qty,
                updated_at = NOW()
            WHERE account_id = :account_id
              AND instrument_symbol = :symbol
              AND custody_account_id = :custody_id
              AND quantity_total >= :qty
            """;

    private static final String SETTLE_SELL_SQL = """
            UPDATE positions SET
                quantity_pending_sell_settle = quantity_pending_sell_settle - :qty,
                updated_at = NOW()
            WHERE account_id = :account_id
              AND instrument_symbol = :symbol
              AND custody_account_id = :custody_id
              AND quantity_pending_sell_settle >= :qty
            """;

    private static final String SETTLE_BUY_SQL = """
            UPDATE positions SET
                quantity_pending_buy_settle = quantity_pending_buy_settle - :qty,
                quantity_settled = quantity_settled + :qty,
                updated_at = NOW()
            WHERE account_id = :account_id
              AND instrument_symbol = :symbol
              AND custody_account_id = :custody_id
              AND quantity_pending_buy_settle >= :qty
            """;

    private static final String SELECT_POSITION_PB_SETTLED = """
            SELECT quantity_pending_buy_settle, quantity_settled FROM positions
            WHERE account_id = :account_id AND instrument_symbol = :symbol AND custody_account_id = :custody_id
            """;

    private static final String SELECT_QUANTITY_TOTAL = """
            SELECT COALESCE(quantity_total, 0) AS quantity_total FROM positions
            WHERE account_id = :account_id AND instrument_symbol = :symbol AND custody_account_id = :custody_id
            """;

    private static final String REVERT_BUY_MARK_FAILED_SQL = """
            UPDATE positions SET
                quantity_total = quantity_total - :qty,
                quantity_pending_buy_settle = quantity_pending_buy_settle - :qty,
                updated_at = NOW()
            WHERE account_id = :account_id
              AND instrument_symbol = :symbol
              AND custody_account_id = :custody_id
              AND quantity_total >= :qty
              AND quantity_pending_buy_settle >= :qty
            """;

    private static final String REVERT_SELL_MARK_FAILED_SQL = """
            UPDATE positions SET
                quantity_pending_buy_settle = quantity_pending_buy_settle + :fpb,
                quantity_settled = quantity_settled + :fst,
                quantity_total = quantity_total + :qty,
                quantity_pending_sell_settle = quantity_pending_sell_settle - :qty,
                updated_at = NOW()
            WHERE account_id = :account_id
              AND instrument_symbol = :symbol
              AND custody_account_id = :custody_id
              AND quantity_pending_sell_settle >= :qty
            """;

    private static final String INSERT_HISTORY_SQL = """
            INSERT INTO position_history (
                account_id, instrument_symbol, custody_account_id,
                event_type, quantity_delta, execution_id, payload_json
            ) VALUES (
                :account_id, :symbol, :custody_id,
                :event_type, :delta, :execution_id, CAST(:payload AS JSONB)
            )
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public PositionsRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Applies a trade fill to {@code positions} + {@code position_history} (same txn as caller).
     *
     * @return for SELL fills only, the split applied to pending-buy vs settled columns (persist on
     *     {@code executions} for mark-failed unwind); empty for BUY or when no position row was updated
     */
    public Optional<SellFillPositionSplit> recordTradeFill(
            Order order, long executionId, BigDecimal fillQuantity, UUID custodyAccountId) {
        if (fillQuantity == null || fillQuantity.compareTo(BigDecimal.ZERO) <= 0) {
            return Optional.empty();
        }
        String sym = order.instrumentSymbol() == null ? "" : order.instrumentSymbol().trim();
        if (sym.isEmpty()) {
            return Optional.empty();
        }
        if (order.side() == Side.BUY) {
            jdbc.update(
                    UPSERT_BUY_SQL,
                    new MapSqlParameterSource()
                            .addValue("account_id", order.accountId())
                            .addValue("symbol", sym)
                            .addValue("custody_id", custodyAccountId)
                            .addValue("qty", fillQuantity));
            insertHistory(
                    order.accountId(),
                    sym,
                    custodyAccountId,
                    "TRADE_BUY_FILL",
                    fillQuantity,
                    executionId);
            return Optional.empty();
        }
        SellFillPositionSplit split = readSellFillSplitBeforeUpdate(order.accountId(), sym, custodyAccountId, fillQuantity);
        int updated = jdbc.update(
                SELL_UPDATE_SQL,
                new MapSqlParameterSource()
                        .addValue("account_id", order.accountId())
                        .addValue("symbol", sym)
                        .addValue("custody_id", custodyAccountId)
                        .addValue("qty", fillQuantity));
        if (updated > 0) {
            insertHistory(
                    order.accountId(),
                    sym,
                    custodyAccountId,
                    "TRADE_SELL_FILL",
                    fillQuantity.negate(),
                    executionId);
            return Optional.of(split);
        }
        return Optional.empty();
    }

    /**
     * Reverses {@link #recordTradeFill} for a non-terminal {@code TRADE} when operator mark-failed runs
     * (§12.7). SELL requires {@code executions.sell_position_from_*} populated when the fill was applied.
     */
    public void revertPositionForMarkTradeFailed(SettlementExecutionRow snap, UUID custodyAccountId) {
        BigDecimal q = snap.lastQuantity();
        if (q == null || q.compareTo(BigDecimal.ZERO) <= 0) {
            return;
        }
        String sym = snap.instrumentSymbol() == null ? "" : snap.instrumentSymbol().trim();
        if (sym.isEmpty()) {
            return;
        }
        String side = snap.side() == null ? "" : snap.side().trim();
        if ("BUY".equalsIgnoreCase(side)) {
            int updated = jdbc.update(
                    REVERT_BUY_MARK_FAILED_SQL,
                    new MapSqlParameterSource()
                            .addValue("account_id", snap.accountId())
                            .addValue("symbol", sym)
                            .addValue("custody_id", custodyAccountId)
                            .addValue("qty", q));
            if (updated != 1) {
                throw new IllegalStateException(
                        "buy mark-failed unwind expected 1 position row, got %s (account=%s symbol=%s execution=%s)"
                                .formatted(updated, snap.accountId(), sym, snap.executionId()));
            }
            insertHistory(snap.accountId(), sym, custodyAccountId, "MARK_FAILED_UNWIND_BUY", q.negate(), snap.executionId());
        } else if ("SELL".equalsIgnoreCase(side)) {
            if (snap.sellPositionFromPendingBuy() == null && snap.sellPositionFromSettled() == null) {
                log.warn(
                        "sell mark-failed position unwind skipped (missing sell_position split) executionId={}",
                        snap.executionId());
                return;
            }
            BigDecimal fpb =
                    snap.sellPositionFromPendingBuy() == null ? BigDecimal.ZERO : snap.sellPositionFromPendingBuy();
            BigDecimal fst =
                    snap.sellPositionFromSettled() == null ? BigDecimal.ZERO : snap.sellPositionFromSettled();
            int updated = jdbc.update(
                    REVERT_SELL_MARK_FAILED_SQL,
                    new MapSqlParameterSource()
                            .addValue("account_id", snap.accountId())
                            .addValue("symbol", sym)
                            .addValue("custody_id", custodyAccountId)
                            .addValue("qty", q)
                            .addValue("fpb", fpb)
                            .addValue("fst", fst));
            if (updated != 1) {
                throw new IllegalStateException(
                        "sell mark-failed unwind expected 1 position row, got %s (account=%s symbol=%s execution=%s)"
                                .formatted(updated, snap.accountId(), sym, snap.executionId()));
            }
            insertHistory(snap.accountId(), sym, custodyAccountId, "MARK_FAILED_UNWIND_SELL", q, snap.executionId());
        }
    }

    private SellFillPositionSplit readSellFillSplitBeforeUpdate(
            UUID accountId, String sym, UUID custodyAccountId, BigDecimal fillQuantity) {
        var params = new MapSqlParameterSource()
                .addValue("account_id", accountId)
                .addValue("symbol", sym)
                .addValue("custody_id", custodyAccountId);
        List<SellFillPositionSplit> rows = jdbc.query(
                SELECT_POSITION_PB_SETTLED,
                params,
                (rs, rowNum) -> {
                    BigDecimal pb = rs.getBigDecimal("quantity_pending_buy_settle");
                    BigDecimal settled = rs.getBigDecimal("quantity_settled");
                    return computeSellFillSplit(pb, settled, fillQuantity);
                });
        if (rows.isEmpty()) {
            return computeSellFillSplit(BigDecimal.ZERO, BigDecimal.ZERO, fillQuantity);
        }
        return rows.getFirst();
    }

    static SellFillPositionSplit computeSellFillSplit(BigDecimal pendingBuy, BigDecimal settled, BigDecimal qty) {
        BigDecimal pb = pendingBuy == null ? BigDecimal.ZERO : pendingBuy;
        BigDecimal st = settled == null ? BigDecimal.ZERO : settled;
        BigDecimal pbDec = pb.min(qty);
        BigDecimal settledDec = st.max(BigDecimal.ZERO).min(qty.subtract(pbDec));
        return new SellFillPositionSplit(pbDec, settledDec);
    }

    /**
     * Moves {@code quantity_pending_buy_settle} → {@code quantity_settled} for a BUY leg (§12.3 terminal).
     */
    public void recordBuySettled(
            UUID accountId, String instrumentSymbol, UUID custodyAccountId, BigDecimal settleQuantity, long executionId) {
        if (settleQuantity == null || settleQuantity.compareTo(BigDecimal.ZERO) <= 0) {
            return;
        }
        String sym = instrumentSymbol == null ? "" : instrumentSymbol.trim();
        if (sym.isEmpty()) {
            return;
        }
        int updated = jdbc.update(
                SETTLE_BUY_SQL,
                new MapSqlParameterSource()
                        .addValue("account_id", accountId)
                        .addValue("symbol", sym)
                        .addValue("custody_id", custodyAccountId)
                        .addValue("qty", settleQuantity));
        if (updated != 1) {
            throw new IllegalStateException(
                    "buy settle expected 1 position row, got %s (account=%s symbol=%s execution=%s)"
                            .formatted(updated, accountId, sym, executionId));
        }
        insertHistory(accountId, sym, custodyAccountId, "SETTLEMENT_BUY_SETTLED", settleQuantity, executionId);
    }

    /**
     * Clears {@code quantity_pending_sell_settle} for a SELL leg after broker settlement pipeline reaches
     * {@code settled} (§12.3 terminal).
     */
    public void recordSellSettled(
            UUID accountId, String instrumentSymbol, UUID custodyAccountId, BigDecimal settleQuantity, long executionId) {
        if (settleQuantity == null || settleQuantity.compareTo(BigDecimal.ZERO) <= 0) {
            return;
        }
        String sym = instrumentSymbol == null ? "" : instrumentSymbol.trim();
        if (sym.isEmpty()) {
            return;
        }
        int updated = jdbc.update(
                SETTLE_SELL_SQL,
                new MapSqlParameterSource()
                        .addValue("account_id", accountId)
                        .addValue("symbol", sym)
                        .addValue("custody_id", custodyAccountId)
                        .addValue("qty", settleQuantity));
        if (updated != 1) {
            throw new IllegalStateException(
                    "sell settle expected 1 position row, got %s (account=%s symbol=%s execution=%s)"
                            .formatted(updated, accountId, sym, executionId));
        }
        insertHistory(accountId, sym, custodyAccountId, "SETTLEMENT_SELL_SETTLED", settleQuantity, executionId);
    }

    public record SettledHolder(UUID accountId, BigDecimal quantitySettled) {}

    /** Holders with settled quantity on record date (corporate-action entitlement calc). */
    public List<SettledHolder> listSettledHolders(String instrumentSymbol, UUID custodyAccountId) {
        String sym = instrumentSymbol == null ? "" : instrumentSymbol.trim().toUpperCase(Locale.ROOT);
        return jdbc.query(
                """
                        SELECT account_id, quantity_settled
                        FROM positions
                        WHERE instrument_symbol = :symbol
                          AND custody_account_id = :custody
                          AND quantity_settled > 0
                        """,
                new MapSqlParameterSource()
                        .addValue("symbol", sym)
                        .addValue("custody", custodyAccountId),
                (rs, rowNum) ->
                        new SettledHolder(
                                (UUID) rs.getObject("account_id"), rs.getBigDecimal("quantity_settled")));
    }

    /** Applies forward split ratio to settled and total quantities (gap plan §5.9). */
    public void applyCorporateActionSplit(
            UUID accountId, String instrumentSymbol, UUID custodyAccountId, BigDecimal ratio) {
        String sym = instrumentSymbol == null ? "" : instrumentSymbol.trim().toUpperCase(Locale.ROOT);
        int updated =
                jdbc.update(
                        """
                                UPDATE positions
                                SET quantity_total = quantity_total * :ratio,
                                    quantity_settled = quantity_settled * :ratio,
                                    updated_at = NOW()
                                WHERE account_id = :accountId
                                  AND instrument_symbol = :symbol
                                  AND custody_account_id = :custody
                                """,
                        new MapSqlParameterSource()
                                .addValue("ratio", ratio)
                                .addValue("accountId", accountId)
                                .addValue("symbol", sym)
                                .addValue("custody", custodyAccountId));
        if (updated != 1) {
            throw new IllegalStateException(
                    "corporate action split expected 1 position row, got " + updated);
        }
    }

    /**
     * Credits settled quantity on a symbol, creating the row when absent (merger survivor / spin-off child).
     */
    public void applyCorporateActionCreditPosition(
            UUID accountId, String instrumentSymbol, UUID custodyAccountId, BigDecimal settledQty) {
        String sym = instrumentSymbol == null ? "" : instrumentSymbol.trim().toUpperCase(Locale.ROOT);
        if (settledQty == null || settledQty.signum() <= 0) {
            return;
        }
        jdbc.update(
                """
                        INSERT INTO positions (
                            account_id, instrument_symbol, custody_account_id,
                            quantity_total, quantity_settled, updated_at
                        ) VALUES (
                            :accountId, :symbol, :custody, :qty, :qty, NOW()
                        )
                        ON CONFLICT (account_id, instrument_symbol, custody_account_id) DO UPDATE SET
                            quantity_total = positions.quantity_total + EXCLUDED.quantity_total,
                            quantity_settled = positions.quantity_settled + EXCLUDED.quantity_settled,
                            updated_at = NOW()
                        """,
                new MapSqlParameterSource()
                        .addValue("accountId", accountId)
                        .addValue("symbol", sym)
                        .addValue("custody", custodyAccountId)
                        .addValue("qty", settledQty));
    }

    /** Zeros all quantity columns for a symbol (bankruptcy / merger source leg). */
    public void applyCorporateActionZeroOut(
            UUID accountId, String instrumentSymbol, UUID custodyAccountId) {
        String sym = instrumentSymbol == null ? "" : instrumentSymbol.trim().toUpperCase(Locale.ROOT);
        jdbc.update(
                """
                        UPDATE positions
                        SET quantity_total = 0,
                            quantity_settled = 0,
                            quantity_pending_buy_settle = 0,
                            quantity_pending_sell_settle = 0,
                            updated_at = NOW()
                        WHERE account_id = :accountId
                          AND instrument_symbol = :symbol
                          AND custody_account_id = :custody
                        """,
                new MapSqlParameterSource()
                        .addValue("accountId", accountId)
                        .addValue("symbol", sym)
                        .addValue("custody", custodyAccountId));
    }

    /** Sets settled + total to an explicit post-action quantity (spin-off parent retention). */
    public void applyCorporateActionSetSettledQuantity(
            UUID accountId, String instrumentSymbol, UUID custodyAccountId, BigDecimal settledQty) {
        String sym = instrumentSymbol == null ? "" : instrumentSymbol.trim().toUpperCase(Locale.ROOT);
        BigDecimal qty = settledQty == null ? BigDecimal.ZERO : settledQty;
        int updated =
                jdbc.update(
                        """
                                UPDATE positions
                                SET quantity_total = :qty,
                                    quantity_settled = :qty,
                                    quantity_pending_buy_settle = 0,
                                    quantity_pending_sell_settle = 0,
                                    updated_at = NOW()
                                WHERE account_id = :accountId
                                  AND instrument_symbol = :symbol
                                  AND custody_account_id = :custody
                                """,
                        new MapSqlParameterSource()
                                .addValue("qty", qty)
                                .addValue("accountId", accountId)
                                .addValue("symbol", sym)
                                .addValue("custody", custodyAccountId));
        if (updated != 1) {
            throw new IllegalStateException(
                    "corporate action set quantity expected 1 position row, got " + updated);
        }
    }

    /** Renames {@code instrument_symbol} for all settled quantity (gap plan §5.9 mandatory symbol change). */
    public void applyCorporateActionSymbolRename(
            UUID accountId, String oldSymbol, String newSymbol, UUID custodyAccountId) {
        String from = oldSymbol == null ? "" : oldSymbol.trim().toUpperCase(Locale.ROOT);
        String to = newSymbol == null ? "" : newSymbol.trim().toUpperCase(Locale.ROOT);
        int updated =
                jdbc.update(
                        """
                                UPDATE positions
                                SET instrument_symbol = :newSymbol, updated_at = NOW()
                                WHERE account_id = :accountId
                                  AND instrument_symbol = :oldSymbol
                                  AND custody_account_id = :custody
                                """,
                        new MapSqlParameterSource()
                                .addValue("newSymbol", to)
                                .addValue("accountId", accountId)
                                .addValue("oldSymbol", from)
                                .addValue("custody", custodyAccountId));
        if (updated != 1) {
            throw new IllegalStateException(
                    "corporate action symbol rename expected 1 position row, got " + updated);
        }
    }

    /**
     * Read-side projection: positions joined with a lifetime-BUY cost aggregation derived from
     * {@code executions} so the customer "My positions" view can show a real "Invested" value.
     *
     * <h3>Why a join rather than a denormalised column</h3>
     * The {@code positions} row used to have an {@code avg_cost_amount} column (V11) that no
     * projector ever wrote, and we briefly considered closing that gap. We deliberately did not,
     * for two reasons that bite specifically in this domain:
     * <ol>
     *   <li><strong>Cancellation is trivial here</strong>: a mark-failed execution simply gets
     *       excluded by the {@code settlement_status NOT IN ('failed')} filter below. The
     *       denormalised path would have to <em>invert</em> a weighted-average update, which
     *       requires snapshotting the per-fill avg-before-fill onto {@code executions} and
     *       carefully reversing it (drift-prone, and the recovery path is "run this same join
     *       and overwrite the column" — so the join is authoritative anyway).</li>
     *   <li><strong>Volume is retail-shaped</strong>: a position accumulates O(10) fills over its
     *       lifetime, not O(1000). With {@code idx_executions_order_time} and the indexed
     *       {@code account_id}, this aggregation is microseconds — well below the network /
     *       BFF cost.</li>
     * </ol>
     * The V34 migration drops the now-redundant column.
     *
     * <h3>Cost basis semantics</h3>
     * <ul>
     *   <li>{@code buy_total_qty} / {@code buy_total_cost} — sum across all historical BUY
     *       {@code TRADE} executions for the (account, symbol) <em>that are not marked-failed</em>.
     *       All other settlement states (executed / matched / settling / settled) count: the
     *       money is committed at TRADE time via the inflight hold, so a not-yet-settled fill is
     *       still real cost basis from the customer's perspective. The filter intentionally uses
     *       {@code NOT IN ('failed')} (allow-by-default) rather than an explicit allowlist so a
     *       newly added settlement state doesn't silently get excluded from cost basis.</li>
     *   <li>{@code avg_buy_fill_price} = {@code buy_total_cost / buy_total_qty} (lifetime
     *       volume-weighted average BUY price). For BUY-only history this is just "what the
     *       customer paid"; under partial sells from one lot it represents the average price of
     *       the remaining shares (true FIFO/LIFO lot accounting is a separate follow-up tied to
     *       the realised-gain tax-reporting decision).</li>
     *   <li>{@code invested_amount} = {@code quantity_total * avg_buy_fill_price}. We multiply
     *       by the live {@code quantity_total} (not by {@code buy_total_qty}) so that a fully
     *       closed position shows {@code 0} invested rather than the historical cost of long-gone
     *       shares. Holding 1 of 2 originally-bought shares = half the original cost.</li>
     * </ul>
     */
    private static final String SELECT_BY_ACCOUNT_SQL = """
            WITH buy_cost AS (
                SELECT o.account_id,
                       o.instrument_symbol,
                       SUM(e.last_quantity)              AS buy_total_qty,
                       SUM(e.last_quantity * e.last_price) AS buy_total_cost
                FROM executions e
                JOIN orders o ON o.id = e.order_id
                WHERE o.account_id = :account_id
                  AND o.side = 'BUY'
                  AND e.exec_type = 'TRADE'
                  AND e.last_price IS NOT NULL
                  AND e.settlement_status NOT IN ('failed')
                GROUP BY o.account_id, o.instrument_symbol
            )
            SELECT p.instrument_symbol,
                   p.custody_account_id,
                   p.quantity_total,
                   p.quantity_settled,
                   p.quantity_pending_buy_settle,
                   p.quantity_pending_sell_settle,
                   p.currency,
                   p.updated_at,
                   CASE
                       WHEN bc.buy_total_qty IS NULL OR bc.buy_total_qty = 0 THEN NULL
                       ELSE bc.buy_total_cost / bc.buy_total_qty
                   END AS avg_buy_fill_price,
                   CASE
                       WHEN bc.buy_total_qty IS NULL OR bc.buy_total_qty = 0 THEN NULL
                       ELSE p.quantity_total * (bc.buy_total_cost / bc.buy_total_qty)
                   END AS invested_amount
            FROM positions p
            LEFT JOIN buy_cost bc
                   ON bc.account_id        = p.account_id
                  AND bc.instrument_symbol = p.instrument_symbol
            WHERE p.account_id = :account_id
              AND p.quantity_total > 0
            ORDER BY p.instrument_symbol
            """;

    /**
     * Read-side projection of {@code positions} for a single account: rows with
     * {@code quantity_total > 0} only (closed positions filter out at the SQL boundary
     * so callers cannot accidentally render zero-quantity rows). Powers the customer
     * "My positions" view; ordered by {@code instrument_symbol} for deterministic UI.
     *
     * <p>Each row carries {@code averageFillPrice} and {@code investedAmount} derived from the
     * {@code executions} table (see {@link #SELECT_BY_ACCOUNT_SQL} for the exact semantics).
     * Both are {@code null} when no BUY {@code TRADE} executions exist (e.g. a synthetic /
     * seed-data position, or every prior fill was marked-failed) — callers should treat
     * {@code null} as "cost basis unknown" rather than as {@code 0}.
     */
    public List<PositionRow> findByAccountId(UUID accountId) {
        return jdbc.query(
                SELECT_BY_ACCOUNT_SQL,
                new MapSqlParameterSource().addValue("account_id", accountId),
                (rs, rowNum) ->
                        new PositionRow(
                                rs.getString("instrument_symbol"),
                                (UUID) rs.getObject("custody_account_id"),
                                rs.getBigDecimal("quantity_total"),
                                rs.getBigDecimal("quantity_settled"),
                                rs.getBigDecimal("quantity_pending_buy_settle"),
                                rs.getBigDecimal("quantity_pending_sell_settle"),
                                rs.getString("currency"),
                                rs.getTimestamp("updated_at").toInstant(),
                                rs.getBigDecimal("avg_buy_fill_price"),
                                rs.getBigDecimal("invested_amount")));
    }

    private static final String SELECT_AVG_BUY_FILL_PRICE =
            """
            SELECT CASE
                       WHEN SUM(e.last_quantity) IS NULL OR SUM(e.last_quantity) = 0 THEN NULL
                       ELSE SUM(e.last_quantity * e.last_price) / SUM(e.last_quantity)
                   END AS avg_buy_fill_price
            FROM executions e
            JOIN orders o ON o.id = e.order_id
            WHERE o.account_id = :account_id
              AND o.instrument_symbol = :symbol
              AND o.side = 'BUY'
              AND e.exec_type = 'TRADE'
              AND e.last_price IS NOT NULL
              AND e.settlement_status NOT IN ('failed')
            """;

    /** Lifetime volume-weighted average BUY price for corporate-action cost-basis allocation. */
    public java.util.Optional<BigDecimal> findAverageBuyFillPrice(UUID accountId, String symbol) {
        BigDecimal avg =
                jdbc.queryForObject(
                        SELECT_AVG_BUY_FILL_PRICE,
                        new MapSqlParameterSource()
                                .addValue("account_id", accountId)
                                .addValue("symbol", symbol.trim().toUpperCase(java.util.Locale.ROOT)),
                        BigDecimal.class);
        return java.util.Optional.ofNullable(avg);
    }

    /** Read-side row for {@link #findByAccountId(UUID)}. */
    public record PositionRow(
            String instrumentSymbol,
            UUID custodyAccountId,
            BigDecimal quantityTotal,
            BigDecimal quantitySettled,
            BigDecimal quantityPendingBuySettle,
            BigDecimal quantityPendingSellSettle,
            String currency,
            java.time.Instant updatedAt,
            BigDecimal averageFillPrice,
            BigDecimal investedAmount) {}

    public record AccountPositionKeyRow(
            UUID accountId,
            String instrumentSymbol,
            UUID custodyAccountId,
            BigDecimal quantityTotal,
            BigDecimal quantitySettled,
            BigDecimal quantityPendingBuySettle,
            BigDecimal quantityPendingSellSettle) {}

    private static final String SELECT_NONZERO_BY_ACCOUNTS =
            """
                    SELECT account_id, instrument_symbol, custody_account_id,
                           quantity_total, quantity_settled,
                           quantity_pending_buy_settle, quantity_pending_sell_settle
                    FROM positions
                    WHERE account_id IN (:account_ids)
                      AND (quantity_total <> 0
                           OR quantity_settled <> 0
                           OR quantity_pending_buy_settle <> 0
                           OR quantity_pending_sell_settle <> 0)
                    """;

    private static final String SELECT_NONZERO_BY_SYMBOL =
            """
                    SELECT account_id, instrument_symbol, custody_account_id,
                           quantity_total, quantity_settled,
                           quantity_pending_buy_settle, quantity_pending_sell_settle
                    FROM positions
                    WHERE instrument_symbol = :symbol
                      AND quantity_total <> 0
                    """;

    /** Phase B: holders with non-zero quantity on a contract at resolution time. */
    public List<AccountPositionKeyRow> findNonZeroPositionsForSymbol(String instrumentSymbol) {
        String sym = instrumentSymbol == null ? "" : instrumentSymbol.trim();
        if (sym.isEmpty()) {
            return List.of();
        }
        return jdbc.query(
                SELECT_NONZERO_BY_SYMBOL,
                new MapSqlParameterSource("symbol", sym),
                (rs, rowNum) ->
                        new AccountPositionKeyRow(
                                (UUID) rs.getObject("account_id"),
                                rs.getString("instrument_symbol"),
                                (UUID) rs.getObject("custody_account_id"),
                                rs.getBigDecimal("quantity_total"),
                                rs.getBigDecimal("quantity_settled"),
                                rs.getBigDecimal("quantity_pending_buy_settle"),
                                rs.getBigDecimal("quantity_pending_sell_settle")));
    }

    /** Non-zero positions for accounts included in a broker snapshot reconcile pass. */
    public List<AccountPositionKeyRow> findNonZeroPositionsForAccounts(java.util.Collection<UUID> accountIds) {
        if (accountIds == null || accountIds.isEmpty()) {
            return List.of();
        }
        return jdbc.query(
                SELECT_NONZERO_BY_ACCOUNTS,
                new MapSqlParameterSource("account_ids", accountIds),
                (rs, rowNum) ->
                        new AccountPositionKeyRow(
                                (UUID) rs.getObject("account_id"),
                                rs.getString("instrument_symbol"),
                                (UUID) rs.getObject("custody_account_id"),
                                rs.getBigDecimal("quantity_total"),
                                rs.getBigDecimal("quantity_settled"),
                                rs.getBigDecimal("quantity_pending_buy_settle"),
                                rs.getBigDecimal("quantity_pending_sell_settle")));
    }

    public BigDecimal findQuantityTotal(UUID accountId, String instrumentSymbol, UUID custodyAccountId) {
        String sym = instrumentSymbol == null ? "" : instrumentSymbol.trim();
        if (sym.isEmpty()) {
            return BigDecimal.ZERO;
        }
        List<BigDecimal> rows =
                jdbc.query(
                        SELECT_QUANTITY_TOTAL,
                        new MapSqlParameterSource()
                                .addValue("account_id", accountId)
                                .addValue("symbol", sym)
                                .addValue("custody_id", custodyAccountId),
                        (rs, rowNum) -> rs.getBigDecimal("quantity_total"));
        return rows.isEmpty() ? BigDecimal.ZERO : rows.getFirst();
    }

    /** Positions with non-zero pending buy or sell settlement quantity (ISK close guard input). */
    public int countPositionsWithPendingSettlement(UUID accountId) {
        if (accountId == null) {
            return 0;
        }
        Integer count =
                jdbc.queryForObject(
                        """
                                SELECT COUNT(*)::int FROM positions
                                WHERE account_id = :accountId
                                  AND (quantity_pending_buy_settle <> 0
                                       OR quantity_pending_sell_settle <> 0)
                                """,
                        new MapSqlParameterSource("accountId", accountId),
                        Integer.class);
        return count == null ? 0 : count;
    }

    private void insertHistory(
            UUID accountId,
            String symbol,
            UUID custodyId,
            String eventType,
            BigDecimal delta,
            long executionId) {
        jdbc.update(
                INSERT_HISTORY_SQL,
                new MapSqlParameterSource()
                        .addValue("account_id", accountId)
                        .addValue("symbol", symbol)
                        .addValue("custody_id", custodyId)
                        .addValue("event_type", eventType)
                        .addValue("delta", delta)
                        .addValue("execution_id", executionId)
                        .addValue("payload", "{}"));
    }
}
