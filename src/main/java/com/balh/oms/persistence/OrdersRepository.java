package com.balh.oms.persistence;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.OrderAdmittedEvent;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.RejectCode;
import com.balh.oms.domain.Side;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Postgres-backed repository for the {@code orders} table.
 *
 * <p>All authoritative state changes go through here. Mutations use CAS on
 * {@code version}; if the update affects 0 rows, the caller must reload the
 * row (something else won the race).
 *
 * <p>Inserts rely on {@code UNIQUE (account_id, client_idempotency_key)} for
 * idempotency. Duplicates surface as {@link DuplicateOrderException} so the
 * controller can return the existing order rather than creating a new one.
 */
@Repository
public class OrdersRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public OrdersRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public static final class DuplicateOrderException extends RuntimeException {
        public DuplicateOrderException(String message) { super(message); }
    }

    // ON CONFLICT DO NOTHING keeps the JDBC transaction healthy on idempotent re-submits so the
    // caller can SELECT the pre-existing row in the same transaction. A raw duplicate-key error
    // would abort the transaction at the Postgres level (every later statement returns
    // "current transaction is aborted, commands ignored until end of transaction block").
    private static final String INSERT_SQL = """
            INSERT INTO orders (
                id, account_id, client_idempotency_key, shard_id, version,
                status, terminal_reason, side, instrument_symbol,
                quantity, limit_price, time_in_force, ord_type,
                received_at, accepted_at, terminal_at, account_id_hash, ledger_balance_id,
                cum_filled_quantity
            ) VALUES (
                :id, :account_id, :client_idempotency_key, :shard_id, 0,
                CAST(:status AS order_status), CAST(:terminal_reason AS reject_code),
                CAST(:side AS order_side), :instrument_symbol,
                :quantity, :limit_price, :time_in_force, :ord_type,
                :received_at, :accepted_at, :terminal_at, :account_id_hash, :ledger_balance_id,
                :cum_filled_quantity
            )
            ON CONFLICT (account_id, client_idempotency_key) DO NOTHING
            """;

    /**
     * Projector-only insert (slice 2d). Bare {@code ON CONFLICT DO NOTHING} swallows any unique
     * constraint violation — pkey OR {@code (account_id, client_idempotency_key)} — because the
     * projector is purely idempotent on replay and may race a concurrent writer (test
     * {@code TestPostgresProjectorSingleton}, sibling Spring context, restart-from-cursor) that
     * already committed the same row. The legacy ingress path keeps {@link #INSERT_SQL} and its
     * targeted {@code (account_id, client_idempotency_key)} arbiter so callers there can
     * distinguish duplicate idempotency-key submissions from other constraint failures.
     */
    private static final String PROJECTOR_INSERT_SQL = """
            INSERT INTO orders (
                id, account_id, client_idempotency_key, shard_id, version,
                status, terminal_reason, side, instrument_symbol,
                quantity, limit_price, time_in_force, ord_type,
                received_at, accepted_at, terminal_at, account_id_hash, ledger_balance_id,
                cum_filled_quantity
            ) VALUES (
                :id, :account_id, :client_idempotency_key, :shard_id, 0,
                CAST(:status AS order_status), CAST(:terminal_reason AS reject_code),
                CAST(:side AS order_side), :instrument_symbol,
                :quantity, :limit_price, :time_in_force, :ord_type,
                :received_at, :accepted_at, :terminal_at, :account_id_hash, :ledger_balance_id,
                :cum_filled_quantity
            )
            ON CONFLICT DO NOTHING
            """;

    private static final String SELECT_BY_ID_SQL = """
            SELECT id, account_id, client_idempotency_key, shard_id, version,
                   status::text AS status,
                   terminal_reason::text AS terminal_reason,
                   side::text AS side,
                   instrument_symbol, quantity, limit_price, time_in_force, ord_type,
                   received_at, accepted_at, terminal_at, account_id_hash, ledger_balance_id,
                   cum_filled_quantity
            FROM orders WHERE id = :id
            """;

    private static final String SELECT_BY_IDEMPOTENCY_SQL = """
            SELECT id, account_id, client_idempotency_key, shard_id, version,
                   status::text AS status,
                   terminal_reason::text AS terminal_reason,
                   side::text AS side,
                   instrument_symbol, quantity, limit_price, time_in_force, ord_type,
                   received_at, accepted_at, terminal_at, account_id_hash, ledger_balance_id,
                   cum_filled_quantity
            FROM orders
            WHERE account_id = :account_id AND client_idempotency_key = :key
            """;

    /**
     * Wed-demo (d2_fe_swap_oms): account-scoped list query for the customer-FE "my orders"
     * surface. Ordered newest-first by {@code received_at} so the most recent order tops the
     * list (matches Alpaca semantics the FE already renders). Bounded by {@code :limit} —
     * caller picks 50 today; tighten when we add server-side pagination.
     */
    private static final String SELECT_BY_ACCOUNT_SQL = """
            SELECT id, account_id, client_idempotency_key, shard_id, version,
                   status::text AS status,
                   terminal_reason::text AS terminal_reason,
                   side::text AS side,
                   instrument_symbol, quantity, limit_price, time_in_force, ord_type,
                   received_at, accepted_at, terminal_at, account_id_hash, ledger_balance_id,
                   cum_filled_quantity
            FROM orders
            WHERE account_id = :account_id
            ORDER BY received_at DESC
            LIMIT :limit
            """;

    // Active snapshot: every order in a non-terminal status, regardless of received_at. This is
    // the GTC-correctness fix — a Good-Til-Cancel LIMIT placed weeks ago is still WORKING today,
    // and date-windowing it would render the operator blind to it on the live blotter. PENDING_NEW
    // is included so a freshly-admitted row that hasn't finished its first apply hop is visible.
    // Ordered newest-first so the cap, if hit, drops the oldest active rather than something the
    // operator likely cares about.
    private static final String SELECT_DESK_ACTIVE_SNAPSHOT_SQL = """
            SELECT id, account_id, instrument_symbol, side::text AS side, status::text AS status,
                   version, received_at, ord_type, quantity, limit_price, cum_filled_quantity
            FROM orders
            WHERE status IN ('PENDING_NEW', 'NEW', 'WORKING', 'PARTIALLY_FILLED')
            ORDER BY received_at DESC
            LIMIT :limit
            """;

    // Terminal snapshot: date-windowed view of FILLED/CANCELLED/REJECTED/EXPIRED orders. The
    // window arguments mirror the old SELECT_DESK_SNAPSHOT_SQL so the controller's existing
    // since/until clamp logic still applies. Spring JDBC binds java.sql.Timestamp -> timestamptz
    // when not null; `CAST(:max_received AS timestamptz) IS NULL` lets a single prepared
    // statement serve both since-only (open upper bound) and since+until (yesterday-style) calls.
    private static final String SELECT_DESK_TERMINAL_SNAPSHOT_SQL = """
            SELECT id, account_id, instrument_symbol, side::text AS side, status::text AS status,
                   version, received_at, ord_type, quantity, limit_price, cum_filled_quantity
            FROM orders
            WHERE status IN ('FILLED', 'CANCELLED', 'REJECTED', 'EXPIRED')
              AND received_at >= :min_received
              AND (CAST(:max_received AS timestamptz) IS NULL OR received_at < :max_received)
            ORDER BY received_at DESC
            LIMIT :limit
            """;

    private static final String UPDATE_CAS_SQL = """
            UPDATE orders
               SET status = CAST(:status AS order_status),
                   terminal_reason = CAST(:terminal_reason AS reject_code),
                   accepted_at = COALESCE(:accepted_at, accepted_at),
                   terminal_at = COALESCE(:terminal_at, terminal_at),
                   version = version + 1
             WHERE id = :id AND version = :expected_version
            """;

    private static final String UPDATE_FILL_CAS_SQL = """
            UPDATE orders
               SET cum_filled_quantity = :new_cum_filled,
                   status = CAST(:status AS order_status),
                   terminal_reason = CAST(:terminal_reason AS reject_code),
                   terminal_at = COALESCE(:terminal_at, terminal_at),
                   version = version + 1
             WHERE id = :id AND version = :expected_version
            """;

    private static final String UPDATE_REPLACE_CAS_SQL = """
            UPDATE orders
               SET quantity = :new_quantity,
                   limit_price = :new_limit_price,
                   status = CAST(:status AS order_status),
                   version = version + 1
             WHERE id = :id AND version = :expected_version
            """;

    public void insert(Order o) {
        int affected = jdbc.update(INSERT_SQL, params(o));
        if (affected == 0) {
            // ON CONFLICT (account_id, client_idempotency_key) suppressed the insert — already exists.
            throw new DuplicateOrderException(
                    "duplicate order for account=%s key=%s".formatted(
                            o.accountId(), o.clientIdempotencyKey()));
        }
    }

    /**
     * Phase 2 projector path: idempotent insert from a cluster-emitted {@link OrderAdmittedEvent}.
     *
     * <p>Unlike {@link #insert(Order)}, this method does not throw on duplicates — the projector reaches
     * here on every fresh admission, and a duplicate is the expected outcome whenever the projector
     * replays past its cursor on restart, or when an integration-test
     * {@code TestPostgresProjectorSingleton} has already projected the same event. {@link #INSERT_SQL}
     * arbitrates on {@code (account_id, client_idempotency_key)}, which is the canonical duplicate
     * check, but a concurrent insert that has already committed the matching pkey row first can race
     * past that arbiter and surface as an {@code orders_pkey} violation. We use a separate
     * {@code INSERT ... ON CONFLICT DO NOTHING} (no target) here so any unique-constraint
     * violation — pkey or {@code (account_id, client_idempotency_key)} — is suppressed idempotently.
     *
     * <p>Returns {@code true} when the row was inserted, {@code false} when ON CONFLICT swallowed it.
     */
    /**
     * Projector hot-path result: one scaled-parameter pass for both the INSERT and the in-memory
     * {@link Order} handed to {@link com.balh.oms.tailer.OrderControlAdmission}.
     */
    public record ProjectorAdmitInsert(boolean fresh, Order order) {}

    public boolean insertFromAdmittedEvent(OrderAdmittedEvent ev) {
        return insertFromAdmittedEventWithOrder(ev).fresh();
    }

    /**
     * Idempotent projector insert plus the matching in-memory {@link Order} (version 0,
     * {@link OrderStatus#PENDING_NEW}). Computes scaled quantity/limit once so
     * {@link #orderFromAdmittedEvent} does not repeat the same {@link BigDecimal} work on the hot
     * path.
     */
    public ProjectorAdmitInsert insertFromAdmittedEventWithOrder(OrderAdmittedEvent ev) {
        ProjectorAdmitFields fields = projectorAdmitFields(ev);
        boolean fresh = jdbc.update(PROJECTOR_INSERT_SQL, fields.params()) == 1;
        return new ProjectorAdmitInsert(fresh, fields.order());
    }

    /** Phase E: pin fee model + estimated taker fee quoted at cluster admit (PREDMKT orders). */
    public void pinPredictionMarketFeeAtAdmit(
            UUID orderId,
            String feeModelId,
            int feeScheduleVersion,
            BigDecimal estimatedFee,
            String feeCurrency) {
        jdbc.update(
                """
                        UPDATE orders
                           SET pinned_fee_model_id = :feeModelId,
                               pinned_fee_schedule_version = :feeScheduleVersion,
                               pinned_estimated_fee = :estimatedFee,
                               pinned_fee_currency = :feeCurrency
                         WHERE id = :id
                        """,
                new MapSqlParameterSource()
                        .addValue("id", orderId)
                        .addValue("feeModelId", feeModelId)
                        .addValue("feeScheduleVersion", feeScheduleVersion)
                        .addValue("estimatedFee", estimatedFee)
                        .addValue("feeCurrency", feeCurrency));
    }

    /**
     * In-memory {@link Order} view matching the row {@link #insertFromAdmittedEvent} just wrote
     * (version 0, {@link OrderStatus#PENDING_NEW}). Lets the projector hot path skip a
     * {@link #findById(java.util.UUID)} round-trip inside the same transaction.
     */
    public Order orderFromAdmittedEvent(OrderAdmittedEvent ev) {
        return projectorAdmitFields(ev).order();
    }

    private record ProjectorAdmitFields(MapSqlParameterSource params, Order order) {}

    private static ProjectorAdmitFields projectorAdmitFields(OrderAdmittedEvent ev) {
        BigDecimal quantity = BigDecimal.valueOf(ev.quantityScaled())
                .divide(BigDecimal.valueOf(AcceptOrderCommand.QUANTITY_SCALE), 10, RoundingMode.UNNECESSARY);
        BigDecimal limitPrice = ev.limitPriceScaledOrZero() == 0L
                ? null
                : BigDecimal.valueOf(ev.limitPriceScaledOrZero())
                        .divide(BigDecimal.valueOf(AcceptOrderCommand.PRICE_SCALE), 10, RoundingMode.UNNECESSARY);
        Instant receivedAt = nanosToInstant(ev.clientTimestampNanos());
        // ev.acceptedAtMillis() is the Aeron Cluster timestamp (epoch millis; ConsensusModule.Context
        // default TimeUnit.MILLISECONDS — verified in OmsClusterNodeBootstrap.buildConsensusModuleContext
        // which does not override timeUnit). Earlier code mistakenly fed it through nanosToInstant, which
        // collapsed accepted_at to year 1970. Phase 4j slice closes the OrderAdmittedEvent rename and the
        // matching arithmetic fix in one diff.
        Instant acceptedAt = Instant.ofEpochMilli(ev.acceptedAtMillis());
        UUID accountId = UUID.fromString(ev.accountId());
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", ev.orderId())
                .addValue("account_id", accountId)
                .addValue("client_idempotency_key", ev.clientIdempotencyKey())
                .addValue("shard_id", ev.shardId())
                .addValue("status", OrderStatus.PENDING_NEW.name())
                .addValue("terminal_reason", null)
                .addValue("side", AcceptOrderCommand.sideName(ev.side()))
                .addValue("instrument_symbol", ev.instrumentSymbol())
                .addValue("quantity", quantity)
                .addValue("limit_price", limitPrice)
                .addValue("time_in_force", AcceptOrderCommand.timeInForceName(ev.timeInForceCode()))
                .addValue("ord_type", AcceptOrderCommand.ordTypeName(ev.ordTypeCode()))
                .addValue("received_at", Timestamp.from(receivedAt))
                .addValue("accepted_at", Timestamp.from(acceptedAt))
                .addValue("terminal_at", null)
                .addValue("account_id_hash", ev.accountIdHash())
                .addValue("ledger_balance_id", ev.ledgerBalanceIdOrNull())
                .addValue("cum_filled_quantity", BigDecimal.ZERO);
        Order order = new Order(
                ev.orderId(),
                accountId,
                ev.clientIdempotencyKey(),
                ev.shardId(),
                0,
                OrderStatus.PENDING_NEW,
                null,
                Side.valueOf(AcceptOrderCommand.sideName(ev.side())),
                ev.instrumentSymbol(),
                quantity,
                limitPrice,
                AcceptOrderCommand.timeInForceName(ev.timeInForceCode()),
                receivedAt,
                acceptedAt,
                null,
                ev.accountIdHash(),
                ev.ledgerBalanceIdOrNull(),
                BigDecimal.ZERO,
                AcceptOrderCommand.ordTypeName(ev.ordTypeCode()));
        return new ProjectorAdmitFields(params, order);
    }

    private static Instant nanosToInstant(long epochNanos) {
        long seconds = Math.floorDiv(epochNanos, 1_000_000_000L);
        long nanoAdjustment = Math.floorMod(epochNanos, 1_000_000_000L);
        return Instant.ofEpochSecond(seconds, nanoAdjustment);
    }

    // sideName / timeInForceName moved to AcceptOrderCommand (Phase 4 Tier 2.5 phase D-3) so
    // the projector's domain-envelope-from-OrderAdmittedEvent path shares one canonical mapping
    // with this orders-row mapping. Calls below are static imports through AcceptOrderCommand.

    public Optional<Order> findById(UUID id) {
        var rows = jdbc.query(SELECT_BY_ID_SQL,
                new MapSqlParameterSource("id", id),
                ROW_MAPPER);
        return rows.stream().findFirst();
    }

    public Optional<Order> findByIdempotency(UUID accountId, String idempotencyKey) {
        var params = new MapSqlParameterSource()
                .addValue("account_id", accountId)
                .addValue("key", idempotencyKey);
        var rows = jdbc.query(SELECT_BY_IDEMPOTENCY_SQL, params, ROW_MAPPER);
        return rows.stream().findFirst();
    }

    /**
     * Wed-demo (d2_fe_swap_oms): newest-first list of {@code orders} for a single account,
     * bounded by {@code limit}. Used by {@code GET /internal/v1/orders?accountId=...} which
     * the customer-FE BFF calls when {@code OMS_BROKER_TRADE_LINK} is on. The query reads
     * straight from Postgres — already the source of truth for the read DTO — so no extra
     * load on the cluster.
     */
    public List<Order> findByAccount(UUID accountId, int limit) {
        var params = new MapSqlParameterSource()
                .addValue("account_id", accountId)
                .addValue("limit", Math.max(1, limit));
        return jdbc.query(SELECT_BY_ACCOUNT_SQL, params, ROW_MAPPER);
    }

    /** Bounded recent rows for desk / attendant snapshot (internal API only). */
    /**
     * Wed-demo extension. Trading-desk blotter needs price + volume per row to be useful for
     * cross-customer monitoring (operators currently have to click into each order to see
     * the numbers). Added fields:
     *
     * <ul>
     *   <li>{@code ordType} — {@code "MARKET"} / {@code "LIMIT"} (from {@code orders.ord_type}).
     *       Lets the UI render the type chip without inferring from {@code limitPrice == null}.</li>
     *   <li>{@code quantity} — current order quantity (post-replace if applicable).</li>
     *   <li>{@code limitPrice} — nullable; null on a MARKET order, the working limit on LIMIT.</li>
     *   <li>{@code cumFilledQuantity} — venue-side filled-so-far. Lets the UI show
     *       progress (e.g. "2 / 5 filled") without a secondary fetch.</li>
     * </ul>
     *
     * Average fill price intentionally omitted — it would require an aggregate join on
     * {@code executions} per row, which would degrade the snapshot read latency under load.
     * Operators who need it can click through to the per-order detail (separate endpoint,
     * planned).
     */
    public record DeskSnapshotRow(
            UUID id,
            UUID accountId,
            String instrumentSymbol,
            String side,
            String status,
            int version,
            Instant receivedAt,
            String ordType,
            BigDecimal quantity,
            BigDecimal limitPrice,
            BigDecimal cumFilledQuantity) {}

    private static final RowMapper<DeskSnapshotRow> DESK_SNAPSHOT_ROW_MAPPER = (rs, rowNum) ->
            new DeskSnapshotRow(
                    (UUID) rs.getObject("id"),
                    (UUID) rs.getObject("account_id"),
                    rs.getString("instrument_symbol"),
                    rs.getString("side"),
                    rs.getString("status"),
                    rs.getInt("version"),
                    rs.getTimestamp("received_at").toInstant(),
                    rs.getString("ord_type"),
                    rs.getBigDecimal("quantity"),
                    rs.getBigDecimal("limit_price"),
                    rs.getBigDecimal("cum_filled_quantity"));

    /**
     * Active-orders snapshot for the trading desk blotter (GTC-correct: no age window). Returns
     * every order in a non-terminal status (PENDING_NEW / NEW / WORKING / PARTIALLY_FILLED)
     * regardless of {@code received_at}. Bounded by {@code limit} so a runaway desk with thousands
     * of working orders does not consume unbounded memory in the snapshot response — the operator
     * should escalate via the search endpoint for such cases.
     */
    public List<DeskSnapshotRow> findActiveDeskSnapshot(int limit) {
        var params = new MapSqlParameterSource().addValue("limit", limit);
        return jdbc.query(SELECT_DESK_ACTIVE_SNAPSHOT_SQL, params, DESK_SNAPSHOT_ROW_MAPPER);
    }

    /** Minimal projection for venue order-state reconciliation. */
    public record VenueReconcileCandidate(UUID id, String instrumentSymbol, int version) {}

    private static final RowMapper<VenueReconcileCandidate> VENUE_RECONCILE_ROW_MAPPER = (rs, rowNum) ->
            new VenueReconcileCandidate(
                    (UUID) rs.getObject("id"),
                    rs.getString("instrument_symbol"),
                    rs.getInt("version"));

    // WORKING = confirmed live at the venue (set on venue-new ack). Bounded by accepted_at so the
    // reconciler only ever looks at orders old enough that an in-flight venue fill would already have
    // propagated back through the resolver — see OmsConfig.Cluster.VenueReconciler#minOrderAgeMs.
    // Oldest-first so a capped pass drains the longest-standing potential orphans first.
    private static final String SELECT_VENUE_RECONCILE_CANDIDATES_SQL = """
            SELECT id, instrument_symbol, version
            FROM orders
            WHERE status = 'WORKING'
              AND accepted_at IS NOT NULL
              AND accepted_at < :older_than
            ORDER BY accepted_at ASC
            LIMIT :limit
            """;

    /**
     * WORKING orders whose {@code accepted_at} is older than {@code olderThan}, oldest first, capped
     * at {@code limit}. The caller filters to venue-routed symbols and queries the venue for each.
     */
    public List<VenueReconcileCandidate> findWorkingVenueReconcileCandidates(Instant olderThan, int limit) {
        var params = new MapSqlParameterSource()
                .addValue("older_than", Timestamp.from(olderThan))
                .addValue("limit", limit);
        return jdbc.query(SELECT_VENUE_RECONCILE_CANDIDATES_SQL, params, VENUE_RECONCILE_ROW_MAPPER);
    }

    /**
     * Terminal-orders snapshot for the trading desk blotter, date-windowed. The window's lower
     * bound is enforced by the caller (DeskSnapshotController clamps to snapshot-max-age-hours);
     * the upper bound is optional ({@code maxReceivedExclusive == null} means "no upper bound").
     */
    public List<DeskSnapshotRow> findTerminalDeskSnapshot(
            Instant minReceived, Instant maxReceivedExclusive, int limit) {
        var params = new MapSqlParameterSource()
                .addValue("min_received", Timestamp.from(minReceived))
                .addValue("max_received", maxReceivedExclusive == null ? null : Timestamp.from(maxReceivedExclusive))
                .addValue("limit", limit);
        return jdbc.query(SELECT_DESK_TERMINAL_SNAPSHOT_SQL, params, DESK_SNAPSHOT_ROW_MAPPER);
    }

    /**
     * Operator-driven historical search. Each filter is independently optional and AND-combined.
     * Cursor pagination keyed on the natural sort {@code (received_at DESC, id DESC)} — opaque to
     * the client, encoded by {@link com.balh.oms.ingress.DeskOrderSearchController}.
     *
     * <p>{@code statusList} is bound as a Postgres {@code order_status[]} via
     * {@code = ANY(...::order_status[])} so a single prepared statement covers 1..N status
     * filters. Status enum values are validated against {@link OrderStatus} on the controller
     * before reaching here.
     *
     * <p>If {@code orderId} is non-null, the search degenerates to a primary-key lookup — the
     * controller short-circuits all other filters in that case.
     */
    public List<DeskSnapshotRow> searchOrders(SearchParams p) {
        var params = new MapSqlParameterSource()
                .addValue("account_id", p.accountId())
                .addValue("symbol", p.symbol())
                .addValue("status_list", p.statusList())
                .addValue("side", p.side())
                .addValue("ord_type", p.ordType())
                .addValue("received_from", p.receivedFrom() == null ? null : Timestamp.from(p.receivedFrom()))
                .addValue("received_to", p.receivedTo() == null ? null : Timestamp.from(p.receivedTo()))
                .addValue("client_request_key", p.clientRequestKey())
                .addValue("cursor_received",
                        p.cursorReceived() == null ? null : Timestamp.from(p.cursorReceived()))
                .addValue("cursor_id", p.cursorId())
                .addValue("limit", p.limit());
        return jdbc.query(SELECT_ORDERS_SEARCH_SQL, params, DESK_SNAPSHOT_ROW_MAPPER);
    }

    /**
     * Search parameters for {@link #searchOrders(SearchParams)}. Nullable fields mean "no filter
     * applied"; the SQL guards each one with {@code IS NULL OR ...}.
     */
    public record SearchParams(
            UUID accountId,
            String symbol,
            String[] statusList,
            String side,
            String ordType,
            Instant receivedFrom,
            Instant receivedTo,
            String clientRequestKey,
            Instant cursorReceived,
            UUID cursorId,
            int limit) {}

    // Cursor predicate: (received_at, id) < (:cursor_received, :cursor_id) is a Postgres row
    // comparison that the planner walks efficiently when the leading column has an index in the
    // right direction. V36 adds idx_orders_received_at_id (received_at DESC, id DESC) precisely
    // for this. Without the cursor (first page), the predicate evaluates to TRUE via the
    // `cursor_received IS NULL` short-circuit.
    private static final String SELECT_ORDERS_SEARCH_SQL = """
            SELECT id, account_id, instrument_symbol, side::text AS side, status::text AS status,
                   version, received_at, ord_type, quantity, limit_price, cum_filled_quantity
            FROM orders
            WHERE (CAST(:account_id AS uuid) IS NULL OR account_id = :account_id)
              AND (CAST(:symbol AS text) IS NULL OR instrument_symbol = :symbol)
              AND (CAST(:status_list AS text[]) IS NULL
                   OR status::text = ANY(CAST(:status_list AS text[])))
              AND (CAST(:side AS text) IS NULL OR side::text = :side)
              AND (CAST(:ord_type AS text) IS NULL OR ord_type = :ord_type)
              AND (CAST(:received_from AS timestamptz) IS NULL OR received_at >= :received_from)
              AND (CAST(:received_to AS timestamptz) IS NULL OR received_at < :received_to)
              AND (CAST(:client_request_key AS text) IS NULL
                   OR client_idempotency_key = :client_request_key)
              AND (CAST(:cursor_received AS timestamptz) IS NULL
                   OR (received_at, id) < (:cursor_received, CAST(:cursor_id AS uuid)))
            ORDER BY received_at DESC, id DESC
            LIMIT :limit
            """;

    /**
     * @return {@code true} if the CAS update applied (we hold the latest version).
     *         {@code false} means the row's version moved on; the caller must reload.
     */
    public boolean updateWithCas(
            UUID id,
            int expectedVersion,
            OrderStatus newStatus,
            RejectCode terminalReason,
            Instant acceptedAt,
            Instant terminalAt) {
        var params = new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("expected_version", expectedVersion)
                .addValue("status", newStatus.name())
                .addValue("terminal_reason", terminalReason == null ? null : terminalReason.name())
                .addValue("accepted_at", acceptedAt == null ? null : Timestamp.from(acceptedAt))
                .addValue("terminal_at", terminalAt == null ? null : Timestamp.from(terminalAt));
        return jdbc.update(UPDATE_CAS_SQL, params) == 1;
    }

    /**
     * Applies a venue fill or cancel outcome with CAS on {@code version}.
     *
     * @param terminalAt pass non-null when transitioning to a terminal non-reject status
     *                     ({@link OrderStatus#FILLED}, {@link OrderStatus#CANCELLED}).
     */
    public boolean updateFillOrCancelWithCas(
            UUID id,
            int expectedVersion,
            BigDecimal newCumFilled,
            OrderStatus newStatus,
            RejectCode terminalReason,
            Instant terminalAt) {
        var params = new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("expected_version", expectedVersion)
                .addValue("new_cum_filled", newCumFilled)
                .addValue("status", newStatus.name())
                .addValue("terminal_reason", terminalReason == null ? null : terminalReason.name())
                .addValue("terminal_at", terminalAt == null ? null : Timestamp.from(terminalAt));
        return jdbc.update(UPDATE_FILL_CAS_SQL, params) == 1;
    }

    /**
     * Applies a venue REPLACE (broker ACK of a 35=G) with CAS on {@code version}: overwrites
     * {@code quantity} and {@code limit_price} to the broker-authoritative replacement values,
     * updates {@code status} (cluster recomputes WORKING vs PARTIALLY_FILLED vs FILLED depending
     * on the new qty vs unchanged cumQty), bumps version. {@code cum_filled_quantity},
     * {@code terminal_reason}, and {@code terminal_at} are intentionally untouched: a pure replace
     * has no fill side-effect and never lands the order in a terminal state by itself.
     *
     * @param newLimitPrice pass {@code null} only on a market-order replace (rare); pass the new
     *                      limit price on a limit-order replace. Market orders don't have a
     *                      meaningful limit_price column, so this branch is here mostly for
     *                      completeness — the demo only exercises limit replaces.
     * @return {@code true} if the CAS update applied. {@code false} means another writer beat us;
     *         the cluster's apply path has at-most-once semantics on
     *         {@code (orderId, venueExecRef)} so a false here is the duplicate-replay path
     *         (caller logs at debug and advances the cursor without an envelope emission).
     */
    public boolean updateReplaceWithCas(
            UUID id,
            int expectedVersion,
            BigDecimal newQuantity,
            BigDecimal newLimitPrice,
            OrderStatus newStatus) {
        var params = new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("expected_version", expectedVersion)
                .addValue("new_quantity", newQuantity)
                .addValue("new_limit_price", newLimitPrice)
                .addValue("status", newStatus.name());
        return jdbc.update(UPDATE_REPLACE_CAS_SQL, params) == 1;
    }

    private MapSqlParameterSource params(Order o) {
        return new MapSqlParameterSource()
                .addValue("id", o.id())
                .addValue("account_id", o.accountId())
                .addValue("client_idempotency_key", o.clientIdempotencyKey())
                .addValue("shard_id", o.shardId())
                .addValue("status", o.status().name())
                .addValue("terminal_reason", o.terminalReason() == null ? null : o.terminalReason().name())
                .addValue("side", o.side().name())
                .addValue("instrument_symbol", o.instrumentSymbol())
                .addValue("quantity", o.quantity())
                .addValue("limit_price", o.limitPrice())
                .addValue("time_in_force", o.timeInForce())
                .addValue("ord_type", o.ordType() == null ? "MARKET" : o.ordType())
                .addValue("received_at", Timestamp.from(o.receivedAt()))
                .addValue("accepted_at", o.acceptedAt() == null ? null : Timestamp.from(o.acceptedAt()))
                .addValue("terminal_at", o.terminalAt() == null ? null : Timestamp.from(o.terminalAt()))
                .addValue("account_id_hash", o.accountIdHash())
                .addValue("ledger_balance_id", o.ledgerBalanceId())
                .addValue("cum_filled_quantity", o.cumFilledQuantity());
    }

    private static final RowMapper<Order> ROW_MAPPER = (rs, rowNum) -> {
        BigDecimal limitPrice = rs.getBigDecimal("limit_price");
        Timestamp acceptedAt = rs.getTimestamp("accepted_at");
        Timestamp terminalAt = rs.getTimestamp("terminal_at");
        String terminalReason = rs.getString("terminal_reason");
        String ledgerBalanceId = rs.getString("ledger_balance_id");
        BigDecimal cumFilled = rs.getBigDecimal("cum_filled_quantity");
        // V33: ord_type column is NOT NULL after the migration, but tolerate null defensively
        // (e.g. row inserted by a Spring context that ran against an older schema in a wider
        // test harness). Same fallback the Order back-compat ctor uses.
        String ordTypeRaw = rs.getString("ord_type");
        String ordType = ordTypeRaw != null ? ordTypeRaw : (limitPrice == null ? "MARKET" : "LIMIT");
        return new Order(
                (UUID) rs.getObject("id"),
                (UUID) rs.getObject("account_id"),
                rs.getString("client_idempotency_key"),
                rs.getInt("shard_id"),
                rs.getInt("version"),
                OrderStatus.valueOf(rs.getString("status")),
                terminalReason == null ? null : RejectCode.valueOf(terminalReason),
                Side.valueOf(rs.getString("side")),
                rs.getString("instrument_symbol"),
                rs.getBigDecimal("quantity"),
                limitPrice,
                rs.getString("time_in_force"),
                rs.getTimestamp("received_at").toInstant(),
                acceptedAt == null ? null : acceptedAt.toInstant(),
                terminalAt == null ? null : terminalAt.toInstant(),
                rs.getString("account_id_hash"),
                ledgerBalanceId,
                cumFilled == null ? BigDecimal.ZERO : cumFilled,
                ordType
        );
    };
}
