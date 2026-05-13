package com.balh.oms.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Named limit and config keys for the OMS slice 1.
 *
 * <p>Per the Balh ecosystem config-and-limits rule, no bare numeric literals
 * appear in business logic for timeouts, queue sizes, batch sizes, or shard
 * counts. They are bound here from {@code application.yaml} / env.
 */
@Configuration
@ConfigurationProperties(prefix = "oms")
public class OmsConfig {

    private final Http http = new Http();
    private final Grpc grpc = new Grpc();
    private final Shard shard = new Shard();
    private final Control control = new Control();
    private final Events events = new Events();
    private final Ledger ledger = new Ledger();
    private final DomainEvents domainEvents = new DomainEvents();
    private final Pii pii = new Pii();
    private final Risk risk = new Risk();
    private final Routing routing = new Routing();
    private final Fix fix = new Fix();
    private final Settlement settlement = new Settlement();
    private final Marketdata marketdata = new Marketdata();
    private final CorporateAction corporateAction = new CorporateAction();
    private final Desk desk = new Desk();
    private final Fx fx = new Fx();
    private final Otel otel = new Otel();
    private final Cluster cluster = new Cluster();

    public Http getHttp() { return http; }
    public Grpc getGrpc() { return grpc; }
    public Shard getShard() { return shard; }
    public Control getControl() { return control; }
    public Events getEvents() { return events; }
    public Ledger getLedger() { return ledger; }
    public DomainEvents getDomainEvents() { return domainEvents; }
    public Pii getPii() { return pii; }
    public Risk getRisk() { return risk; }
    public Routing getRouting() { return routing; }
    public Fix getFix() { return fix; }
    public Settlement getSettlement() { return settlement; }
    public Marketdata getMarketdata() { return marketdata; }
    public CorporateAction getCorporateAction() { return corporateAction; }
    public Desk getDesk() { return desk; }
    public Fx getFx() { return fx; }
    public Otel getOtel() { return otel; }
    public Cluster getCluster() { return cluster; }

    public static class Http {
        private String internalApiKey = "";
        public String getInternalApiKey() { return internalApiKey; }
        public void setInternalApiKey(String v) { this.internalApiKey = v; }
    }

    /** Optional gRPC ingress (same order accept semantics as HTTP). */
    public static class Grpc {
        private boolean enabled = false;
        private int port = 9099;
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean v) { this.enabled = v; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = Math.max(1, Math.min(port, 65535)); }
    }

    public static class Shard {
        private int id = 0;
        private int count = 1;
        public int getId() { return id; }
        public void setId(int id) { this.id = id; }
        public int getCount() { return count; }
        public void setCount(int count) { this.count = count; }
    }

    /**
     * Control-plane admission knobs. Phase 3 slice 3g of the Aeron Cluster substrate plan removed
     * the {@code postgres-write-path} switch (the cluster admission service is now the only writer)
     * and the unused {@code tailer-batch-size} (only the deleted {@code ChronicleControlTailReader}
     * read it). {@link #maxJobAgeMs} survives — it still drives {@link com.balh.oms.tailer.StaleJobGuard}
     * inside {@link com.balh.oms.tailer.OrderControlAdmission}.
     */
    public static class Control {
        /** Default 5 minutes — see plans/oms-phase0-interim-decisions.md until trading-ops signs a stricter value. */
        private long maxJobAgeMs = 300_000L;
        public long getMaxJobAgeMs() { return maxJobAgeMs; }
        public void setMaxJobAgeMs(long v) { this.maxJobAgeMs = v; }
    }

    public static class Events {
        private final Nats nats = new Nats();
        public Nats getNats() { return nats; }

        public static class Nats {
            private boolean enabled = false;
            private String url = "nats://localhost:4222";
            private String subjectPrefix = "oms.events";
            private String streamName = "OMS_EVENTS";
            private long connectionTimeoutMs = 5000L;
            public boolean isEnabled() { return enabled; }
            public void setEnabled(boolean v) { this.enabled = v; }
            public String getUrl() { return url; }
            public void setUrl(String v) { this.url = v; }
            public String getSubjectPrefix() { return subjectPrefix; }
            public void setSubjectPrefix(String v) { this.subjectPrefix = v; }
            public String getStreamName() { return streamName; }
            public void setStreamName(String v) { this.streamName = v; }
            public long getConnectionTimeoutMs() { return connectionTimeoutMs; }
            public void setConnectionTimeoutMs(long v) { this.connectionTimeoutMs = v; }
        }
    }

    public static class Ledger {
        private boolean enabled = false;
        /** When true, OMS POSTs a Ledger sync inflight hold on BUY accept (see {@link LedgerInflightReservationClient}). */
        private boolean inflightReservationEnabled = false;
        /**
         * Ledger {@code balance_id} that receives the customer leg of the inflight hold
         * (bank-side suspense / OMS hold account). Required when {@link #inflightReservationEnabled} is true.
         */
        private String inflightHoldDestinationBalanceId = "";
        /** ISO currency code for the inflight hold (major unit in {@code amount}). */
        private String inflightReservationCurrency = "EUR";
        /** Ledger amount scaling (e.g. 100 = cents). */
        private int inflightReservationPrecision = 100;
        private String baseUrl = "http://localhost:5001";
        private String apiKey = "";
        private long connectTimeoutMs = 2000L;
        private long readTimeoutMs = 5000L;
        /**
         * When true with {@link #inflightReservationEnabled}, BUY inflight hold is enqueued to
         * {@code ledger_inflight_outbox} in the same DB transaction; {@link com.balh.oms.reconciler.LedgerInflightOutboxReconciler}
         * calls Ledger after commit.
         */
        private boolean inflightAsyncEnabled = false;
        private long inflightOutboxReconcilerAgeMs = 2000L;
        private int inflightOutboxReconcilerBatchSize = 50;
        private long inflightOutboxReconcilerIntervalMs = 500L;
        /**
         * When {@code true}, a row is written to {@code ledger_settlement_outbox} in the same transaction as OMS
         * transition to {@code settled} on {@code TRADE} executions; {@link com.balh.oms.reconciler.LedgerSettlementOutboxReconciler}
         * delivers when {@link #settlementOutboxReconcilerEnabled} is {@code true}.
         */
        private boolean settlementOutboxEnabled = false;
        /**
         * When {@code true} with {@link #enabled}, {@link com.balh.oms.reconciler.LedgerSettlementOutboxReconciler} POSTs
         * unposted rows to Ledger and sets {@code posted_at}.
         */
        private boolean settlementOutboxReconcilerEnabled = false;
        private long settlementOutboxReconcilerAgeMs = 2000L;
        private int settlementOutboxReconcilerBatchSize = 50;
        private long settlementOutboxReconcilerIntervalMs = 500L;
        /**
         * Path on the Ledger HTTP base URL for settlement outbox POST (must start with {@code /} or be relative segment;
         * normalized to a leading slash). Finance must align Ledger route with this value.
         */
        private String settlementPostingHttpPath = "/internal/v0/settlement-outbox";

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean v) { this.enabled = v; }
        public boolean isInflightReservationEnabled() { return inflightReservationEnabled; }
        public void setInflightReservationEnabled(boolean v) { this.inflightReservationEnabled = v; }
        public String getInflightHoldDestinationBalanceId() { return inflightHoldDestinationBalanceId; }
        public void setInflightHoldDestinationBalanceId(String v) { this.inflightHoldDestinationBalanceId = v; }
        public String getInflightReservationCurrency() { return inflightReservationCurrency; }
        public void setInflightReservationCurrency(String v) { this.inflightReservationCurrency = v; }
        public int getInflightReservationPrecision() { return inflightReservationPrecision; }
        public void setInflightReservationPrecision(int v) { this.inflightReservationPrecision = v; }
        public String getBaseUrl() { return baseUrl; }
        public void setBaseUrl(String v) { this.baseUrl = v; }
        public String getApiKey() { return apiKey; }
        public void setApiKey(String v) { this.apiKey = v; }
        public long getConnectTimeoutMs() { return connectTimeoutMs; }
        public void setConnectTimeoutMs(long v) { this.connectTimeoutMs = v; }
        public long getReadTimeoutMs() { return readTimeoutMs; }
        public void setReadTimeoutMs(long v) { this.readTimeoutMs = v; }
        public boolean isInflightAsyncEnabled() { return inflightAsyncEnabled; }
        public void setInflightAsyncEnabled(boolean v) { this.inflightAsyncEnabled = v; }
        public long getInflightOutboxReconcilerAgeMs() { return inflightOutboxReconcilerAgeMs; }
        public void setInflightOutboxReconcilerAgeMs(long v) { this.inflightOutboxReconcilerAgeMs = v; }
        public int getInflightOutboxReconcilerBatchSize() { return inflightOutboxReconcilerBatchSize; }
        public void setInflightOutboxReconcilerBatchSize(int v) { this.inflightOutboxReconcilerBatchSize = v; }
        public long getInflightOutboxReconcilerIntervalMs() { return inflightOutboxReconcilerIntervalMs; }
        public void setInflightOutboxReconcilerIntervalMs(long v) { this.inflightOutboxReconcilerIntervalMs = v; }

        public boolean isSettlementOutboxEnabled() {
            return settlementOutboxEnabled;
        }

        public void setSettlementOutboxEnabled(boolean settlementOutboxEnabled) {
            this.settlementOutboxEnabled = settlementOutboxEnabled;
        }

        public boolean isSettlementOutboxReconcilerEnabled() {
            return settlementOutboxReconcilerEnabled;
        }

        public void setSettlementOutboxReconcilerEnabled(boolean settlementOutboxReconcilerEnabled) {
            this.settlementOutboxReconcilerEnabled = settlementOutboxReconcilerEnabled;
        }

        public long getSettlementOutboxReconcilerAgeMs() {
            return settlementOutboxReconcilerAgeMs;
        }

        public void setSettlementOutboxReconcilerAgeMs(long settlementOutboxReconcilerAgeMs) {
            this.settlementOutboxReconcilerAgeMs = Math.max(0L, settlementOutboxReconcilerAgeMs);
        }

        public int getSettlementOutboxReconcilerBatchSize() {
            return settlementOutboxReconcilerBatchSize;
        }

        public void setSettlementOutboxReconcilerBatchSize(int settlementOutboxReconcilerBatchSize) {
            this.settlementOutboxReconcilerBatchSize = Math.max(1, settlementOutboxReconcilerBatchSize);
        }

        public long getSettlementOutboxReconcilerIntervalMs() {
            return settlementOutboxReconcilerIntervalMs;
        }

        public void setSettlementOutboxReconcilerIntervalMs(long settlementOutboxReconcilerIntervalMs) {
            this.settlementOutboxReconcilerIntervalMs = Math.max(100L, settlementOutboxReconcilerIntervalMs);
        }

        public String getSettlementPostingHttpPath() {
            return settlementPostingHttpPath;
        }

        public void setSettlementPostingHttpPath(String settlementPostingHttpPath) {
            this.settlementPostingHttpPath = settlementPostingHttpPath == null ? "" : settlementPostingHttpPath.trim();
        }
    }

    public static class DomainEvents {
        private long reconcilerAgeMs = 2000L;
        private int reconcilerBatchSize = 100;
        private long reconcilerIntervalMs = 500L;
        public long getReconcilerAgeMs() { return reconcilerAgeMs; }
        public void setReconcilerAgeMs(long v) { this.reconcilerAgeMs = v; }
        public int getReconcilerBatchSize() { return reconcilerBatchSize; }
        public void setReconcilerBatchSize(int v) { this.reconcilerBatchSize = v; }
        public long getReconcilerIntervalMs() { return reconcilerIntervalMs; }
        public void setReconcilerIntervalMs(long v) { this.reconcilerIntervalMs = v; }
    }

    public static class Pii {
        private boolean auditTraceEnabled = false;
        private String hashSecret = "dev-only-change-me";
        public boolean isAuditTraceEnabled() { return auditTraceEnabled; }
        public void setAuditTraceEnabled(boolean v) { this.auditTraceEnabled = v; }
        public String getHashSecret() { return hashSecret; }
        public void setHashSecret(String v) { this.hashSecret = v; }
    }

    /**
     * Pre-trade limits read on each control decision. Zero or negative numeric
     * thresholds mean "check disabled" for that dimension.
     */
    public static class Risk {
        private boolean instrumentAllowlistEnabled = false;
        private String allowedInstrumentSymbols = "";
        /**
         * When {@code true}, symbols must appear in {@link #tradableInstrumentSymbolSet()} or the order is rejected
         * with {@link com.balh.oms.domain.RejectCode#RISK_INSTRUMENT_NOT_ALLOWED} (slice 5 v1; replaces full
         * marketdata-backed {@code instruments} cache until that integration exists).
         */
        private boolean instrumentTradabilityCheckEnabled = false;
        private String tradableInstrumentSymbols = "";
        /**
         * When {@code true} with {@link #instrumentTradabilityCheckEnabled} and {@link OmsConfig#getMarketdata()}
         * {@code enabled}, {@link com.balh.oms.marketdata.MarketdataInstrumentsCache} symbols override CSV when non-empty;
         * otherwise CSV {@link #tradableInstrumentSymbols} applies.
         */
        private boolean instrumentTradabilityFromMarketdataEnabled = false;
        /**
         * When {@code true}, symbols in {@link #haltedInstrumentSymbolSet()} reject with {@link
         * com.balh.oms.domain.RejectCode#RISK_SYMBOL_HALT} (evaluated before allowlist / tradability).
         */
        private boolean instrumentSymbolHaltCheckEnabled = false;
        private String haltedInstrumentSymbols = "";
        private BigDecimal fatFingerMaxLimitPrice = BigDecimal.ZERO;
        private BigDecimal fatFingerMaxOrderQuantity = BigDecimal.ZERO;
        private BigDecimal maxOrderNotional = BigDecimal.ZERO;
        /**
         * When {@code true}, {@link com.balh.oms.risk.SanctionsExecutionGate} runs before other risk checks
         * (after global halt). Uses {@link #sanctionsCacheMaxAgeSeconds} for permissive cache; {@link
         * #sanctionsRecheckStrict} rejects every order until a real downstream client exists.
         */
        private boolean sanctionsRecheckEnabled = false;
        private boolean sanctionsRecheckStrict = false;
        private long sanctionsCacheMaxAgeSeconds = 3_600L;
        /**
         * Minimum wall-clock spacing between accepted control evaluations per {@code account_id}; {@code 0}
         * disables ({@link com.balh.oms.domain.RejectCode#RISK_RATE_LIMIT} on violation).
         */
        private long orderMinIntervalMsPerAccount = 0L;
        /** When {@code true}, limit prices must align to {@link #tickSizeIncrement} grid (zero disables increment). */
        private boolean tickSizeCheckEnabled = false;
        private java.math.BigDecimal tickSizeIncrement = java.math.BigDecimal.ZERO;
        /**
         * Reserved STP / venue-calendar gate (slice 8). When enabled without venue calendar wiring, evaluator
         * returns {@link com.balh.oms.domain.RejectCode#RISK_STP_GATE} only if {@link #stpGateRejectAll} is true
         * (testing knob; default false).
         */
        private boolean stpGateEnabled = false;
        private boolean stpGateRejectAll = false;
        /**
         * When {@code true} with {@code oms.routing.backend=fix}, rejects at control if {@code fix_route_state.send_enabled}
         * is false for {@code oms.fix.route-key} ({@link com.balh.oms.domain.RejectCode#RISK_MARKET_SESSION_CLOSED}).
         */
        private boolean fixRouteSendEnabledCheckEnabled = false;
        /**
         * When {@code true}, BUY orders where {@code positions.quantity_total + order.quantity} exceeds
         * {@link #maxAggregatePositionQuantity} reject with {@link com.balh.oms.domain.RejectCode#RISK_CONCENTRATION_LIMIT}.
         */
        private boolean maxAggregatePositionQuantityCheckEnabled = false;
        /** Max position quantity per account+symbol+custody (default omnibus); {@code 0} disables even when check enabled. */
        private java.math.BigDecimal maxAggregatePositionQuantity = java.math.BigDecimal.ZERO;

        public boolean isInstrumentAllowlistEnabled() { return instrumentAllowlistEnabled; }
        public void setInstrumentAllowlistEnabled(boolean v) { this.instrumentAllowlistEnabled = v; }
        public String getAllowedInstrumentSymbols() { return allowedInstrumentSymbols; }
        public void setAllowedInstrumentSymbols(String v) { this.allowedInstrumentSymbols = v == null ? "" : v; }

        public boolean isInstrumentTradabilityCheckEnabled() {
            return instrumentTradabilityCheckEnabled;
        }

        public void setInstrumentTradabilityCheckEnabled(boolean instrumentTradabilityCheckEnabled) {
            this.instrumentTradabilityCheckEnabled = instrumentTradabilityCheckEnabled;
        }

        public String getTradableInstrumentSymbols() {
            return tradableInstrumentSymbols;
        }

        public void setTradableInstrumentSymbols(String tradableInstrumentSymbols) {
            this.tradableInstrumentSymbols = tradableInstrumentSymbols == null ? "" : tradableInstrumentSymbols;
        }

        public boolean isInstrumentTradabilityFromMarketdataEnabled() {
            return instrumentTradabilityFromMarketdataEnabled;
        }

        public void setInstrumentTradabilityFromMarketdataEnabled(boolean instrumentTradabilityFromMarketdataEnabled) {
            this.instrumentTradabilityFromMarketdataEnabled = instrumentTradabilityFromMarketdataEnabled;
        }

        public boolean isInstrumentSymbolHaltCheckEnabled() {
            return instrumentSymbolHaltCheckEnabled;
        }

        public void setInstrumentSymbolHaltCheckEnabled(boolean instrumentSymbolHaltCheckEnabled) {
            this.instrumentSymbolHaltCheckEnabled = instrumentSymbolHaltCheckEnabled;
        }

        public String getHaltedInstrumentSymbols() {
            return haltedInstrumentSymbols;
        }

        public void setHaltedInstrumentSymbols(String haltedInstrumentSymbols) {
            this.haltedInstrumentSymbols = haltedInstrumentSymbols == null ? "" : haltedInstrumentSymbols;
        }

        /** Uppercased symbols from {@link #haltedInstrumentSymbols}, comma-separated. */
        public Set<String> haltedInstrumentSymbolSet() {
            if (!instrumentSymbolHaltCheckEnabled
                    || haltedInstrumentSymbols == null
                    || haltedInstrumentSymbols.isBlank()) {
                return Collections.emptySet();
            }
            return Arrays.stream(haltedInstrumentSymbols.split(","))
                    .map(s -> s.trim().toUpperCase(Locale.ROOT))
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toUnmodifiableSet());
        }

        /** Uppercased symbols from {@link #tradableInstrumentSymbols}, comma-separated. */
        public Set<String> tradableInstrumentSymbolSet() {
            if (!instrumentTradabilityCheckEnabled
                    || tradableInstrumentSymbols == null
                    || tradableInstrumentSymbols.isBlank()) {
                return Collections.emptySet();
            }
            return Arrays.stream(tradableInstrumentSymbols.split(","))
                    .map(s -> s.trim().toUpperCase(Locale.ROOT))
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toUnmodifiableSet());
        }

        public BigDecimal getFatFingerMaxLimitPrice() { return fatFingerMaxLimitPrice; }
        public void setFatFingerMaxLimitPrice(BigDecimal v) { this.fatFingerMaxLimitPrice = v == null ? BigDecimal.ZERO : v; }
        public BigDecimal getFatFingerMaxOrderQuantity() { return fatFingerMaxOrderQuantity; }
        public void setFatFingerMaxOrderQuantity(BigDecimal v) { this.fatFingerMaxOrderQuantity = v == null ? BigDecimal.ZERO : v; }
        public BigDecimal getMaxOrderNotional() { return maxOrderNotional; }
        public void setMaxOrderNotional(BigDecimal v) { this.maxOrderNotional = v == null ? BigDecimal.ZERO : v; }

        public boolean isSanctionsRecheckEnabled() {
            return sanctionsRecheckEnabled;
        }

        public void setSanctionsRecheckEnabled(boolean sanctionsRecheckEnabled) {
            this.sanctionsRecheckEnabled = sanctionsRecheckEnabled;
        }

        public boolean isSanctionsRecheckStrict() {
            return sanctionsRecheckStrict;
        }

        public void setSanctionsRecheckStrict(boolean sanctionsRecheckStrict) {
            this.sanctionsRecheckStrict = sanctionsRecheckStrict;
        }

        public long getSanctionsCacheMaxAgeSeconds() {
            return sanctionsCacheMaxAgeSeconds;
        }

        public void setSanctionsCacheMaxAgeSeconds(long sanctionsCacheMaxAgeSeconds) {
            this.sanctionsCacheMaxAgeSeconds = Math.max(0L, sanctionsCacheMaxAgeSeconds);
        }

        public long getOrderMinIntervalMsPerAccount() {
            return orderMinIntervalMsPerAccount;
        }

        public void setOrderMinIntervalMsPerAccount(long orderMinIntervalMsPerAccount) {
            this.orderMinIntervalMsPerAccount = Math.max(0L, orderMinIntervalMsPerAccount);
        }

        public boolean isTickSizeCheckEnabled() {
            return tickSizeCheckEnabled;
        }

        public void setTickSizeCheckEnabled(boolean tickSizeCheckEnabled) {
            this.tickSizeCheckEnabled = tickSizeCheckEnabled;
        }

        public java.math.BigDecimal getTickSizeIncrement() {
            return tickSizeIncrement;
        }

        public void setTickSizeIncrement(java.math.BigDecimal tickSizeIncrement) {
            this.tickSizeIncrement = tickSizeIncrement == null ? java.math.BigDecimal.ZERO : tickSizeIncrement;
        }

        public boolean isStpGateEnabled() {
            return stpGateEnabled;
        }

        public void setStpGateEnabled(boolean stpGateEnabled) {
            this.stpGateEnabled = stpGateEnabled;
        }

        public boolean isStpGateRejectAll() {
            return stpGateRejectAll;
        }

        public void setStpGateRejectAll(boolean stpGateRejectAll) {
            this.stpGateRejectAll = stpGateRejectAll;
        }

        public boolean isFixRouteSendEnabledCheckEnabled() {
            return fixRouteSendEnabledCheckEnabled;
        }

        public void setFixRouteSendEnabledCheckEnabled(boolean fixRouteSendEnabledCheckEnabled) {
            this.fixRouteSendEnabledCheckEnabled = fixRouteSendEnabledCheckEnabled;
        }

        public boolean isMaxAggregatePositionQuantityCheckEnabled() {
            return maxAggregatePositionQuantityCheckEnabled;
        }

        public void setMaxAggregatePositionQuantityCheckEnabled(boolean maxAggregatePositionQuantityCheckEnabled) {
            this.maxAggregatePositionQuantityCheckEnabled = maxAggregatePositionQuantityCheckEnabled;
        }

        public java.math.BigDecimal getMaxAggregatePositionQuantity() {
            return maxAggregatePositionQuantity;
        }

        public void setMaxAggregatePositionQuantity(java.math.BigDecimal maxAggregatePositionQuantity) {
            this.maxAggregatePositionQuantity =
                    maxAggregatePositionQuantity == null
                            ? java.math.BigDecimal.ZERO
                            : maxAggregatePositionQuantity.max(java.math.BigDecimal.ZERO);
        }

        /** Uppercased symbols from {@link #allowedInstrumentSymbols}, comma-separated. */
        public Set<String> allowedInstrumentSymbolSet() {
            if (!instrumentAllowlistEnabled || allowedInstrumentSymbols == null || allowedInstrumentSymbols.isBlank()) {
                return Collections.emptySet();
            }
            return Arrays.stream(allowedInstrumentSymbols.split(","))
                    .map(s -> s.trim().toUpperCase(Locale.ROOT))
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toUnmodifiableSet());
        }
    }

    /**
     * Outbound routing / market-context evidence config. {@code backend=noop} is the default safe
     * mode; {@code fix} drives QuickFIX/J via the {@code oms-fix-egress} JVM (Phase 3 of the Aeron
     * Cluster substrate plan). The legacy {@code simulated} backend (along with {@code FixRouteDispatcher}
     * / {@code FixOutboundDispatchWorker} / {@code ExecutionReportApplier}) was removed in slice 3g.
     */
    public static class Routing {
        private String backend = "noop";
        private String marketContextStubJson = "{\"stub\":true}";
        private boolean nbboReferenceInMarketContextEnabled = false;
        private BigDecimal nbboStubBidPrice = BigDecimal.ZERO;
        private BigDecimal nbboStubAskPrice = BigDecimal.ZERO;

        public String getBackend() { return backend; }
        public void setBackend(String v) { this.backend = v == null ? "noop" : v; }
        public String getMarketContextStubJson() { return marketContextStubJson; }
        public void setMarketContextStubJson(String v) {
            this.marketContextStubJson = v == null || v.isBlank() ? "{\"stub\":true}" : v;
        }

        /**
         * When {@code true} with positive {@link #getNbboStubBidPrice()} and {@link #getNbboStubAskPrice()}, each trade
         * apply merges an NBBO-class reference object into {@code market_context.snapshot_json} (slice 5 stub until
         * Marketdata Platform quotes are wired).
         */
        public boolean isNbboReferenceInMarketContextEnabled() {
            return nbboReferenceInMarketContextEnabled;
        }

        public void setNbboReferenceInMarketContextEnabled(boolean nbboReferenceInMarketContextEnabled) {
            this.nbboReferenceInMarketContextEnabled = nbboReferenceInMarketContextEnabled;
        }

        public BigDecimal getNbboStubBidPrice() {
            return nbboStubBidPrice;
        }

        public void setNbboStubBidPrice(BigDecimal nbboStubBidPrice) {
            this.nbboStubBidPrice = nbboStubBidPrice == null ? BigDecimal.ZERO : nbboStubBidPrice;
        }

        public BigDecimal getNbboStubAskPrice() {
            return nbboStubAskPrice;
        }

        public void setNbboStubAskPrice(BigDecimal nbboStubAskPrice) {
            this.nbboStubAskPrice = nbboStubAskPrice == null ? BigDecimal.ZERO : nbboStubAskPrice;
        }
    }

    /**
     * QuickFIX/J initiator / store paths when {@code oms.routing.backend=fix}.
     */
    public static class Fix {
        private boolean autoStart = false;
        private String fileStorePath = "./queues/fix";
        private String socketConnectHost = "127.0.0.1";
        private int socketConnectPort = 9876;
        private String senderCompId = "OMS_INIT";
        private String targetCompId = "BROKER_ACCEPT";
        private int heartBtInt = 30;
        /** Venue id stamped on {@code ExecutionTradeCommand} from inbound ERs. */
        private String venueIdForExecutions = "FIX";
        private boolean useDataDictionary = false;
        /**
         * JSON object: OMS {@code instrument_symbol} (any case) → broker {@code Symbol} on outbound {@code NewOrderSingle}
         * (e.g. {@code {"AAPL":"AAPL.NMS"}}). {@code {}} or empty = identity mapping.
         */
        private String symbolMapJson = "{}";
        /** Logical FIX route key for {@code fix_route_state} (default single route). */
        private String routeKey = "default";
        /**
         * {@code file} — {@link quickfix.FileStoreFactory}; {@code jdbc} — {@link quickfix.JdbcStoreFactory}.
         * Default pool is the application {@link javax.sql.DataSource}; optional dedicated pool via
         * {@link #sessionJdbcDatasourceEnabled} / {@link #sessionJdbcUrl}.
         */
        private String sessionStoreType = "file";
        /** When {@code true}, {@link com.balh.oms.fix.FixRouteStateSodScheduler} sets {@code send_enabled} on all {@code fix_route_state} rows on a cron. */
        private boolean routeStateSodEnabled = false;
        /** Spring 6-field cron; only used when {@link #routeStateSodEnabled} (default from {@code application.yaml}). */
        private String routeStateSodCron = "";
        /**
         * {@code always} (default) — run SOD on every cron fire. {@code weekdays} — Mon–Fri only in
         * {@link #routeStateSodPolicyZoneId}. {@code region_calendar} — JSON in {@link #routeStateSodPolicyCalendarJson}.
         */
        private String routeStateSodPolicyMode = "always";
        /** IANA zone id for weekday / region calendar evaluation (default UTC). */
        private String routeStateSodPolicyZoneId = "UTC";
        /**
         * When {@link #routeStateSodPolicyMode} is {@code region_calendar}, JSON document with {@code activeRegionId}
         * and {@code regions} map (see {@link FixSodPolicyEngine}).
         */
        private String routeStateSodPolicyCalendarJson = "{}";
        /** QuickFIX/J initiator TLS (broker UAT / prod). */
        private boolean socketUseSsl = false;
        private String socketKeyStore = "";
        private String socketKeyStorePassword = "";
        private String socketTrustStore = "";
        private String socketTrustStorePassword = "";
        /** Optional; comma-separated protocols for QuickFIX {@code EnabledProtocols} (e.g. {@code TLSv1.2}). */
        private String enabledSslProtocols = "";
        /**
         * When {@code true} with {@link #sessionStoreType} {@code jdbc}, QuickFIX {@link quickfix.JdbcStoreFactory} uses a
         * dedicated pool ({@link #sessionJdbcUrl}) instead of the application {@link javax.sql.DataSource}.
         */
        private boolean sessionJdbcDatasourceEnabled = false;
        private String sessionJdbcUrl = "";
        private String sessionJdbcUser = "";
        private String sessionJdbcPassword = "";
        private int sessionJdbcPoolMaxSize = 5;
        private int sessionJdbcPoolMinIdle = 1;
        private long sessionJdbcConnectionTimeoutMs = 2000L;
        /** When {@code true}, initiator {@code onLogout} increments {@code oms_fix_mass_cancel_disconnect_signal_total}. */
        private boolean massCancelOnDisconnectEnabled = false;
        /**
         * When {@code true}, {@code POST /internal/v1/fix/mass-cancel-request} is allowed (signal-only unless wire flag is also on).
         */
        private boolean manualMassCancelEnabled = false;
        /**
         * When {@code true} with a logged-on FIX session, {@code OrderMassCancelRequest} may be sent (broker contract required).
         */
        private boolean manualMassCancelWireEnabled = false;
        /** Max UTF-16 length for optional {@code reason} on manual mass-cancel POST. */
        private int manualMassCancelReasonMaxChars = 512;
        public boolean isAutoStart() { return autoStart; }
        public void setAutoStart(boolean autoStart) { this.autoStart = autoStart; }
        public String getFileStorePath() { return fileStorePath; }
        public void setFileStorePath(String fileStorePath) { this.fileStorePath = fileStorePath == null ? "./queues/fix" : fileStorePath; }
        public String getSocketConnectHost() { return socketConnectHost; }
        public void setSocketConnectHost(String socketConnectHost) {
            this.socketConnectHost = socketConnectHost == null ? "127.0.0.1" : socketConnectHost;
        }
        public int getSocketConnectPort() { return socketConnectPort; }
        public void setSocketConnectPort(int socketConnectPort) { this.socketConnectPort = socketConnectPort; }
        public String getSenderCompId() { return senderCompId; }
        public void setSenderCompId(String senderCompId) { this.senderCompId = senderCompId == null ? "OMS_INIT" : senderCompId; }
        public String getTargetCompId() { return targetCompId; }
        public void setTargetCompId(String targetCompId) { this.targetCompId = targetCompId == null ? "BROKER_ACCEPT" : targetCompId; }
        public int getHeartBtInt() { return heartBtInt; }
        public void setHeartBtInt(int heartBtInt) { this.heartBtInt = Math.max(1, heartBtInt); }
        public String getVenueIdForExecutions() { return venueIdForExecutions; }
        public void setVenueIdForExecutions(String venueIdForExecutions) {
            this.venueIdForExecutions = venueIdForExecutions == null ? "FIX" : venueIdForExecutions;
        }
        public boolean isUseDataDictionary() { return useDataDictionary; }
        public void setUseDataDictionary(boolean useDataDictionary) { this.useDataDictionary = useDataDictionary; }

        public String getSymbolMapJson() {
            return symbolMapJson;
        }

        public void setSymbolMapJson(String symbolMapJson) {
            this.symbolMapJson = symbolMapJson == null || symbolMapJson.isBlank() ? "{}" : symbolMapJson.trim();
        }

        public String getRouteKey() {
            return routeKey;
        }

        public void setRouteKey(String routeKey) {
            this.routeKey = routeKey == null || routeKey.isBlank() ? "default" : routeKey;
        }

        public String getSessionStoreType() {
            return sessionStoreType;
        }

        public void setSessionStoreType(String sessionStoreType) {
            this.sessionStoreType =
                    sessionStoreType == null || sessionStoreType.isBlank() ? "file" : sessionStoreType.trim();
        }

        public boolean isJdbcSessionStore() {
            return "jdbc".equalsIgnoreCase(sessionStoreType);
        }

        public boolean isRouteStateSodEnabled() {
            return routeStateSodEnabled;
        }

        public void setRouteStateSodEnabled(boolean routeStateSodEnabled) {
            this.routeStateSodEnabled = routeStateSodEnabled;
        }

        public String getRouteStateSodCron() {
            return routeStateSodCron;
        }

        public void setRouteStateSodCron(String routeStateSodCron) {
            this.routeStateSodCron = routeStateSodCron == null ? "" : routeStateSodCron.trim();
        }

        public String getRouteStateSodPolicyMode() {
            return routeStateSodPolicyMode;
        }

        public void setRouteStateSodPolicyMode(String routeStateSodPolicyMode) {
            this.routeStateSodPolicyMode =
                    routeStateSodPolicyMode == null || routeStateSodPolicyMode.isBlank()
                            ? "always"
                            : routeStateSodPolicyMode.trim().toLowerCase(Locale.ROOT);
        }

        public String getRouteStateSodPolicyZoneId() {
            return routeStateSodPolicyZoneId;
        }

        public void setRouteStateSodPolicyZoneId(String routeStateSodPolicyZoneId) {
            this.routeStateSodPolicyZoneId =
                    routeStateSodPolicyZoneId == null || routeStateSodPolicyZoneId.isBlank()
                            ? "UTC"
                            : routeStateSodPolicyZoneId.trim();
        }

        public String getRouteStateSodPolicyCalendarJson() {
            return routeStateSodPolicyCalendarJson;
        }

        public void setRouteStateSodPolicyCalendarJson(String routeStateSodPolicyCalendarJson) {
            this.routeStateSodPolicyCalendarJson =
                    routeStateSodPolicyCalendarJson == null || routeStateSodPolicyCalendarJson.isBlank()
                            ? "{}"
                            : routeStateSodPolicyCalendarJson.trim();
        }

        public boolean isSocketUseSsl() {
            return socketUseSsl;
        }

        public void setSocketUseSsl(boolean socketUseSsl) {
            this.socketUseSsl = socketUseSsl;
        }

        public String getSocketKeyStore() {
            return socketKeyStore;
        }

        public void setSocketKeyStore(String socketKeyStore) {
            this.socketKeyStore = socketKeyStore == null ? "" : socketKeyStore;
        }

        public String getSocketKeyStorePassword() {
            return socketKeyStorePassword;
        }

        public void setSocketKeyStorePassword(String socketKeyStorePassword) {
            this.socketKeyStorePassword = socketKeyStorePassword == null ? "" : socketKeyStorePassword;
        }

        public String getSocketTrustStore() {
            return socketTrustStore;
        }

        public void setSocketTrustStore(String socketTrustStore) {
            this.socketTrustStore = socketTrustStore == null ? "" : socketTrustStore;
        }

        public String getSocketTrustStorePassword() {
            return socketTrustStorePassword;
        }

        public void setSocketTrustStorePassword(String socketTrustStorePassword) {
            this.socketTrustStorePassword = socketTrustStorePassword == null ? "" : socketTrustStorePassword;
        }

        public String getEnabledSslProtocols() {
            return enabledSslProtocols;
        }

        public void setEnabledSslProtocols(String enabledSslProtocols) {
            this.enabledSslProtocols = enabledSslProtocols == null ? "" : enabledSslProtocols.trim();
        }

        public boolean isSessionJdbcDatasourceEnabled() {
            return sessionJdbcDatasourceEnabled;
        }

        public void setSessionJdbcDatasourceEnabled(boolean sessionJdbcDatasourceEnabled) {
            this.sessionJdbcDatasourceEnabled = sessionJdbcDatasourceEnabled;
        }

        public String getSessionJdbcUrl() {
            return sessionJdbcUrl;
        }

        public void setSessionJdbcUrl(String sessionJdbcUrl) {
            this.sessionJdbcUrl = sessionJdbcUrl == null ? "" : sessionJdbcUrl.trim();
        }

        public String getSessionJdbcUser() {
            return sessionJdbcUser;
        }

        public void setSessionJdbcUser(String sessionJdbcUser) {
            this.sessionJdbcUser = sessionJdbcUser == null ? "" : sessionJdbcUser.trim();
        }

        public String getSessionJdbcPassword() {
            return sessionJdbcPassword;
        }

        public void setSessionJdbcPassword(String sessionJdbcPassword) {
            this.sessionJdbcPassword = sessionJdbcPassword == null ? "" : sessionJdbcPassword;
        }

        public int getSessionJdbcPoolMaxSize() {
            return sessionJdbcPoolMaxSize;
        }

        public void setSessionJdbcPoolMaxSize(int sessionJdbcPoolMaxSize) {
            this.sessionJdbcPoolMaxSize = Math.max(1, sessionJdbcPoolMaxSize);
        }

        public int getSessionJdbcPoolMinIdle() {
            return sessionJdbcPoolMinIdle;
        }

        public void setSessionJdbcPoolMinIdle(int sessionJdbcPoolMinIdle) {
            this.sessionJdbcPoolMinIdle = Math.max(0, sessionJdbcPoolMinIdle);
        }

        public long getSessionJdbcConnectionTimeoutMs() {
            return sessionJdbcConnectionTimeoutMs;
        }

        public void setSessionJdbcConnectionTimeoutMs(long sessionJdbcConnectionTimeoutMs) {
            this.sessionJdbcConnectionTimeoutMs = Math.max(250L, sessionJdbcConnectionTimeoutMs);
        }

        public boolean isMassCancelOnDisconnectEnabled() {
            return massCancelOnDisconnectEnabled;
        }

        public void setMassCancelOnDisconnectEnabled(boolean massCancelOnDisconnectEnabled) {
            this.massCancelOnDisconnectEnabled = massCancelOnDisconnectEnabled;
        }

        public boolean isManualMassCancelEnabled() {
            return manualMassCancelEnabled;
        }

        public void setManualMassCancelEnabled(boolean manualMassCancelEnabled) {
            this.manualMassCancelEnabled = manualMassCancelEnabled;
        }

        public boolean isManualMassCancelWireEnabled() {
            return manualMassCancelWireEnabled;
        }

        public void setManualMassCancelWireEnabled(boolean manualMassCancelWireEnabled) {
            this.manualMassCancelWireEnabled = manualMassCancelWireEnabled;
        }

        public int getManualMassCancelReasonMaxChars() {
            return manualMassCancelReasonMaxChars;
        }

        public void setManualMassCancelReasonMaxChars(int manualMassCancelReasonMaxChars) {
            this.manualMassCancelReasonMaxChars = Math.min(4000, Math.max(32, manualMassCancelReasonMaxChars));
        }
    }

    /**
     * Post-trade settlement / custody (slice 6). Defaults align with Flyway {@code V11__settlement_positions.sql}.
     */
    public static class Settlement {
        private String defaultCustodyAccountId = "a0000001-0000-4000-8000-000000000001";
        private boolean brokerConfirmReconcilerEnabled = false;
        private long brokerConfirmReconcilerIntervalMs = 10_000L;
        private int brokerConfirmReconcilerBatchSize = 50;
        private int brokerConfirmHttpMaxExecutionIds = 100;
        /** Max bytes accepted for {@code POST …/settlement/file-import} body (before SHA + parse). */
        private long fileImportMaxBytes = 10_000_000L;
        /** Max broker fixture rows read from one file (beyond this are counted as skipped-invalid in ingest). */
        private int fileImportMaxRows = 10_000;
        /** Max execution ids registered per DB transaction during file ingest slices. */
        private int fileImportRegisterSliceSize = 50;
        /** Truncation bound for {@code settlement_file_import_batch.error_summary}. */
        private int fileImportErrorSummaryMaxChars = 2000;
        /** Max rows returned by {@code GET /internal/v1/settlement/file-import-batches}. */
        private int fileImportListMaxLimit = 200;
        /** Default page size for file import batch list. */
        private int fileImportListDefaultLimit = 50;
        /** Max rows returned by {@code GET /internal/v1/settlement/manual-actions}. */
        private int manualActionListMaxLimit = 200;
        /** Default page size for manual settlement action list. */
        private int manualActionListDefaultLimit = 50;
        /** Max {@code action_type} length (UTF-16 code units). */
        private int manualActionTypeMaxLength = 128;
        /** Max JSON text length accepted for {@code payload_json} on create. */
        private int manualActionPayloadJsonMaxChars = 20_000;
        /**
         * When {@code true} (default), {@code POST …/manual-actions/{id}/approve} runs supported
         * {@code action_type} handlers in the same transaction as the approve CAS (see {@link ManualSettlementActionTypes}).
         */
        private boolean manualActionAutoApplyEnabled = true;
        /** When true, {@link com.balh.oms.settlement.SettlementFileDropFolderIngestScheduler} ingests {@code .json} files. */
        private boolean fileImportDropFolderEnabled = false;
        /** Absolute or relative path watched for incoming broker JSON files (see docs/broker-eod-file-contract.md). */
        private String fileImportDropFolderPath = "";
        private long fileImportDropFolderPollIntervalMs = 30_000L;
        private int fileImportDropFolderMaxFilesPerPoll = 20;
        /**
         * When {@code true}, BUY {@code TRADE} fills may append prior unsettled BUY execution ids to
         * {@code executions.unsettled_funded_by_exec_ids} (stub attribution — finance replaces rules later).
         */
        private boolean freeRidingAttributionEnabled = false;
        /** Max prior execution ids merged into {@code unsettled_funded_by_exec_ids} per fill. */
        private int freeRidingAttributionMaxFundingExecutions = 8;

        public String getDefaultCustodyAccountId() {
            return defaultCustodyAccountId;
        }

        public void setDefaultCustodyAccountId(String defaultCustodyAccountId) {
            String trimmed = defaultCustodyAccountId == null || defaultCustodyAccountId.isBlank()
                    ? "a0000001-0000-4000-8000-000000000001"
                    : defaultCustodyAccountId.trim();
            UUID.fromString(trimmed);
            this.defaultCustodyAccountId = trimmed;
        }

        public boolean isBrokerConfirmReconcilerEnabled() {
            return brokerConfirmReconcilerEnabled;
        }

        public void setBrokerConfirmReconcilerEnabled(boolean brokerConfirmReconcilerEnabled) {
            this.brokerConfirmReconcilerEnabled = brokerConfirmReconcilerEnabled;
        }

        public long getBrokerConfirmReconcilerIntervalMs() {
            return brokerConfirmReconcilerIntervalMs;
        }

        public void setBrokerConfirmReconcilerIntervalMs(long brokerConfirmReconcilerIntervalMs) {
            this.brokerConfirmReconcilerIntervalMs = Math.max(100L, brokerConfirmReconcilerIntervalMs);
        }

        public int getBrokerConfirmReconcilerBatchSize() {
            return brokerConfirmReconcilerBatchSize;
        }

        public void setBrokerConfirmReconcilerBatchSize(int brokerConfirmReconcilerBatchSize) {
            this.brokerConfirmReconcilerBatchSize = Math.max(1, brokerConfirmReconcilerBatchSize);
        }

        public int getBrokerConfirmHttpMaxExecutionIds() {
            return brokerConfirmHttpMaxExecutionIds;
        }

        public void setBrokerConfirmHttpMaxExecutionIds(int brokerConfirmHttpMaxExecutionIds) {
            this.brokerConfirmHttpMaxExecutionIds = Math.min(10_000, Math.max(1, brokerConfirmHttpMaxExecutionIds));
        }

        public long getFileImportMaxBytes() {
            return fileImportMaxBytes;
        }

        public void setFileImportMaxBytes(long fileImportMaxBytes) {
            this.fileImportMaxBytes = Math.min(100_000_000L, Math.max(1024L, fileImportMaxBytes));
        }

        public int getFileImportMaxRows() {
            return fileImportMaxRows;
        }

        public void setFileImportMaxRows(int fileImportMaxRows) {
            this.fileImportMaxRows = Math.min(500_000, Math.max(1, fileImportMaxRows));
        }

        public int getFileImportRegisterSliceSize() {
            return fileImportRegisterSliceSize;
        }

        public void setFileImportRegisterSliceSize(int fileImportRegisterSliceSize) {
            this.fileImportRegisterSliceSize = Math.min(10_000, Math.max(1, fileImportRegisterSliceSize));
        }

        public int getFileImportErrorSummaryMaxChars() {
            return fileImportErrorSummaryMaxChars;
        }

        public void setFileImportErrorSummaryMaxChars(int fileImportErrorSummaryMaxChars) {
            this.fileImportErrorSummaryMaxChars = Math.min(50_000, Math.max(256, fileImportErrorSummaryMaxChars));
        }

        public int getFileImportListMaxLimit() {
            return fileImportListMaxLimit;
        }

        public void setFileImportListMaxLimit(int fileImportListMaxLimit) {
            this.fileImportListMaxLimit = Math.min(500, Math.max(1, fileImportListMaxLimit));
        }

        public int getFileImportListDefaultLimit() {
            return fileImportListDefaultLimit;
        }

        public void setFileImportListDefaultLimit(int fileImportListDefaultLimit) {
            this.fileImportListDefaultLimit = Math.min(getFileImportListMaxLimit(), Math.max(1, fileImportListDefaultLimit));
        }

        public int getManualActionListMaxLimit() {
            return manualActionListMaxLimit;
        }

        public void setManualActionListMaxLimit(int manualActionListMaxLimit) {
            this.manualActionListMaxLimit = Math.min(500, Math.max(1, manualActionListMaxLimit));
        }

        public int getManualActionListDefaultLimit() {
            return manualActionListDefaultLimit;
        }

        public void setManualActionListDefaultLimit(int manualActionListDefaultLimit) {
            this.manualActionListDefaultLimit = Math.min(getManualActionListMaxLimit(), Math.max(1, manualActionListDefaultLimit));
        }

        public int getManualActionTypeMaxLength() {
            return manualActionTypeMaxLength;
        }

        public void setManualActionTypeMaxLength(int manualActionTypeMaxLength) {
            this.manualActionTypeMaxLength = Math.min(512, Math.max(1, manualActionTypeMaxLength));
        }

        public int getManualActionPayloadJsonMaxChars() {
            return manualActionPayloadJsonMaxChars;
        }

        public void setManualActionPayloadJsonMaxChars(int manualActionPayloadJsonMaxChars) {
            this.manualActionPayloadJsonMaxChars = Math.min(500_000, Math.max(256, manualActionPayloadJsonMaxChars));
        }

        public boolean isManualActionAutoApplyEnabled() {
            return manualActionAutoApplyEnabled;
        }

        public void setManualActionAutoApplyEnabled(boolean manualActionAutoApplyEnabled) {
            this.manualActionAutoApplyEnabled = manualActionAutoApplyEnabled;
        }

        public boolean isFileImportDropFolderEnabled() {
            return fileImportDropFolderEnabled;
        }

        public void setFileImportDropFolderEnabled(boolean fileImportDropFolderEnabled) {
            this.fileImportDropFolderEnabled = fileImportDropFolderEnabled;
        }

        public String getFileImportDropFolderPath() {
            return fileImportDropFolderPath;
        }

        public void setFileImportDropFolderPath(String fileImportDropFolderPath) {
            this.fileImportDropFolderPath = fileImportDropFolderPath == null ? "" : fileImportDropFolderPath.trim();
        }

        public long getFileImportDropFolderPollIntervalMs() {
            return fileImportDropFolderPollIntervalMs;
        }

        public void setFileImportDropFolderPollIntervalMs(long fileImportDropFolderPollIntervalMs) {
            this.fileImportDropFolderPollIntervalMs = Math.max(5_000L, fileImportDropFolderPollIntervalMs);
        }

        public int getFileImportDropFolderMaxFilesPerPoll() {
            return fileImportDropFolderMaxFilesPerPoll;
        }

        public void setFileImportDropFolderMaxFilesPerPoll(int fileImportDropFolderMaxFilesPerPoll) {
            this.fileImportDropFolderMaxFilesPerPoll = Math.min(500, Math.max(1, fileImportDropFolderMaxFilesPerPoll));
        }

        public boolean isFreeRidingAttributionEnabled() {
            return freeRidingAttributionEnabled;
        }

        public void setFreeRidingAttributionEnabled(boolean freeRidingAttributionEnabled) {
            this.freeRidingAttributionEnabled = freeRidingAttributionEnabled;
        }

        public int getFreeRidingAttributionMaxFundingExecutions() {
            return freeRidingAttributionMaxFundingExecutions;
        }

        public void setFreeRidingAttributionMaxFundingExecutions(int freeRidingAttributionMaxFundingExecutions) {
            this.freeRidingAttributionMaxFundingExecutions = Math.min(256, Math.max(1, freeRidingAttributionMaxFundingExecutions));
        }
    }

    /**
     * Desk / attendant read APIs (bounded list — internal key only). See {@code GET /internal/v1/desk/orders/snapshot}.
     */
    public static class Desk {
        private boolean snapshotEnabled = false;
        private int snapshotMaxLimit = 50;
        private int snapshotMaxAgeHours = 24;

        public boolean isSnapshotEnabled() {
            return snapshotEnabled;
        }

        public void setSnapshotEnabled(boolean snapshotEnabled) {
            this.snapshotEnabled = snapshotEnabled;
        }

        public int getSnapshotMaxLimit() {
            return snapshotMaxLimit;
        }

        public void setSnapshotMaxLimit(int snapshotMaxLimit) {
            this.snapshotMaxLimit = Math.min(500, Math.max(1, snapshotMaxLimit));
        }

        public int getSnapshotMaxAgeHours() {
            return snapshotMaxAgeHours;
        }

        public void setSnapshotMaxAgeHours(int snapshotMaxAgeHours) {
            this.snapshotMaxAgeHours = Math.min(168, Math.max(1, snapshotMaxAgeHours));
        }
    }

    /**
     * FX module (§11.5) — off by default; health endpoint documents rollout state.
     * Sub-tracks are gated independently; see {@code oms/docs/fx-backend-slice-m3.md}.
     */
    public static class Fx {
        private boolean moduleEnabled = false;
        private boolean quoteStubEnabled = false;
        private int quoteStubSchemaVersion = 1;
        private boolean nostroReadEnabled = false;
        /** Comma-separated Ledger {@code balance_id} values for {@code GET /internal/v1/fx/nostro/snapshot}. */
        private String nostroBalanceIdsCsv = "";
        private boolean multiLegAtomicityStubEnabled = false;
        private boolean hedgeHooksEnabled = false;
        private boolean eodFlattenEnabled = false;
        private long eodFlattenIntervalMs = 86_400_000L;

        public boolean isModuleEnabled() {
            return moduleEnabled;
        }

        public void setModuleEnabled(boolean moduleEnabled) {
            this.moduleEnabled = moduleEnabled;
        }

        public boolean isQuoteStubEnabled() {
            return quoteStubEnabled;
        }

        public void setQuoteStubEnabled(boolean quoteStubEnabled) {
            this.quoteStubEnabled = quoteStubEnabled;
        }

        public int getQuoteStubSchemaVersion() {
            return quoteStubSchemaVersion;
        }

        public void setQuoteStubSchemaVersion(int quoteStubSchemaVersion) {
            this.quoteStubSchemaVersion = Math.min(99, Math.max(1, quoteStubSchemaVersion));
        }

        public boolean isNostroReadEnabled() {
            return nostroReadEnabled;
        }

        public void setNostroReadEnabled(boolean nostroReadEnabled) {
            this.nostroReadEnabled = nostroReadEnabled;
        }

        public String getNostroBalanceIdsCsv() {
            return nostroBalanceIdsCsv;
        }

        public void setNostroBalanceIdsCsv(String nostroBalanceIdsCsv) {
            this.nostroBalanceIdsCsv = nostroBalanceIdsCsv == null ? "" : nostroBalanceIdsCsv;
        }

        public List<String> nostroBalanceIds() {
            if (nostroBalanceIdsCsv == null || nostroBalanceIdsCsv.isBlank()) {
                return List.of();
            }
            return Arrays.stream(nostroBalanceIdsCsv.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toList();
        }

        public boolean isMultiLegAtomicityStubEnabled() {
            return multiLegAtomicityStubEnabled;
        }

        public void setMultiLegAtomicityStubEnabled(boolean multiLegAtomicityStubEnabled) {
            this.multiLegAtomicityStubEnabled = multiLegAtomicityStubEnabled;
        }

        public boolean isHedgeHooksEnabled() {
            return hedgeHooksEnabled;
        }

        public void setHedgeHooksEnabled(boolean hedgeHooksEnabled) {
            this.hedgeHooksEnabled = hedgeHooksEnabled;
        }

        public boolean isEodFlattenEnabled() {
            return eodFlattenEnabled;
        }

        public void setEodFlattenEnabled(boolean eodFlattenEnabled) {
            this.eodFlattenEnabled = eodFlattenEnabled;
        }

        public long getEodFlattenIntervalMs() {
            return eodFlattenIntervalMs;
        }

        public void setEodFlattenIntervalMs(long eodFlattenIntervalMs) {
            this.eodFlattenIntervalMs = Math.max(60_000L, eodFlattenIntervalMs);
        }
    }

    /**
     * Corporate action inbox (slice 8 stub): ingest via internal HTTP; optional processor marks rows processed.
     */
    public static class CorporateAction {
        private boolean processorEnabled = false;
        private long processorIntervalMs = 60_000L;
        private int processorBatchSize = 50;
        private int ingestInstrumentSymbolMaxLength = 64;
        private int ingestActionTypeMaxLength = 64;
        private int ingestPayloadJsonMaxChars = 16_000;
        private int listMaxLimit = 200;
        private int listDefaultLimit = 50;

        public boolean isProcessorEnabled() {
            return processorEnabled;
        }

        public void setProcessorEnabled(boolean processorEnabled) {
            this.processorEnabled = processorEnabled;
        }

        public long getProcessorIntervalMs() {
            return processorIntervalMs;
        }

        public void setProcessorIntervalMs(long processorIntervalMs) {
            this.processorIntervalMs = Math.max(5_000L, processorIntervalMs);
        }

        public int getProcessorBatchSize() {
            return processorBatchSize;
        }

        public void setProcessorBatchSize(int processorBatchSize) {
            this.processorBatchSize = Math.min(500, Math.max(1, processorBatchSize));
        }

        public int getIngestInstrumentSymbolMaxLength() {
            return ingestInstrumentSymbolMaxLength;
        }

        public void setIngestInstrumentSymbolMaxLength(int ingestInstrumentSymbolMaxLength) {
            this.ingestInstrumentSymbolMaxLength = Math.min(256, Math.max(1, ingestInstrumentSymbolMaxLength));
        }

        public int getIngestActionTypeMaxLength() {
            return ingestActionTypeMaxLength;
        }

        public void setIngestActionTypeMaxLength(int ingestActionTypeMaxLength) {
            this.ingestActionTypeMaxLength = Math.min(256, Math.max(1, ingestActionTypeMaxLength));
        }

        public int getIngestPayloadJsonMaxChars() {
            return ingestPayloadJsonMaxChars;
        }

        public void setIngestPayloadJsonMaxChars(int ingestPayloadJsonMaxChars) {
            this.ingestPayloadJsonMaxChars = Math.min(500_000, Math.max(2, ingestPayloadJsonMaxChars));
        }

        public int getListMaxLimit() {
            return listMaxLimit;
        }

        public void setListMaxLimit(int listMaxLimit) {
            this.listMaxLimit = Math.min(500, Math.max(1, listMaxLimit));
        }

        public int getListDefaultLimit() {
            return listDefaultLimit;
        }

        public void setListDefaultLimit(int listDefaultLimit) {
            this.listDefaultLimit = Math.min(200, Math.max(1, listDefaultLimit));
        }
    }

    /**
     * Optional HTTP integration to a Marketdata-style service (instruments list + NBBO quotes for evidence).
     * Defaults are safe-off; paths are relative to {@link #baseUrl}.
     */
    public static class Marketdata {
        private boolean enabled = false;
        private String baseUrl = "";
        private String apiKey = "";
        private long connectTimeoutMs = 2000L;
        private long readTimeoutMs = 5000L;
        private String instrumentsHttpPath = "/internal/v0/instruments";
        private String nbboHttpPath = "/internal/v0/nbbo";
        private long instrumentsRefreshIntervalMs = 60_000L;
        /**
         * When {@code true} with {@link com.balh.oms.config.OmsConfig.Routing#isNbboReferenceInMarketContextEnabled()},
         * trade applies try HTTP NBBO
         * before stub bid/ask (symbol query param {@code symbol}).
         */
        private boolean nbboInMarketContextEnabled = false;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getBaseUrl() {
            return baseUrl;
        }

        public void setBaseUrl(String baseUrl) {
            this.baseUrl = baseUrl == null ? "" : baseUrl.trim();
        }

        public String getApiKey() {
            return apiKey;
        }

        public void setApiKey(String apiKey) {
            this.apiKey = apiKey == null ? "" : apiKey;
        }

        public long getConnectTimeoutMs() {
            return connectTimeoutMs;
        }

        public void setConnectTimeoutMs(long connectTimeoutMs) {
            this.connectTimeoutMs = Math.max(250L, connectTimeoutMs);
        }

        public long getReadTimeoutMs() {
            return readTimeoutMs;
        }

        public void setReadTimeoutMs(long readTimeoutMs) {
            this.readTimeoutMs = Math.max(250L, readTimeoutMs);
        }

        public String getInstrumentsHttpPath() {
            return instrumentsHttpPath;
        }

        public void setInstrumentsHttpPath(String instrumentsHttpPath) {
            this.instrumentsHttpPath = instrumentsHttpPath == null ? "" : instrumentsHttpPath.trim();
        }

        public String getNbboHttpPath() {
            return nbboHttpPath;
        }

        public void setNbboHttpPath(String nbboHttpPath) {
            this.nbboHttpPath = nbboHttpPath == null ? "" : nbboHttpPath.trim();
        }

        public long getInstrumentsRefreshIntervalMs() {
            return instrumentsRefreshIntervalMs;
        }

        public void setInstrumentsRefreshIntervalMs(long instrumentsRefreshIntervalMs) {
            this.instrumentsRefreshIntervalMs = Math.max(5_000L, instrumentsRefreshIntervalMs);
        }

        public boolean isNbboInMarketContextEnabled() {
            return nbboInMarketContextEnabled;
        }

        public void setNbboInMarketContextEnabled(boolean nbboInMarketContextEnabled) {
            this.nbboInMarketContextEnabled = nbboInMarketContextEnabled;
        }
    }

    /**
     * OpenTelemetry SDK metrics (optional). When {@code metrics-enabled=false}, only Micrometer
     * {@code /actuator/prometheus} is used (default).
     */
    public static class Otel {
        private boolean metricsEnabled = false;
        private int prometheusPort = 9464;
        private long ingressToNosSampleTtlMs = 1_800_000L;
        private long ingressToNosEvictIntervalMs = 60_000L;

        public boolean isMetricsEnabled() {
            return metricsEnabled;
        }

        public void setMetricsEnabled(boolean metricsEnabled) {
            this.metricsEnabled = metricsEnabled;
        }

        public int getPrometheusPort() {
            return prometheusPort;
        }

        public void setPrometheusPort(int prometheusPort) {
            this.prometheusPort = Math.min(65_535, Math.max(1, prometheusPort));
        }

        public long getIngressToNosSampleTtlMs() {
            return ingressToNosSampleTtlMs;
        }

        public void setIngressToNosSampleTtlMs(long ingressToNosSampleTtlMs) {
            this.ingressToNosSampleTtlMs = Math.max(60_000L, ingressToNosSampleTtlMs);
        }

        public long getIngressToNosEvictIntervalMs() {
            return ingressToNosEvictIntervalMs;
        }

        public void setIngressToNosEvictIntervalMs(long ingressToNosEvictIntervalMs) {
            this.ingressToNosEvictIntervalMs = Math.max(5_000L, ingressToNosEvictIntervalMs);
        }
    }

    /**
     * Aeron Cluster substrate config (ADR 0001 / plan
     * {@code system-documentation/plans/oms-aeron-cluster-substrate.md}).
     *
     * <p>Phase 1a wires only the cluster <em>client</em> half (Spring bean
     * {@code OmsClusterIngressClient}). The cluster <em>node</em> reads its own
     * config from environment variables in {@code OmsClusterNodeBootstrap} —
     * cluster nodes are plain-Java mains, not Spring apps.
     */
    public static class Cluster {

        private final Client client = new Client();
        private final Projector projector = new Projector();
        private final FixEgress fixEgress = new FixEgress();

        public Client getClient() { return client; }
        public Projector getProjector() { return projector; }
        public FixEgress getFixEgress() { return fixEgress; }

        /**
         * Phase 3 of the Aeron Cluster substrate plan: configuration for the
         * {@code oms-fix-egress} JVM. Mirrors {@link Projector} on the FIX-side: subscribes to
         * the same cluster events recording (Aeron Archive replay), translates admitted /
         * working orders into QuickFIX {@code NewOrderSingle}, and (Phase 3 slice 3d) submits
         * inbound {@code ExecutionReport} back to the cluster as an {@code ApplyExecutionReport}
         * command.
         *
         * <p>Slice 3a shipped only the topology scaffold ({@link #isEnabled()}). Slice 3b-1
         * adds the replay-channel / archive-control fields needed to actually consume cluster
         * events. {@link #replayStreamId} defaults to {@code 4323} so the egress JVM does not
         * collide with the production projector's stream id (4321) or the in-test projector
         * singleton's id (4322); the setter rejects equality with
         * {@link com.balh.oms.cluster.OmsClusterWireFormat#EVENTS_STREAM_ID} for the same
         * reason {@link Projector#setReplayStreamId(int)} does.
         */
        public static class FixEgress {

            private static final long DEFAULT_POLL_PARK_NANOS = 1_000_000L;
            private static final int DEFAULT_FRAGMENT_LIMIT = 64;
            private static final long DEFAULT_RECORDING_LOOKUP_PARK_MS = 100L;
            private static final int DEFAULT_REPLAY_STREAM_ID = 4323;
            /**
             * Default park between {@code Session.sendToTarget} retry attempts while the FIX
             * session is reconnecting / re-logging-on. Same shape as
             * {@code oms.fix.outbound-dedicated-not-ready-park-nanos} on the legacy dispatch
             * worker; named separately so the egress JVM can be tuned independently.
             */
            private static final long DEFAULT_SESSION_NOT_READY_PARK_NANOS = 50_000_000L;

            private boolean enabled = false;
            private String aeronDirectory = "";
            private String archiveControlRequestChannel = "aeron:ipc?term-length=64k";
            private String archiveControlResponseChannel = "aeron:ipc?term-length=64k";
            private String replayChannel = "aeron:ipc?term-length=64k";
            private int replayStreamId = DEFAULT_REPLAY_STREAM_ID;
            private long pollParkNanos = DEFAULT_POLL_PARK_NANOS;
            private int fragmentLimit = DEFAULT_FRAGMENT_LIMIT;
            private long recordingLookupParkMs = DEFAULT_RECORDING_LOOKUP_PARK_MS;
            private long sessionNotReadyParkNanos = DEFAULT_SESSION_NOT_READY_PARK_NANOS;

            public boolean isEnabled() { return enabled; }
            public void setEnabled(boolean enabled) { this.enabled = enabled; }

            public String getAeronDirectory() { return aeronDirectory; }
            public void setAeronDirectory(String aeronDirectory) {
                this.aeronDirectory = aeronDirectory == null ? "" : aeronDirectory.trim();
            }

            public String getArchiveControlRequestChannel() { return archiveControlRequestChannel; }
            public void setArchiveControlRequestChannel(String archiveControlRequestChannel) {
                this.archiveControlRequestChannel =
                        archiveControlRequestChannel == null || archiveControlRequestChannel.isBlank()
                                ? "aeron:ipc?term-length=64k"
                                : archiveControlRequestChannel.trim();
            }

            public String getArchiveControlResponseChannel() { return archiveControlResponseChannel; }
            public void setArchiveControlResponseChannel(String archiveControlResponseChannel) {
                this.archiveControlResponseChannel =
                        archiveControlResponseChannel == null || archiveControlResponseChannel.isBlank()
                                ? "aeron:ipc?term-length=64k"
                                : archiveControlResponseChannel.trim();
            }

            public String getReplayChannel() { return replayChannel; }
            public void setReplayChannel(String replayChannel) {
                this.replayChannel =
                        replayChannel == null || replayChannel.isBlank()
                                ? "aeron:ipc?term-length=64k"
                                : replayChannel.trim();
            }

            public int getReplayStreamId() { return replayStreamId; }
            public void setReplayStreamId(int replayStreamId) {
                if (replayStreamId == com.balh.oms.cluster.OmsClusterWireFormat.EVENTS_STREAM_ID) {
                    throw new IllegalArgumentException(
                            "oms.cluster.fix-egress.replay-stream-id must differ from EVENTS_STREAM_ID="
                                    + com.balh.oms.cluster.OmsClusterWireFormat.EVENTS_STREAM_ID);
                }
                this.replayStreamId = replayStreamId;
            }

            public long getPollParkNanos() { return pollParkNanos; }
            public void setPollParkNanos(long pollParkNanos) {
                this.pollParkNanos = Math.max(1_000L, pollParkNanos);
            }

            public int getFragmentLimit() { return fragmentLimit; }
            public void setFragmentLimit(int fragmentLimit) {
                this.fragmentLimit = Math.max(1, fragmentLimit);
            }

            public long getRecordingLookupParkMs() { return recordingLookupParkMs; }
            public void setRecordingLookupParkMs(long recordingLookupParkMs) {
                this.recordingLookupParkMs = Math.max(10L, recordingLookupParkMs);
            }

            public long getSessionNotReadyParkNanos() { return sessionNotReadyParkNanos; }
            public void setSessionNotReadyParkNanos(long sessionNotReadyParkNanos) {
                this.sessionNotReadyParkNanos = Math.max(1_000L, sessionNotReadyParkNanos);
            }
        }

        /**
         * Phase 2 of the Aeron Cluster substrate plan: configuration for {@code OmsPostgresProjector}.
         *
         * <p>Used only by JVMs running the {@code oms-postgres-projector} profile. The projector connects
         * to the cluster member's local Aeron MediaDriver via {@link #aeronDirectory}, opens an
         * {@code AeronArchive} session via {@link #archiveControlRequestChannel} /
         * {@link #archiveControlResponseChannel}, locates the recording for
         * {@code OmsClusterWireFormat.EVENTS_CHANNEL}/{@code EVENTS_STREAM_ID}, and replays from the
         * cursor stored in {@code aeron_projector_cursor}.
         */
        public static class Projector {

            /** Default poll cadence inside the projector replay loop. */
            private static final long DEFAULT_POLL_PARK_NANOS = 1_000_000L;

            /** Default fragment limit per replay poll. Keeps a single poll bounded. */
            private static final int DEFAULT_FRAGMENT_LIMIT = 64;

            /** Default backoff between {@code listRecordings} retries while waiting for the recording to appear. */
            private static final long DEFAULT_RECORDING_LOOKUP_PARK_MS = 100L;

            private boolean enabled = false;
            private String aeronDirectory = "";
            private String archiveControlRequestChannel = "aeron:ipc?term-length=64k";
            private String archiveControlResponseChannel = "aeron:ipc?term-length=64k";
            private String replayChannel = "aeron:ipc?term-length=64k";
            private int replayStreamId = 4321;
            private long pollParkNanos = DEFAULT_POLL_PARK_NANOS;
            private int fragmentLimit = DEFAULT_FRAGMENT_LIMIT;
            private long recordingLookupParkMs = DEFAULT_RECORDING_LOOKUP_PARK_MS;

            public boolean isEnabled() { return enabled; }
            public void setEnabled(boolean enabled) { this.enabled = enabled; }

            public String getAeronDirectory() { return aeronDirectory; }
            public void setAeronDirectory(String aeronDirectory) {
                this.aeronDirectory = aeronDirectory == null ? "" : aeronDirectory.trim();
            }

            public String getArchiveControlRequestChannel() { return archiveControlRequestChannel; }
            public void setArchiveControlRequestChannel(String archiveControlRequestChannel) {
                this.archiveControlRequestChannel =
                        archiveControlRequestChannel == null || archiveControlRequestChannel.isBlank()
                                ? "aeron:ipc?term-length=64k"
                                : archiveControlRequestChannel.trim();
            }

            public String getArchiveControlResponseChannel() { return archiveControlResponseChannel; }
            public void setArchiveControlResponseChannel(String archiveControlResponseChannel) {
                this.archiveControlResponseChannel =
                        archiveControlResponseChannel == null || archiveControlResponseChannel.isBlank()
                                ? "aeron:ipc?term-length=64k"
                                : archiveControlResponseChannel.trim();
            }

            public String getReplayChannel() { return replayChannel; }
            public void setReplayChannel(String replayChannel) {
                this.replayChannel =
                        replayChannel == null || replayChannel.isBlank()
                                ? "aeron:ipc?term-length=64k"
                                : replayChannel.trim();
            }

            public int getReplayStreamId() { return replayStreamId; }
            public void setReplayStreamId(int replayStreamId) {
                if (replayStreamId == com.balh.oms.cluster.OmsClusterWireFormat.EVENTS_STREAM_ID) {
                    throw new IllegalArgumentException(
                            "oms.cluster.projector.replay-stream-id must differ from EVENTS_STREAM_ID="
                                    + com.balh.oms.cluster.OmsClusterWireFormat.EVENTS_STREAM_ID);
                }
                this.replayStreamId = replayStreamId;
            }

            public long getPollParkNanos() { return pollParkNanos; }
            public void setPollParkNanos(long pollParkNanos) {
                this.pollParkNanos = Math.max(1_000L, pollParkNanos);
            }

            public int getFragmentLimit() { return fragmentLimit; }
            public void setFragmentLimit(int fragmentLimit) {
                this.fragmentLimit = Math.max(1, fragmentLimit);
            }

            public long getRecordingLookupParkMs() { return recordingLookupParkMs; }
            public void setRecordingLookupParkMs(long recordingLookupParkMs) {
                this.recordingLookupParkMs = Math.max(10L, recordingLookupParkMs);
            }
        }

        public static class Client {

            /** Default per-call timeout for {@code submitAcceptOrder}. */
            private static final long DEFAULT_SUBMIT_TIMEOUT_MS = 5_000L;
            /** Default Aeron message timeout (single-node startup needs >= 5s in cold-start). */
            private static final long DEFAULT_AERON_MESSAGE_TIMEOUT_MS = 10_000L;
            /** Default initial-connect retry budget. */
            private static final long DEFAULT_CONNECT_TIMEOUT_MS = 30_000L;
            /** Park between back-pressure retries on cluster offer. */
            private static final long DEFAULT_OFFER_BACKPRESSURE_PARK_NANOS = 100_000L;
            /** Park between egress poll passes when waiting for our correlation id. */
            private static final long DEFAULT_EGRESS_POLL_PARK_NANOS = 100_000L;
            /**
             * Default heartbeat interval. Aeron Cluster {@code ConsensusModule} default
             * {@code sessionTimeoutNs} is 5s; we keep the session warm well below that to
             * survive idle periods between {@code submitAcceptOrder} calls (e.g. a quiet
             * trading night, a stalled producer, or a Spring context that loaded the bean
             * but hasn't sent a command yet).
             */
            private static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 1_000L;

            private boolean enabled = false;
            private String aeronDirectory = "";
            private String ingressEndpoints = "0=localhost:20110";
            private String ingressChannel = "aeron:udp?endpoint=localhost:0";
            private String egressChannel = "aeron:udp?endpoint=localhost:0";
            private long submitTimeoutMs = DEFAULT_SUBMIT_TIMEOUT_MS;
            private long messageTimeoutMs = DEFAULT_AERON_MESSAGE_TIMEOUT_MS;
            private long connectTimeoutMs = DEFAULT_CONNECT_TIMEOUT_MS;
            private long offerBackpressureParkNanos = DEFAULT_OFFER_BACKPRESSURE_PARK_NANOS;
            private long egressPollParkNanos = DEFAULT_EGRESS_POLL_PARK_NANOS;
            private long heartbeatIntervalMs = DEFAULT_HEARTBEAT_INTERVAL_MS;

            public boolean isEnabled() { return enabled; }
            public void setEnabled(boolean enabled) { this.enabled = enabled; }

            public String getAeronDirectory() { return aeronDirectory; }
            public void setAeronDirectory(String aeronDirectory) {
                this.aeronDirectory = aeronDirectory == null ? "" : aeronDirectory.trim();
            }

            public String getIngressEndpoints() { return ingressEndpoints; }
            public void setIngressEndpoints(String ingressEndpoints) {
                this.ingressEndpoints =
                        ingressEndpoints == null || ingressEndpoints.isBlank()
                                ? "0=localhost:20110"
                                : ingressEndpoints.trim();
            }

            public String getIngressChannel() { return ingressChannel; }
            public void setIngressChannel(String ingressChannel) {
                this.ingressChannel =
                        ingressChannel == null || ingressChannel.isBlank()
                                ? "aeron:udp?endpoint=localhost:0"
                                : ingressChannel.trim();
            }

            public String getEgressChannel() { return egressChannel; }
            public void setEgressChannel(String egressChannel) {
                this.egressChannel =
                        egressChannel == null || egressChannel.isBlank()
                                ? "aeron:udp?endpoint=localhost:0"
                                : egressChannel.trim();
            }

            public long getSubmitTimeoutMs() { return submitTimeoutMs; }
            public void setSubmitTimeoutMs(long submitTimeoutMs) {
                this.submitTimeoutMs = Math.max(100L, submitTimeoutMs);
            }

            public long getMessageTimeoutMs() { return messageTimeoutMs; }
            public void setMessageTimeoutMs(long messageTimeoutMs) {
                this.messageTimeoutMs = Math.max(1_000L, messageTimeoutMs);
            }

            public long getConnectTimeoutMs() { return connectTimeoutMs; }
            public void setConnectTimeoutMs(long connectTimeoutMs) {
                this.connectTimeoutMs = Math.max(1_000L, connectTimeoutMs);
            }

            public long getOfferBackpressureParkNanos() { return offerBackpressureParkNanos; }
            public void setOfferBackpressureParkNanos(long offerBackpressureParkNanos) {
                this.offerBackpressureParkNanos = Math.max(1_000L, offerBackpressureParkNanos);
            }

            public long getEgressPollParkNanos() { return egressPollParkNanos; }
            public void setEgressPollParkNanos(long egressPollParkNanos) {
                this.egressPollParkNanos = Math.max(1_000L, egressPollParkNanos);
            }

            public long getHeartbeatIntervalMs() { return heartbeatIntervalMs; }
            public void setHeartbeatIntervalMs(long heartbeatIntervalMs) {
                this.heartbeatIntervalMs = Math.max(50L, heartbeatIntervalMs);
            }
        }
    }
}
