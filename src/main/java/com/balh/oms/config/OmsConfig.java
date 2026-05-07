package com.balh.oms.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;
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
    private final Shard shard = new Shard();
    private final Control control = new Control();
    private final Outbox outbox = new Outbox();
    private final Chronicle chronicle = new Chronicle();
    private final Events events = new Events();
    private final Ledger ledger = new Ledger();
    private final DomainEvents domainEvents = new DomainEvents();
    private final Pii pii = new Pii();
    private final Risk risk = new Risk();
    private final Routing routing = new Routing();
    private final Fix fix = new Fix();

    public Http getHttp() { return http; }
    public Shard getShard() { return shard; }
    public Control getControl() { return control; }
    public Outbox getOutbox() { return outbox; }
    public Chronicle getChronicle() { return chronicle; }
    public Events getEvents() { return events; }
    public Ledger getLedger() { return ledger; }
    public DomainEvents getDomainEvents() { return domainEvents; }
    public Pii getPii() { return pii; }
    public Risk getRisk() { return risk; }
    public Routing getRouting() { return routing; }
    public Fix getFix() { return fix; }

    public static class Http {
        private String internalApiKey = "";
        public String getInternalApiKey() { return internalApiKey; }
        public void setInternalApiKey(String v) { this.internalApiKey = v; }
    }

    public static class Shard {
        private int id = 0;
        private int count = 1;
        public int getId() { return id; }
        public void setId(int id) { this.id = id; }
        public int getCount() { return count; }
        public void setCount(int count) { this.count = count; }
    }

    public static class Control {
        /** Default 5 minutes — see plans/oms-phase0-interim-decisions.md until trading-ops signs a stricter value. */
        private long maxJobAgeMs = 300_000L;
        private int tailerBatchSize = 100;
        public long getMaxJobAgeMs() { return maxJobAgeMs; }
        public void setMaxJobAgeMs(long v) { this.maxJobAgeMs = v; }
        public int getTailerBatchSize() { return tailerBatchSize; }
        public void setTailerBatchSize(int v) { this.tailerBatchSize = v; }
    }

    public static class Outbox {
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

    public static class Chronicle {
        private boolean enabled = true;
        private String queueDir = "./queues/control";
        private String rollCycle = "DAILY";
        private long tailPollIntervalMs = 50L;
        private int tailBatchMaxMessages = 200;
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean v) { this.enabled = v; }
        public String getQueueDir() { return queueDir; }
        public void setQueueDir(String v) { this.queueDir = v; }
        public String getRollCycle() { return rollCycle; }
        public void setRollCycle(String v) { this.rollCycle = v; }
        public long getTailPollIntervalMs() { return tailPollIntervalMs; }
        public void setTailPollIntervalMs(long v) { this.tailPollIntervalMs = v; }
        public int getTailBatchMaxMessages() { return tailBatchMaxMessages; }
        public void setTailBatchMaxMessages(int v) { this.tailBatchMaxMessages = v; }
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
        private BigDecimal fatFingerMaxLimitPrice = BigDecimal.ZERO;
        private BigDecimal fatFingerMaxOrderQuantity = BigDecimal.ZERO;
        private BigDecimal maxOrderNotional = BigDecimal.ZERO;

        public boolean isInstrumentAllowlistEnabled() { return instrumentAllowlistEnabled; }
        public void setInstrumentAllowlistEnabled(boolean v) { this.instrumentAllowlistEnabled = v; }
        public String getAllowedInstrumentSymbols() { return allowedInstrumentSymbols; }
        public void setAllowedInstrumentSymbols(String v) { this.allowedInstrumentSymbols = v == null ? "" : v; }
        public BigDecimal getFatFingerMaxLimitPrice() { return fatFingerMaxLimitPrice; }
        public void setFatFingerMaxLimitPrice(BigDecimal v) { this.fatFingerMaxLimitPrice = v == null ? BigDecimal.ZERO : v; }
        public BigDecimal getFatFingerMaxOrderQuantity() { return fatFingerMaxOrderQuantity; }
        public void setFatFingerMaxOrderQuantity(BigDecimal v) { this.fatFingerMaxOrderQuantity = v == null ? BigDecimal.ZERO : v; }
        public BigDecimal getMaxOrderNotional() { return maxOrderNotional; }
        public void setMaxOrderNotional(BigDecimal v) { this.maxOrderNotional = v == null ? BigDecimal.ZERO : v; }

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
     * Outbound routing / return-path simulation (slice 3). {@code backend=noop} is the
     * default safe mode; {@code simulated} drives {@link com.balh.oms.routing.SimulatedBrokerDispatcher}
     * + {@link com.balh.oms.routing.SimulatedReturnPathProjectionWorker}; {@code fix} drives QuickFIX/J
     * ({@link com.balh.oms.fix.FixRouteDispatcher}).
     */
    public static class Routing {
        private String backend = "noop";
        private String marketContextStubJson = "{\"stub\":true}";
        private final Simulated simulated = new Simulated();

        public String getBackend() { return backend; }
        public void setBackend(String v) { this.backend = v == null ? "noop" : v; }
        public String getMarketContextStubJson() { return marketContextStubJson; }
        public void setMarketContextStubJson(String v) {
            this.marketContextStubJson = v == null || v.isBlank() ? "{\"stub\":true}" : v;
        }
        public Simulated getSimulated() { return simulated; }

        public static class Simulated {
            private String venueId = "SIM";
            private int queueCapacity = 10_000;
            private long pollIntervalMs = 50L;
            private boolean schedulerEnabled = true;

            public String getVenueId() { return venueId; }
            public void setVenueId(String venueId) { this.venueId = venueId == null ? "SIM" : venueId; }
            public int getQueueCapacity() { return queueCapacity; }
            public void setQueueCapacity(int queueCapacity) { this.queueCapacity = queueCapacity; }
            public long getPollIntervalMs() { return pollIntervalMs; }
            public void setPollIntervalMs(long pollIntervalMs) { this.pollIntervalMs = pollIntervalMs; }
            public boolean isSchedulerEnabled() { return schedulerEnabled; }
            public void setSchedulerEnabled(boolean schedulerEnabled) { this.schedulerEnabled = schedulerEnabled; }
        }
    }

    /**
     * QuickFIX/J initiator / store paths when {@code oms.routing.backend=fix}.
     */
    public static class Fix {
        private boolean autoStart = false;
        private int outboundQueueCapacity = 10_000;
        private String fileStorePath = "./queues/fix";
        private String socketConnectHost = "127.0.0.1";
        private int socketConnectPort = 9876;
        private String senderCompId = "OMS_INIT";
        private String targetCompId = "BROKER_ACCEPT";
        private int heartBtInt = 30;
        private long outboundPollIntervalMs = 100L;
        /** 0 = disabled; otherwise reject WORKING orders at FIX dequeue when older than this (ms). */
        private long maxOutboundJobAgeMs = 0L;
        /** Venue id stamped on {@code ExecutionTradeCommand} from inbound ERs. */
        private String venueIdForExecutions = "FIX";
        private boolean useDataDictionary = false;
        /** Logical FIX route key for {@code fix_route_state} (default single route). */
        private String routeKey = "default";
        /** NOS rate limit; {@code <= 0} disables token bucket (default). */
        private double outboundTokensPerSecond = 0;
        /** Bucket capacity when rate limiting is enabled. */
        private int outboundTokenBurst = 100;

        public boolean isAutoStart() { return autoStart; }
        public void setAutoStart(boolean autoStart) { this.autoStart = autoStart; }
        public int getOutboundQueueCapacity() { return outboundQueueCapacity; }
        public void setOutboundQueueCapacity(int outboundQueueCapacity) {
            this.outboundQueueCapacity = Math.max(1, outboundQueueCapacity);
        }
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
        public long getOutboundPollIntervalMs() { return outboundPollIntervalMs; }
        public void setOutboundPollIntervalMs(long outboundPollIntervalMs) {
            this.outboundPollIntervalMs = Math.max(1L, outboundPollIntervalMs);
        }
        public long getMaxOutboundJobAgeMs() {
            return maxOutboundJobAgeMs;
        }

        public void setMaxOutboundJobAgeMs(long maxOutboundJobAgeMs) {
            this.maxOutboundJobAgeMs = Math.max(0L, maxOutboundJobAgeMs);
        }

        public String getVenueIdForExecutions() { return venueIdForExecutions; }
        public void setVenueIdForExecutions(String venueIdForExecutions) {
            this.venueIdForExecutions = venueIdForExecutions == null ? "FIX" : venueIdForExecutions;
        }
        public boolean isUseDataDictionary() { return useDataDictionary; }
        public void setUseDataDictionary(boolean useDataDictionary) { this.useDataDictionary = useDataDictionary; }

        public String getRouteKey() {
            return routeKey;
        }

        public void setRouteKey(String routeKey) {
            this.routeKey = routeKey == null || routeKey.isBlank() ? "default" : routeKey;
        }

        public double getOutboundTokensPerSecond() {
            return outboundTokensPerSecond;
        }

        public void setOutboundTokensPerSecond(double outboundTokensPerSecond) {
            this.outboundTokensPerSecond = outboundTokensPerSecond;
        }

        public int getOutboundTokenBurst() {
            return outboundTokenBurst;
        }

        public void setOutboundTokenBurst(int outboundTokenBurst) {
            this.outboundTokenBurst = Math.max(1, outboundTokenBurst);
        }
    }
}
