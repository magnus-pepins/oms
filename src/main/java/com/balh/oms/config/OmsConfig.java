package com.balh.oms.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

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
    private final Pii pii = new Pii();

    public Http getHttp() { return http; }
    public Shard getShard() { return shard; }
    public Control getControl() { return control; }
    public Outbox getOutbox() { return outbox; }
    public Chronicle getChronicle() { return chronicle; }
    public Events getEvents() { return events; }
    public Pii getPii() { return pii; }

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
        private long maxJobAgeMs = 5000L;
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
            public boolean isEnabled() { return enabled; }
            public void setEnabled(boolean v) { this.enabled = v; }
            public String getUrl() { return url; }
            public void setUrl(String v) { this.url = v; }
            public String getSubjectPrefix() { return subjectPrefix; }
            public void setSubjectPrefix(String v) { this.subjectPrefix = v; }
        }
    }

    public static class Pii {
        private boolean auditTraceEnabled = false;
        private String hashSecret = "dev-only-change-me";
        public boolean isAuditTraceEnabled() { return auditTraceEnabled; }
        public void setAuditTraceEnabled(boolean v) { this.auditTraceEnabled = v; }
        public String getHashSecret() { return hashSecret; }
        public void setHashSecret(String v) { this.hashSecret = v; }
    }
}
