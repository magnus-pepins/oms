package com.balh.oms.fx;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Cross-JVM cache invalidation for {@link FxTierKillsService}, sibling
 * of {@link FxMarkupOverridesNatsInvalidationBus}.
 *
 * <p>Every JVM hosting {@code FxTierKillsService} (today: ingress for
 * the write API + postgres-projector for the publisher; plus any future
 * processes) has its own in-process kill cache. The controller write
 * path calls {@code refreshNow()} on the JVM that handled the request,
 * but remote JVMs only converge on the next scheduled refresh
 * (60s default) — meaning a "kill business now" click would take up to
 * a minute before the projector's publisher stops emitting that tier.
 * Operator clicks the button, watches MQTT, sees no change, panics.
 *
 * <p>This bus publishes a tiny core-NATS message ({@code subject =
 * oms.fx.tier_kills.changed}, ~80 byte JSON body) on every local write,
 * and subscribes to the same subject so every <em>other</em> JVM runs
 * {@code refreshNow()} sub-second after the write. Self-originated
 * messages are filtered by a per-JVM UUID so we don't loop.
 *
 * <p>Best-effort by design — durability lives in the database, the
 * scheduled refresh remains a safety net, and a missed NATS message
 * simply means convergence takes up to one refresh tick instead of
 * a few milliseconds. The bus does not use JetStream (no need to pay
 * for storage on something the scheduled refresh already covers).
 *
 * <p>Wires itself into the service via setter injection during
 * {@link PostConstruct} so {@code FxTierKillsService} stays testable
 * without any NATS dependency. Inactive when NATS is off
 * ({@code oms.events.nats.enabled=false}).
 */
@Component
@ConditionalOnProperty(prefix = "oms.events.nats", name = "enabled", havingValue = "true")
public class FxTierKillsNatsInvalidationBus implements TierKillsChangePropagator {

    private static final Logger log = LoggerFactory.getLogger(FxTierKillsNatsInvalidationBus.class);

    /**
     * Cache-invalidation subject. Same naming pattern as the markup-
     * overrides bus ({@code oms.fx.<entity>.changed}), so monitoring
     * a single {@code oms.fx.>} subscription tap (e.g. in NATS CLI)
     * surfaces both streams of invalidations.
     */
    static final String SUBJECT = "oms.fx.tier_kills.changed";

    private final Connection nats;
    private final FxTierKillsService service;
    private final ObjectMapper mapper;
    private final String selfJvmId = UUID.randomUUID().toString();

    private final Counter publishedCounter;
    private final Counter publishFailCounter;
    private final Counter receivedCounter;
    private final Counter selfFilteredCounter;
    private final Counter applyFailCounter;

    private volatile Dispatcher dispatcher;

    public FxTierKillsNatsInvalidationBus(
            Connection nats,
            FxTierKillsService service,
            ObjectMapper mapper,
            MeterRegistry registry) {
        this.nats = nats;
        this.service = service;
        this.mapper = mapper;
        this.publishedCounter = Counter.builder("oms_fx_tier_kills_invalidation_total")
                .tag("direction", "out").tag("outcome", "ok")
                .description("Cross-JVM tier-kills cache-invalidation messages")
                .register(registry);
        this.publishFailCounter = Counter.builder("oms_fx_tier_kills_invalidation_total")
                .tag("direction", "out").tag("outcome", "fail")
                .register(registry);
        this.receivedCounter = Counter.builder("oms_fx_tier_kills_invalidation_total")
                .tag("direction", "in").tag("outcome", "applied")
                .register(registry);
        this.selfFilteredCounter = Counter.builder("oms_fx_tier_kills_invalidation_total")
                .tag("direction", "in").tag("outcome", "self_filtered")
                .register(registry);
        this.applyFailCounter = Counter.builder("oms_fx_tier_kills_invalidation_total")
                .tag("direction", "in").tag("outcome", "apply_fail")
                .register(registry);
    }

    @PostConstruct
    void start() {
        dispatcher = nats.createDispatcher(this::handleInbound);
        dispatcher.subscribe(SUBJECT);
        service.setChangePropagator(this);
        log.info("[fx-tier-kills-bus] online jvmId={} subject={}", selfJvmId, SUBJECT);
    }

    @PreDestroy
    void stop() {
        if (dispatcher != null) {
            try {
                nats.closeDispatcher(dispatcher);
            } catch (RuntimeException e) {
                log.debug("[fx-tier-kills-bus] dispatcher close: {}", e.getMessage());
            }
        }
        service.setChangePropagator(TierKillsChangePropagator.NOOP);
    }

    @Override
    public void localChanged(String action, long id) {
        try {
            ObjectNode body = mapper.createObjectNode();
            body.put("jvmId", selfJvmId);
            body.put("action", action);
            body.put("id", id);
            nats.publish(SUBJECT, mapper.writeValueAsBytes(body));
            publishedCounter.increment();
        } catch (Exception e) {
            publishFailCounter.increment();
            log.debug("[fx-tier-kills-bus] publish failed action={} id={}: {}", action, id, e.getMessage());
        }
    }

    void handleInbound(Message msg) {
        try {
            JsonNode root = mapper.readTree(msg.getData());
            String sender = root.path("jvmId").asText("");
            if (selfJvmId.equals(sender)) {
                selfFilteredCounter.increment();
                return;
            }
            service.refreshNow();
            receivedCounter.increment();
            if (log.isDebugEnabled()) {
                log.debug("[fx-tier-kills-bus] applied remote {} id={} from jvmId={}",
                        root.path("action").asText("?"),
                        root.path("id").asLong(-1L),
                        sender);
            }
        } catch (Exception e) {
            applyFailCounter.increment();
            log.warn("[fx-tier-kills-bus] inbound apply failed: {}", e.getMessage());
        }
    }

    /** Test hook — exposes the per-JVM UUID so tests can simulate self-filtering. */
    String getSelfJvmId() {
        return selfJvmId;
    }
}
