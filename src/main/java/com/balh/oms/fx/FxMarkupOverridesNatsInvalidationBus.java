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

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Cross-JVM cache invalidation for {@link FxMarkupOverridesService}.
 *
 * <p>Every JVM hosting {@code FxMarkupOverridesService} (today: ingress
 * + postgres-projector; tomorrow whatever else holds the service bean)
 * has its own in-process override cache. The controller write path
 * calls {@code refreshNow()} on the JVM that handled the request, but
 * remote JVMs only converge on the next scheduled refresh (60s default).
 *
 * <p>During that window:
 *
 * <ul>
 *   <li>BFF on submit hits the ingress {@code /fx/quote} and gets the
 *       new (wider) rate immediately;
 *   <li>customer UI subscribes to the projector's MQTT stream which is
 *       still publishing the pre-override rate.
 * </ul>
 *
 * <p>If the spread widened by more than the BFF drift threshold (5 bps
 * default), every cross-currency order rejects with {@code RATE_MOVED}
 * until the projector catches up. The reverse problem applies to revoke
 * and to auto-expire (the projector keeps applying a row the ingress
 * has already dropped from its active set).
 *
 * <p>This bus publishes a tiny core-NATS message ({@code subject =
 * oms.fx.markup_overrides.changed}, ~80 byte JSON body) on every local
 * write, and subscribes to the same subject so every <em>other</em> JVM
 * runs {@code refreshNow()} sub-second after the write. Self-originated
 * messages are filtered by a per-JVM UUID so we don't loop.
 *
 * <p>Best-effort by design — durability lives in the database, the
 * scheduled refresh remains a safety net, and a missed NATS message
 * simply means convergence takes up to one refresh tick instead of
 * a few milliseconds. The bus does not use JetStream (no need to
 * pay for storage on something the scheduled refresh already covers).
 *
 * <p>Wires itself into the service via setter injection during
 * {@link PostConstruct} so {@code FxMarkupOverridesService} stays
 * testable without any NATS dependency. Inactive when NATS is off
 * ({@code oms.events.nats.enabled=false} — typical for unit tests and
 * single-JVM dev), at which point the existing local {@code refreshNow()}
 * fully covers the write path.
 *
 * <p>P5 of {@code plans/fx-tier-quotes-production.md}.
 */
@Component
@ConditionalOnProperty(prefix = "oms.events.nats", name = "enabled", havingValue = "true")
public class FxMarkupOverridesNatsInvalidationBus implements OverridesChangePropagator {

    private static final Logger log = LoggerFactory.getLogger(FxMarkupOverridesNatsInvalidationBus.class);

    /**
     * Cache-invalidation subject. Deliberately <em>not</em> under
     * {@code oms.events.>} (the JetStream domain-event prefix); a missed
     * invalidation is recoverable via the scheduled refresh, so paying
     * for durable storage adds nothing.
     */
    static final String SUBJECT = "oms.fx.markup_overrides.changed";

    private final Connection nats;
    private final FxMarkupOverridesService service;
    private final ObjectMapper mapper;
    private final String selfJvmId = UUID.randomUUID().toString();

    private final Counter publishedCounter;
    private final Counter publishFailCounter;
    private final Counter receivedCounter;
    private final Counter selfFilteredCounter;
    private final Counter applyFailCounter;

    private volatile Dispatcher dispatcher;

    public FxMarkupOverridesNatsInvalidationBus(
            Connection nats,
            FxMarkupOverridesService service,
            ObjectMapper mapper,
            MeterRegistry registry) {
        this.nats = nats;
        this.service = service;
        this.mapper = mapper;
        this.publishedCounter = Counter.builder("oms_fx_markup_overrides_invalidation_total")
                .tag("direction", "out").tag("outcome", "ok")
                .description("Cross-JVM markup-overrides cache-invalidation messages")
                .register(registry);
        this.publishFailCounter = Counter.builder("oms_fx_markup_overrides_invalidation_total")
                .tag("direction", "out").tag("outcome", "fail")
                .register(registry);
        this.receivedCounter = Counter.builder("oms_fx_markup_overrides_invalidation_total")
                .tag("direction", "in").tag("outcome", "applied")
                .register(registry);
        this.selfFilteredCounter = Counter.builder("oms_fx_markup_overrides_invalidation_total")
                .tag("direction", "in").tag("outcome", "self_filtered")
                .register(registry);
        this.applyFailCounter = Counter.builder("oms_fx_markup_overrides_invalidation_total")
                .tag("direction", "in").tag("outcome", "apply_fail")
                .register(registry);
    }

    @PostConstruct
    void start() {
        dispatcher = nats.createDispatcher(this::handleInbound);
        dispatcher.subscribe(SUBJECT);
        service.setChangePropagator(this);
        log.info("[fx-mk-ovr-bus] online jvmId={} subject={}", selfJvmId, SUBJECT);
    }

    @PreDestroy
    void stop() {
        if (dispatcher != null) {
            try {
                nats.closeDispatcher(dispatcher);
            } catch (RuntimeException e) {
                log.debug("[fx-mk-ovr-bus] dispatcher close: {}", e.getMessage());
            }
        }
        // Restore the service to no-op so a controller write after PreDestroy
        // doesn't NPE through a half-shutdown bus.
        service.setChangePropagator(OverridesChangePropagator.NOOP);
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
            // Local refreshNow has already run; the remote JVM falls back
            // to its scheduled refresh. Logged at debug because the safety
            // net keeps the system correct.
            publishFailCounter.increment();
            log.debug("[fx-mk-ovr-bus] publish failed action={} id={}: {}", action, id, e.getMessage());
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
                log.debug("[fx-mk-ovr-bus] applied remote {} id={} from jvmId={}",
                        root.path("action").asText("?"),
                        root.path("id").asLong(-1L),
                        sender);
            }
        } catch (Exception e) {
            applyFailCounter.increment();
            log.warn("[fx-mk-ovr-bus] inbound apply failed: {}", e.getMessage());
        }
    }

    /** Test hook — exposes the per-JVM UUID so tests can simulate self-filtering. */
    String getSelfJvmId() {
        return selfJvmId;
    }
}
