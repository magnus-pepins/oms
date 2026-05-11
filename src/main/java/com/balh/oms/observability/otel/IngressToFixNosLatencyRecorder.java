package com.balh.oms.observability.otel;

import com.balh.oms.config.OmsConfig;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Tracks wall time from successful internal HTTP order accept (Postgres commit of a new order) until
 * QuickFIX {@code NewOrderSingle} is sent, when {@code oms.routing.backend=fix} and OTel metrics are enabled.
 *
 * <p>Recorded on the OpenTelemetry {@link DoubleHistogram} {@value IngressToFixNosLatencyLimits#OTEL_HISTOGRAM_NAME}
 * (unit {@code ms}), scraped via {@code oms.otel.metrics-enabled=true} Prometheus listener. Use Prometheus
 * {@code histogram_quantile} on {@code _bucket} series for p50 / p95 load SLOs.
 */
@Component
public class IngressToFixNosLatencyRecorder {

    private final OmsConfig omsConfig;
    private final DoubleHistogram ingressToNosMs;
    private final LongCounter samplesDiscarded;
    private final ConcurrentHashMap<UUID, Long> startNanosByOrderId = new ConcurrentHashMap<>();

    public IngressToFixNosLatencyRecorder(OmsConfig omsConfig, ObjectProvider<OpenTelemetry> openTelemetry) {
        this.omsConfig = omsConfig;
        OpenTelemetry otel = openTelemetry.getIfAvailable();
        if (otel == null) {
            this.ingressToNosMs = null;
            this.samplesDiscarded = null;
            return;
        }
        Meter meter = otel.getMeter("com.balh.oms");
        this.ingressToNosMs = meter.histogramBuilder(IngressToFixNosLatencyLimits.OTEL_HISTOGRAM_NAME)
                .setDescription(
                        "Latency from internal HTTP order accept (committed NEW order) to FIX NewOrderSingle send")
                .setUnit("ms")
                .setExplicitBucketBoundariesAdvice(IngressToFixNosLatencyLimits.INGRESS_TO_NOS_HISTOGRAM_BOUNDARIES_MS)
                .build();
        this.samplesDiscarded = meter.counterBuilder(IngressToFixNosLatencyLimits.OTEL_DISCARDED_COUNTER_NAME)
                .setDescription("Ingress→NOS timing samples removed without recording a histogram (reject, TTL, …)")
                .build();
    }

    /** After successful insert of a new order; no-op unless routing backend is {@code fix} and OTel histogram is on. */
    public void onOrderIngressCommitted(UUID orderId) {
        if (ingressToNosMs == null) {
            return;
        }
        if (!"fix".equalsIgnoreCase(omsConfig.getRouting().getBackend())) {
            return;
        }
        startNanosByOrderId.put(orderId, System.nanoTime());
    }

    public void recordNewOrderSingleSent(UUID orderId) {
        if (ingressToNosMs == null) {
            return;
        }
        Long startNanos = startNanosByOrderId.remove(orderId);
        if (startNanos == null) {
            return;
        }
        double ms = (System.nanoTime() - startNanos) / 1_000_000d;
        ingressToNosMs.record(ms);
    }

    public void discard(UUID orderId, String reason) {
        if (samplesDiscarded == null) {
            return;
        }
        Long removed = startNanosByOrderId.remove(orderId);
        if (removed == null) {
            return;
        }
        samplesDiscarded.add(1, Attributes.of(AttributeKey.stringKey(IngressToFixNosLatencyLimits.ATTR_REASON), reason));
    }

    @Scheduled(fixedDelayString = "${oms.otel.ingress-to-nos-evict-interval-ms:60000}")
    public void evictStaleSamples() {
        if (ingressToNosMs == null || samplesDiscarded == null) {
            return;
        }
        long ttlNanos = TimeUnit.MILLISECONDS.toNanos(omsConfig.getOtel().getIngressToNosSampleTtlMs());
        long now = System.nanoTime();
        List<UUID> stale = new ArrayList<>();
        for (var e : startNanosByOrderId.entrySet()) {
            if (now - e.getValue() > ttlNanos) {
                stale.add(e.getKey());
            }
        }
        for (UUID id : stale) {
            Long t = startNanosByOrderId.get(id);
            if (t != null && now - t > ttlNanos && startNanosByOrderId.remove(id, t)) {
                samplesDiscarded.add(
                        1,
                        Attributes.of(
                                AttributeKey.stringKey(IngressToFixNosLatencyLimits.ATTR_REASON),
                                IngressToFixNosLatencyLimits.REASON_TTL_EVICT));
            }
        }
    }
}
