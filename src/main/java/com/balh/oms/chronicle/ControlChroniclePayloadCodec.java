package com.balh.oms.chronicle;

import com.balh.oms.proto.control.v1.ControlPendingEvent;
import com.balh.oms.proto.control.v1.TelemetryHop;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.balh.oms.chronicle.ControlChronicleWireFormat.CHRONICLE_PROTO_PREFIX;
import static com.balh.oms.chronicle.ControlChronicleWireFormat.CHRONICLE_PROTO_PREFIX_LENGTH;
import static com.balh.oms.chronicle.ControlChronicleWireFormat.chronicleExcerptStartsWithProtoPrefix;

/**
 * Encodes/decodes control payloads: Chronicle excerpts carry
 * {@link ControlChronicleWireFormat#CHRONICLE_PROTO_PREFIX} + protobuf body.
 *
 * <p>Slice 3f (oms-aeron-cluster-substrate plan) removed the JSON-wrapped outbox payload helpers
 * because the {@code control_outbox} table is gone — only the direct Chronicle encode / decode
 * path survives, and slice 3g removes that too along with the rest of the chronicle module.
 */
@Component
public final class ControlChroniclePayloadCodec {

    /** Chronicle bytes for a domain event (stamps materialization + reconciler hop). */
    public byte[] chronicleAppendBytes(PendingControlEvent event) {
        return chronicleBytesWithMaterialization(toProtoForChronicle(event));
    }

    /**
     * Second Chronicle frame after a successful {@link ControlChronicleEventTypes#ORDER_ACCEPTED} tail apply: same
     * correlation fields and cumulative {@code telemetry_hops} as {@code appliedOrderAccepted}, plus
     * {@link PipelineTelemetryStages#CONTROL_TAIL_APPLY} at {@code tailObservedAt}. Not written to {@code control_outbox}.
     */
    public byte[] chronicleAppendAfterControlTailApply(PendingControlEvent appliedOrderAccepted, Instant tailObservedAt) {
        if (!ControlChronicleEventTypes.ORDER_ACCEPTED.equals(appliedOrderAccepted.type())) {
            throw new IllegalArgumentException(
                    "chronicleAppendAfterControlTailApply expects type OrderAccepted, got " + appliedOrderAccepted.type());
        }
        ControlPendingEvent.Builder b = ControlPendingEvent.newBuilder()
                .setType(ControlChronicleEventTypes.CONTROL_PIPELINE_TELEMETRY)
                .setOrderId(appliedOrderAccepted.orderId().toString())
                .setOrderVersion(appliedOrderAccepted.orderVersion())
                .setShardId(appliedOrderAccepted.shardId())
                .setAccountIdHash(appliedOrderAccepted.accountIdHash())
                .setOrderTimestamp(instantToTimestamp(appliedOrderAccepted.orderTimestamp()))
                .setEnqueuedAt(instantToTimestamp(appliedOrderAccepted.enqueuedAt()));
        appliedOrderAccepted
                .chronicleMaterializedAt()
                .ifPresent(i -> b.setChronicleMaterializedAt(instantToTimestamp(i)));
        for (ControlTelemetryHop h : appliedOrderAccepted.telemetryHops()) {
            b.addTelemetryHops(TelemetryHop.newBuilder()
                    .setStage(h.stage())
                    .setObservedAt(instantToTimestamp(h.observedAt()))
                    .build());
        }
        b.addTelemetryHops(TelemetryHop.newBuilder()
                .setStage(PipelineTelemetryStages.CONTROL_TAIL_APPLY)
                .setObservedAt(instantToTimestamp(tailObservedAt))
                .build());
        return withChroniclePrefix(b.build().toByteArray());
    }

    public PendingControlEvent decodeChronicleExcerpt(byte[] excerpt) {
        if (!chronicleExcerptStartsWithProtoPrefix(excerpt)) {
            throw new IllegalStateException(
                    "Chronicle control excerpt must start with OMS proto prefix (length>=" + CHRONICLE_PROTO_PREFIX_LENGTH + ")");
        }
        try {
            byte[] body = Arrays.copyOfRange(excerpt, CHRONICLE_PROTO_PREFIX_LENGTH, excerpt.length);
            return fromProto(ControlPendingEvent.parseFrom(body));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("Chronicle control protobuf decode failed", e);
        }
    }

    private static byte[] chronicleBytesWithMaterialization(ControlPendingEvent base) {
        Instant now = Instant.now();
        Timestamp t = instantToTimestamp(now);
        ControlPendingEvent.Builder b = base.toBuilder().setChronicleMaterializedAt(t);
        b.addTelemetryHops(TelemetryHop.newBuilder()
                .setStage(PipelineTelemetryStages.RECONCILER_CHRONICLE_APPEND)
                .setObservedAt(t)
                .build());
        return withChroniclePrefix(b.build().toByteArray());
    }

    private static byte[] withChroniclePrefix(byte[] protoBody) {
        byte[] out = new byte[CHRONICLE_PROTO_PREFIX_LENGTH + protoBody.length];
        System.arraycopy(CHRONICLE_PROTO_PREFIX, 0, out, 0, CHRONICLE_PROTO_PREFIX_LENGTH);
        System.arraycopy(protoBody, 0, out, CHRONICLE_PROTO_PREFIX_LENGTH, protoBody.length);
        return out;
    }

    /** Protobuf for Chronicle (no {@code chronicle_materialized_at}; ingress hop on {@code telemetry_hops}). */
    private static ControlPendingEvent toProtoForChronicle(PendingControlEvent e) {
        ControlPendingEvent.Builder b = ControlPendingEvent.newBuilder()
                .setType(e.type())
                .setOrderId(e.orderId().toString())
                .setOrderVersion(e.orderVersion())
                .setShardId(e.shardId())
                .setAccountIdHash(e.accountIdHash())
                .setOrderTimestamp(instantToTimestamp(e.orderTimestamp()))
                .setEnqueuedAt(instantToTimestamp(e.enqueuedAt()));
        e.chronicleMaterializedAt()
                .ifPresent(i -> b.setChronicleMaterializedAt(instantToTimestamp(i)));
        b.addTelemetryHops(TelemetryHop.newBuilder()
                .setStage(PipelineTelemetryStages.INGRESS)
                .setObservedAt(instantToTimestamp(e.enqueuedAt()))
                .build());
        for (ControlTelemetryHop hop : e.telemetryHops()) {
            b.addTelemetryHops(TelemetryHop.newBuilder()
                    .setStage(hop.stage())
                    .setObservedAt(instantToTimestamp(hop.observedAt()))
                    .build());
        }
        return b.build();
    }

    private static PendingControlEvent fromProto(ControlPendingEvent p) {
        Optional<Instant> materialized = p.hasChronicleMaterializedAt()
                ? Optional.of(timestampToInstant(p.getChronicleMaterializedAt()))
                : Optional.empty();
        List<ControlTelemetryHop> hops = p.getTelemetryHopsList().stream()
                .map(ControlChroniclePayloadCodec::mapProtoHop)
                .toList();
        return new PendingControlEvent(
                p.getType(),
                UUID.fromString(p.getOrderId()),
                p.getOrderVersion(),
                p.getShardId(),
                p.getAccountIdHash(),
                timestampToInstant(p.getOrderTimestamp()),
                timestampToInstant(p.getEnqueuedAt()),
                materialized,
                hops);
    }

    private static ControlTelemetryHop mapProtoHop(TelemetryHop h) {
        return new ControlTelemetryHop(h.getStage(), timestampToInstant(h.getObservedAt()));
    }

    private static Timestamp instantToTimestamp(Instant i) {
        return Timestamp.newBuilder().setSeconds(i.getEpochSecond()).setNanos(i.getNano()).build();
    }

    private static Instant timestampToInstant(Timestamp ts) {
        return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
    }
}
