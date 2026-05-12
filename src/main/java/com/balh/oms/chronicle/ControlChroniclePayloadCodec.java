package com.balh.oms.chronicle;

import com.balh.oms.proto.control.v1.ControlPendingEvent;
import com.balh.oms.proto.control.v1.TelemetryHop;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.balh.oms.chronicle.ControlChronicleWireFormat.CHRONICLE_PROTO_PREFIX;
import static com.balh.oms.chronicle.ControlChronicleWireFormat.CHRONICLE_PROTO_PREFIX_LENGTH;
import static com.balh.oms.chronicle.ControlChronicleWireFormat.OUTBOX_JSON_KEY_FORMAT;
import static com.balh.oms.chronicle.ControlChronicleWireFormat.OUTBOX_JSON_KEY_PROTO_BASE64;
import static com.balh.oms.chronicle.ControlChronicleWireFormat.OUTBOX_PAYLOAD_FORMAT_PROTO_WRAPPED;
import static com.balh.oms.chronicle.ControlChronicleWireFormat.chronicleExcerptStartsWithProtoPrefix;

/**
 * Encodes/decodes control payloads: {@code control_outbox} stores JSONB with a base64 protobuf body; Chronicle stores
 * {@link ControlChronicleWireFormat#CHRONICLE_PROTO_PREFIX} + protobuf.
 */
@Component
public final class ControlChroniclePayloadCodec {

    private final ObjectMapper objectMapper;

    public ControlChroniclePayloadCodec(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /** JSON string suitable for {@code CAST(:payload AS jsonb)} — wrapped protobuf. */
    public String outboxPayloadJson(PendingControlEvent event) {
        try {
            String b64 = Base64.getEncoder().encodeToString(toProtoForOutbox(event).toByteArray());
            ObjectNode n = objectMapper.createObjectNode();
            n.put(OUTBOX_JSON_KEY_FORMAT, OUTBOX_PAYLOAD_FORMAT_PROTO_WRAPPED);
            n.put(OUTBOX_JSON_KEY_PROTO_BASE64, b64);
            return objectMapper.writeValueAsString(n);
        } catch (Exception e) {
            throw new IllegalStateException("control outbox proto wrap failed for orderId=" + event.orderId(), e);
        }
    }

    /**
     * Bytes appended to Chronicle for this outbox row (prefix + protobuf). Expects v2 wrapped JSON only; stamps
     * {@code chronicle_materialized_at} and a reconciler telemetry hop.
     */
    public byte[] chronicleAppendBytesFromOutboxPayloadText(String outboxPayloadText) {
        try {
            JsonNode root = objectMapper.readTree(outboxPayloadText);
            requireWrappedProtoOutbox(root);
            byte[] body = Base64.getDecoder().decode(root.path(OUTBOX_JSON_KEY_PROTO_BASE64).asText());
            ControlPendingEvent parsed = ControlPendingEvent.parseFrom(body);
            return chronicleBytesWithMaterialization(parsed);
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("control outbox payload decode failed", e);
        }
    }

    /** Decode for tests / diagnostics: outbox {@code payload::text} → domain event. */
    public PendingControlEvent decodeFromOutboxPayloadText(String outboxPayloadText) {
        try {
            JsonNode root = objectMapper.readTree(outboxPayloadText);
            requireWrappedProtoOutbox(root);
            byte[] body = Base64.getDecoder().decode(root.path(OUTBOX_JSON_KEY_PROTO_BASE64).asText());
            return fromProto(ControlPendingEvent.parseFrom(body));
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("control outbox payload decode failed", e);
        }
    }

    /** Chronicle bytes for a domain event (stamps materialization + reconciler hop). */
    public byte[] chronicleAppendBytes(PendingControlEvent event) {
        return chronicleBytesWithMaterialization(toProtoForOutbox(event));
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

    private static void requireWrappedProtoOutbox(JsonNode root) {
        if (!root.isObject()
                || !root.path(OUTBOX_JSON_KEY_FORMAT).isInt()
                || root.path(OUTBOX_JSON_KEY_FORMAT).asInt() != OUTBOX_PAYLOAD_FORMAT_PROTO_WRAPPED
                || !root.path(OUTBOX_JSON_KEY_PROTO_BASE64).isTextual()) {
            throw new IllegalStateException(
                    "control outbox payload must be v%d proto-wrapped (keys %s, %s); legacy flat JSON is not supported"
                            .formatted(
                                    OUTBOX_PAYLOAD_FORMAT_PROTO_WRAPPED,
                                    OUTBOX_JSON_KEY_FORMAT,
                                    OUTBOX_JSON_KEY_PROTO_BASE64));
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

    private static byte[] controlPendingEventToBytes(ControlPendingEvent p) {
        return p.toByteArray();
    }

    /** Protobuf for outbox / base64 (no {@code chronicle_materialized_at}; ingress hop on {@code telemetry_hops}). */
    private static ControlPendingEvent toProtoForOutbox(PendingControlEvent e) {
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
