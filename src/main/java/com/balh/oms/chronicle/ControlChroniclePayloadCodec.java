package com.balh.oms.chronicle;

import com.balh.oms.proto.control.v1.ControlPendingEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
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
 * {@link ControlChronicleWireFormat#CHRONICLE_PROTO_PREFIX} + protobuf. Legacy UTF-8 JSON excerpts and flat JSON outbox
 * rows are still readable.
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
            String b64 = Base64.getEncoder().encodeToString(controlPendingEventToBytes(toProtoForOutbox(event)));
            ObjectNode n = objectMapper.createObjectNode();
            n.put(OUTBOX_JSON_KEY_FORMAT, OUTBOX_PAYLOAD_FORMAT_PROTO_WRAPPED);
            n.put(OUTBOX_JSON_KEY_PROTO_BASE64, b64);
            return objectMapper.writeValueAsString(n);
        } catch (Exception e) {
            throw new IllegalStateException("control outbox proto wrap failed for orderId=" + event.orderId(), e);
        }
    }

    /**
     * Bytes appended to Chronicle for this outbox row (prefix + protobuf). Accepts wrapped JSON outbox text or legacy
     * flat {@link PendingControlEvent} JSON. Stamps {@code chronicle_materialized_at} on the protobuf for telemetry.
     */
    public byte[] chronicleAppendBytesFromOutboxPayloadText(String outboxPayloadText) {
        try {
            JsonNode root = objectMapper.readTree(outboxPayloadText);
            if (root.isObject()
                    && root.path(OUTBOX_JSON_KEY_FORMAT).isInt()
                    && root.path(OUTBOX_JSON_KEY_FORMAT).asInt() == OUTBOX_PAYLOAD_FORMAT_PROTO_WRAPPED
                    && root.path(OUTBOX_JSON_KEY_PROTO_BASE64).isTextual()) {
                byte[] body = Base64.getDecoder().decode(root.path(OUTBOX_JSON_KEY_PROTO_BASE64).asText());
                ControlPendingEvent parsed = ControlPendingEvent.parseFrom(body);
                return chronicleBytesWithMaterialization(parsed);
            }
            PendingControlEvent legacy = objectMapper.treeToValue(root, PendingControlEvent.class);
            return chronicleBytesWithMaterialization(toProtoForOutbox(legacy));
        } catch (Exception e) {
            throw new IllegalStateException("control outbox payload decode failed", e);
        }
    }

    /** Decode for tests / diagnostics: outbox {@code payload::text} → domain event. */
    public PendingControlEvent decodeFromOutboxPayloadText(String outboxPayloadText) {
        try {
            JsonNode root = objectMapper.readTree(outboxPayloadText);
            if (root.isObject()
                    && root.path(OUTBOX_JSON_KEY_FORMAT).isInt()
                    && root.path(OUTBOX_JSON_KEY_FORMAT).asInt() == OUTBOX_PAYLOAD_FORMAT_PROTO_WRAPPED
                    && root.path(OUTBOX_JSON_KEY_PROTO_BASE64).isTextual()) {
                byte[] body = Base64.getDecoder().decode(root.path(OUTBOX_JSON_KEY_PROTO_BASE64).asText());
                return fromProto(ControlPendingEvent.parseFrom(body));
            }
            return objectMapper.treeToValue(root, PendingControlEvent.class);
        } catch (Exception e) {
            throw new IllegalStateException("control outbox payload decode failed", e);
        }
    }

    /** Chronicle bytes for a domain event (stamps {@code chronicle_materialized_at}). */
    public byte[] chronicleAppendBytes(PendingControlEvent event) {
        return chronicleBytesWithMaterialization(toProtoForOutbox(event));
    }

    public PendingControlEvent decodeChronicleExcerpt(byte[] excerpt) {
        try {
            if (chronicleExcerptStartsWithProtoPrefix(excerpt)) {
                byte[] body = Arrays.copyOfRange(excerpt, CHRONICLE_PROTO_PREFIX_LENGTH, excerpt.length);
                return fromProto(ControlPendingEvent.parseFrom(body));
            }
            return objectMapper.readValue(excerpt, PendingControlEvent.class);
        } catch (InvalidProtocolBufferException e) {
            try {
                return objectMapper.readValue(excerpt, PendingControlEvent.class);
            } catch (Exception e2) {
                e2.addSuppressed(e);
                throw new IllegalStateException("Chronicle excerpt is neither valid proto nor JSON", e2);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Chronicle excerpt decode failed", e);
        }
    }

    private static byte[] chronicleBytesWithMaterialization(ControlPendingEvent base) {
        ControlPendingEvent stamped =
                base.toBuilder().setChronicleMaterializedAt(instantToTimestamp(Instant.now())).build();
        return withChroniclePrefix(stamped.toByteArray());
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

    /** Protobuf for outbox / base64 (no {@code chronicle_materialized_at}). */
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
        return b.build();
    }

    private static PendingControlEvent fromProto(ControlPendingEvent p) {
        Optional<Instant> materialized = p.hasChronicleMaterializedAt()
                ? Optional.of(timestampToInstant(p.getChronicleMaterializedAt()))
                : Optional.empty();
        return new PendingControlEvent(
                p.getType(),
                UUID.fromString(p.getOrderId()),
                p.getOrderVersion(),
                p.getShardId(),
                p.getAccountIdHash(),
                timestampToInstant(p.getOrderTimestamp()),
                timestampToInstant(p.getEnqueuedAt()),
                materialized);
    }

    private static Timestamp instantToTimestamp(Instant i) {
        return Timestamp.newBuilder().setSeconds(i.getEpochSecond()).setNanos(i.getNano()).build();
    }

    private static Instant timestampToInstant(Timestamp ts) {
        return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
    }
}
