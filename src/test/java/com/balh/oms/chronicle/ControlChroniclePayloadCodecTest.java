package com.balh.oms.chronicle;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ControlChroniclePayloadCodecTest {

    private static ObjectMapper objectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .registerModule(new Jdk8Module());
    }

    private final ControlChroniclePayloadCodec codec = new ControlChroniclePayloadCodec(objectMapper());

    @Test
    void roundTrip_outboxJson_chronicleBytes_domainEvent() {
        Instant t1 = Instant.parse("2026-05-10T12:00:00Z");
        Instant t2 = Instant.parse("2026-05-10T12:00:01Z");
        var ev = new PendingControlEvent("OrderAccepted", UUID.randomUUID(), 0, 3, "h", t1, t2);

        String outbox = codec.outboxPayloadJson(ev);
        assertThat(outbox).contains("\"v\":2").contains("\"d\":");

        PendingControlEvent fromOutbox = codec.decodeFromOutboxPayloadText(outbox);
        assertThat(fromOutbox)
                .usingRecursiveComparison()
                .ignoringFields("telemetryHops")
                .isEqualTo(ev);
        assertThat(fromOutbox.telemetryHops()).hasSize(1);
        assertThat(fromOutbox.telemetryHops().getFirst().stage()).isEqualTo(PipelineTelemetryStages.INGRESS);
        assertThat(fromOutbox.telemetryHops().getFirst().observedAt()).isEqualTo(ev.enqueuedAt());

        byte[] chronicle = codec.chronicleAppendBytesFromOutboxPayloadText(outbox);
        assertThat(ControlChronicleWireFormat.chronicleExcerptStartsWithProtoPrefix(chronicle)).isTrue();
        PendingControlEvent fromChronicle = codec.decodeChronicleExcerpt(chronicle);
        assertThat(fromChronicle)
                .usingRecursiveComparison()
                .ignoringFields("chronicleMaterializedAt", "telemetryHops")
                .isEqualTo(ev);
        assertThat(fromChronicle.chronicleMaterializedAt()).isPresent();
        assertThat(fromChronicle.telemetryHops()).hasSize(2);
        assertThat(fromChronicle.telemetryHops().get(0).stage()).isEqualTo(PipelineTelemetryStages.INGRESS);
        assertThat(fromChronicle.telemetryHops().get(0).observedAt()).isEqualTo(ev.enqueuedAt());
        assertThat(fromChronicle.telemetryHops().get(1).stage())
                .isEqualTo(PipelineTelemetryStages.RECONCILER_CHRONICLE_APPEND);
        assertThat(fromChronicle.telemetryHops().get(1).observedAt())
                .isEqualTo(fromChronicle.chronicleMaterializedAt().orElseThrow());
    }

    @Test
    void controlPipelineTelemetry_carriesFullHopListAfterTail() {
        Instant t1 = Instant.parse("2026-05-10T12:00:00Z");
        Instant t2 = Instant.parse("2026-05-10T12:00:01Z");
        var ev = new PendingControlEvent("OrderAccepted", UUID.randomUUID(), 1, 0, "x", t1, t2);
        String outbox = codec.outboxPayloadJson(ev);
        byte[] chronicleOrderAccepted = codec.chronicleAppendBytesFromOutboxPayloadText(outbox);
        PendingControlEvent applied = codec.decodeChronicleExcerpt(chronicleOrderAccepted);
        assertThat(applied.type()).isEqualTo(ControlChronicleEventTypes.ORDER_ACCEPTED);

        Instant tailAt = Instant.parse("2026-05-10T14:00:00Z");
        byte[] telemetryFrame = codec.chronicleAppendAfterControlTailApply(applied, tailAt);
        PendingControlEvent tailMsg = codec.decodeChronicleExcerpt(telemetryFrame);
        assertThat(tailMsg.type()).isEqualTo(ControlChronicleEventTypes.CONTROL_PIPELINE_TELEMETRY);
        assertThat(tailMsg.telemetryHops()).hasSize(3);
        assertThat(tailMsg.telemetryHops().get(0).stage()).isEqualTo(PipelineTelemetryStages.INGRESS);
        assertThat(tailMsg.telemetryHops().get(1).stage()).isEqualTo(PipelineTelemetryStages.RECONCILER_CHRONICLE_APPEND);
        assertThat(tailMsg.telemetryHops().get(2).stage()).isEqualTo(PipelineTelemetryStages.CONTROL_TAIL_APPLY);
        assertThat(tailMsg.telemetryHops().get(2).observedAt()).isEqualTo(tailAt);
        assertThat(tailMsg.orderId()).isEqualTo(ev.orderId());
        assertThat(tailMsg.orderVersion()).isEqualTo(ev.orderVersion());
    }
}
