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

        assertThat(codec.decodeFromOutboxPayloadText(outbox)).isEqualTo(ev);

        byte[] chronicle = codec.chronicleAppendBytesFromOutboxPayloadText(outbox);
        assertThat(ControlChronicleWireFormat.chronicleExcerptStartsWithProtoPrefix(chronicle)).isTrue();
        PendingControlEvent fromChronicle = codec.decodeChronicleExcerpt(chronicle);
        assertThat(fromChronicle)
                .usingRecursiveComparison()
                .ignoringFields("chronicleMaterializedAt")
                .isEqualTo(ev);
        assertThat(fromChronicle.chronicleMaterializedAt()).isPresent();
    }

    @Test
    void legacyFlatJson_outbox_decodesAndProducesChronicleProto() throws Exception {
        var ev = new PendingControlEvent(
                "OrderAccepted", UUID.randomUUID(), 1, 0, "x", Instant.EPOCH, Instant.EPOCH);
        String legacy = objectMapper().writeValueAsString(ev);

        assertThat(codec.decodeFromOutboxPayloadText(legacy)).isEqualTo(ev);

        byte[] chronicle = codec.chronicleAppendBytesFromOutboxPayloadText(legacy);
        assertThat(ControlChronicleWireFormat.chronicleExcerptStartsWithProtoPrefix(chronicle)).isTrue();
        PendingControlEvent fromChronicle = codec.decodeChronicleExcerpt(chronicle);
        assertThat(fromChronicle)
                .usingRecursiveComparison()
                .ignoringFields("chronicleMaterializedAt")
                .isEqualTo(ev);
        assertThat(fromChronicle.chronicleMaterializedAt()).isPresent();
    }

    @Test
    void legacyJsonChronicleExcerpt_withoutPrefix_stillReads() throws Exception {
        var ev = new PendingControlEvent(
                "OrderAccepted", UUID.randomUUID(), 0, 1, "z", Instant.parse("2020-01-01T00:00:00Z"), Instant.now());
        byte[] legacyUtf8 = objectMapper().writeValueAsBytes(ev);
        assertThat(codec.decodeChronicleExcerpt(legacyUtf8)).isEqualTo(ev);
    }
}
