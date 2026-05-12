package com.balh.oms.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChronicleTailIdConfigTest {

    @Test
    void defaultTailId() {
        OmsConfig cfg = new OmsConfig();
        assertThat(cfg.getChronicle().getControlTailId()).isEqualTo("oms-control");
    }

    @ParameterizedTest
    @ValueSource(strings = {"a", "worker-1", "shard_0", "oms.control.A"})
    void validTailIds(String id) {
        OmsConfig cfg = new OmsConfig();
        cfg.getChronicle().setControlTailId(id);
        assertThat(cfg.getChronicle().getControlTailId()).isEqualTo(id);
    }

    @Test
    void trimsWhitespace() {
        OmsConfig cfg = new OmsConfig();
        cfg.getChronicle().setControlTailId("  worker-a  ");
        assertThat(cfg.getChronicle().getControlTailId()).isEqualTo("worker-a");
    }

    @Test
    void blankResetsToDefault() {
        OmsConfig cfg = new OmsConfig();
        cfg.getChronicle().setControlTailId("worker-x");
        cfg.getChronicle().setControlTailId("   ");
        assertThat(cfg.getChronicle().getControlTailId()).isEqualTo("oms-control");
    }

    @Test
    void nullResetsToDefault() {
        OmsConfig cfg = new OmsConfig();
        cfg.getChronicle().setControlTailId("worker-x");
        cfg.getChronicle().setControlTailId(null);
        assertThat(cfg.getChronicle().getControlTailId()).isEqualTo("oms-control");
    }

    @Test
    void rejectsInvalidCharacters() {
        OmsConfig cfg = new OmsConfig();
        assertThatThrownBy(() -> cfg.getChronicle().setControlTailId("bad id"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("control-tail-id");
    }

    @Test
    void rejectsTooLong() {
        OmsConfig cfg = new OmsConfig();
        assertThatThrownBy(() -> cfg.getChronicle().setControlTailId("x".repeat(200)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds");
    }
}
