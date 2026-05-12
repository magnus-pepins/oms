package com.balh.oms.chronicle;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ControlChronicleAppendModeTest {

    @Test
    void normalize_defaultsAndTrims() {
        assertThat(ControlChronicleAppendMode.normalize(null)).isEqualTo(ControlChronicleAppendMode.RECONCILER);
        assertThat(ControlChronicleAppendMode.normalize("  ")).isEqualTo(ControlChronicleAppendMode.RECONCILER);
        assertThat(ControlChronicleAppendMode.normalize(" RECONCILER ")).isEqualTo(ControlChronicleAppendMode.RECONCILER);
        assertThat(ControlChronicleAppendMode.normalize("Ingress-After-Commit"))
                .isEqualTo(ControlChronicleAppendMode.INGRESS_AFTER_COMMIT);
    }

    @Test
    void validate_rejectsUnknown() {
        assertThatThrownBy(() -> ControlChronicleAppendMode.validate("kafka"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("chronicle-append-mode");
    }
}
