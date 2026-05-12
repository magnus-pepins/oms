package com.balh.oms.config;

import com.balh.oms.chronicle.ControlChronicleAppendMode;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

/**
 * Fails fast on incompatible topology flags (see {@code plans/oms-ingress-control-fix-topology.md}).
 */
@Component
public class ControlChronicleAppendModeValidator {

    private final OmsConfig omsConfig;

    public ControlChronicleAppendModeValidator(OmsConfig omsConfig) {
        this.omsConfig = omsConfig;
    }

    @PostConstruct
    void validate() {
        if (omsConfig.getControl().isChronicleAppendIngressAfterCommit() && !omsConfig.getChronicle().isEnabled()) {
            throw new IllegalStateException(
                    "oms.control.chronicle-append-mode="
                            + ControlChronicleAppendMode.INGRESS_AFTER_COMMIT
                            + " requires oms.chronicle.enabled=true (Chronicle is the durable control journal)");
        }
    }
}
