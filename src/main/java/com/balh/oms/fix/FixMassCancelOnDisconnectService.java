package com.balh.oms.fix;

import com.balh.oms.config.OmsConfig;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import quickfix.SessionID;

/**
 * Slice 8 exit-gate hook: on FIX initiator disconnect, optionally signal mass-cancel policy (counter + log).
 * Venue {@code OrderCancelRequest} fan-out remains broker-specific; ops runbooks describe manual kill until wired.
 */
@Component
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "fix")
public class FixMassCancelOnDisconnectService {

    private static final Logger log = LoggerFactory.getLogger(FixMassCancelOnDisconnectService.class);

    private final OmsConfig omsConfig;
    private final MeterRegistry meterRegistry;

    public FixMassCancelOnDisconnectService(OmsConfig omsConfig, MeterRegistry meterRegistry) {
        this.omsConfig = omsConfig;
        this.meterRegistry = meterRegistry;
    }

    public void onInitiatorLogout(SessionID sessionId) {
        if (!omsConfig.getFix().isMassCancelOnDisconnectEnabled()) {
            return;
        }
        meterRegistry.counter("oms_fix_mass_cancel_disconnect_signal_total").increment();
        log.warn(
                "FIX initiator logout {} — mass-cancel-on-disconnect policy enabled (signal only; wire venue cancels separately)",
                sessionId);
    }
}
