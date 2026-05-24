package com.balh.oms.fixin;

import com.balh.oms.config.OmsProfiles;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile(OmsProfiles.FIX_INGRESS)
public class FixInOpsMetrics {

    private final Counter auditWriteFailures;
    private final Counter rateLimitRejects;
    private final Counter adminActionSuccess;
    private final Counter adminActionFailure;

    public FixInOpsMetrics(MeterRegistry registry) {
        this.auditWriteFailures =
                Counter.builder("oms_fix_in_audit_write_failures_total").register(registry);
        this.rateLimitRejects =
                Counter.builder("oms_fix_in_rate_limit_rejects_total").register(registry);
        this.adminActionSuccess =
                Counter.builder("oms_fix_in_admin_action_success_total").register(registry);
        this.adminActionFailure =
                Counter.builder("oms_fix_in_admin_action_failure_total").register(registry);
    }

    public void auditWriteFailed() {
        auditWriteFailures.increment();
    }

    public void rateLimitRejected() {
        rateLimitRejects.increment();
    }

    public void adminActionSucceeded() {
        adminActionSuccess.increment();
    }

    public void adminActionFailed() {
        adminActionFailure.increment();
    }
}
