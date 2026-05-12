package com.balh.oms.fix;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.domain.RejectCode;
import com.balh.oms.returnpath.ExecutionReportApplier;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import quickfix.FieldNotFound;
import quickfix.Message;

/**
 * Legacy applier path: applies inbound venue traffic on a managed transaction boundary by
 * routing through {@link ExecutionReportApplier} (Postgres-side state machine). Loaded on every
 * FIX-routing JVM <em>except</em> {@value OmsProfiles#FIX_EGRESS}: slice 3d's
 * {@code FixInboundClusterSink} replaces this path on the egress JVM (cluster service becomes the
 * source of truth, the Postgres applier becomes the projector at slice 3e).
 */
@Service
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "fix")
@Profile("!" + OmsProfiles.FIX_EGRESS)
public class FixInboundHandler implements FixInboundExecutionReportSink {

    private static final Logger log = LoggerFactory.getLogger(FixInboundHandler.class);

    private final ExecutionReportApplier applier;
    private final FixExecutionReportMapper mapper;
    private final OmsConfig omsConfig;
    private final MeterRegistry meterRegistry;

    public FixInboundHandler(
            ExecutionReportApplier applier,
            FixExecutionReportMapper mapper,
            OmsConfig omsConfig,
            MeterRegistry meterRegistry) {
        this.applier = applier;
        this.mapper = mapper;
        this.omsConfig = omsConfig;
        this.meterRegistry = meterRegistry;
    }

    @Transactional
    public void handleExecutionReport(Message message) throws FieldNotFound {
        String venueId = omsConfig.getFix().getVenueIdForExecutions();
        var trade = mapper.tryParseTrade(message, venueId);
        if (trade.isPresent()) {
            ExecutionReportApplier.TradeApplyOutcome out = applier.applyTrade(trade.get());
            meterRegistry
                    .counter(FixMetrics.METRIC_INBOUND_ER, FixMetrics.TAG_DISPOSITION, "trade_" + out.name())
                    .increment();
            return;
        }
        var cancel = mapper.tryParseCancel(message, venueId);
        if (cancel.isPresent()) {
            ExecutionReportApplier.CancelApplyOutcome cout = applier.applyCancel(cancel.get());
            meterRegistry
                    .counter(FixMetrics.METRIC_INBOUND_ER, FixMetrics.TAG_DISPOSITION, "cancel_" + cout.name())
                    .increment();
            return;
        }
        var venueReject = mapper.tryParseVenueReject(message, venueId);
        if (venueReject.isPresent()) {
            ExecutionReportApplier.VenueRejectApplyOutcome vrOut =
                    applier.applyVenueReject(venueReject.get(), RejectCode.VENUE_REJECT);
            meterRegistry
                    .counter(FixMetrics.METRIC_INBOUND_ER, FixMetrics.TAG_DISPOSITION, "venue_reject_" + vrOut.name())
                    .increment();
            return;
        }
        log.debug("Ignoring ExecutionReport (no mapping), message={}", message);
        meterRegistry.counter(FixMetrics.METRIC_INBOUND_ER, FixMetrics.TAG_DISPOSITION, "ignored").increment();
    }

    @Transactional
    public void handleOrderCancelReject(Message message) throws FieldNotFound {
        String venueId = omsConfig.getFix().getVenueIdForExecutions();
        var cmd = mapper.tryParseOrderCancelReject(message, venueId);
        if (cmd.isEmpty()) {
            meterRegistry.counter(FixMetrics.METRIC_INBOUND_ER, FixMetrics.TAG_DISPOSITION, "ocr_ignored").increment();
            return;
        }
        ExecutionReportApplier.VenueRejectApplyOutcome out =
                applier.applyVenueReject(cmd.get(), RejectCode.VENUE_REJECT);
        meterRegistry
                .counter(FixMetrics.METRIC_INBOUND_ER, FixMetrics.TAG_DISPOSITION, "ocr_" + out.name())
                .increment();
    }
}
