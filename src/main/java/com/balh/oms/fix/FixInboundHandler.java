package com.balh.oms.fix;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.returnpath.ExecutionReportApplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import quickfix.FieldNotFound;
import quickfix.Message;

/**
 * Applies inbound venue traffic on a managed transaction boundary (slice 4).
 */
@Service
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "fix")
public class FixInboundHandler {

    private static final Logger log = LoggerFactory.getLogger(FixInboundHandler.class);

    private final ExecutionReportApplier applier;
    private final FixExecutionReportMapper mapper;
    private final OmsConfig omsConfig;

    public FixInboundHandler(ExecutionReportApplier applier, FixExecutionReportMapper mapper, OmsConfig omsConfig) {
        this.applier = applier;
        this.mapper = mapper;
        this.omsConfig = omsConfig;
    }

    @Transactional
    public void handleExecutionReport(Message message) throws FieldNotFound {
        String venueId = omsConfig.getFix().getVenueIdForExecutions();
        var trade = mapper.tryParseTrade(message, venueId);
        if (trade.isPresent()) {
            applier.applyTrade(trade.get());
            return;
        }
        var cancel = mapper.tryParseCancel(message, venueId);
        if (cancel.isEmpty()) {
            log.debug("Ignoring ExecutionReport (no trade/cancel mapping), message={}", message);
            return;
        }
        applier.applyCancel(cancel.get());
    }
}
