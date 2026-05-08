package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.persistence.ApproveManualSettlementResult;
import com.balh.oms.persistence.ManualSettlementActionRow;
import com.balh.oms.persistence.ManualSettlementActionsRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Four-eyes approve then apply supported {@code action_type} handlers in one transaction (slice 6).
 */
@Service
public class ManualSettlementActionApplicationService {

    private static final Logger log = LoggerFactory.getLogger(ManualSettlementActionApplicationService.class);

    private final ManualSettlementActionsRepository manualRepo;
    private final SettlementConfirmProcessor settlement;
    private final OmsConfig config;

    public ManualSettlementActionApplicationService(
            ManualSettlementActionsRepository manualRepo,
            SettlementConfirmProcessor settlement,
            OmsConfig config) {
        this.manualRepo = manualRepo;
        this.settlement = settlement;
        this.config = config;
    }

    /**
     * CAS-approves the row then, when enabled and {@code action_type} is known, runs the settlement side-effect.
     */
    @Transactional
    public ApproveManualSettlementResult approve(long id, String approvedBy) {
        ApproveManualSettlementResult r = manualRepo.approve(id, approvedBy);
        if (r != ApproveManualSettlementResult.OK) {
            return r;
        }
        if (!config.getSettlement().isManualActionAutoApplyEnabled()) {
            return r;
        }
        ManualSettlementActionRow row = manualRepo.findById(id).orElseThrow();
        applyApproved(row);
        return r;
    }

    private void applyApproved(ManualSettlementActionRow row) {
        String type = row.actionType() == null ? "" : row.actionType().trim();
        if (type.equalsIgnoreCase(ManualSettlementActionTypes.MARK_TRADE_FAILED)) {
            MarkTradeFailedResult mr = settlement.markTradeFailed(row.executionId());
            switch (mr) {
                case APPLIED, ALREADY_FAILED -> log.info(
                        "manual settlement {} applied executionId={} result={}",
                        ManualSettlementActionTypes.MARK_TRADE_FAILED,
                        row.executionId(),
                        mr);
                default -> throw new IllegalStateException(
                        "manual %s could not apply: executionId=%s result=%s"
                                .formatted(ManualSettlementActionTypes.MARK_TRADE_FAILED, row.executionId(), mr));
            }
            return;
        }
        if (type.equalsIgnoreCase(ManualSettlementActionTypes.ADVANCE_SETTLEMENT_ONE_STEP)) {
            String next = settlement.advanceOneSettlementStep(row.executionId());
            if (next == null) {
                throw new IllegalStateException(
                        "manual %s: execution not found id=%s"
                                .formatted(ManualSettlementActionTypes.ADVANCE_SETTLEMENT_ONE_STEP, row.executionId()));
            }
            log.info(
                    "manual settlement {} applied executionId={} settlementStatusAfter={}",
                    ManualSettlementActionTypes.ADVANCE_SETTLEMENT_ONE_STEP,
                    row.executionId(),
                    next);
            return;
        }
        log.debug("manual settlement action approved with no auto-executor type={} id={}", type, row.id());
    }
}
