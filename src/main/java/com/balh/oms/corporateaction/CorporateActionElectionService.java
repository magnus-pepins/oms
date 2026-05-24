package com.balh.oms.corporateaction;

import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class CorporateActionElectionService {

    public enum ApproveResult {
        APPROVED,
        NOT_FOUND,
        ALREADY_APPROVED,
        SAME_ACTOR
    }

    private final CorporateActionElectionRepository elections;

    public CorporateActionElectionService(CorporateActionElectionRepository elections) {
        this.elections = elections;
    }

    public long submit(long eventId, UUID accountId, String electionChoice, String requestedBy) {
        if (electionChoice == null || electionChoice.isBlank()) {
            throw new IllegalArgumentException("electionChoice required");
        }
        if (requestedBy == null || requestedBy.isBlank()) {
            throw new IllegalArgumentException("requestedBy required");
        }
        return elections.upsert(eventId, accountId, electionChoice.trim(), requestedBy.trim());
    }

    public ApproveResult approve(long electionId, String approvedBy) {
        if (approvedBy == null || approvedBy.isBlank()) {
            return ApproveResult.SAME_ACTOR;
        }
        var row = elections.findById(electionId);
        if (row.isEmpty()) {
            return ApproveResult.NOT_FOUND;
        }
        if (row.get().approvedAt() != null) {
            return ApproveResult.ALREADY_APPROVED;
        }
        if (approvedBy.trim().equalsIgnoreCase(row.get().requestedBy())) {
            return ApproveResult.SAME_ACTOR;
        }
        int updated = elections.approve(electionId, approvedBy.trim());
        return updated == 1 ? ApproveResult.APPROVED : ApproveResult.ALREADY_APPROVED;
    }

    /** Throws when any entitled holder lacks an approved election. */
    public void requireAllApproved(long eventId, List<CorporateActionRecordDateSnapshotService.ResolvedHolder> holders) {
        if (holders.isEmpty()) {
            return;
        }
        List<UUID> accountIds = holders.stream().map(CorporateActionRecordDateSnapshotService.ResolvedHolder::accountId).toList();
        int approved = elections.countApprovedForAccounts(eventId, accountIds);
        if (approved < accountIds.size()) {
            throw new CorporateActionProcessingService.UnsupportedCorporateActionException(
                    "voluntary corporate action missing approved elections: "
                            + approved
                            + "/"
                            + accountIds.size());
        }
    }
}
