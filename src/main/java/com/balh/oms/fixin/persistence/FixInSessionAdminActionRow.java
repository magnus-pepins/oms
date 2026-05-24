package com.balh.oms.fixin.persistence;

import java.time.Instant;
import java.util.UUID;

public record FixInSessionAdminActionRow(
        UUID id,
        String sessionRole,
        UUID fixSessionIdOrNull,
        String brokerRouteKeyOrNull,
        String actionType,
        String requestedBy,
        String approvedByOrNull,
        String reason,
        String counterpartyReferenceOrNull,
        String payloadJsonOrNull,
        Instant createdAt) {

    public FixInSessionAdminActionRow(
            UUID id,
            String sessionRole,
            UUID fixSessionIdOrNull,
            String brokerRouteKeyOrNull,
            String actionType,
            String requestedBy,
            String approvedByOrNull,
            String reason,
            String counterpartyReferenceOrNull,
            String payloadJsonOrNull) {
        this(
                id,
                sessionRole,
                fixSessionIdOrNull,
                brokerRouteKeyOrNull,
                actionType,
                requestedBy,
                approvedByOrNull,
                reason,
                counterpartyReferenceOrNull,
                payloadJsonOrNull,
                null);
    }
}
