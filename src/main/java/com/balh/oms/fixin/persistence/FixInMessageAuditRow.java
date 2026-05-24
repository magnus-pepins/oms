package com.balh.oms.fixin.persistence;

import java.util.UUID;

public record FixInMessageAuditRow(
        UUID id,
        String direction,
        String sessionRole,
        UUID fixSessionIdOrNull,
        String msgTypeOrNull,
        Integer msgSeqNumOrNull,
        String clOrdIdOrNull,
        String origClOrdIdOrNull,
        UUID omsOrderIdOrNull,
        String execIdOrNull,
        String rawStoreRefOrNull,
        String summaryOrNull,
        java.time.Instant createdAt) {

    /** Constructor for inserts (no created_at — DB default). */
    public FixInMessageAuditRow(
            UUID id,
            String direction,
            String sessionRole,
            UUID fixSessionIdOrNull,
            String msgTypeOrNull,
            Integer msgSeqNumOrNull,
            String clOrdIdOrNull,
            String origClOrdIdOrNull,
            UUID omsOrderIdOrNull,
            String execIdOrNull,
            String rawStoreRefOrNull,
            String summaryOrNull) {
        this(
                id,
                direction,
                sessionRole,
                fixSessionIdOrNull,
                msgTypeOrNull,
                msgSeqNumOrNull,
                clOrdIdOrNull,
                origClOrdIdOrNull,
                omsOrderIdOrNull,
                execIdOrNull,
                rawStoreRefOrNull,
                summaryOrNull,
                null);
    }
}
