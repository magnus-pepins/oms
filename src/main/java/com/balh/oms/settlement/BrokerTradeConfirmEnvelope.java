package com.balh.oms.settlement;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

/**
 * Top-level JSON envelope for the production broker trade confirm file
 * (system-documentation/plans/stock-settlement-production-gap-plan.md §5.1).
 *
 * <p>This is the economic confirm contract: every row carries the broker's
 * authoritative trade record (price, quantity, fees, settlement date, correction
 * type, raw row JSON) so the matcher (gap plan §5.2) can detect mismatches
 * before the existing {@code broker_settlement_confirm} queue advances
 * {@code execution_settlement_status}. The v1 fixture format
 * ({@link BrokerFixtureFilePayload}) stays in place for dev/UAT.
 *
 * <p>Idempotency: the ingest service treats files as duplicates on either
 * {@code file_sha256_hex} or {@code (brokerId, fileId)}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BrokerTradeConfirmEnvelope(
        int schemaVersion,
        String brokerId,
        String fileId,
        LocalDate businessDate,
        Instant generatedAt,
        List<BrokerTradeConfirmRow> rows) {

    /**
     * Supported envelope schema version. Bump (and add an explicit converter) when
     * the row shape changes; never reuse a number for an incompatible shape.
     */
    public static final int CURRENT_SCHEMA_VERSION = 1;
}
