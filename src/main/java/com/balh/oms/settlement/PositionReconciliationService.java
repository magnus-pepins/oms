package com.balh.oms.settlement;

import com.balh.oms.persistence.PositionsRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Compares a parsed broker position snapshot batch to OMS {@code positions} (gap plan §5.6).
 *
 * <p>Customer-level rows ({@code account_id} present) compare per
 * {@code (account_id, instrument_symbol, custody_account_id)}. Omnibus rows ({@code account_id}
 * absent, {@code custody_account_id} present) compare aggregated OMS quantities for that custody
 * account and symbol.
 */
@Service
public class PositionReconciliationService {

    private static final ObjectMapper JSON = new ObjectMapper();

    public record Result(
            long reportId,
            long batchId,
            String status,
            int rowCount,
            int matchedCount,
            int mismatchCount,
            int missingInOmsCount,
            int missingInBrokerCount) {}

    private record PositionKey(UUID accountId, UUID custodyAccountId, String instrumentSymbol) {}

    private record OmnibusKey(UUID custodyAccountId, String instrumentSymbol) {}

    private record QuantitySnapshot(
            BigDecimal quantityTotal,
            BigDecimal quantitySettled,
            BigDecimal quantityPendingBuySettle,
            BigDecimal quantityPendingSellSettle) {

        static QuantitySnapshot zero() {
            return new QuantitySnapshot(
                    BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO);
        }

        static QuantitySnapshot fromOms(PositionsRepository.AccountPositionKeyRow row) {
            return new QuantitySnapshot(
                    nz(row.quantityTotal()),
                    nz(row.quantitySettled()),
                    nz(row.quantityPendingBuySettle()),
                    nz(row.quantityPendingSellSettle()));
        }

        static QuantitySnapshot fromBroker(BrokerPositionSnapshotRowRepository.SnapshotRow row) {
            return new QuantitySnapshot(
                    nz(row.quantityTotal()),
                    nz(row.quantitySettled()),
                    nz(row.quantityPendingBuySettle()),
                    nz(row.quantityPendingSellSettle()));
        }

        QuantitySnapshot add(QuantitySnapshot other) {
            return new QuantitySnapshot(
                    quantityTotal.add(other.quantityTotal),
                    quantitySettled.add(other.quantitySettled),
                    quantityPendingBuySettle.add(other.quantityPendingBuySettle),
                    quantityPendingSellSettle.add(other.quantityPendingSellSettle));
        }
    }

    private record PendingDetail(
            String outcome,
            UUID accountId,
            String symbol,
            UUID custodyAccountId,
            BigDecimal brokerQty,
            BigDecimal omsQty,
            String diffJson,
            Long breakId) {}

    private final BrokerPositionSnapshotBatchRepository batches;
    private final BrokerPositionSnapshotRowRepository snapshotRows;
    private final PositionsRepository positions;
    private final PositionReconciliationReportRepository reports;
    private final ReconciliationBreakRepository breaks;

    public PositionReconciliationService(
            BrokerPositionSnapshotBatchRepository batches,
            BrokerPositionSnapshotRowRepository snapshotRows,
            PositionsRepository positions,
            PositionReconciliationReportRepository reports,
            ReconciliationBreakRepository breaks) {
        this.batches = batches;
        this.snapshotRows = snapshotRows;
        this.positions = positions;
        this.reports = reports;
        this.breaks = breaks;
    }

    @Transactional
    public Optional<Result> reconcile(long batchId) {
        Optional<BrokerPositionSnapshotBatchRepository.BatchRow> batchOpt = batches.findById(batchId);
        if (batchOpt.isEmpty()) {
            return Optional.empty();
        }
        BrokerPositionSnapshotBatchRepository.BatchRow batch = batchOpt.get();
        if (!"parsed".equals(batch.status())) {
            throw new IllegalStateException(
                    "batch " + batchId + " status must be parsed, was " + batch.status());
        }

        List<BrokerPositionSnapshotRowRepository.SnapshotRow> brokerRows = snapshotRows.listByBatch(batchId);
        Set<UUID> accountIds = brokerRows.stream()
                .map(BrokerPositionSnapshotRowRepository.SnapshotRow::accountId)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        List<PositionsRepository.AccountPositionKeyRow> omsPositions =
                positions.findNonZeroPositionsForAccounts(accountIds);
        Map<PositionKey, PositionsRepository.AccountPositionKeyRow> omsByKey = new HashMap<>();
        Map<OmnibusKey, QuantitySnapshot> omsAggregated = new HashMap<>();
        for (PositionsRepository.AccountPositionKeyRow row : omsPositions) {
            PositionKey key = positionKey(row.accountId(), row.custodyAccountId(), row.instrumentSymbol());
            omsByKey.put(key, row);
            OmnibusKey omnibusKey = new OmnibusKey(row.custodyAccountId(), row.instrumentSymbol());
            omsAggregated.merge(omnibusKey, QuantitySnapshot.fromOms(row), QuantitySnapshot::add);
        }

        int matched = 0;
        int mismatch = 0;
        int missingInOms = 0;
        int missingInBroker = 0;
        Set<PositionKey> matchedCustomerKeys = new HashSet<>();
        List<PendingDetail> detailRows = new ArrayList<>();

        for (BrokerPositionSnapshotRowRepository.SnapshotRow brokerRow : brokerRows) {
            String symbol = brokerRow.instrumentSymbol();
            QuantitySnapshot brokerQty = QuantitySnapshot.fromBroker(brokerRow);

            if (brokerRow.accountId() != null) {
                PositionKey key = positionKey(brokerRow.accountId(), brokerRow.custodyAccountId(), symbol);
                PositionsRepository.AccountPositionKeyRow omsRow = omsByKey.get(key);
                if (omsRow == null) {
                    missingInOms++;
                    String diffJson = buildDiffJson(brokerQty, null, "missing_in_oms");
                    long breakId = openBreak(batch.businessDate(), brokerRow.accountId(), diffJson);
                    detailRows.add(pendingDetail(
                            "missing_in_oms",
                            brokerRow.accountId(),
                            symbol,
                            brokerRow.custodyAccountId(),
                            brokerQty.quantityTotal,
                            null,
                            diffJson,
                            breakId));
                } else {
                    matchedCustomerKeys.add(key);
                    QuantitySnapshot omsQty = QuantitySnapshot.fromOms(omsRow);
                    if (quantitiesMatch(brokerQty, omsQty)) {
                        matched++;
                        detailRows.add(pendingDetail(
                                "matched",
                                brokerRow.accountId(),
                                symbol,
                                brokerRow.custodyAccountId(),
                                brokerQty.quantityTotal,
                                omsQty.quantityTotal,
                                buildDiffJson(brokerQty, omsQty, "matched"),
                                null));
                    } else {
                        mismatch++;
                        String diffJson = buildDiffJson(brokerQty, omsQty, "mismatch");
                        long breakId = openBreak(batch.businessDate(), brokerRow.accountId(), diffJson);
                        detailRows.add(pendingDetail(
                                "mismatch",
                                brokerRow.accountId(),
                                symbol,
                                brokerRow.custodyAccountId(),
                                brokerQty.quantityTotal,
                                omsQty.quantityTotal,
                                diffJson,
                                breakId));
                    }
                }
            } else if (brokerRow.custodyAccountId() != null) {
                OmnibusKey omnibusKey = new OmnibusKey(brokerRow.custodyAccountId(), symbol);
                QuantitySnapshot omsQty = omsAggregated.getOrDefault(omnibusKey, QuantitySnapshot.zero());
                if (quantitiesMatch(brokerQty, omsQty)) {
                    matched++;
                    detailRows.add(pendingDetail(
                            "matched",
                            null,
                            symbol,
                            brokerRow.custodyAccountId(),
                            brokerQty.quantityTotal,
                            omsQty.quantityTotal,
                            buildDiffJson(brokerQty, omsQty, "matched"),
                            null));
                } else {
                    mismatch++;
                    String diffJson = buildDiffJson(brokerQty, omsQty, "mismatch");
                    long breakId = openBreak(batch.businessDate(), null, diffJson);
                    detailRows.add(pendingDetail(
                            "mismatch",
                            null,
                            symbol,
                            brokerRow.custodyAccountId(),
                            brokerQty.quantityTotal,
                            omsQty.quantityTotal,
                            diffJson,
                            breakId));
                }
            }
        }

        for (PositionsRepository.AccountPositionKeyRow omsRow : omsPositions) {
            PositionKey key = positionKey(omsRow.accountId(), omsRow.custodyAccountId(), omsRow.instrumentSymbol());
            if (matchedCustomerKeys.contains(key)) {
                continue;
            }
            missingInBroker++;
            QuantitySnapshot omsQty = QuantitySnapshot.fromOms(omsRow);
            String diffJson = buildDiffJson(null, omsQty, "missing_in_broker");
            long breakId = openBreak(batch.businessDate(), omsRow.accountId(), diffJson);
            detailRows.add(pendingDetail(
                    "missing_in_broker",
                    omsRow.accountId(),
                    omsRow.instrumentSymbol(),
                    omsRow.custodyAccountId(),
                    null,
                    omsQty.quantityTotal,
                    diffJson,
                    breakId));
        }

        long reportId = reports.insertReport(new PositionReconciliationReportRepository.ReportInsert(
                batchId,
                batch.brokerId(),
                batch.businessDate(),
                "completed",
                brokerRows.size(),
                matched,
                mismatch,
                missingInOms,
                missingInBroker));

        for (PendingDetail pending : detailRows) {
            reports.insertDetail(new PositionReconciliationReportRepository.DetailInsert(
                    reportId,
                    pending.outcome(),
                    pending.accountId(),
                    pending.symbol(),
                    pending.custodyAccountId(),
                    pending.brokerQty(),
                    pending.omsQty(),
                    pending.diffJson(),
                    pending.breakId()));
        }

        return Optional.of(new Result(
                reportId,
                batchId,
                "completed",
                brokerRows.size(),
                matched,
                mismatch,
                missingInOms,
                missingInBroker));
    }

    private long openBreak(LocalDate businessDate, UUID accountId, String diffJson) {
        return breaks.insert(new ReconciliationBreakRepository.InsertCommand(
                ReconciliationBreakRepository.BREAK_POSITION_MISMATCH,
                ReconciliationBreakRepository.SEVERITY_HIGH,
                ReconciliationBreakRepository.SOURCE_BROKER,
                null,
                null,
                accountId,
                businessDate,
                diffJson,
                "system"));
    }

    private static PendingDetail pendingDetail(
            String outcome,
            UUID accountId,
            String symbol,
            UUID custodyAccountId,
            BigDecimal brokerQty,
            BigDecimal omsQty,
            String diffJson,
            Long breakId) {
        return new PendingDetail(outcome, accountId, symbol, custodyAccountId, brokerQty, omsQty, diffJson, breakId);
    }

    private static PositionKey positionKey(UUID accountId, UUID custodyAccountId, String symbol) {
        return new PositionKey(accountId, custodyAccountId, symbol == null ? "" : symbol.trim());
    }

    private static boolean quantitiesMatch(QuantitySnapshot broker, QuantitySnapshot oms) {
        return broker.quantityTotal.compareTo(oms.quantityTotal) == 0
                && broker.quantitySettled.compareTo(oms.quantitySettled) == 0
                && broker.quantityPendingBuySettle.compareTo(oms.quantityPendingBuySettle) == 0
                && broker.quantityPendingSellSettle.compareTo(oms.quantityPendingSellSettle) == 0;
    }

    private static BigDecimal nz(BigDecimal value) {
        return value == null ? BigDecimal.ZERO : value;
    }

    private static String buildDiffJson(QuantitySnapshot broker, QuantitySnapshot oms, String outcome) {
        ObjectNode root = JSON.createObjectNode();
        root.put("outcome", outcome);
        if (broker != null) {
            ObjectNode b = root.putObject("broker");
            b.put("quantityTotal", broker.quantityTotal.toPlainString());
            b.put("quantitySettled", broker.quantitySettled.toPlainString());
            b.put("quantityPendingBuySettle", broker.quantityPendingBuySettle.toPlainString());
            b.put("quantityPendingSellSettle", broker.quantityPendingSellSettle.toPlainString());
        }
        if (oms != null) {
            ObjectNode o = root.putObject("oms");
            o.put("quantityTotal", oms.quantityTotal.toPlainString());
            o.put("quantitySettled", oms.quantitySettled.toPlainString());
            o.put("quantityPendingBuySettle", oms.quantityPendingBuySettle.toPlainString());
            o.put("quantityPendingSellSettle", oms.quantityPendingSellSettle.toPlainString());
        }
        if (broker != null && oms != null && !quantitiesMatch(broker, oms)) {
            ObjectNode delta = root.putObject("delta");
            delta.put("quantityTotal", broker.quantityTotal.subtract(oms.quantityTotal).toPlainString());
            delta.put("quantitySettled", broker.quantitySettled.subtract(oms.quantitySettled).toPlainString());
            delta.put(
                    "quantityPendingBuySettle",
                    broker.quantityPendingBuySettle
                            .subtract(oms.quantityPendingBuySettle)
                            .toPlainString());
            delta.put(
                    "quantityPendingSellSettle",
                    broker.quantityPendingSellSettle
                            .subtract(oms.quantityPendingSellSettle)
                            .toPlainString());
        }
        try {
            return JSON.writeValueAsString(root);
        } catch (Exception e) {
            return "{\"outcome\":\"" + outcome.toLowerCase(Locale.ROOT) + "\"}";
        }
    }
}
