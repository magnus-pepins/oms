package com.balh.oms.settlement;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.Month;
import org.springframework.stereotype.Service;

/** Quarterly ISK valuation snapshot stub (Phase E Slice 15a — gap plan §5.10 / I4). */
@Service
public class IskValuationSnapshotService {

    private final OmsAccountTaxWrapperRepository taxWrappers;
    private final IskValuationSnapshotRepository snapshots;

    public IskValuationSnapshotService(
            OmsAccountTaxWrapperRepository taxWrappers, IskValuationSnapshotRepository snapshots) {
        this.taxWrappers = taxWrappers;
        this.snapshots = snapshots;
    }

    public int captureQuarterStub(LocalDate asOf) {
        LocalDate quarterStart = quarterStartFor(asOf);
        int count = 0;
        for (OmsAccountTaxWrapperRepository.AccountTaxWrapperRow row :
                taxWrappers.listIskAccounts()) {
            snapshots.upsert(
                    row.iskAccountId() != null ? row.iskAccountId() : row.accountId(),
                    row.accountId(),
                    quarterStart,
                    BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    "oms_stub_v1");
            count++;
        }
        return count;
    }

    public static LocalDate quarterStartFor(LocalDate asOf) {
        Month m = asOf.getMonth();
        int startMonth = m.firstMonthOfQuarter().getValue();
        return LocalDate.of(asOf.getYear(), startMonth, 1);
    }
}
