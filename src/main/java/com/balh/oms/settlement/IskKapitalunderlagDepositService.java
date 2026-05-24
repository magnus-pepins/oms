package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.ledger.LedgerIskReadClient;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;

/** Sums ledger {@code isk_deposit_events} toward kapitalunderlag for a tax year (§5.10 / I5). */
@Service
public class IskKapitalunderlagDepositService {

    private static final Logger log = LoggerFactory.getLogger(IskKapitalunderlagDepositService.class);
    private static final ZoneId STOCKHOLM = ZoneId.of("Europe/Stockholm");
    private static final int MONEY_SCALE = 2;
    private static final int MINOR_UNITS_SEK = 100;

    private final ObjectProvider<LedgerIskReadClient> ledgerIskRead;
    private final OmsConfig config;

    public IskKapitalunderlagDepositService(ObjectProvider<LedgerIskReadClient> ledgerIskRead, OmsConfig config) {
        this.ledgerIskRead = ledgerIskRead;
        this.config = config;
    }

    public BigDecimal sumDepositsSek(UUID iskAccountId, int taxYear) {
        LedgerIskReadClient client = ledgerIskRead.getIfAvailable();
        if (client == null || iskAccountId == null) {
            return BigDecimal.ZERO.setScale(MONEY_SCALE, RoundingMode.HALF_UP);
        }
        Instant from = LocalDate.of(taxYear, 1, 1).atStartOfDay(STOCKHOLM).toInstant();
        Instant to = LocalDate.of(taxYear, 12, 31).atTime(23, 59, 59).atZone(STOCKHOLM).toInstant();
        try {
            BigDecimal total = BigDecimal.ZERO;
            for (LedgerIskReadClient.DepositRow row : client.listDeposits(iskAccountId.toString(), from, to)) {
                if (!row.countsTowardKapitalunderlag()) {
                    continue;
                }
                total = total.add(toSekMajor(row.amountMinor(), row.currency()));
            }
            return total.setScale(MONEY_SCALE, RoundingMode.HALF_UP);
        } catch (LedgerIskReadClient.LedgerIskReadException e) {
            log.warn("isk kapitalunderlag deposit read failed iskAccountId={}: {}", iskAccountId, e.getMessage());
            return BigDecimal.ZERO.setScale(MONEY_SCALE, RoundingMode.HALF_UP);
        }
    }

    private BigDecimal toSekMajor(long amountMinor, String currency) {
        BigDecimal major =
                BigDecimal.valueOf(amountMinor)
                        .divide(BigDecimal.valueOf(MINOR_UNITS_SEK), MONEY_SCALE, RoundingMode.HALF_UP);
        if (currency == null || "SEK".equalsIgnoreCase(currency.trim())) {
            return major;
        }
        Map<String, BigDecimal> rates = config.getIskTax().getFxToSekRates();
        BigDecimal rate = rates.get(currency.trim().toUpperCase(Locale.ROOT));
        if (rate == null) {
            rate = config.getIskTax().getDefaultFxToSekRate();
        }
        return major.multiply(rate).setScale(MONEY_SCALE, RoundingMode.HALF_UP);
    }
}
