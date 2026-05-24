package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.ledger.LedgerBalanceClient;
import com.balh.oms.marketdata.MarketdataNbboQuote;
import com.balh.oms.marketdata.MarketdataPlatformHttpClient;
import com.balh.oms.persistence.PositionsRepository;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.Month;
import java.util.Locale;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;

/** Quarterly ISK valuation capture (Phase E — gap plan §5.10 / I4). */
@Service
public class IskValuationSnapshotService {

    private static final Logger log = LoggerFactory.getLogger(IskValuationSnapshotService.class);

    private static final int MONEY_SCALE = 2;
    private static final String SOURCE_MARKETDATA = "marketdata_nbbo_v1";
    private static final String SOURCE_STUB = "oms_stub_v1";

    private final OmsAccountTaxWrapperRepository taxWrappers;
    private final IskValuationSnapshotRepository snapshots;
    private final PositionsRepository positions;
    private final ObjectProvider<MarketdataPlatformHttpClient> marketdata;
    private final ObjectProvider<LedgerBalanceClient> ledgerBalance;
    private final OmsConfig config;

    public IskValuationSnapshotService(
            OmsAccountTaxWrapperRepository taxWrappers,
            IskValuationSnapshotRepository snapshots,
            PositionsRepository positions,
            ObjectProvider<MarketdataPlatformHttpClient> marketdata,
            ObjectProvider<LedgerBalanceClient> ledgerBalance,
            OmsConfig config) {
        this.taxWrappers = taxWrappers;
        this.snapshots = snapshots;
        this.positions = positions;
        this.marketdata = marketdata;
        this.ledgerBalance = ledgerBalance;
        this.config = config;
    }

    public int captureQuarter(LocalDate asOf) {
        LocalDate quarterStart = quarterStartFor(asOf);
        int count = 0;
        for (OmsAccountTaxWrapperRepository.AccountTaxWrapperRow row : taxWrappers.listIskAccounts()) {
            Valuation valuation = valueAccount(row);
            snapshots.upsert(
                    row.iskAccountId() != null ? row.iskAccountId() : row.accountId(),
                    row.accountId(),
                    quarterStart,
                    valuation.cashSek(),
                    valuation.securitiesSek(),
                    valuation.source());
            count++;
        }
        return count;
    }

    /** @deprecated use {@link #captureQuarter(LocalDate)} */
    @Deprecated
    public int captureQuarterStub(LocalDate asOf) {
        return captureQuarter(asOf);
    }

    public static LocalDate quarterStartFor(LocalDate asOf) {
        Month m = asOf.getMonth();
        int startMonth = m.firstMonthOfQuarter().getValue();
        return LocalDate.of(asOf.getYear(), startMonth, 1);
    }

    private Valuation valueAccount(OmsAccountTaxWrapperRepository.AccountTaxWrapperRow row) {
        BigDecimal cashSek = cashSekForAccount(row);
        BigDecimal securitiesSek = securitiesSekForAccount(row.accountId());
        boolean usedMarketdata = marketdata.getIfAvailable() != null && securitiesSek.signum() > 0;
        String source = usedMarketdata ? SOURCE_MARKETDATA : SOURCE_STUB;
        if (securitiesSek.signum() == 0 && cashSek.signum() == 0) {
            source = SOURCE_STUB;
        }
        return new Valuation(cashSek, securitiesSek, source);
    }

    private BigDecimal cashSekForAccount(OmsAccountTaxWrapperRepository.AccountTaxWrapperRow row) {
        if (row.ledgerBalanceId() == null || row.ledgerBalanceId().isBlank()) {
            return BigDecimal.ZERO.setScale(MONEY_SCALE, RoundingMode.HALF_UP);
        }
        LedgerBalanceClient client = ledgerBalance.getIfAvailable();
        if (client == null) {
            return BigDecimal.ZERO.setScale(MONEY_SCALE, RoundingMode.HALF_UP);
        }
        try {
            var readModel = client.fetchBalanceReadModel(row.ledgerBalanceId());
            BigDecimal available = readModel.availableBalance();
            String currency =
                    readModel.currency() == null ? "SEK" : readModel.currency().trim().toUpperCase(Locale.ROOT);
            return toSek(available, currency);
        } catch (LedgerBalanceClient.LedgerServiceException e) {
            log.debug("isk valuation skipped ledger cash accountId={}: {}", row.accountId(), e.getMessage());
            return BigDecimal.ZERO.setScale(MONEY_SCALE, RoundingMode.HALF_UP);
        }
    }

    private BigDecimal securitiesSekForAccount(java.util.UUID accountId) {
        BigDecimal total = BigDecimal.ZERO;
        MarketdataPlatformHttpClient md = marketdata.getIfAvailable();
        for (PositionsRepository.PositionRow position : positions.findByAccountId(accountId)) {
            BigDecimal qty = position.quantityTotal();
            if (qty == null || qty.signum() <= 0) {
                continue;
            }
            BigDecimal price = midPrice(md, position.instrumentSymbol());
            String ccy =
                    position.currency() == null ? "USD" : position.currency().trim().toUpperCase(Locale.ROOT);
            total = total.add(toSek(qty.multiply(price), ccy));
        }
        return total.setScale(MONEY_SCALE, RoundingMode.HALF_UP);
    }

    private BigDecimal midPrice(MarketdataPlatformHttpClient md, String symbol) {
        if (md != null && symbol != null && !symbol.isBlank()) {
            var quote = md.fetchNbbo(symbol.trim().toUpperCase(Locale.ROOT));
            if (quote.isPresent()) {
                return nbboMid(quote.get());
            }
        }
        return BigDecimal.ZERO;
    }

    private static BigDecimal nbboMid(MarketdataNbboQuote quote) {
        if (quote.bid() != null && quote.ask() != null && quote.bid().signum() > 0 && quote.ask().signum() > 0) {
            return quote.bid().add(quote.ask()).divide(new BigDecimal("2"), 6, RoundingMode.HALF_UP);
        }
        if (quote.ask() != null && quote.ask().signum() > 0) {
            return quote.ask();
        }
        if (quote.bid() != null && quote.bid().signum() > 0) {
            return quote.bid();
        }
        return BigDecimal.ZERO;
    }

    private BigDecimal toSek(BigDecimal amount, String currency) {
        if (amount == null || amount.signum() == 0) {
            return BigDecimal.ZERO.setScale(MONEY_SCALE, RoundingMode.HALF_UP);
        }
        if ("SEK".equalsIgnoreCase(currency)) {
            return amount.setScale(MONEY_SCALE, RoundingMode.HALF_UP);
        }
        Map<String, BigDecimal> rates = config.getIskTax().getFxToSekRates();
        BigDecimal rate = rates.get(currency.toUpperCase(Locale.ROOT));
        if (rate == null) {
            rate = config.getIskTax().getDefaultFxToSekRate();
        }
        return amount.multiply(rate).setScale(MONEY_SCALE, RoundingMode.HALF_UP);
    }

    private record Valuation(BigDecimal cashSek, BigDecimal securitiesSek, String source) {}
}
