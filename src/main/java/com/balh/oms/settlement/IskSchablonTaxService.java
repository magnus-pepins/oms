package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.springframework.stereotype.Service;

/** Schablonintäkt computation (gap plan §5.10 / ledger-swedish-isk-account-type.md §3.2). */
@Service
public class IskSchablonTaxService {

    private static final int MONEY_SCALE = 2;
    private static final BigDecimal DEFAULT_STATSLANERANTA = new BigDecimal("0.0188");

    private final IskTaxParametersRepository parameters;
    private final OmsConfig config;

    public IskSchablonTaxService(IskTaxParametersRepository parameters, OmsConfig config) {
        this.parameters = parameters;
        this.config = config;
    }

    public BigDecimal schablonRateForYear(int taxYear) {
        return parameters
                .findByYear(taxYear)
                .map(IskTaxParametersRepository.TaxParametersRow::schablonRate)
                .orElseGet(() -> {
                    BigDecimal override = config.getIskTax().getStatslanerantaOverride();
                    BigDecimal stats = override != null ? override : DEFAULT_STATSLANERANTA;
                    return IskTaxParametersRepository.deriveSchablonRate(stats);
                });
    }

    public BigDecimal schablonintakt(BigDecimal kapitalunderlagSek, int taxYear) {
        if (kapitalunderlagSek == null || kapitalunderlagSek.signum() <= 0) {
            return BigDecimal.ZERO.setScale(MONEY_SCALE, RoundingMode.HALF_UP);
        }
        return kapitalunderlagSek
                .multiply(schablonRateForYear(taxYear))
                .setScale(MONEY_SCALE, RoundingMode.HALF_UP);
    }
}
