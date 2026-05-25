package com.balh.oms.corporateaction;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Locale;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * Resolves dividend / tender withholding using treaty map {@code withholdingRates} keyed by
 * ISO-3166 alpha-2 tax country, with fallback to flat {@code withholdingRate} (gap plan §5.9).
 */
@Component
public class CorporateActionWithholdingResolver {

    private static final int MONEY_SCALE = 10;

    private final AccountTaxResidencyRepository taxResidency;

    public CorporateActionWithholdingResolver(AccountTaxResidencyRepository taxResidency) {
        this.taxResidency = taxResidency;
    }

    public BigDecimal resolve(BigDecimal gross, JsonNode payload, UUID accountId) {
        if (gross == null || gross.signum() <= 0) {
            return BigDecimal.ZERO.setScale(MONEY_SCALE, RoundingMode.HALF_UP);
        }
        JsonNode rates = payload == null ? null : payload.get("withholdingRates");
        if (rates != null && rates.isObject() && accountId != null) {
            String country =
                    taxResidency
                            .findTaxCountry(accountId)
                            .map(c -> c.trim().toUpperCase(Locale.ROOT))
                            .orElse(null);
            BigDecimal treatyRate = lookupRate(rates, country);
            if (treatyRate == null) {
                treatyRate = lookupRate(rates, "default");
            }
            if (treatyRate != null && treatyRate.signum() >= 0) {
                return gross.multiply(treatyRate).setScale(MONEY_SCALE, RoundingMode.HALF_UP);
            }
        }
        return CorporateActionProcessingService.withholdingAmount(gross, payload);
    }

    private static BigDecimal lookupRate(JsonNode rates, String key) {
        if (key == null || key.isBlank()) {
            return null;
        }
        JsonNode node = rates.get(key);
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isNumber()) {
            return node.decimalValue();
        }
        if (node.isTextual()) {
            try {
                return new BigDecimal(node.asText().trim());
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }
}
