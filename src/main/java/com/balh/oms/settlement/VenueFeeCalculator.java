package com.balh.oms.settlement;

import com.balh.oms.predictionmarket.PredictionMarketContractRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Component;

/** Computes per-fill venue trade fees from contract model + participant overrides (Phase E). */
@Component
public class VenueFeeCalculator {

    private static final int MONEY_SCALE = 2;
    private static final int PROBABILITY_SCALE = 6;
    private static final BigDecimal PRICE_SCALE = new BigDecimal("1000000");
    private static final BigDecimal QTY_SCALE = new BigDecimal("1000000000");

    private final PredictionMarketContractRepository contractRepository;
    private final VenueParticipantFeeOverrideRepository overrideRepository;
    private final FixInCounterpartyLookupRepository counterpartyLookup;
    private final ObjectMapper objectMapper;

    public VenueFeeCalculator(
            PredictionMarketContractRepository contractRepository,
            VenueParticipantFeeOverrideRepository overrideRepository,
            FixInCounterpartyLookupRepository counterpartyLookup,
            ObjectMapper objectMapper) {
        this.contractRepository = contractRepository;
        this.overrideRepository = overrideRepository;
        this.counterpartyLookup = counterpartyLookup;
        this.objectMapper = objectMapper;
    }

    public record FeeQuote(
            VenueFeeModelId modelId,
            int scheduleVersion,
            BigDecimal feeAmount,
            String feeCurrency,
            VenueLiquidityRole liquidityRole,
            String feeSource) {}

    public Optional<FeeQuote> quoteForFill(
            String instrumentSymbol,
            UUID accountId,
            boolean retailParticipant,
            VenueLiquidityRole liquidityRole,
            long lastQtyScaled,
            long lastPxScaled,
            String settlementCurrency) {
        if (instrumentSymbol == null || !instrumentSymbol.startsWith("PREDMKT")) {
            return Optional.empty();
        }
        Optional<PredictionMarketContractRepository.ContractRow> contractOpt =
                contractRepository.findByYesSymbol(baseYesSymbol(instrumentSymbol));
        if (contractOpt.isEmpty()) {
            return Optional.empty();
        }
        PredictionMarketContractRepository.ContractRow contract = contractOpt.get();
        ResolvedModel resolved = resolveModel(contract, accountId, retailParticipant);
        BigDecimal fee =
                computeFee(
                        resolved.modelId(),
                        resolved.params(),
                        liquidityRole,
                        lastQtyScaled,
                        lastPxScaled);
        String currency =
                settlementCurrency == null || settlementCurrency.isBlank()
                        ? settlementCurrencyFor(instrumentSymbol)
                        : settlementCurrency.trim().toUpperCase();
        if (fee.signum() <= 0) {
            return Optional.of(
                    new FeeQuote(
                            resolved.modelId(),
                            resolved.scheduleVersion(),
                            BigDecimal.ZERO.setScale(MONEY_SCALE, RoundingMode.HALF_UP),
                            currency,
                            liquidityRole,
                            resolved.feeSource()));
        }
        return Optional.of(
                new FeeQuote(
                        resolved.modelId(),
                        resolved.scheduleVersion(),
                        fee,
                        currency,
                        liquidityRole,
                        resolved.feeSource()));
    }

    private record ResolvedModel(
            VenueFeeModelId modelId, int scheduleVersion, VenueFeeParams params, String feeSource) {}

    private ResolvedModel resolveModel(
            PredictionMarketContractRepository.ContractRow contract, UUID accountId, boolean retailParticipant) {
        VenueFeeModelId baseModel =
                retailParticipant && contract.retailFeeModelId() != null && !contract.retailFeeModelId().isBlank()
                        ? VenueFeeModelId.parse(contract.retailFeeModelId())
                        : VenueFeeModelId.parse(contract.feeModelId());
        VenueFeeParams params = VenueFeeParams.fromJson(contract.feeParamsJson(), objectMapper);
        String feeSource = "contract_default";

        Optional<UUID> counterpartyId = counterpartyLookup.findCounterpartyIdForAccount(accountId);
        if (counterpartyId.isPresent()) {
            Optional<VenueParticipantFeeOverrideRepository.OverrideRow> override =
                    overrideRepository.findEnabled(
                            VenueParticipantFeeOverrideRepository.TYPE_FIX_COUNTERPARTY,
                            counterpartyId.get(),
                            contract.id());
            if (override.isPresent()) {
                if (override.get().feeModelId() != null && !override.get().feeModelId().isBlank()) {
                    baseModel = VenueFeeModelId.parse(override.get().feeModelId());
                }
                if (override.get().feeParamsJson() != null && !override.get().feeParamsJson().isBlank()) {
                    params = VenueFeeParams.fromJson(override.get().feeParamsJson(), objectMapper);
                }
                feeSource = "fix_counterparty_override";
            }
        } else {
            Optional<VenueParticipantFeeOverrideRepository.OverrideRow> accountOverride =
                    overrideRepository.findEnabled(
                            VenueParticipantFeeOverrideRepository.TYPE_OMS_ACCOUNT, accountId, contract.id());
            if (accountOverride.isPresent()) {
                if (accountOverride.get().feeModelId() != null && !accountOverride.get().feeModelId().isBlank()) {
                    baseModel = VenueFeeModelId.parse(accountOverride.get().feeModelId());
                }
                if (accountOverride.get().feeParamsJson() != null
                        && !accountOverride.get().feeParamsJson().isBlank()) {
                    params = VenueFeeParams.fromJson(accountOverride.get().feeParamsJson(), objectMapper);
                }
                feeSource = "account_override";
            }
        }
        return new ResolvedModel(baseModel, contract.feeScheduleVersion(), params, feeSource);
    }

    static BigDecimal computeFee(
            VenueFeeModelId modelId,
            VenueFeeParams params,
            VenueLiquidityRole role,
            long lastQtyScaled,
            long lastPxScaled) {
        if (modelId == VenueFeeModelId.ZERO) {
            return BigDecimal.ZERO;
        }
        BigDecimal qty = scaledQty(lastQtyScaled);
        BigDecimal price = scaledPrice(lastPxScaled);
        BigDecimal notional = qty.multiply(price);
        if (notional.signum() <= 0) {
            return BigDecimal.ZERO;
        }
        return switch (modelId) {
            case TAKER_ONLY -> role == VenueLiquidityRole.TAKER
                    ? bpsFee(notional, params.takerBps())
                    : BigDecimal.ZERO;
            case SYMMETRIC, ALL_IN -> bpsFee(notional, params.symmetricBps());
            case MAKER_TAKER -> role == VenueLiquidityRole.MAKER
                    ? bpsFee(notional, params.makerBps())
                    : bpsFee(notional, params.takerBps());
            case KALSHI -> kalshiFee(qty, price, params, role);
            default -> BigDecimal.ZERO;
        };
    }

    private static BigDecimal kalshiFee(
            BigDecimal qty, BigDecimal price, VenueFeeParams params, VenueLiquidityRole role) {
        if (role == VenueLiquidityRole.MAKER && !params.makerFeesEnabled()) {
            return BigDecimal.ZERO;
        }
        BigDecimal k = role == VenueLiquidityRole.MAKER ? params.makerK() : params.takerK();
        BigDecimal factor = price.multiply(BigDecimal.ONE.subtract(price));
        BigDecimal raw = k.multiply(qty).multiply(factor);
        return raw.setScale(MONEY_SCALE, RoundingMode.CEILING);
    }

    private static BigDecimal bpsFee(BigDecimal notional, BigDecimal bps) {
        if (bps == null || bps.signum() <= 0) {
            return BigDecimal.ZERO;
        }
        return notional
                .multiply(bps)
                .divide(new BigDecimal("10000"), 10, RoundingMode.HALF_UP)
                .setScale(MONEY_SCALE, RoundingMode.HALF_UP);
    }

    private static BigDecimal scaledQty(long scaled) {
        return BigDecimal.valueOf(scaled).divide(QTY_SCALE, 10, RoundingMode.HALF_UP);
    }

    private static BigDecimal scaledPrice(long scaled) {
        return BigDecimal.valueOf(scaled).divide(PRICE_SCALE, PROBABILITY_SCALE, RoundingMode.HALF_UP);
    }

    private String settlementCurrencyFor(String instrumentSymbol) {
        return contractRepository
                .findByYesSymbol(baseYesSymbol(instrumentSymbol))
                .map(PredictionMarketContractRepository.ContractRow::settlementCurrency)
                .filter(s -> s != null && !s.isBlank())
                .map(s -> s.trim().toUpperCase())
                .orElse("USD");
    }

    private static String baseYesSymbol(String symbol) {
        String s = symbol.trim();
        return s.endsWith("-NO") ? s.substring(0, s.length() - 3) : s;
    }
}
