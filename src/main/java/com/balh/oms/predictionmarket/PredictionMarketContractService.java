package com.balh.oms.predictionmarket;

import com.balh.oms.venue.VenueContractRegistryClient;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import org.springframework.stereotype.Service;

/** Operator catalog CRUD + validation for {@link PredictionMarketContractRepository}. */
@Service
public class PredictionMarketContractService {

    private static final Set<String> ALLOWED_STATUS = Set.of("OPEN", "HALTED", "CLOSED", "RESOLVED");
    private static final String SYMBOL_PREFIX = "PREDMKT-";

    private final PredictionMarketContractRepository repository;
    private final VenueContractRegistryClient venueRegistry;

    public PredictionMarketContractService(
            PredictionMarketContractRepository repository, VenueContractRegistryClient venueRegistry) {
        this.repository = repository;
        this.venueRegistry = venueRegistry;
    }

    public record CreateRequest(
            String slug,
            String title,
            String yesSymbol,
            String noSymbol,
            String resolutionSource,
            String settlementCurrency,
            BigDecimal tickSize,
            BigDecimal payoutPerContract,
            Instant closesAt,
            String status) {}

    public record UpdateRequest(
            String title,
            String resolutionSource,
            String settlementCurrency,
            BigDecimal tickSize,
            BigDecimal payoutPerContract,
            Instant closesAt,
            String status) {}

    public PredictionMarketContractRepository.ContractRow create(CreateRequest req) {
        String slug = normalizeSlug(req.slug());
        String title = requireNonBlank(req.title(), "title");
        String settlementCurrency = normalizeCurrency(req.settlementCurrency());
        String yesSymbol = normalizeYesSymbol(req.yesSymbol(), slug);
        String noSymbol = normalizeNoSymbol(req.noSymbol(), yesSymbol);
        String status = normalizeStatus(req.status(), "OPEN");
        BigDecimal tickSize = req.tickSize() != null ? req.tickSize() : new BigDecimal("0.01");
        BigDecimal payout =
                req.payoutPerContract() != null ? req.payoutPerContract() : new BigDecimal("1.00");
        if (tickSize.signum() <= 0 || payout.signum() <= 0) {
            throw new IllegalArgumentException("tickSize and payoutPerContract must be positive");
        }
        PredictionMarketContractRepository.ContractRow row =
                repository.insert(
                        slug,
                        title,
                        yesSymbol,
                        noSymbol,
                        trimToNull(req.resolutionSource()),
                        status,
                        settlementCurrency,
                        tickSize,
                        payout,
                        req.closesAt());
        venueRegistry.syncContract(row);
        return row;
    }

    public Optional<PredictionMarketContractRepository.ContractRow> update(long id, UpdateRequest req) {
        return repository
                .findById(id)
                .map(
                        existing -> {
                            String title =
                                    req.title() != null ? requireNonBlank(req.title(), "title") : existing.title();
                            String resolutionSource =
                                    req.resolutionSource() != null
                                            ? trimToNull(req.resolutionSource())
                                            : existing.resolutionSource();
                            String settlementCurrency =
                                    req.settlementCurrency() != null
                                            ? normalizeCurrency(req.settlementCurrency())
                                            : existing.settlementCurrency();
                            String status =
                                    req.status() != null
                                            ? normalizeStatus(req.status(), existing.status())
                                            : existing.status();
                            BigDecimal tickSize =
                                    req.tickSize() != null ? req.tickSize() : existing.tickSize();
                            BigDecimal payout =
                                    req.payoutPerContract() != null
                                            ? req.payoutPerContract()
                                            : existing.payoutPerContract();
                            if (tickSize.signum() <= 0 || payout.signum() <= 0) {
                                throw new IllegalArgumentException(
                                        "tickSize and payoutPerContract must be positive");
                            }
                            Instant closesAt =
                                    req.closesAt() != null ? req.closesAt() : existing.closesAt();
                            PredictionMarketContractRepository.ContractRow updated =
                                    repository.update(
                                            id,
                                            title,
                                            resolutionSource,
                                            status,
                                            settlementCurrency,
                                            tickSize,
                                            payout,
                                            closesAt);
                            venueRegistry.syncContract(updated);
                            return updated;
                        });
    }

    public static String normalizeCurrency(String raw) {
        if (raw == null || raw.isBlank()) {
            return "USD";
        }
        String ccy = raw.trim().toUpperCase(Locale.ROOT);
        if (!ccy.matches("[A-Z]{3}")) {
            throw new IllegalArgumentException("settlementCurrency must be ISO 4217 (3 letters)");
        }
        return ccy;
    }

    private static String normalizeSlug(String raw) {
        String slug = requireNonBlank(raw, "slug").toLowerCase(Locale.ROOT);
        if (!slug.matches("[a-z0-9][a-z0-9-]{0,62}")) {
            throw new IllegalArgumentException("slug must be lowercase alphanumeric with hyphens");
        }
        return slug;
    }

    private static String normalizeYesSymbol(String raw, String slug) {
        if (raw != null && !raw.isBlank()) {
            String sym = raw.trim().toUpperCase(Locale.ROOT);
            if (!sym.startsWith(SYMBOL_PREFIX)) {
                throw new IllegalArgumentException("yesSymbol must start with " + SYMBOL_PREFIX);
            }
            return sym;
        }
        return SYMBOL_PREFIX + slug.toUpperCase(Locale.ROOT).replace('-', '_');
    }

    private static String normalizeNoSymbol(String raw, String yesSymbol) {
        if (raw != null && !raw.isBlank()) {
            String sym = raw.trim().toUpperCase(Locale.ROOT);
            if (!sym.endsWith("-NO")) {
                throw new IllegalArgumentException("noSymbol must end with -NO");
            }
            return sym;
        }
        return yesSymbol + "-NO";
    }

    private static String normalizeStatus(String raw, String defaultStatus) {
        String status = raw == null || raw.isBlank() ? defaultStatus : raw.trim().toUpperCase(Locale.ROOT);
        if (!ALLOWED_STATUS.contains(status)) {
            throw new IllegalArgumentException("status must be one of " + ALLOWED_STATUS);
        }
        return status;
    }

    private static String requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value.trim();
    }

    private static String trimToNull(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }
}
