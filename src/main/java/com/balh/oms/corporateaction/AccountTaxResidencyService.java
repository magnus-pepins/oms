package com.balh.oms.corporateaction;

import java.util.Locale;
import java.util.UUID;
import org.springframework.stereotype.Service;

/** Upsert customer tax residency for treaty withholding (gap plan §5.9 Phase 2). */
@Service
public class AccountTaxResidencyService {

    private static final int ISO_COUNTRY_LEN = 2;

    private final AccountTaxResidencyRepository repository;

    public AccountTaxResidencyService(AccountTaxResidencyRepository repository) {
        this.repository = repository;
    }

    public record UpsertResult(UUID accountId, String taxCountry, String source) {}

    public UpsertResult upsert(UUID accountId, String taxCountry, String source) {
        if (accountId == null) {
            throw new IllegalArgumentException("account_id_required");
        }
        if (taxCountry == null || taxCountry.isBlank()) {
            throw new IllegalArgumentException("tax_country_required");
        }
        String normalized = taxCountry.trim().toUpperCase(Locale.ROOT);
        if (normalized.length() != ISO_COUNTRY_LEN || !normalized.chars().allMatch(Character::isLetter)) {
            throw new IllegalArgumentException("tax_country_must_be_iso_alpha2");
        }
        repository.upsert(accountId, normalized, source);
        return new UpsertResult(accountId, normalized, source == null ? "" : source.trim());
    }
}
