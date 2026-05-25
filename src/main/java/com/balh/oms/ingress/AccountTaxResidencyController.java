package com.balh.oms.ingress;

import com.balh.oms.corporateaction.AccountTaxResidencyRepository;
import com.balh.oms.corporateaction.AccountTaxResidencyService;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Internal sync surface for {@code oms_account_tax_residency}. Upstream systems
 * (KYC gateway post-Tier-3, onboarding BFF) call this instead of writing SQL.
 */
@RestController
@RequestMapping("/internal/v1/account-tax-residency")
public class AccountTaxResidencyController {

    public record UpsertBody(String taxCountry, String source) {}

    private final AccountTaxResidencyService service;
    private final AccountTaxResidencyRepository repository;

    public AccountTaxResidencyController(
            AccountTaxResidencyService service, AccountTaxResidencyRepository repository) {
        this.service = service;
        this.repository = repository;
    }

    @PutMapping("/{accountId}")
    public ResponseEntity<Map<String, Object>> upsert(
            @PathVariable UUID accountId, @RequestBody UpsertBody body) {
        if (body == null || body.taxCountry() == null || body.taxCountry().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "tax_country_required"));
        }
        try {
            AccountTaxResidencyService.UpsertResult r =
                    service.upsert(accountId, body.taxCountry(), body.source());
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("accountId", r.accountId().toString());
            out.put("taxCountry", r.taxCountry());
            out.put("source", r.source());
            return ResponseEntity.ok(out);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/{accountId}")
    public ResponseEntity<Map<String, Object>> get(@PathVariable UUID accountId) {
        return repository
                .findTaxCountry(accountId)
                .map(
                        c -> {
                            Map<String, Object> out = new LinkedHashMap<>();
                            out.put("accountId", accountId.toString());
                            out.put("taxCountry", c);
                            return ResponseEntity.ok(out);
                        })
                .orElse(ResponseEntity.notFound().build());
    }
}
