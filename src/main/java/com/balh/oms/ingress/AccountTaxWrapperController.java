package com.balh.oms.ingress;

import com.balh.oms.settlement.OmsAccountTaxWrapperRepository;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Internal CRUD for {@code oms_account_tax_wrapper} (ISK / investment tax-wrapper mappings).
 * Secured by {@link ApiKeyFilter} like other {@code /internal/v1/**} routes.
 */
@RestController
@RequestMapping("/internal/v1/settlement/tax-wrappers")
public class AccountTaxWrapperController {

    private static final int LIST_DEFAULT_LIMIT = 100;
    private static final int LIST_MAX_LIMIT = 500;
    private static final int MAX_LIST_OFFSET = 10_000;

    private static final Set<String> ALLOWED_TAX_WRAPPERS =
            Set.of(
                    OmsAccountTaxWrapperRepository.TAX_WRAPPER_NONE,
                    OmsAccountTaxWrapperRepository.TAX_WRAPPER_INVESTMENT,
                    OmsAccountTaxWrapperRepository.TAX_WRAPPER_ISK);

    public record TaxWrapperItemResponse(
            UUID accountId, String taxWrapper, UUID iskAccountId, String ledgerBalanceId) {}

    public record TaxWrapperListResponse(List<TaxWrapperItemResponse> items, int limit, int offset) {}

    public record UpsertTaxWrapperRequest(String taxWrapper, UUID iskAccountId, String ledgerBalanceId) {}

    private final OmsAccountTaxWrapperRepository taxWrappers;

    public AccountTaxWrapperController(OmsAccountTaxWrapperRepository taxWrappers) {
        this.taxWrappers = taxWrappers;
    }

    @GetMapping
    public ResponseEntity<?> list(
            @RequestParam(required = false) String taxWrapper,
            @RequestParam(required = false) Integer limit,
            @RequestParam(required = false) Integer offset) {
        if (taxWrapper != null && !taxWrapper.isBlank() && !ALLOWED_TAX_WRAPPERS.contains(taxWrapper.trim())) {
            return ResponseEntity.badRequest().build();
        }
        int lim = limit == null ? LIST_DEFAULT_LIMIT : limit;
        if (lim < 1) {
            lim = LIST_DEFAULT_LIMIT;
        }
        lim = Math.min(lim, LIST_MAX_LIMIT);
        int off = offset == null ? 0 : offset;
        if (off < 0 || off > MAX_LIST_OFFSET) {
            return ResponseEntity.badRequest().build();
        }
        String filter = taxWrapper == null || taxWrapper.isBlank() ? null : taxWrapper.trim();
        var items = taxWrappers.listPage(filter, lim, off).stream()
                .map(AccountTaxWrapperController::toResponse)
                .toList();
        return ResponseEntity.ok(new TaxWrapperListResponse(items, lim, off));
    }

    @PutMapping("/{accountId}")
    public ResponseEntity<?> upsert(
            @PathVariable UUID accountId, @RequestBody UpsertTaxWrapperRequest body) {
        if (body == null || body.taxWrapper() == null || body.taxWrapper().isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        String wrapper = body.taxWrapper().trim();
        if (!ALLOWED_TAX_WRAPPERS.contains(wrapper)) {
            return ResponseEntity.badRequest().build();
        }
        String ledgerBalanceId = body.ledgerBalanceId() == null ? null : body.ledgerBalanceId().trim();
        if (ledgerBalanceId != null && ledgerBalanceId.isEmpty()) {
            ledgerBalanceId = null;
        }
        taxWrappers.upsert(accountId, wrapper, body.iskAccountId(), ledgerBalanceId);
        return taxWrappers
                .findByAccountId(accountId)
                .map(AccountTaxWrapperController::toResponse)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElse(ResponseEntity.internalServerError().build());
    }

    private static TaxWrapperItemResponse toResponse(OmsAccountTaxWrapperRepository.AccountTaxWrapperRow row) {
        return new TaxWrapperItemResponse(
                row.accountId(), row.taxWrapper(), row.iskAccountId(), row.ledgerBalanceId());
    }
}
