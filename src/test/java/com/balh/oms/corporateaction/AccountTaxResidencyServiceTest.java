package com.balh.oms.corporateaction;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class AccountTaxResidencyServiceTest {

    private static final UUID ACCOUNT = UUID.fromString("ee69e1be-c1f1-4dfa-b3e8-2ecd9fb90970");

    @Mock AccountTaxResidencyRepository repository;

    @Test
    void upsert_normalizesCountry() {
        var svc = new AccountTaxResidencyService(repository);
        var result = svc.upsert(ACCOUNT, " se ", "kyc.tier3");
        assertThat(result.taxCountry()).isEqualTo("SE");
        verify(repository).upsert(ACCOUNT, "SE", "kyc.tier3");
    }

    @Test
    void upsert_rejectsInvalidCountry() {
        var svc = new AccountTaxResidencyService(repository);
        assertThatThrownBy(() -> svc.upsert(ACCOUNT, "SWE", "kyc"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
