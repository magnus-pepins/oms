package com.balh.oms.ingress;

import com.balh.oms.AbstractPostgresIntegrationTest;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.assertj.core.api.Assertions.assertThat;

class AccountTaxWrapperControllerIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired TestRestTemplate rest;

    HttpHeaders internalHeaders;
    UUID accountId;

    @BeforeEach
    void headers() {
        internalHeaders = new HttpHeaders();
        internalHeaders.set("X-OMS-Internal-Key", "test-key");
        accountId = UUID.randomUUID();
    }

    @Test
    void listAndUpsertTaxWrapper_roundTrip() {
        ResponseEntity<AccountTaxWrapperController.TaxWrapperListResponse> empty =
                rest.exchange(
                        "/internal/v1/settlement/tax-wrappers?taxWrapper=isk",
                        HttpMethod.GET,
                        new HttpEntity<>(internalHeaders),
                        AccountTaxWrapperController.TaxWrapperListResponse.class);
        assertThat(empty.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(empty.getBody()).isNotNull();

        UUID iskAccountId = UUID.randomUUID();
        var upsertBody =
                new AccountTaxWrapperController.UpsertTaxWrapperRequest("isk", iskAccountId, "bal-isk-test");

        ResponseEntity<AccountTaxWrapperController.TaxWrapperItemResponse> upsert =
                rest.exchange(
                        "/internal/v1/settlement/tax-wrappers/" + accountId,
                        HttpMethod.PUT,
                        new HttpEntity<>(upsertBody, internalHeaders),
                        AccountTaxWrapperController.TaxWrapperItemResponse.class);
        assertThat(upsert.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(upsert.getBody()).isNotNull();
        assertThat(upsert.getBody().accountId()).isEqualTo(accountId);
        assertThat(upsert.getBody().taxWrapper()).isEqualTo("isk");
        assertThat(upsert.getBody().iskAccountId()).isEqualTo(iskAccountId);
        assertThat(upsert.getBody().ledgerBalanceId()).isEqualTo("bal-isk-test");

        ResponseEntity<AccountTaxWrapperController.TaxWrapperListResponse> listed =
                rest.exchange(
                        "/internal/v1/settlement/tax-wrappers?taxWrapper=isk&limit=50",
                        HttpMethod.GET,
                        new HttpEntity<>(internalHeaders),
                        AccountTaxWrapperController.TaxWrapperListResponse.class);
        assertThat(listed.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(listed.getBody()).isNotNull();
        assertThat(listed.getBody().items()).anyMatch(
                row -> row.accountId().equals(accountId) && row.ledgerBalanceId().equals("bal-isk-test"));
    }

    @Test
    void upsert_rejectsInvalidTaxWrapper() {
        var body = new AccountTaxWrapperController.UpsertTaxWrapperRequest("invalid", null, null);
        ResponseEntity<String> res =
                rest.exchange(
                        "/internal/v1/settlement/tax-wrappers/" + accountId,
                        HttpMethod.PUT,
                        new HttpEntity<>(body, internalHeaders),
                        String.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
}
