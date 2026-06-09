package com.balh.oms.events;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class OmsDomainFanoutAccountFilterTest {

    @Test
    void whenAllowlistUnset_publishesAll() {
        if (OmsDomainFanoutAccountFilter.allowlistForTest() != null) {
            // Pop CI may set the env; skip when configured.
            return;
        }
        assertThat(OmsDomainFanoutAccountFilter.shouldPublishForAccount(UUID.randomUUID())).isTrue();
    }
}
