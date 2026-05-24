package com.balh.oms.fixin.persistence;

import java.util.UUID;

/** Row from {@code oms_fix_in_account_binding}. */
public record FixInAccountBindingRow(
        UUID id,
        UUID sessionId,
        String fixAccountTagOrEmpty,
        UUID omsAccountId,
        String ledgerIdentityIdOrNull,
        String ledgerBalanceIdOrNull,
        boolean defaultBinding,
        boolean enabled) {}
