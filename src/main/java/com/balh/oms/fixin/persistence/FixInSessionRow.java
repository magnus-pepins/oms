package com.balh.oms.fixin.persistence;

import java.util.UUID;

/** Row from {@code oms_fix_in_session}. Comp IDs are from the client's perspective on the wire. */
public record FixInSessionRow(
        UUID id,
        UUID counterpartyId,
        String environment,
        String sessionMode,
        String senderCompId,
        String targetCompId,
        String sessionQualifierOrNull,
        String logonUsernameOrNull,
        String passwordHashOrNull,
        int heartbeatSeconds,
        boolean enabled) {}
