package com.balh.oms.fixin.persistence;

import java.util.UUID;

public record FixInOrderMapRow(
        UUID sessionId, String clientClOrdId, UUID omsOrderId, String origClientClOrdIdOrNull) {}
