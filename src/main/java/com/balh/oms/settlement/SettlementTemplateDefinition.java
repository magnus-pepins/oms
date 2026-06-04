package com.balh.oms.settlement;

/** Row from {@code settlement_template} (H6). */
public record SettlementTemplateDefinition(
        String templateId, int version, String outboxTable, String description, boolean active) {}
