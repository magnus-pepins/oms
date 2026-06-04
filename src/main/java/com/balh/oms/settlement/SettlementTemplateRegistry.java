package com.balh.oms.settlement;

import jakarta.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.stereotype.Component;

/**
 * H6 settlement template registry: validates {@code template} + {@code templateVersion} on outbox payloads
 * before Ledger posting. Templates are seeded in Flyway {@code V92__settlement_template_registry.sql}.
 */
@Component
public class SettlementTemplateRegistry {

    public static final String OUTBOX_LEDGER_SETTLEMENT = "ledger_settlement_outbox";
    public static final String OUTBOX_PREDICTION_MARKET = "prediction_market_ledger_outbox";

    private final SettlementTemplateRepository repository;
    private volatile Map<String, SettlementTemplateDefinition> byKey = Map.of();

    public SettlementTemplateRegistry(SettlementTemplateRepository repository) {
        this.repository = repository;
    }

    @PostConstruct
    void load() {
        reload();
    }

    public void reload() {
        List<SettlementTemplateDefinition> active = repository.findAllActive();
        Map<String, SettlementTemplateDefinition> next = new HashMap<>();
        for (SettlementTemplateDefinition def : active) {
            next.put(key(def.templateId(), def.version()), def);
        }
        byKey = Map.copyOf(next);
    }

    public Optional<SettlementTemplateDefinition> find(String templateId, int version) {
        return Optional.ofNullable(byKey.get(key(templateId, version)));
    }

    /** Ensures the template is registered, active, and bound to the expected outbox table. */
    public void requireForOutbox(String templateId, int version, String expectedOutboxTable) {
        SettlementTemplateDefinition def =
                find(templateId, version)
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "unknown settlement template: "
                                                        + templateId
                                                        + " v"
                                                        + version));
        if (!def.active()) {
            throw new IllegalArgumentException(
                    "inactive settlement template: " + templateId + " v" + version);
        }
        if (!expectedOutboxTable.equals(def.outboxTable())) {
            throw new IllegalArgumentException(
                    "settlement template "
                            + templateId
                            + " v"
                            + version
                            + " is bound to outbox "
                            + def.outboxTable()
                            + ", not "
                            + expectedOutboxTable);
        }
    }

    private static String key(String templateId, int version) {
        return templateId + "@" + version;
    }
}
