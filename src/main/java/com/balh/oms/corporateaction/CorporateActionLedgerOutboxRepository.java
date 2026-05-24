package com.balh.oms.corporateaction;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/** Outbox for corporate-action payable-date Ledger legs (V76). */
@Repository
public class CorporateActionLedgerOutboxRepository {

    private static final String INSERT =
            """
                    INSERT INTO corporate_action_ledger_outbox (cash_impact_id, leg_kind, payload_json)
                    VALUES (:impactId, :legKind, CAST(:payload AS JSONB))
                    ON CONFLICT (cash_impact_id, leg_kind) DO NOTHING
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public CorporateActionLedgerOutboxRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public int insertIgnore(long cashImpactId, String legKind, String payloadJson) {
        return jdbc.update(
                INSERT,
                new MapSqlParameterSource()
                        .addValue("impactId", cashImpactId)
                        .addValue("legKind", legKind)
                        .addValue("payload", payloadJson));
    }
}
