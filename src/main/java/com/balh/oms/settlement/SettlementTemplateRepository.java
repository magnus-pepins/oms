package com.balh.oms.settlement;

import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class SettlementTemplateRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public SettlementTemplateRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<SettlementTemplateDefinition> findAllActive() {
        return jdbc.query(
                """
                        SELECT template_id, version, outbox_table, description, active
                        FROM settlement_template
                        WHERE active = TRUE
                        ORDER BY template_id, version
                        """,
                (rs, rowNum) ->
                        new SettlementTemplateDefinition(
                                rs.getString("template_id"),
                                rs.getInt("version"),
                                rs.getString("outbox_table"),
                                rs.getString("description"),
                                rs.getBoolean("active")));
    }

    public Optional<SettlementTemplateDefinition> find(String templateId, int version) {
        List<SettlementTemplateDefinition> rows =
                jdbc.query(
                        """
                                SELECT template_id, version, outbox_table, description, active
                                FROM settlement_template
                                WHERE template_id = :templateId AND version = :version
                                """,
                        new MapSqlParameterSource()
                                .addValue("templateId", templateId)
                                .addValue("version", version),
                        (rs, rowNum) ->
                                new SettlementTemplateDefinition(
                                        rs.getString("template_id"),
                                        rs.getInt("version"),
                                        rs.getString("outbox_table"),
                                        rs.getString("description"),
                                        rs.getBoolean("active")));
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }
}
