package com.balh.oms.corporateaction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Repository;

@Repository
public class ManualCorporateActionEventRepository {

    public record Row(
            long id,
            String templateType,
            String payloadJson,
            String status,
            String createdBy,
            String approvedBy,
            Instant approvedAt,
            Long corporateActionEventId,
            Instant createdAt) {}

    private final NamedParameterJdbcTemplate jdbc;

    public ManualCorporateActionEventRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public long insert(String templateType, String payloadJson, String createdBy) {
        var kh = new GeneratedKeyHolder();
        jdbc.update(
                """
                        INSERT INTO manual_corporate_action_event (template_type, payload_json, created_by)
                        VALUES (:type, CAST(:payload AS JSONB), :createdBy)
                        """,
                new MapSqlParameterSource()
                        .addValue("type", templateType)
                        .addValue("payload", payloadJson)
                        .addValue("createdBy", createdBy),
                kh,
                new String[] {"id"});
        Number key = kh.getKey();
        if (key == null) {
            throw new IllegalStateException("manual_corporate_action_event insert returned no id");
        }
        return key.longValue();
    }

    public Optional<Row> findById(long id) {
        List<Row> rows =
                jdbc.query(
                        """
                                SELECT id, template_type, payload_json::text, status, created_by, approved_by,
                                       approved_at, corporate_action_event_id, created_at
                                FROM manual_corporate_action_event WHERE id = :id
                                """,
                        new MapSqlParameterSource("id", id),
                        (rs, rowNum) ->
                                new Row(
                                        rs.getLong("id"),
                                        rs.getString("template_type"),
                                        rs.getString("payload_json"),
                                        rs.getString("status"),
                                        rs.getString("created_by"),
                                        rs.getString("approved_by"),
                                        rs.getTimestamp("approved_at") == null
                                                ? null
                                                : rs.getTimestamp("approved_at").toInstant(),
                                        (Long) rs.getObject("corporate_action_event_id"),
                                        rs.getTimestamp("created_at").toInstant()));
        return rows.stream().findFirst();
    }

    public boolean approve(long id, String approvedBy, long corporateActionEventId) {
        return jdbc.update(
                        """
                                UPDATE manual_corporate_action_event
                                SET status = 'applied', approved_by = :approvedBy, approved_at = NOW(),
                                    corporate_action_event_id = :eventId
                                WHERE id = :id AND status = 'pending_approval'
                                """,
                        new MapSqlParameterSource()
                                .addValue("id", id)
                                .addValue("approvedBy", approvedBy)
                                .addValue("eventId", corporateActionEventId))
                > 0;
    }

    public List<Row> listRecent(int limit) {
        return jdbc.query(
                """
                        SELECT id, template_type, payload_json::text, status, created_by, approved_by,
                               approved_at, corporate_action_event_id, created_at
                        FROM manual_corporate_action_event
                        ORDER BY created_at DESC
                        LIMIT :lim
                        """,
                new MapSqlParameterSource("lim", limit),
                (rs, rowNum) ->
                        new Row(
                                rs.getLong("id"),
                                rs.getString("template_type"),
                                rs.getString("payload_json"),
                                rs.getString("status"),
                                rs.getString("created_by"),
                                rs.getString("approved_by"),
                                rs.getTimestamp("approved_at") == null
                                        ? null
                                        : rs.getTimestamp("approved_at").toInstant(),
                                (Long) rs.getObject("corporate_action_event_id"),
                                rs.getTimestamp("created_at").toInstant()));
    }

    public static void validateTemplatePayload(String templateType, JsonNode payload, ObjectMapper mapper) {
        if (templateType == null || templateType.isBlank()) {
            throw new IllegalArgumentException("templateType required");
        }
        if (payload == null || !payload.isObject()) {
            throw new IllegalArgumentException("payload_json object required");
        }
        requireText(payload, "instrumentSymbol");
        requireText(payload, "effectiveDate");
        switch (templateType.trim().toUpperCase()) {
            case "MERGER" -> requireText(payload, "survivorSymbol");
            case "SPIN_OFF" -> requireText(payload, "spunOffSymbol");
            case "BANKRUPTCY_DELISTING" -> { /* symbol zero-out only */ }
            default -> throw new IllegalArgumentException("unsupported templateType: " + templateType);
        }
    }

    private static void requireText(JsonNode node, String field) {
        if (!node.has(field) || node.get(field).isNull() || node.get(field).asText().isBlank()) {
            throw new IllegalArgumentException("payload." + field + " required");
        }
    }
}
