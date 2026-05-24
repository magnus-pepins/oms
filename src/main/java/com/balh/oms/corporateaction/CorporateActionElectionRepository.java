package com.balh.oms.corporateaction;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class CorporateActionElectionRepository {

    public record ElectionRow(
            long id,
            long corporateActionEventId,
            UUID accountId,
            String electionChoice,
            String requestedBy,
            String approvedBy,
            Instant approvedAt,
            Instant createdAt) {}

    private static final String INSERT =
            """
                    INSERT INTO corporate_action_election (
                        corporate_action_event_id, account_id, election_choice, requested_by
                    ) VALUES (
                        :eventId, :accountId, :choice, :requestedBy
                    )
                    ON CONFLICT (corporate_action_event_id, account_id) DO UPDATE SET
                        election_choice = EXCLUDED.election_choice,
                        requested_by = EXCLUDED.requested_by,
                        approved_by = NULL,
                        approved_at = NULL,
                        created_at = NOW()
                    RETURNING id
                    """;

    private static final String APPROVE =
            """
                    UPDATE corporate_action_election
                    SET approved_by = :approvedBy, approved_at = NOW()
                    WHERE id = :id
                      AND approved_by IS NULL
                      AND LOWER(requested_by) <> LOWER(:approvedBy)
                    """;

    private static final String SELECT_BY_EVENT =
            """
                    SELECT id, corporate_action_event_id, account_id, election_choice,
                           requested_by, approved_by, approved_at, created_at
                    FROM corporate_action_election
                    WHERE corporate_action_event_id = :eventId
                    ORDER BY id
                    """;

    private static final String SELECT_BY_ID =
            """
                    SELECT id, corporate_action_event_id, account_id, election_choice,
                           requested_by, approved_by, approved_at, created_at
                    FROM corporate_action_election
                    WHERE id = :id
                    """;

    private static final String COUNT_APPROVED_FOR_ACCOUNTS =
            """
                    SELECT COUNT(*)::int
                    FROM corporate_action_election
                    WHERE corporate_action_event_id = :eventId
                      AND account_id = ANY(CAST(:accountIds AS UUID[]))
                      AND approved_at IS NOT NULL
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public CorporateActionElectionRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public long upsert(long eventId, UUID accountId, String electionChoice, String requestedBy) {
        List<Long> ids =
                jdbc.query(
                        INSERT,
                        new MapSqlParameterSource()
                                .addValue("eventId", eventId)
                                .addValue("accountId", accountId)
                                .addValue("choice", electionChoice)
                                .addValue("requestedBy", requestedBy),
                        (rs, rowNum) -> rs.getLong("id"));
        return ids.getFirst();
    }

    public int approve(long electionId, String approvedBy) {
        return jdbc.update(
                APPROVE,
                new MapSqlParameterSource()
                        .addValue("id", electionId)
                        .addValue("approvedBy", approvedBy));
    }

    public Optional<ElectionRow> findById(long id) {
        return jdbc.query(
                        SELECT_BY_ID,
                        new MapSqlParameterSource("id", id),
                        (rs, rowNum) -> mapRow(rs))
                .stream()
                .findFirst();
    }

    public List<ElectionRow> listByEvent(long eventId) {
        return jdbc.query(
                SELECT_BY_EVENT, new MapSqlParameterSource("eventId", eventId), (rs, rowNum) -> mapRow(rs));
    }

    public int countApprovedForAccounts(long eventId, List<UUID> accountIds) {
        if (accountIds == null || accountIds.isEmpty()) {
            return 0;
        }
        Integer count =
                jdbc.queryForObject(
                        COUNT_APPROVED_FOR_ACCOUNTS,
                        new MapSqlParameterSource()
                                .addValue("eventId", eventId)
                                .addValue("accountIds", accountIds.toArray(new UUID[0])),
                        Integer.class);
        return count == null ? 0 : count;
    }

    private static ElectionRow mapRow(java.sql.ResultSet rs) throws java.sql.SQLException {
        var approvedAt = rs.getTimestamp("approved_at");
        return new ElectionRow(
                rs.getLong("id"),
                rs.getLong("corporate_action_event_id"),
                (UUID) rs.getObject("account_id"),
                rs.getString("election_choice"),
                rs.getString("requested_by"),
                rs.getString("approved_by"),
                approvedAt == null ? null : approvedAt.toInstant(),
                rs.getTimestamp("created_at").toInstant());
    }
}
