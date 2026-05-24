package com.balh.oms.fixin.persistence;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/** Reads QuickFIX/J {@code oms_fix_sessions} seq state (JDBC store). */
@Repository
public class FixInQuickFixSessionStateRepository {

    private static final String SELECT_SEQ = """
            SELECT incoming_seqnum, outgoing_seqnum
              FROM oms_fix_sessions
             WHERE beginstring = 'FIX.4.4'
               AND sendercompid = :sender_comp_id
               AND targetcompid = :target_comp_id
               AND session_qualifier = COALESCE(:session_qualifier, '')
               AND sendersubid = ''
               AND senderlocid = ''
               AND targetsubid = ''
               AND targetlocid = ''
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public FixInQuickFixSessionStateRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<SeqState> findSeqState(String omsSenderCompId, String clientTargetCompId, String qualifierOrNull) {
        try {
            return Optional.of(jdbc.queryForObject(
                    SELECT_SEQ,
                    new MapSqlParameterSource()
                            .addValue("sender_comp_id", omsSenderCompId)
                            .addValue("target_comp_id", clientTargetCompId)
                            .addValue("session_qualifier", qualifierOrNull == null ? "" : qualifierOrNull),
                    (rs, rowNum) -> new SeqState(rs.getInt("incoming_seqnum"), rs.getInt("outgoing_seqnum"))));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public record SeqState(int incomingSeqNum, int outgoingSeqNum) {}
}
