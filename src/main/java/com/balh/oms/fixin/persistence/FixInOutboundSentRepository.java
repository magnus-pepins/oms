package com.balh.oms.fixin.persistence;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public class FixInOutboundSentRepository {

    private static final String INSERT = """
            INSERT INTO oms_fix_in_outbound_sent (session_id, dedupe_key)
            VALUES (:session_id, :dedupe_key)
            ON CONFLICT DO NOTHING
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public FixInOutboundSentRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** @return {@code true} if this key was newly inserted (caller should send). */
    public boolean tryMarkSent(UUID sessionId, String dedupeKey) {
        return jdbc.update(
                        INSERT,
                        new MapSqlParameterSource()
                                .addValue("session_id", sessionId)
                                .addValue("dedupe_key", dedupeKey))
                == 1;
    }
}
