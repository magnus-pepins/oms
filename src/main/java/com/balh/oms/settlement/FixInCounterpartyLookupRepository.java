package com.balh.oms.settlement;

import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class FixInCounterpartyLookupRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public FixInCounterpartyLookupRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<UUID> findCounterpartyIdForAccount(UUID omsAccountId) {
        var ids =
                jdbc.query(
                        """
                                SELECT DISTINCT s.counterparty_id
                                FROM oms_fix_in_account_binding b
                                JOIN oms_fix_in_session s ON s.id = b.session_id
                                WHERE b.oms_account_id = :accountId
                                  AND b.enabled = TRUE
                                  AND s.enabled = TRUE
                                LIMIT 1
                                """,
                        new MapSqlParameterSource("accountId", omsAccountId),
                        (rs, rowNum) -> (UUID) rs.getObject("counterparty_id"));
        return ids.isEmpty() ? Optional.empty() : Optional.of(ids.getFirst());
    }

    public boolean isFixAccount(UUID omsAccountId) {
        return findCounterpartyIdForAccount(omsAccountId).isPresent();
    }
}
