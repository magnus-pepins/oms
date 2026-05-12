package com.balh.oms.persistence;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.UUID;

/**
 * Postgres idempotency for {@code RouteDispatcher.enqueueWorkingOrder} when the Chronicle tail uses dispatch-only mode
 * ({@code oms.control.postgres-write-path=ingress}) — avoids duplicate {@code NewOrderSingle} offers on replay.
 */
@Repository
public class FixNosRouteEnqueueClaimRepository {

    private static final String INSERT_CLAIM =
            """
            INSERT INTO fix_nos_route_enqueue_claim (order_id) VALUES (:order_id)
            ON CONFLICT (order_id) DO NOTHING
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public FixNosRouteEnqueueClaimRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * @return {@code true} if this call inserted the claim (caller should enqueue); {@code false} if already claimed
     */
    public boolean tryClaim(UUID orderId) {
        int n = jdbc.update(INSERT_CLAIM, new MapSqlParameterSource("order_id", orderId));
        return n == 1;
    }
}
