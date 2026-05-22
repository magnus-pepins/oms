package com.balh.oms.ingress.reconcile;

import com.balh.oms.config.OmsProfiles;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.EnumMap;
import java.util.Map;

/** Projector-side {@code count(*)} for reconcile (Phase 3). */
@Profile(OmsProfiles.ORDER_ACCEPT_PROFILE)
@ConditionalOnExpression("'${spring.datasource.url:}' != ''")
@Repository
public class OmsProjectorOrderCountsRepository {

    private static final String COUNT_OPEN_ORDERS_SQL = """
            SELECT count(*) FROM orders
            WHERE status IN ('PENDING_NEW', 'NEW', 'WORKING', 'PARTIALLY_FILLED')
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public OmsProjectorOrderCountsRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Map<ReconcileEntityKind, Long> countAll() {
        Long open =
                jdbc.queryForObject(COUNT_OPEN_ORDERS_SQL, new MapSqlParameterSource(), Long.class);
        EnumMap<ReconcileEntityKind, Long> out = new EnumMap<>(ReconcileEntityKind.class);
        out.put(ReconcileEntityKind.OPEN_ORDERS, open == null ? 0L : open);
        return out;
    }
}
