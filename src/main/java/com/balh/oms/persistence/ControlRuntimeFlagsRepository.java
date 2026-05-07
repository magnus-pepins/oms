package com.balh.oms.persistence;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Reads {@code oms_runtime_flags} (Flyway V5). Missing row means {@code false}.
 */
@Repository
public class ControlRuntimeFlagsRepository {

    private final JdbcTemplate jdbc;

    public ControlRuntimeFlagsRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public boolean isGlobalHalt() {
        Integer n = jdbc.queryForObject(
                """
                        SELECT COUNT(*)::int FROM oms_runtime_flags
                         WHERE flag_key = ? AND value_boolean = true
                        """,
                Integer.class,
                OmsRuntimeFlagKeys.GLOBAL_HALT);
        return n != null && n > 0;
    }
}
