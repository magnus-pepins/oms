package com.balh.oms.persistence;

import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Reads and updates {@code oms_runtime_flags} (Flyway V5). Missing row means {@code false} for halt.
 */
@Repository
public class ControlRuntimeFlagsRepository {

    private final JdbcTemplate jdbc;

    public ControlRuntimeFlagsRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public boolean isGlobalHalt() {
        return findGlobalHaltRow().map(GlobalHaltRow::value).orElse(false);
    }

    /** Present when a row exists for {@code global_halt}; absent when no row has been written yet. */
    public Optional<GlobalHaltRow> findGlobalHaltRow() {
        List<GlobalHaltRow> rows =
                jdbc.query(
                        """
                                SELECT value_boolean, updated_at FROM oms_runtime_flags
                                 WHERE flag_key = ?
                                """,
                        (rs, rowNum) ->
                                new GlobalHaltRow(
                                        rs.getBoolean("value_boolean"),
                                        rs.getTimestamp("updated_at").toInstant()),
                        OmsRuntimeFlagKeys.GLOBAL_HALT);
        return rows.stream().findFirst();
    }

    public void setGlobalHalt(boolean halted) {
        jdbc.update(
                """
                        INSERT INTO oms_runtime_flags (flag_key, value_boolean, updated_at)
                        VALUES (?, ?, NOW())
                        ON CONFLICT (flag_key) DO UPDATE SET
                            value_boolean = EXCLUDED.value_boolean,
                            updated_at = NOW()
                        """,
                OmsRuntimeFlagKeys.GLOBAL_HALT,
                halted);
    }
}
