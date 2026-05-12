package com.balh.oms.fixegress;

import com.balh.oms.AbstractPostgresIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Slice 3b-1 coverage: V26 migration applied, cursor repository upserts and reads cleanly,
 * the monotonic {@code WHERE last_applied_position &lt; EXCLUDED.last_applied_position}
 * clause is enforced, and {@link OmsFixEgressCursorRepository#reset} bypasses that guard for
 * the recording-clamp path.
 */
class OmsFixEgressCursorRepositoryIntegrationTest extends AbstractPostgresIntegrationTest {

    private static final String EGRESS_ID = "test-egress";
    private static final int STREAM_ID = 42;

    @Autowired
    OmsFixEgressCursorRepository cursorRepository;

    @Autowired
    JdbcTemplate jdbc;

    @BeforeEach
    void cleanCursor() {
        jdbc.update("DELETE FROM oms_fix_egress_cursor WHERE egress_id = ?", EGRESS_ID);
    }

    @Test
    void noPriorRow_returnsEmpty() {
        assertThat(cursorRepository.findLastAppliedPosition(EGRESS_ID, STREAM_ID)).isEmpty();
    }

    @Test
    void firstAdvance_inserts() {
        boolean changed = cursorRepository.advance(EGRESS_ID, STREAM_ID, 100L);
        assertThat(changed).isTrue();
        OptionalLong pos = cursorRepository.findLastAppliedPosition(EGRESS_ID, STREAM_ID);
        assertThat(pos).hasValue(100L);
    }

    @Test
    void monotonicAdvance_updatesForwards_noopForOlder() {
        cursorRepository.advance(EGRESS_ID, STREAM_ID, 100L);

        boolean forwards = cursorRepository.advance(EGRESS_ID, STREAM_ID, 200L);
        assertThat(forwards).isTrue();
        assertThat(cursorRepository.findLastAppliedPosition(EGRESS_ID, STREAM_ID)).hasValue(200L);

        boolean older = cursorRepository.advance(EGRESS_ID, STREAM_ID, 150L);
        assertThat(older).isFalse();
        assertThat(cursorRepository.findLastAppliedPosition(EGRESS_ID, STREAM_ID)).hasValue(200L);

        boolean equal = cursorRepository.advance(EGRESS_ID, STREAM_ID, 200L);
        assertThat(equal).isFalse();
        assertThat(cursorRepository.findLastAppliedPosition(EGRESS_ID, STREAM_ID)).hasValue(200L);
    }

    @Test
    void streamsAreIndependent() {
        cursorRepository.advance(EGRESS_ID, STREAM_ID, 100L);
        cursorRepository.advance(EGRESS_ID, STREAM_ID + 1, 50L);

        assertThat(cursorRepository.findLastAppliedPosition(EGRESS_ID, STREAM_ID)).hasValue(100L);
        assertThat(cursorRepository.findLastAppliedPosition(EGRESS_ID, STREAM_ID + 1)).hasValue(50L);
    }

    @Test
    void reset_bypassesMonotonicGuard() {
        cursorRepository.advance(EGRESS_ID, STREAM_ID, 500L);
        assertThat(cursorRepository.findLastAppliedPosition(EGRESS_ID, STREAM_ID)).hasValue(500L);

        boolean rewindByAdvance = cursorRepository.advance(EGRESS_ID, STREAM_ID, 100L);
        assertThat(rewindByAdvance).isFalse();
        assertThat(cursorRepository.findLastAppliedPosition(EGRESS_ID, STREAM_ID)).hasValue(500L);

        cursorRepository.reset(EGRESS_ID, STREAM_ID, 100L);
        assertThat(cursorRepository.findLastAppliedPosition(EGRESS_ID, STREAM_ID)).hasValue(100L);
    }

    @Test
    void egressIdsAreIndependent() {
        cursorRepository.advance(EGRESS_ID, STREAM_ID, 100L);
        cursorRepository.advance(EGRESS_ID + "-other", STREAM_ID, 50L);

        assertThat(cursorRepository.findLastAppliedPosition(EGRESS_ID, STREAM_ID)).hasValue(100L);
        assertThat(cursorRepository.findLastAppliedPosition(EGRESS_ID + "-other", STREAM_ID))
                .hasValue(50L);

        jdbc.update("DELETE FROM oms_fix_egress_cursor WHERE egress_id = ?", EGRESS_ID + "-other");
    }
}
