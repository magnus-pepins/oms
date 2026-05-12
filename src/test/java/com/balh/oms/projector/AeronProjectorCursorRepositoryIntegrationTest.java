package com.balh.oms.projector;

import com.balh.oms.AbstractPostgresIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Slice 2a coverage: V24 migration applied, cursor repository upserts and reads cleanly, and the monotonic
 * {@code WHERE last_applied_position &lt; EXCLUDED.last_applied_position} clause is enforced.
 */
class AeronProjectorCursorRepositoryIntegrationTest extends AbstractPostgresIntegrationTest {

    private static final String PROJECTOR_ID = "test-projector";
    private static final int STREAM_ID = 42;

    @Autowired
    AeronProjectorCursorRepository cursorRepository;

    @Autowired
    JdbcTemplate jdbc;

    @BeforeEach
    void cleanCursor() {
        jdbc.update("DELETE FROM aeron_projector_cursor WHERE projector_id = ?", PROJECTOR_ID);
    }

    @Test
    void noPriorRow_returnsEmpty() {
        assertThat(cursorRepository.findLastAppliedPosition(PROJECTOR_ID, STREAM_ID)).isEmpty();
    }

    @Test
    void firstAdvance_inserts() {
        boolean changed = cursorRepository.advance(PROJECTOR_ID, STREAM_ID, 100L);
        assertThat(changed).isTrue();
        OptionalLong pos = cursorRepository.findLastAppliedPosition(PROJECTOR_ID, STREAM_ID);
        assertThat(pos).hasValue(100L);
    }

    @Test
    void monotonicAdvance_updatesForwards_noopForOlder() {
        cursorRepository.advance(PROJECTOR_ID, STREAM_ID, 100L);

        boolean forwards = cursorRepository.advance(PROJECTOR_ID, STREAM_ID, 200L);
        assertThat(forwards).isTrue();
        assertThat(cursorRepository.findLastAppliedPosition(PROJECTOR_ID, STREAM_ID)).hasValue(200L);

        boolean older = cursorRepository.advance(PROJECTOR_ID, STREAM_ID, 150L);
        assertThat(older).isFalse();
        assertThat(cursorRepository.findLastAppliedPosition(PROJECTOR_ID, STREAM_ID)).hasValue(200L);

        boolean equal = cursorRepository.advance(PROJECTOR_ID, STREAM_ID, 200L);
        assertThat(equal).isFalse();
        assertThat(cursorRepository.findLastAppliedPosition(PROJECTOR_ID, STREAM_ID)).hasValue(200L);
    }

    @Test
    void streamsAreIndependent() {
        cursorRepository.advance(PROJECTOR_ID, STREAM_ID, 100L);
        cursorRepository.advance(PROJECTOR_ID, STREAM_ID + 1, 50L);

        assertThat(cursorRepository.findLastAppliedPosition(PROJECTOR_ID, STREAM_ID)).hasValue(100L);
        assertThat(cursorRepository.findLastAppliedPosition(PROJECTOR_ID, STREAM_ID + 1)).hasValue(50L);
    }
}
