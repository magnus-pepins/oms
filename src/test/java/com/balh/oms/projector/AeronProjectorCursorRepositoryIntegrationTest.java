package com.balh.oms.projector;

import com.balh.oms.AbstractPostgresIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
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

    @Test
    void findLastAppliedAt_emptyBeforeFirstAdvance_thenTracksAdvance() {
        assertThat(cursorRepository.findLastAppliedAt(PROJECTOR_ID, STREAM_ID)).isEmpty();

        Instant before = Instant.now();
        cursorRepository.advance(PROJECTOR_ID, STREAM_ID, 100L);
        Instant after = Instant.now();

        Optional<Instant> ts = cursorRepository.findLastAppliedAt(PROJECTOR_ID, STREAM_ID);
        assertThat(ts).isPresent();
        assertThat(ts.get()).isBetween(before.minus(Duration.ofSeconds(2)), after.plus(Duration.ofSeconds(2)));
    }

    @Test
    void findLastAppliedAt_advancesOnUpsert_butNotOnNoopOlderWrite() throws InterruptedException {
        cursorRepository.advance(PROJECTOR_ID, STREAM_ID, 200L);
        Optional<Instant> firstTs = cursorRepository.findLastAppliedAt(PROJECTOR_ID, STREAM_ID);
        assertThat(firstTs).isPresent();

        Thread.sleep(50);

        cursorRepository.advance(PROJECTOR_ID, STREAM_ID, 100L);
        Optional<Instant> staleTs = cursorRepository.findLastAppliedAt(PROJECTOR_ID, STREAM_ID);
        assertThat(staleTs).contains(firstTs.get());

        Thread.sleep(50);

        cursorRepository.advance(PROJECTOR_ID, STREAM_ID, 300L);
        Optional<Instant> bumpedTs = cursorRepository.findLastAppliedAt(PROJECTOR_ID, STREAM_ID);
        assertThat(bumpedTs).isPresent();
        assertThat(bumpedTs.get()).isAfter(firstTs.get());
    }
}
