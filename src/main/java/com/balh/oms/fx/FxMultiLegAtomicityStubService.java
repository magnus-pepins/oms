package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * M3 Track 3: two-leg insert in one transaction (rollback verified in integration tests).
 */
@Service
public class FxMultiLegAtomicityStubService {

    private final JdbcTemplate jdbcTemplate;
    private final OmsConfig omsConfig;

    public FxMultiLegAtomicityStubService(JdbcTemplate jdbcTemplate, OmsConfig omsConfig) {
        this.jdbcTemplate = jdbcTemplate;
        this.omsConfig = omsConfig;
    }

    @Transactional
    public UUID createTwoLegStub(List<String> currencies) {
        requireEnabled();
        return insertLegPair(currencies);
    }

    /** Inserts then throws — entire transaction must roll back. */
    @Transactional
    public void createTwoLegStubThenFail(List<String> currencies) {
        requireEnabled();
        insertLegPair(currencies);
        throw new IllegalStateException("fx_stub_rollback_probe");
    }

    public int countLegGroups() {
        Integer n = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM fx_stub_leg_group", Integer.class);
        return n == null ? 0 : n;
    }

    private UUID insertLegPair(List<String> currencies) {
        if (currencies.size() != 2) {
            throw new IllegalArgumentException("fx_stub_requires_two_ccy");
        }
        UUID gid = UUID.randomUUID();
        jdbcTemplate.update("INSERT INTO fx_stub_leg_group (id) VALUES (?)", gid);
        for (int i = 0; i < currencies.size(); i++) {
            jdbcTemplate.update(
                    "INSERT INTO fx_stub_leg (group_id, leg_index, ccy) VALUES (?,?,?)",
                    gid,
                    i,
                    currencies.get(i));
        }
        return gid;
    }

    private void requireEnabled() {
        var fx = omsConfig.getFx();
        if (!fx.isModuleEnabled() || !fx.isMultiLegAtomicityStubEnabled()) {
            throw new IllegalStateException("fx_multi_leg_stub_disabled");
        }
    }
}
