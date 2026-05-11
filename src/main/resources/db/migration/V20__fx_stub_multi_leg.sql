-- M3 Track 3 stub: two-leg insert in one DB transaction (rollback tested in IT).
CREATE TABLE fx_stub_leg_group (
    id UUID PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE fx_stub_leg (
    group_id UUID NOT NULL REFERENCES fx_stub_leg_group (id) ON DELETE CASCADE,
    leg_index SMALLINT NOT NULL,
    ccy VARCHAR(8) NOT NULL,
    PRIMARY KEY (group_id, leg_index)
);
