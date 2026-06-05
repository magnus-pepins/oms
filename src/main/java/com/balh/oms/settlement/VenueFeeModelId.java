package com.balh.oms.settlement;

/** Contract-level charging models (Phase E). */
public enum VenueFeeModelId {
    ZERO,
    TAKER_ONLY,
    SYMMETRIC,
    MAKER_TAKER,
    ALL_IN,
    KALSHI;

    public static VenueFeeModelId parse(String raw) {
        if (raw == null || raw.isBlank()) {
            return ZERO;
        }
        try {
            return VenueFeeModelId.valueOf(raw.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return ZERO;
        }
    }
}
