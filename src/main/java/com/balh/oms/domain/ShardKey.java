package com.balh.oms.domain;

import net.openhft.hashing.LongHashFunction;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Maps an {@code account_id} to a shard. Slice 1 runs with one shard, but all
 * write paths still go through the shard mapping so we can grow without
 * touching call sites.
 *
 * <p>Hash function is {@code xxh64} (decision recorded in
 * {@code oms/docs/decisions.md}). Modulo over {@code shardCount} gives the
 * shard id.
 */
public final class ShardKey {

    private static final LongHashFunction XX64 = LongHashFunction.xx();

    private ShardKey() {}

    public static int shardFor(UUID accountId, int shardCount) {
        if (shardCount <= 0) {
            throw new IllegalArgumentException("shardCount must be positive");
        }
        long h = XX64.hashBytes(accountId.toString().getBytes(StandardCharsets.UTF_8));
        int mod = (int) Math.floorMod(h, (long) shardCount);
        return mod;
    }
}
