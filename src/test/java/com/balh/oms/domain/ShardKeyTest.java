package com.balh.oms.domain;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pure unit test — no Docker / Testcontainers. Keeps {@code ./gradlew test}
 * meaningful on machines where integration tests are skipped.
 */
class ShardKeyTest {

    @Test
    void shardIsStableForSameAccount() {
        UUID id = UUID.fromString("11111111-1111-1111-1111-111111111111");
        assertThat(ShardKey.shardFor(id, 4)).isEqualTo(ShardKey.shardFor(id, 4));
    }

    @Test
    void shardFitsRange() {
        UUID id = UUID.randomUUID();
        int shards = 8;
        int s = ShardKey.shardFor(id, shards);
        assertThat(s).isGreaterThanOrEqualTo(0).isLessThan(shards);
    }

    @Test
    void invalidShardCountThrows() {
        assertThatThrownBy(() -> ShardKey.shardFor(UUID.randomUUID(), 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
