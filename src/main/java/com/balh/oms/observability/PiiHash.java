package com.balh.oms.observability;

import com.balh.oms.config.OmsConfig;
import net.openhft.hashing.LongHashFunction;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Hashes PII identifiers (account_id) for safe inclusion in metrics labels and
 * structured logs.
 *
 * <p>Policy (mirrors plan §14.14 and {@code oms/docs/pii-policy.md}):
 * <ul>
 *   <li>Default everywhere: hashed account_id only.</li>
 *   <li>Raw account_id may appear only in audit-grade trace logs gated behind
 *       {@code oms.pii.audit-trace-enabled}.</li>
 *   <li>{@code account_id} must NEVER appear as a Micrometer label.</li>
 * </ul>
 */
@Component
public class PiiHash {

    private final LongHashFunction xx;

    public PiiHash(OmsConfig config) {
        long seed = LongHashFunction.xx()
                .hashBytes(config.getPii().getHashSecret().getBytes(StandardCharsets.UTF_8));
        this.xx = LongHashFunction.xx(seed);
    }

    public String hash(UUID accountId) {
        long h = xx.hashBytes(accountId.toString().getBytes(StandardCharsets.UTF_8));
        return Long.toUnsignedString(h, 16);
    }
}
