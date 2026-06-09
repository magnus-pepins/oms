package com.balh.oms.events;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * When {@value #ENV_ALLOWLIST} is set (comma-separated account UUIDs), domain events for other
 * accounts are not written to {@code domain_event_outbox} — bench load tests stop flooding
 * {@code OMS_EVENTS} while retail customers in the allowlist still fan out to NATS.
 */
public final class OmsDomainFanoutAccountFilter {

    public static final String ENV_ALLOWLIST = "OMS_DOMAIN_FANOUT_ACCOUNT_ALLOWLIST";

    private static final Set<UUID> ALLOWLIST = parseAllowlist();

    private OmsDomainFanoutAccountFilter() {}

    /**
     * @return {@code true} when the account's lifecycle events should be enqueued for NATS fanout.
     *         When the allowlist env is unset/empty, every account is published (production default).
     */
    public static boolean shouldPublishForAccount(UUID accountId) {
        if (ALLOWLIST == null) {
            return true;
        }
        return accountId != null && ALLOWLIST.contains(accountId);
    }

    /** Visible for tests. */
    static Set<UUID> allowlistForTest() {
        return ALLOWLIST;
    }

    private static Set<UUID> parseAllowlist() {
        String raw = System.getenv(ENV_ALLOWLIST);
        if (raw == null || raw.isBlank()) {
            return null;
        }
        Set<UUID> out = new HashSet<>();
        for (String part : raw.split(",")) {
            String trimmed = part.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            out.add(UUID.fromString(trimmed));
        }
        if (out.isEmpty()) {
            return null;
        }
        return Collections.unmodifiableSet(out);
    }
}
