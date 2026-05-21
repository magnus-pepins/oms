package com.balh.oms.fx;

/**
 * Hook called by {@link FxTierKillsService} after a successful local
 * write (create / approve / revoke) so any out-of-JVM listeners can
 * invalidate their copy of the tier-kill cache and converge on the new
 * state without waiting for the next {@code @Scheduled} refresh tick.
 *
 * <p>Production: implemented by {@link FxTierKillsNatsInvalidationBus}
 * which fans the action out over NATS core pub/sub so every JVM hosting
 * {@code FxTierKillsService} (the ingress that serves the write API +
 * the projector that runs the publisher; plus any future processes)
 * calls {@code refreshNow()} sub-second after the write.
 *
 * <p>Without this, a kill written via the ingress applies on the
 * ingress's own cache immediately, but the projector keeps publishing
 * the killed (pair, tier) until its next scheduled refresh — up to a
 * minute of "the kill button did nothing visible" from the operator's
 * point of view.
 *
 * <p>Default {@link #NOOP} is used in single-JVM dev / tests where the
 * existing local {@code refreshNow()} call is sufficient.
 *
 * <p>Sibling of {@link OverridesChangePropagator}; kept separate so the
 * two domain entities can evolve their action vocabulary independently
 * (overrides have create/approve/revoke; kills do too today but a
 * future "force-extend" or "early-expire" naturally lands on this
 * interface rather than the override one).
 */
@FunctionalInterface
public interface TierKillsChangePropagator {

    TierKillsChangePropagator NOOP = (action, id) -> {};

    /**
     * Called after a successful local write. Implementations should be
     * non-blocking — the controller has already responded to the
     * operator. A best-effort publish failure is acceptable because the
     * scheduled refresh remains a safety net.
     *
     * @param action {@code "create"}, {@code "approve"}, or {@code "revoke"}
     * @param id     row id of the affected {@code fx_pair_tier_kills} row
     */
    void localChanged(String action, long id);
}
