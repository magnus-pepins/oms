package com.balh.oms.fx;

/**
 * Hook called by {@link FxMarkupOverridesService} after a successful local
 * write (create / approve / revoke) so any out-of-JVM listeners can
 * invalidate their copy of the override cache and converge on the new
 * state without waiting for the next {@code @Scheduled} refresh tick.
 *
 * <p>Production: implemented by {@link FxMarkupOverridesNatsInvalidationBus}
 * which fans the action out over NATS core pub/sub so every JVM hosting
 * {@code FxMarkupOverridesService} (ingress + projector + any future
 * processes) calls {@code refreshNow()} sub-second after the write.
 *
 * <p>Without this, the ingress (which serves {@code /fx/quote} and the
 * write API) sees a new override immediately because the controller calls
 * {@code refreshNow()} on its own JVM, but the projector — which publishes
 * the customer-tier MQTT stream — only picks the row up on its next
 * scheduled refresh, default 60s. During that window customer UIs render
 * the pre-override rate while the BFF mints at the post-override rate;
 * if the spread widened by more than the drift threshold (5bps default)
 * every cross-currency order rejects with {@code RATE_MOVED} until the
 * projector catches up. The reverse problem applies to revoke + expire.
 *
 * <p>Default {@link #NOOP} is used in single-JVM dev / tests where the
 * existing local {@code refreshNow()} call is sufficient.
 *
 * <p>P5 of {@code plans/fx-tier-quotes-production.md}.
 */
@FunctionalInterface
public interface OverridesChangePropagator {

    OverridesChangePropagator NOOP = (action, id) -> {};

    /**
     * Called after a successful local write. Implementations should be
     * non-blocking — the controller has already responded to the
     * operator. A best-effort publish failure is acceptable because the
     * scheduled refresh remains a safety net.
     *
     * @param action {@code "create"}, {@code "approve"}, or {@code "revoke"}
     * @param id     row id of the affected {@code fx_pair_markup_overrides} row
     */
    void localChanged(String action, long id);
}
