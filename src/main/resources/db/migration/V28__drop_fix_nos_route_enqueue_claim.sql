-- Phase 3 slice 3g (system-documentation/plans/oms-aeron-cluster-substrate.md):
-- drop fix_nos_route_enqueue_claim alongside the legacy ControlTailer / FixOutboundDispatchWorker
-- deletion. The cluster + oms-fix-egress path uses the Aeron-cursor (oms_fix_egress_cursor, V26)
-- to dedupe NOS sends; the per-(order_id, route_key) row in fix_nos_route_enqueue_claim that
-- ControlTailer used as its enqueue dedupe is no longer written or read.
--
-- No FK references this table (V8 introduced fix_route_state separately, V23 introduced this
-- table standalone). Safe to drop unconditionally.

DROP TABLE IF EXISTS fix_nos_route_enqueue_claim;
