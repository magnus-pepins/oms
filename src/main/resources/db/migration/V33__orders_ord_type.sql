-- Decouples order type from limit_price presence. Pre-V33, OMS inferred MARKET vs LIMIT
-- purely from "is limit_price NULL?", which made it impossible to express a MARKET order
-- that carries a reference / cap price (used by OMS to size the BUY inflight hold and by
-- FIX egress to emit a non-zero Price tag so the venue / simulator fills at a realistic
-- price). With ord_type now explicit, limit_price's semantic is:
--   ord_type = LIMIT  -> strict limit price (NOT NULL by convention; not enforced at the
--                         column level because legacy rows already conform).
--   ord_type = MARKET -> optional reference / cap price; may be NULL (legacy) or set
--                         (Wed-demo onward, when the BFF passes the live ask × slippage cap).
--
-- Backfill rule: existing rows with limit_price IS NOT NULL were LIMIT orders under the
-- legacy inference (the only writer that set limit_price did so for LIMITs). All others
-- were MARKET. This matches what the codec back-compat decoder does for in-flight Aeron
-- log entries (reserved=0 byte decodes to ORD_TYPE_MARKET).

ALTER TABLE orders ADD COLUMN ord_type text;

UPDATE orders
   SET ord_type = CASE WHEN limit_price IS NULL THEN 'MARKET' ELSE 'LIMIT' END
 WHERE ord_type IS NULL;

ALTER TABLE orders ALTER COLUMN ord_type SET NOT NULL;
ALTER TABLE orders ALTER COLUMN ord_type SET DEFAULT 'MARKET';

-- Light CHECK so a typo'd projector write blows up loudly rather than silently corrupting
-- the column. Keep the values pinned to the same uppercased strings the AcceptOrderCommand
-- ordTypeName(byte) helper returns; adding STOP / STOP_LIMIT in the future requires
-- extending this CHECK in the same Flyway commit.
ALTER TABLE orders
    ADD CONSTRAINT orders_ord_type_chk
    CHECK (ord_type IN ('MARKET', 'LIMIT'));
