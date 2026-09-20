-- Event production timestamps on the product projection, enabling exact
-- event-to-database latency measurement over recently materialized
-- products.
ALTER TABLE product_projection ADD COLUMN IF NOT EXISTS produced_at TIMESTAMPTZ;

-- Backfill from the source events is not reconstructible after the fact;
-- events projected from this migration onward carry the timestamp.