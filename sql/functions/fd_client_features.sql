-- THE definition of the history features -- REAL-TIME mode (per row).
-- Block: B3, B4.
-- Lot: B3-L4, B4-L0.
-- Competency: B3-C2, B3-C5, B4-C2.
-- Covers: B3-EE07, B4-EC14.
--
-- Its batch counterpart is sql/backfill/features_window.sql. Both must produce
-- the same values; sql/checks/reconcile_features.sql is what proves it, rather
-- than a shared piece of code -- see the S1 plan, section 2.2 / L4.
--
-- THE SIX RULINGS, to be read side by side with those of the backfill file:
--   1. bound      : STRICT (<). A transaction never counts itself.
--   2. ties       : with a strict bound, two transactions sharing a timestamp
--                   exclude each other. The batch mode must therefore use
--                   RANGE ... EXCLUDE GROUP, not ROWS.
--   3. population : the whole fd_transactions table, all batches together.
--   4. type       : numeric(12,2), rounded to 2 decimals. Never a float.
--   5. NULL       : the average of an empty set is NULL, not 0.
--   6. first txn  : tx_count = 0 and avg_amount = NULL.

-- The pre-fd_ name is dropped first: left in place it would be a second
-- definition of the same rule, callable and silently free to diverge.
DROP FUNCTION IF EXISTS client_features(text, timestamptz);

CREATE OR REPLACE FUNCTION fd_client_features(p_client_key text, p_ts timestamptz)
RETURNS TABLE (tx_count bigint, avg_amount numeric(12,2))
LANGUAGE sql
STABLE
AS $$
    SELECT count(*)::bigint,
           round(avg(amt), 2)::numeric(12,2)
    FROM   fd_transactions
    WHERE  client_key = p_client_key
      AND  txn_ts < p_ts;
$$;
