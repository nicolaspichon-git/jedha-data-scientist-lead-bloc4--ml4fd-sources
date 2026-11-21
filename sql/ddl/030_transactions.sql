-- Exploited layer: typed, pseudonymised transactions with their features.
-- Covers: EE08, EI03, EC17   Lot: L3, L4   Competency: C2
--
-- No label column: dissociating the verdict is structural, not a convention
-- (F02.3). No identifying column either: identity data is not copied here
-- (EC17).
--
-- amt is numeric(12,2), never a float: a monetary total has to reconcile to the
-- cent from one store to the next.

CREATE TABLE IF NOT EXISTS fd_transactions (
    txn_id       text          PRIMARY KEY,
    txn_ts       timestamptz   NOT NULL,
    client_key   text          NOT NULL,          -- HMAC(pepper, cc_num)
    merchant     text          NOT NULL,
    category     text          NOT NULL,
    amt          numeric(12,2) NOT NULL CHECK (amt >= 0),
    merch_lat    numeric(9,6),
    merch_long   numeric(9,6),
    -- Features: computed at ingestion in real time, backfilled in batch here.
    f_tx_count   bigint,
    f_avg_amount numeric(12,2),
    -- Minimisation rather than removal, which EC17 allows. lat/long are the
    -- cardholder's home and are constant for each of the 924 cardholders, so
    -- keeping them would keep the home address. Only the distance between home
    -- and merchant is stored: a radius around a merchant, not a point, and the
    -- feature that carries the signal survives.
    --
    -- Unlike the two above, this one is a pure row function with no history, so
    -- it takes no part in L4's two-mode reconciliation.
    f_home_distance_km numeric(8,2),
    batch_id     uuid          NOT NULL,
    ingested_at  timestamptz   NOT NULL DEFAULT now()
);

-- Additive migration, so that init upgrades a database already holding data
-- instead of requiring a reset. The column is declared twice on purpose: once
-- above for a fresh database, once here for an existing one.
ALTER TABLE fd_transactions
    ADD COLUMN IF NOT EXISTS f_home_distance_km numeric(8,2);

-- Required by the features: without it both the backfill and the real-time path
-- scan the whole table per cardholder.
CREATE INDEX IF NOT EXISTS fd_transactions_client_ts_idx
    ON fd_transactions (client_key, txn_ts);
CREATE INDEX IF NOT EXISTS fd_transactions_ts_idx
    ON fd_transactions (txn_ts);

-- THE BATCH, added 21/11/2026 after measurement. fd_transactions is written and
-- read BY BATCH -- sql/scoring/rows_to_score.sql and sql/features/apply_batch.sql
-- both filter on batch_id -- and PostgreSQL does not index a foreign key on its
-- own. Without this index every dag_ingest run scanned the whole table twice to
-- find the five rows of its own batch.
--
-- MEASURED on PostgreSQL 16 with 1,300,400 rows: rows_to_score went from 60 ms
-- of parallel sequential scan to 0.07 ms of index scan, a factor of 850, for
-- 9 MB of index. Cached, on a machine with no other load; on a container disk
-- the sequential side is worse, not better.
--
-- AND IT IS WHAT MADE `fd-manager accept` TAKE TWO MINUTES. s4_acceptance.sql
-- asks which transactions came from the stream; with no index on batch_id the
-- planner chose a nested loop and rescanned the 1,300,406 rows ONCE PER BATCH
-- -- 748 times, 972,699,957 rows rejected by the join filter, 161 seconds. The
-- same plan with this index: 1.6 seconds. The check was never the problem.
CREATE INDEX IF NOT EXISTS fd_transactions_batch_idx
    ON fd_transactions (batch_id);
