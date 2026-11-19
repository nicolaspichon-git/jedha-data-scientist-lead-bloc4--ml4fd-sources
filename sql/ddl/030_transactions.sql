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
