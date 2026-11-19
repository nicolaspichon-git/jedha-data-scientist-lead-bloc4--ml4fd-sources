-- Ground truth. The verdict and the date it became KNOWN.
-- Covers: EI01, EI02   Lot: L5   Competency: C2
--
-- txn_ts is copied from fd_transactions. The redundancy is deliberate: a CHECK
-- constraint cannot reference another table, so without the copy "label_ts is
-- later than txn_ts" would only be guaranteed by code. It also spares the join
-- to every consumer, since they all filter on label_ts <= cut-off.

CREATE TABLE IF NOT EXISTS fd_ground_truth (
    txn_id   text        PRIMARY KEY REFERENCES fd_transactions (txn_id),
    txn_ts   timestamptz NOT NULL,
    label    boolean     NOT NULL,
    label_ts timestamptz NOT NULL,
    source   text        NOT NULL,
    CONSTRAINT fd_ground_truth_label_after_txn CHECK (label_ts >= txn_ts)
);

CREATE INDEX IF NOT EXISTS fd_ground_truth_label_ts_idx
    ON fd_ground_truth (label_ts);
