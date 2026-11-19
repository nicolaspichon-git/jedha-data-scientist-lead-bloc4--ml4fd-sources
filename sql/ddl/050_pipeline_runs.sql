-- Run journal, queryable.
-- Covers: EC12   Lot: L7   Competency: C4
--
-- The counter-closure constraint puts the L2 acceptance criterion in the
-- database: n_read = n_valid + n_rejected stops being a test to remember and
-- becomes a structural impossibility.

CREATE TABLE IF NOT EXISTS fd_pipeline_runs (
    run_id     text        PRIMARY KEY,
    dag_id     text        NOT NULL,
    batch_id   uuid,
    started_at timestamptz NOT NULL,
    ended_at   timestamptz,
    duration_s numeric(12,3),
    state      text        NOT NULL CHECK (state IN ('running', 'success', 'failed')),
    n_read     bigint,
    n_valid    bigint,
    n_rejected bigint,
    digests    jsonb CHECK (digests IS NULL OR jsonb_typeof(digests) = 'object'),
    CONSTRAINT fd_pipeline_runs_counters_close
        CHECK (n_read IS NULL
               OR n_read = coalesce(n_valid, 0) + coalesce(n_rejected, 0))
);

-- S5/Q1: the fingerprint of what a run wrote, STORED. Before this column the
-- digests were only written to the Airflow log, which is neither backed up nor
-- protected, and which rotates (retention.airflow_logs_days). The day S5 purges
-- the raw zone and the logs turn over, nothing would say what a batch held.
--
--   {"raw_batch": {"batch_id": ..., "rows": N, "digest": md5, "origin": ...},
--    "stores":    {"fd_transactions": {"rows": N, "digest": md5}, ...}}
--
-- raw_batch is taken by EVERY writer of the raw zone, right after it writes,
-- and the purge refuses a batch that has none (ml4fd/runs.py, refusal()).
-- stores is the digest of the exploited layer, taken by the historical load.
--
-- Declared twice on purpose, like f_home_distance_km: once in CREATE TABLE for
-- a fresh database, once here for an existing one. Additive, no reset needed.
ALTER TABLE fd_pipeline_runs
    ADD COLUMN IF NOT EXISTS digests jsonb
    CHECK (digests IS NULL OR jsonb_typeof(digests) = 'object');
