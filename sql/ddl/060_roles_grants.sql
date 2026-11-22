-- Roles and append-only.
-- Block: B3, B4.
-- Lot: B3-L0, B4-L0.
-- Competency: B3-C4.
-- Covers: B3-EC19.
--
-- The append-only property of fd_raw_events comes FROM A REVOCATION, not from a
-- convention: the role that ingests has no right to rewrite what it wrote.
-- The DO block keeps the script replayable, like the rest of the DDL.
--
-- fd_ingest and fd_report are created WITHOUT LOGIN: they are group roles and
-- cannot open a connection. The application connects as POSTGRES_USER and then
-- issues SET ROLE fd_ingest. One set of credentials to keep, and the revocation
-- below still bites: after SET ROLE the session loses the superuser attribute
-- and privilege checks apply normally. That is what makes the append-only
-- property DEMONSTRABLE -- an UPDATE on fd_raw_events fails on screen, which is
-- worth more than a promise in a document.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fd_ingest') THEN
        CREATE ROLE fd_ingest;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fd_report') THEN
        CREATE ROLE fd_report;
    END IF;
END
$$;

GRANT SELECT, INSERT
    ON fd_raw_events, fd_quarantine, fd_transactions, fd_ground_truth
    TO fd_ingest;
GRANT SELECT, INSERT, UPDATE ON fd_pipeline_runs TO fd_ingest;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO fd_ingest;

-- UPDATE on the FEATURE COLUMNS ONLY, and this is the point rather than a
-- convenience. The backfill of L4 is an UPDATE over fd_transactions, so the
-- ingesting role needs the privilege -- but granting it on the whole table would
-- also let it rewrite an amount, a timestamp, a client_key or a merchant.
--
-- A column-level grant says exactly what is allowed: the features are derived
-- and may be recomputed; the business facts of a transaction are written once
-- and never again. The same reasoning as the revocation on fd_raw_events above,
-- applied one level finer.
--
-- preflight() checks it the same way: it tries to update a business column and
-- requires the attempt to fail.
GRANT UPDATE (f_tx_count, f_avg_amount, f_home_distance_km)
    ON fd_transactions TO fd_ingest;

REVOKE UPDATE, DELETE, TRUNCATE ON fd_raw_events FROM fd_ingest;

GRANT SELECT
    ON fd_transactions, fd_ground_truth, fd_quarantine, fd_pipeline_runs
    TO fd_report;
