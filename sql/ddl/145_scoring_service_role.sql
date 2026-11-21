-- The role of the scoring service. Covers: EC16, EC22, EC17
-- Lot: L5 (ajoute le 20/11/2026)   Competency: C3, C4
--
-- WHY 145 AND NOT 185, measured on the first real call, 2026-11-20 22:46:
-- "permission denied for database -- User does not have CONNECT privilege".
-- 150_database_connect.sql is the one place that says who may connect, and it
-- grants CONNECT under IF EXISTS (... rolname = 'fd_scoring_service') -- a
-- guard that keeps that file playable on a database built before this one
-- existed. The DDL loop applies files in NAME order, so at 185 the role was
-- created AFTER 150 had already looked for it: the guard skipped, the grant
-- never happened, and the service resolved its champion and then could not
-- open a connection. 150's own header states the rule -- "naming everyone who
-- goes through it, in this file, after 140 has created the role, which the
-- numbering guarantees" -- and 185 inverted that guarantee. 145 restores it.
--
-- LOGIN, LIKE fd_dashboard AND FOR ITS REASON. fd_purge and fd_report have no
-- login: the application connects as the owner and issues SET ROLE, which is
-- enough for a process the owner starts. The two containers a network talks to
-- are the opposite case -- whoever reaches them must not be one SET ROLE away
-- from the owner's privileges. So the service receives credentials of its own,
-- from the environment, and the owner's never enter its container.
--
-- WHAT IT MAY DO, and the list is short because the decoupling L5 is named for
-- is a matter of privilege rather than of intention:
--   * READ three columns of fd_transactions. The history features are defined
--     over that table (sql/functions/fd_client_features.sql, ruling 3) and the
--     service computes them per call, so it must read it. client_key, txn_ts
--     and amt are what the function touches -- count(*) is allowed by a
--     column-level grant on any one of them.
--   * WRITE fd_scoring_service_calls, and only append (190_scoring_service_calls.sql revokes the rest).
--
-- WHAT IT MAY NOT DO, and this is the part that matters:
--   * it cannot INSERT, UPDATE or DELETE in fd_transactions. The service
--     SCORES; it does not ingest. A payment asked about is not a payment
--     received, and the two paths stay apart because the privilege does, not
--     because the code happens to.
--   * it cannot read fd_raw_events or fd_quarantine, which hold the raw record
--     of a payment -- the personal data the exploited layer was built to be
--     free of (EC17). A scoring service has no business there.
--   * it cannot read fd_ground_truth. A service that could see the verdicts
--     could be asked whether it was right, and that question belongs to the
--     monitoring, over a window, not to a caller over one payment.
--   * it cannot read fd_datasets or open a Parquet file: it loads its model
--     through MLflow, by the champion alias, like every other reader (L3).

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fd_scoring_service') THEN
        CREATE ROLE fd_scoring_service LOGIN;
    END IF;
END
$$;

-- Restated on every replay, so a privilege added by hand does not survive an
-- `init` -- the same reasoning as 140_dashboard_role.sql.
ALTER ROLE fd_scoring_service LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
                  NOINHERIT NOREPLICATION NOBYPASSRLS
                  CONNECTION LIMIT 20;

-- First, NOTHING.
REVOKE ALL ON fd_raw_events, fd_quarantine, fd_ground_truth, fd_scores,
              fd_datasets, fd_transactions, fd_frauds, fd_reports,
              fd_pipeline_runs
       FROM fd_scoring_service;
-- fd_monitoring IS NOT IN THAT LIST, and the reason is the numbering: it is
-- created by 170_monitoring.sql, after this file runs, so naming it here would
-- fail with "relation does not exist" on every fresh database. Its revocation
-- lives in 190_scoring_service_calls.sql, which runs after 170.

-- THE THREE COLUMNS THE FEATURE FUNCTION TOUCHES, and no more. The function is
-- LANGUAGE sql and STABLE, not SECURITY DEFINER, so it runs with the caller's
-- privileges: granting EXECUTE would not be enough, and granting the whole
-- table would be more than it reads.
GRANT SELECT (client_key, txn_ts, amt) ON fd_transactions TO fd_scoring_service;
