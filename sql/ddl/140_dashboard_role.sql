-- The role the dashboard logs in as. READS WHAT ITS PAGES SHOW, NOTHING ELSE.
-- Block: B3, B4.
-- Lot: B3-R0, B4-L0.
-- Competency: B3-C2, B3-C5.
-- Covers: B3-EC19.
--
-- THE ONLY COMPONENT OF THE PROJECT OPEN TO A BROWSER, and the design follows
-- from that: it must be UNABLE to write, to see the raw zone, or to learn the
-- stack's secrets -- not merely refrain from it.
--
-- WHY NOT A MEMBER OF fd_report. fd_report PRODUCES the morning report and
-- holds INSERT and UPDATE on fd_reports (110_reports.sql). A dashboard that
-- inherited it could rewrite the journal of the reports, and with it the
-- digest that proves a document was not altered. So fd_dashboard belongs to no
-- role and inherits nothing: it receives SELECT on exactly what its two pages
-- read.
--
-- WHY LOGIN, unlike every other role here. Airflow connects as the OWNER and
-- then issues SET ROLE: a real demotion, but a consented one -- RESET ROLE
-- undoes it. Acceptable in a closed container; not for the one a browser
-- talks to, which would then hold the owner's password. The dashboard
-- container receives fd_dashboard's credentials and never the owner's.
--
-- NO PASSWORD IN THIS FILE. A versioned file is not a place for a secret.
-- The role is created without one -- so it CANNOT log in -- and
-- sql/roles/dashboard_password.sql sets it from FD_DASHBOARD_PASSWORD in .env,
-- which `fd-manager setup` generates. A dashboard that will not start is
-- better than a dashboard that is open.
--
-- Replayable: `init` plays every file of sql/ddl on an existing database.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fd_dashboard') THEN
        CREATE ROLE fd_dashboard LOGIN;
    END IF;
END $$;

-- Stated in full on every replay, so a hand-made change to the role does not
-- survive the next `init`. NOINHERIT is belt and braces: it is member of
-- nothing, and if it ever becomes one, it still inherits nothing.
ALTER ROLE fd_dashboard LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
                        NOINHERIT NOREPLICATION NOBYPASSRLS
                        CONNECTION LIMIT 10;

-- First, NOTHING: every replay of `init` starts from closed tables, so a grant
-- added by hand does not survive it. The REVOKE also makes the intent part of
-- the schema, so the acceptance of S6 (s6_acceptance.sql) has something to read
-- rather than an absence to infer.
REVOKE ALL ON fd_raw_events, fd_quarantine, fd_ground_truth, fd_scores,
              fd_datasets, fd_purges
       FROM fd_dashboard;

-- What the pages read, and nothing else.
--   fd_reports       : the morning reports, their counts and digests (R2)
--   fd_frauds        : the alerts, their score, threshold, model, notification (R3)
--   fd_transactions  : amount, merchant, category and time of an alert (R3);
--                      the stored lines of the volume invariant (R6)
--   fd_pipeline_runs : the runs the four controls are computed from (R6) --
--                      identifiers, dates, counters, fingerprints; no personal
--                      field (governance.yml, retention.kept)
--   fd_ops_state     : the recorded state of each control, and since when (R6)
--   fd_ops_notifications : the messages sent to the operations channel --
--                      control names, states and counts (R9)
--   fd_ground_truth  : a verdict per txn_id and its date; no personal field
--                      (governance.yml, retention.kept) -- the performance (R10)
--   fd_scores        : score, threshold and decision per model version (R10)
GRANT SELECT ON fd_reports, fd_frauds, fd_transactions, fd_pipeline_runs,
                fd_ops_state, fd_ops_notifications, fd_ground_truth, fd_scores
      TO fd_dashboard;

-- THE QUARANTINE, BY COLUMN.
--   batch_id, line_no         : the volume invariant (R6, pipeline_health.sql)
--   reason_class, reason_code : the breakdown by reason, F07.3 (R7)
--   rejected_at               : the calendar day of a rejection (R7)
-- NEVER `payload`, the raw record of a rejected line -- the same personal
-- data as the raw zone -- and NEVER `reason_detail`, which quotes a raw value:
-- for a duplicate it IS the trans_num (ml4fd/checks.py). The REVOKE ALL above
-- runs first on every replay, so these columns are exactly what remains.
GRANT SELECT (batch_id, line_no, reason_class, reason_code, rejected_at)
      ON fd_quarantine TO fd_dashboard;

-- THE CATALOGUE BY COLUMN: the recipe's identity, its base rate and the
-- figures M5 measured (070_datasets.sql, R10). Not the paths of the Parquet
-- files nor the feature list: the dashboard opens no dataset and needs neither.
GRANT SELECT (recipe_digest, version, created_at, base_rate_test,
              test_recall, test_precision, test_alerts_per_day,
              test_threshold, measured_at)
      ON fd_datasets TO fd_dashboard;

REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
       ON fd_reports, fd_frauds, fd_transactions, fd_pipeline_runs, fd_ops_state,
          fd_ops_notifications, fd_ground_truth, fd_scores
       FROM fd_dashboard;
