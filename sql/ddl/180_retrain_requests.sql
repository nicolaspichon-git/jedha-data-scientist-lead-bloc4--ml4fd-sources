-- The retraining requests the monitoring made.
-- Block: B4.
-- Lot: B4-L4 (ajoute le 20/11/2026).
-- Competency: B4-C4, B4-C5.
-- Covers: B4-EC33, B4-EC37.
--
-- WHY THIS TABLE EXISTS, measured rather than foreseen. On 2026-11-20 at 09:34
-- UTC the monitoring saw an injected drift, raised input_drift into alert, and
-- asked for a retraining -- and the run that was to ask for it failed on the
-- next task. The TRANSITION had already been committed by the previous task,
-- so every later run measured the same alert, found no transition, and
-- requested nothing. One crash, and the request was lost for good.
--
-- TRIGGER, THEN RECORD -- the same order and the same reason as fd_frauds and
-- fd_ops_state, which sql/ddl/100_ops_state.sql states for the channel: the
-- record is written only after the thing it records has happened, so a crash
-- between the two repeats one request on the next run. At-least-once, as
-- governance.yml declares for the alerts. The old arrangement was the reverse
-- for the trigger alone: the state was committed first and the request fired
-- afterwards, so a crash lost it silently. This table is what lets the
-- decision read "has this alert been answered?" instead of "did a transition
-- happen in the run I am part of?".
--
-- AND fd_ops_state COULD NOT HOLD IT. Its state column is CHECK (state IN
-- ('ok', 'alert')): a request is neither. It is an event, and the events of
-- this project are append-only tables -- fd_promotions, fd_monitoring, this.
--
-- IT ALSO ANSWERS A QUESTION THE DOSSIER ASKS. airflow/dags/dag_monitor.py
-- wants "did the monitoring ask for a retraining on the 12th" to be answerable
-- from one screen. Airflow's own dag_run table would answer it, but it has
-- lived in a database of its own since B4-L1 and cannot be read from here.

CREATE TABLE IF NOT EXISTS fd_retrain_requests (
    request_id   bigserial   PRIMARY KEY,
    requested_at timestamptz NOT NULL DEFAULT now(),
    -- The monitoring run that asked, so the request and the measurements that
    -- justified it can be put side by side in fd_monitoring.
    run_id       text,
    -- Which controls were in alert, comma-separated, and the sentence the
    -- policy produced. The reason is kept because a request nobody can explain
    -- months later is a request nobody can defend.
    controls     text        NOT NULL,
    reason       text        NOT NULL,

    CONSTRAINT fd_retrain_requests_reason_is_not_empty
        CHECK (length(btrim(reason)) > 0),
    CONSTRAINT fd_retrain_requests_controls_is_not_empty
        CHECK (length(btrim(controls)) > 0)
);

-- The index the decision reads: the latest request, and nothing else.
CREATE INDEX IF NOT EXISTS fd_retrain_requests_requested_at_idx
    ON fd_retrain_requests (requested_at DESC);

GRANT SELECT, INSERT ON fd_retrain_requests TO fd_ingest;
GRANT USAGE, SELECT ON SEQUENCE fd_retrain_requests_request_id_seq TO fd_ingest;
GRANT SELECT ON fd_retrain_requests TO fd_report;
GRANT SELECT ON fd_retrain_requests TO fd_dashboard;
REVOKE UPDATE, DELETE, TRUNCATE ON fd_retrain_requests FROM fd_ingest;
