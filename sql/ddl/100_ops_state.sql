-- The state of each pipeline control, so an incident is announced ONCE.
-- Block: B3, B4.
-- Lot: B3-N9, B4-L0.
-- Competency: B3-C2, B3-C4.
-- Covers: B3-EC14.
--
-- WITHOUT THIS TABLE THE OPERATIONS CHANNEL IS UNUSABLE, and the arithmetic
-- says it plainly: the real-time DAG runs 1,440 times a day, so a freshness
-- control that goes red stays red every minute until somebody acts. Notifying
-- on the CONDITION would send 1,440 identical messages a day; notifying on the
-- TRANSITION sends one when it breaks and one when it recovers.
--
-- The capacity policy of N5 does not help here and was never meant to: it
-- caps the VOLUME of a day's fraud alerts to protect an analyst's attention.
-- Repetition of one message is a different failure and needs a different
-- remedy -- this one.
--
-- THE RETURN TO GREEN IS NOTIFIED TOO. An operator told that ingestion stopped
-- and never told it resumed has to go and look, which is the pull model the
-- channel exists to replace.
--
-- A CONTROL ABSENT FROM THIS TABLE COUNTS AS 'ok'. That is what makes the
-- first run of a healthy pipeline silent: nothing has transitioned. The table
-- therefore holds only the controls that have been in alert at least once,
-- which is also why 1,440 runs a day write to it almost never.

CREATE TABLE IF NOT EXISTS fd_ops_state (
    control     text        PRIMARY KEY,
    state       text        NOT NULL CHECK (state IN ('ok', 'alert')),
    since       timestamptz NOT NULL DEFAULT now(),
    detail      text        NOT NULL,
    -- The run that observed the transition, so the journal and the message can
    -- be put side by side.
    run_id      text,
    notified_at timestamptz NOT NULL DEFAULT now()
);

-- SEND, THEN RECORD -- the same order, and the same reason, as fd_frauds. The
-- state is written only after the message left: a crash between the two repeats
-- one message on the next run, which is at-least-once as declared in
-- governance.yml. The reverse order would mark the control as announced while
-- nobody was told, and no later run would ever announce it again, because there
-- would no longer be a transition to see. That failure is silent and permanent;
-- a duplicate is neither.
GRANT SELECT, INSERT, UPDATE ON fd_ops_state TO fd_ingest;
GRANT SELECT ON fd_ops_state TO fd_report;
REVOKE DELETE, TRUNCATE ON fd_ops_state FROM fd_ingest;
