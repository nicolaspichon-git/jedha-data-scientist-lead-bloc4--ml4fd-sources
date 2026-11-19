-- The journal of the operations channel: one row per message SENT.
-- Covers: EC14, EC17   Lot: R9 (S6)   Competency: C4
--
-- WHY A JOURNAL NEXT TO fd_ops_state. fd_ops_state is a STATE, one row per
-- control, overwritten at each transition: it says where a control stands and
-- since when, and forgets every incident before the last. The channel's own
-- destination -- the service journal or the ops file -- does keep the history,
-- but outside the database, where the dashboard cannot read it and where the
-- file channel and the log channel would each need their own reader. This
-- table holds what was SENT, whatever the channel, where a role that reads can
-- read it.
--
-- ONE ROW PER TRANSITION, NOT PER RUN. The real-time DAG runs 1,440 times a
-- day; a control that goes red and recovers writes two rows. A quiet week
-- writes none. That is why it is not purged: governance.yml, retention.kept.
--
-- WRITTEN AFTER THE SEND, in the same transaction as fd_ops_state (ml4fd/
-- health.py, notify_ops): a row here is a message that left. A crash between
-- the send and the commit re-sends on the next run and journals once, which is
-- at-least-once as governance.yml declares it.
--
-- WHAT IT HOLDS is what the payload holds, and the payload is checked field by
-- field before it leaves (health.payload_of: OPS_FIELDS, FORBIDDEN_FIELDS,
-- facts that are numbers). Counts and control names; no transaction, no
-- client, no raw value.
--
-- APPEND-ONLY: the role that writes it may insert and read, nothing else. A
-- journal that its writer could edit would prove nothing.

CREATE TABLE IF NOT EXISTS fd_ops_notifications (
    notification_id bigserial   PRIMARY KEY,
    sent_at         timestamptz NOT NULL DEFAULT now(),
    control         text        NOT NULL,
    state           text        NOT NULL CHECK (state IN ('ok', 'alert')),
    detail          text        NOT NULL,
    -- The channel the message went to, as governance.yml named it at the time:
    -- a change of channel must not rewrite where past messages went.
    channel         text        NOT NULL,
    dag_id          text,
    run_id          text,
    window_minutes  integer     CHECK (window_minutes IS NULL OR window_minutes > 0),
    -- The counts the control was judged on. A jsonb object of numbers, the
    -- payload's `facts`, as sent.
    facts           jsonb       NOT NULL DEFAULT '{}'::jsonb
                                CHECK (jsonb_typeof(facts) = 'object')
);

CREATE INDEX IF NOT EXISTS fd_ops_notifications_sent_at_idx
    ON fd_ops_notifications (sent_at);
CREATE INDEX IF NOT EXISTS fd_ops_notifications_control_idx
    ON fd_ops_notifications (control, sent_at);

GRANT SELECT, INSERT ON fd_ops_notifications TO fd_ingest;
GRANT USAGE, SELECT ON SEQUENCE fd_ops_notifications_notification_id_seq TO fd_ingest;
GRANT SELECT ON fd_ops_notifications TO fd_report;
REVOKE UPDATE, DELETE, TRUNCATE ON fd_ops_notifications FROM fd_ingest;
