-- Record a control's new state, AFTER the message went out.
-- Covers: EC14   Lot: N9   Competency: C4
--
-- One row per control and not one per transition: this is a state, not a
-- journal. The history of the messages is fd_ops_notifications (S6/R9), one
-- row per message SENT -- a few per incident, not 1,440 a day -- written in the
-- same transaction (sql/ops/journal_notification.sql).
INSERT INTO fd_ops_state (control, state, since, detail, run_id, notified_at)
VALUES (%(control)s, %(state)s, now(), %(detail)s, %(run_id)s, now())
ON CONFLICT (control) DO UPDATE
   SET state       = excluded.state,
       since       = excluded.since,
       detail      = excluded.detail,
       run_id      = excluded.run_id,
       notified_at = excluded.notified_at;
