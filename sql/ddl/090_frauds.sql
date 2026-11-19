-- The alert raised on a scored transaction, and whether it reached anyone.
-- Covers: EE09, EI05, EC14, EC17   Lot: N5   Competency: C2, C3, C4
--
-- THE PRIMARY KEY ON txn_id IS THE IDEMPOTENCE, and it is the whole point: a
-- transaction cannot raise two alerts, whatever the task replays. A retrain
-- that rescores the same transaction under a new model version therefore does
-- NOT raise a second alert -- fd_scores keeps both scores, fd_frauds keeps the
-- one alert and the version that raised it.
--
-- THREE FACTS, NOT ONE. Raising an alert, attempting to notify it and
-- succeeding are different things, and only the last can fail on something
-- outside this system:
--
--   raised_at    never null. The alert exists.
--   notified_at  null until a notification SUCCEEDED.
--   attempts     how many tries. Without it, "notified_at is null" cannot tell
--                "never tried" from "tried eleven times, the channel is dead",
--                and an alert failing forever looks exactly like a fresh one.
--   last_error   why the last attempt failed, so a human can act on it. This
--                project has already lost 28 real observations to an unmounted
--                directory; a file channel fails for reasons worth recording.
--
-- NO IDENTIFYING COLUMN, and nothing to add here: fd_transactions holds none
-- (EC17), so an alert cannot carry a name even by accident. The analyst
-- resolves identity INSIDE the system, never from the message.

-- RENAMED FROM fd_alerts ON 18 NOVEMBER 2026 (S6/R12), and this block is why a
-- replay of the DDL does not strand the data. `CREATE TABLE IF NOT EXISTS
-- fd_frauds` on a database that holds fd_alerts would create a SECOND, empty
-- table and leave every alert behind in the first -- the notifier would then
-- read an empty queue and the dashboard an empty page, with nothing failing.
-- So the rename happens here, in place, before the CREATE: constraints and
-- indexes included, since PostgreSQL keeps their old names after a table
-- rename and a later replay would recreate them under the new ones.
-- Idempotent: on a database that already holds fd_frauds, nothing runs.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'fd_alerts'
                 AND relnamespace = 'public'::regnamespace)
       AND NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'fd_frauds'
                         AND relnamespace = 'public'::regnamespace) THEN
        ALTER TABLE fd_alerts RENAME TO fd_frauds;
        ALTER TABLE fd_frauds RENAME CONSTRAINT fd_alerts_pkey TO fd_frauds_pkey;
        ALTER TABLE fd_frauds RENAME CONSTRAINT fd_alerts_txn_id_fkey
                                             TO fd_frauds_txn_id_fkey;
        ALTER TABLE fd_frauds RENAME CONSTRAINT fd_alerts_above_threshold
                                             TO fd_frauds_above_threshold;
        ALTER TABLE fd_frauds RENAME CONSTRAINT fd_alerts_channel_with_notification
                                             TO fd_frauds_channel_with_notification;
        ALTER TABLE fd_frauds RENAME CONSTRAINT fd_alerts_notified_implies_attempted
                                             TO fd_frauds_notified_implies_attempted;
        ALTER TABLE fd_frauds RENAME CONSTRAINT fd_alerts_score_check
                                             TO fd_frauds_score_check;
        ALTER TABLE fd_frauds RENAME CONSTRAINT fd_alerts_threshold_check
                                             TO fd_frauds_threshold_check;
        ALTER TABLE fd_frauds RENAME CONSTRAINT fd_alerts_attempts_check
                                             TO fd_frauds_attempts_check;
        ALTER INDEX fd_alerts_pending_idx  RENAME TO fd_frauds_pending_idx;
        ALTER INDEX fd_alerts_notified_idx RENAME TO fd_frauds_notified_idx;
        RAISE NOTICE 'fd_alerts renommee en fd_frauds, contraintes et index compris';
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS fd_frauds (
    txn_id        text         PRIMARY KEY REFERENCES fd_transactions (txn_id),
    score         numeric(9,8) NOT NULL CHECK (score >= 0 AND score <= 1),
    threshold     numeric(9,8) NOT NULL CHECK (threshold > 0 AND threshold < 1),
    model_version text         NOT NULL,
    raised_at     timestamptz  NOT NULL DEFAULT now(),
    notified_at   timestamptz,
    channel       text,
    attempts      integer      NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error    text,

    -- An alert is raised because the score crossed the threshold. Storing both
    -- and not re-deriving means the constraint can state it.
    CONSTRAINT fd_frauds_above_threshold CHECK (score >= threshold),
    -- A notification that succeeded names its channel, and one that has not
    -- happened cannot name one. Without this, a channel could be recorded for
    -- a notification that never went out.
    CONSTRAINT fd_frauds_channel_with_notification
        CHECK ((notified_at IS NULL) = (channel IS NULL)),
    -- Nothing is notified without having been tried.
    CONSTRAINT fd_frauds_notified_implies_attempted
        CHECK (notified_at IS NULL OR attempts > 0)
);

-- The query that runs 1,440 times a day must be cheap when there is nothing to
-- do: a partial index on exactly the rows the notifier looks for.
CREATE INDEX IF NOT EXISTS fd_frauds_pending_idx
    ON fd_frauds (raised_at) WHERE notified_at IS NULL;
-- The rolling-window count that enforces the declared capacity.
CREATE INDEX IF NOT EXISTS fd_frauds_notified_idx
    ON fd_frauds (notified_at) WHERE notified_at IS NOT NULL;

-- UPDATE is granted, unlike on fd_scores, and only on the notification
-- columns: raising an alert is an observation and is never amended, but its
-- delivery state changes by nature. A column-level grant says exactly that --
-- the same reasoning as the feature columns of fd_transactions.
GRANT SELECT, INSERT ON fd_frauds TO fd_ingest;
GRANT UPDATE (notified_at, channel, attempts, last_error)
    ON fd_frauds TO fd_ingest;
GRANT SELECT ON fd_frauds TO fd_report;
REVOKE DELETE, TRUNCATE ON fd_frauds FROM fd_ingest;
