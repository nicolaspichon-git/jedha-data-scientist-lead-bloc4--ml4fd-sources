-- The journal of the purges: one row per store and per execution.
-- Block: B3, B4.
-- Lot: B3-Q6, B4-L0.
-- Competency: B3-C4, B3-C5.
-- Covers: B3-EC14, B3-EC18, B3-EC27.
--
-- WHY A TABLE AND NOT A LOG LINE. "Une durée non appliquée n'est pas une
-- politique" (D1, annexe B): the declaration in governance.yml claims that
-- personal data is removed at ninety days, and what turns that claim into a
-- processing one can show is the record that it happened -- including the
-- Sundays when it found nothing to remove, which is the ordinary case and the
-- one a log would lose first.
--
-- ONE ROW PER STORE, NOT PER BATCH. A batch is already journalled where it
-- belongs: in fd_pipeline_runs.digests -> purge, beside the fingerprint of
-- what it held (Q1, Q3). A second row per batch here would be the same fact
-- in two places, and 130 thousand rows a quarter of it.
--
-- NO dry_run COLUMN, deliberately against the plan of S5, which listed one.
-- A dry run writes nothing: `fd-manager purge` without -Apply reads, prints
-- and returns -- the same principle as `abandoned` and `digests`, where
-- looking at a journal must not change it. A column that could only ever be
-- false would invite someone to start recording dry runs, and then "the purge
-- ran" would no longer mean anything in this table.
--
-- APPEND ONLY. fd_purge may insert and nothing else: the process that destroys
-- the data cannot rewrite the record of what it destroyed. The same reasoning
-- as the revocation on fd_raw_events, and as the one that keeps the batch
-- fingerprints out of its reach.

CREATE TABLE IF NOT EXISTS fd_purges (
    purge_id    bigserial   PRIMARY KEY,
    -- The Airflow run, when there was one. Null when a human ran it by hand
    -- (tools/purge_run.py), which is a fact worth keeping rather than hiding
    -- behind an invented identifier. No foreign key: fd_pipeline_runs is
    -- purged by nothing, but a hand run has no row there to point at.
    run_id      text,
    ran_at      timestamptz NOT NULL DEFAULT now(),
    -- The instant the purge was FOR -- data_interval_end in the DAG -- and the
    -- cutoff that followed from it for this store. Both, because the first is
    -- the decision and the second is what it meant for this table.
    as_of       timestamptz NOT NULL,
    cutoff      timestamptz NOT NULL,
    -- Null in production. Set only when the boundary was overridden on the
    -- command line for a demonstration, and recorded because a purge that
    -- deleted beyond the declared retention must be recognisable as such for
    -- ever after.
    before_date timestamptz,
    store       text        NOT NULL,
    n_batches   integer     NOT NULL CHECK (n_batches >= 0),
    n_deleted   bigint      NOT NULL CHECK (n_deleted >= 0),
    n_refused   integer     NOT NULL CHECK (n_refused >= 0),
    n_resumed   integer     NOT NULL CHECK (n_resumed >= 0),
    share       numeric(9,6) CHECK (share IS NULL
                                    OR (share >= 0 AND share <= 1)),

    -- A purge that deleted rows without naming a single batch would be a count
    -- nobody could trace back to anything.
    CONSTRAINT fd_purges_rows_need_batches
        CHECK ((n_deleted = 0) OR (n_batches > 0)),
    -- The cutoff cannot be after the instant the purge was for: that would
    -- delete rows younger than the retention.
    CONSTRAINT fd_purges_cutoff_before_as_of
        CHECK (cutoff <= as_of),
    -- An overridden boundary IS the cutoff of that run, for every store.
    CONSTRAINT fd_purges_override_is_the_cutoff
        CHECK (before_date IS NULL OR before_date = cutoff)
);

CREATE INDEX IF NOT EXISTS fd_purges_ran_at_idx ON fd_purges (ran_at);
CREATE INDEX IF NOT EXISTS fd_purges_store_idx  ON fd_purges (store, ran_at);

GRANT SELECT, INSERT ON fd_purges TO fd_purge;
GRANT USAGE, SELECT ON SEQUENCE fd_purges_purge_id_seq TO fd_purge;
GRANT SELECT ON fd_purges TO fd_report;
REVOKE UPDATE, DELETE, TRUNCATE ON fd_purges FROM fd_purge;

-- Q7: the batch a TARGETED forget removed. Null for a retention purge, which
-- removes whatever the cutoff caught and names no batch.
--
-- ONE JOURNAL, not two. "What was removed from the raw zone, and when" must
-- have a single answer; a second table for targeted removals would be the same
-- fact in two places, and the acceptance of S5 would have to ask both.
-- A forget also sets before_date, because it deletes beyond the declared
-- retention -- which the column exists to keep recognisable for ever.
--
-- Additive, like fd_pipeline_runs.digests: no reset needed.
ALTER TABLE fd_purges ADD COLUMN IF NOT EXISTS batch_id uuid;

-- ADD CONSTRAINT has no IF NOT EXISTS, and this file is replayed by `init`.
DO $$
BEGIN
    ALTER TABLE fd_purges
        ADD CONSTRAINT fd_purges_forget_names_one_batch
        CHECK (batch_id IS NULL OR n_batches <= 1);
EXCEPTION WHEN duplicate_object THEN
    NULL;
END $$;
