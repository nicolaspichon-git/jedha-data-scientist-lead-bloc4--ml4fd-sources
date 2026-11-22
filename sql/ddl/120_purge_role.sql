-- The only role of the project that may destroy.
-- Block: B3, B4.
-- Lot: B3-Q3, B4-L0.
-- Competency: B3-C4.
-- Covers: B3-EC19, B3-EI11.
--
-- WHY A ROLE OF ITS OWN AND NOT A GRANT TO fd_ingest. The append-only property
-- of the raw zone is a REVOCATION (060_roles_grants.sql): the role that writes
-- has no right to delete, and L1 demonstrates it on screen. Granting DELETE to
-- fd_ingest so that the purge could run would undo that property for every
-- task of every DAG -- the whole chain would gain the right to erase the raw
-- zone so that one weekly job could.
--
-- So the right to destroy is a role of its own, used by one process, and the
-- separation is enforced by the database rather than promised in a document.
--
-- WHAT IT CANNOT DO, and this is the part that matters:
--   * it cannot touch fd_transactions, fd_ground_truth, fd_scores, fd_frauds
--     or fd_reports -- no grant at all. "The purge did not overflow into the
--     exploited layer" is therefore not a check that could be forgotten: it is
--     a privilege the session does not hold. Q6 verifies the digest as well,
--     because a property worth having is worth measuring.
--   * it cannot delete from fd_pipeline_runs. The fingerprints recorded in Q1
--     are what remains of a purged batch; the process that deletes the data
--     must not be able to delete the evidence of what it deleted.
--   * it cannot INSERT or UPDATE in the raw zone or the quarantine: a purge
--     removes, it does not rewrite.
--
-- The UPDATE it does hold on fd_pipeline_runs is for the resumption marker of
-- a chunked purge (ml4fd/purge.py): a purge interrupted between two slices
-- must be resumable, and what says so is written beside the fingerprint.
--
-- No LOGIN, like the other two: the application connects as POSTGRES_USER and
-- issues SET ROLE, so the privileges below genuinely apply.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fd_purge') THEN
        CREATE ROLE fd_purge;
    END IF;
END
$$;

GRANT SELECT, DELETE ON fd_raw_events TO fd_purge;
GRANT SELECT, DELETE ON fd_quarantine TO fd_purge;
GRANT SELECT, UPDATE ON fd_pipeline_runs TO fd_purge;

-- Never granted, and said out loud so that a later hand does not add them
-- without meeting this reasoning.
REVOKE INSERT, UPDATE, TRUNCATE ON fd_raw_events  FROM fd_purge;
REVOKE INSERT, UPDATE, TRUNCATE ON fd_quarantine  FROM fd_purge;
REVOKE INSERT, DELETE, TRUNCATE ON fd_pipeline_runs FROM fd_purge;
