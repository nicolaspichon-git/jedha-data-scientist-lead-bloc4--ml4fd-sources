-- Carry the RECORDED paths across the move of reports/ and datasets/ under
-- runtime/ (S6/R13).
-- Block: B3, B4.
-- Lot: B3-R13, B4-L0.
-- Competency: B3-C4.
-- Covers: B3-EC23, B3-EC24.
--
-- WHY A ROW HAS TO FOLLOW A DIRECTORY. Two tables store an absolute path, and
-- both are read to find a file that must be there:
--
--   fd_reports.path     the document the digest of that row vouches for. The
--                       dashboard compares the recorded path with the one
--                       report.path_for() computes and REPORTS a difference
--                       (« document déplacé ») -- which is the right behaviour
--                       for a file somebody moved by hand, and the wrong
--                       answer for every past report after a declared move.
--   fd_datasets.*_path  the Parquet sets a model learned from. This is the
--                       lineage of M8: a row pointing at a path that no longer
--                       exists breaks the chain from a model back to its rows.
--
-- The files themselves moved with the directory; only the strings are stale.
--
-- A PREFIX REWRITE AND NOTHING MORE. Only rows whose path starts with the old
-- location are touched, so a path recorded elsewhere -- a demonstration run
-- from another directory -- is left alone rather than silently relocated.
-- Idempotent: a path already under runtime/ matches nothing.
--
-- Applied by `.\fd-manager.ps1 init`, after the DDL and with it: the layout
-- change and the rewrite belong to the same operation.

DO $$
DECLARE
    moved_reports  integer;
    moved_datasets integer;
BEGIN
    UPDATE fd_reports
       SET path = '/opt/project/runtime/reports/' || right(path, -length('/opt/project/reports/'))
     WHERE path LIKE '/opt/project/reports/%';
    GET DIAGNOSTICS moved_reports = ROW_COUNT;

    UPDATE fd_datasets
       SET train_path = '/opt/project/runtime/datasets/' || right(train_path, -length('/opt/project/datasets/')),
           test_path  = '/opt/project/runtime/datasets/' || right(test_path,  -length('/opt/project/datasets/'))
     WHERE train_path LIKE '/opt/project/datasets/%'
        OR test_path  LIKE '/opt/project/datasets/%';
    GET DIAGNOSTICS moved_datasets = ROW_COUNT;

    IF moved_reports > 0 OR moved_datasets > 0 THEN
        RAISE NOTICE 'chemins deplaces sous runtime/ : % rapport(s), % jeu(x)',
                     moved_reports, moved_datasets;
    END IF;
END $$;

-- WHAT THIS FILE CANNOT REACH, said rather than left to be discovered:
--   * run.json records the model's absolute path. Nothing depends on it --
--     tools/measure_hour_effect.py falls back to the manifest's own directory,
--     and every other reader computes the path from the recipe version -- so
--     the manifests are left exactly as they were written. A manifest is the
--     record of a run, and rewriting one after the fact is a worse habit than
--     a stale string inside it.
--   * MLflow's logged parameters, for the same reason: they are history.
