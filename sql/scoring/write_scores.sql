-- One scored transaction. Idempotent on (txn_id, model_version).
-- Covers: EC23, EC24   Lot: N4   Competency: C3
--
-- DO NOTHING rather than DO UPDATE, and the choice is not a detail: scoring the
-- same transaction twice with the same model version must yield the same score
-- -- the model is deterministic and the features are stored -- so a second
-- write has nothing new to say. If it ever did, that would be a defect to
-- surface, not a row to overwrite.
--
-- A retrain produces a different model_version, so it inserts rather than
-- conflicts: the previous model's score survives, which is what makes "which
-- model let this one through?" answerable after the weekly retrain.

INSERT INTO fd_scores (txn_id, model_version, score, threshold, flagged,
                       recipe_digest)
VALUES (%(txn_id)s, %(model_version)s, %(score)s, %(threshold)s,
        %(flagged)s, %(recipe_digest)s)
ON CONFLICT (txn_id, model_version) DO NOTHING;
