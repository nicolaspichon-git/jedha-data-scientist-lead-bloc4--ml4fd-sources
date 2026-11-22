-- Every decision the model served, whoever asked.
-- Block: B4.
-- Lot: B4-L5 (20/11/2026).
-- Competency: B4-C3, B4-C4.
-- Covers: B4-EC19.
--
-- WHY A VIEW AND NOT A THIRD TABLE. A decision is recorded ONCE, by whoever
-- holds the data that makes it re-derivable:
--   fd_scores                  the transaction is ours, so the features live
--                              in fd_transactions and a foreign key proves
--                              the row exists;
--   fd_scoring_service_calls   the payment is not ours and exists nowhere
--                              else, so the features travel with the decision.
-- Recording a pipeline decision in both would create a second set of figures
-- that could disagree with the first, which is what governance.yml refuses for
-- data_quality -- "Re-measuring them would create a second set of figures that
-- could disagree with the first" -- and what ml4fd/score.py refuses between
-- the manifest and the registry: "neither is authoritative".
--
-- AND WHY A VIEW RATHER THAN TWO QUERIES. "What did the service answer on the
-- 12th" must be one question. Splitting the writes is right; splitting the
-- READS would make every reader carry the split.
--
-- THIS NAME IS THE PUBLIC READING SURFACE, and that is the point of declaring
-- it now. The project's design converges on ONE journal of decisions, written
-- by the service alone: fd_ingest would lose every write on a scoring
-- artefact, and the separation would rest on a privilege rather than on a
-- convention. The day that happens, the body below becomes a SELECT on one
-- table and NO READER CHANGES. The view is therefore not a detour to be
-- undone; it is what lets that change be internal.
--
-- (The trap to meet on that day, measured 2026-11-20:
-- sql/frauds/raise_from_scores.sql reads WHERE s.flagged with no join to
-- fd_transactions, so a merged table would feed third-party calls into
-- fd_frauds -- the analysts' morning queue. It needs AND txn_id IS NOT NULL.
-- One clause, and the kind whose absence puts someone else's payment in your
-- queue.)
--
-- THE COMMON CORE AND NOTHING MORE. Both sides carry the instant, the model,
-- the recipe, the score, the threshold and the flag; only one carries
-- client_key and the features. A column that exists on one side only would
-- read as missing data on the other, so the identifiers are named apart:
-- txn_id for ours, call_id for theirs, and source says which.

CREATE OR REPLACE VIEW fd_scoring_decisions AS
SELECT 'pipeline'::text          AS source,
       s.scored_at               AS decided_at,
       s.txn_id                  AS txn_id,
       NULL::bigint              AS call_id,
       s.model_version,
       s.recipe_digest,
       s.score,
       s.threshold,
       s.flagged
  FROM fd_scores s
UNION ALL
SELECT 'tiers'::text,
       c.called_at,
       NULL::text,
       c.call_id,
       c.model_version,
       c.recipe_digest,
       c.score,
       c.threshold,
       c.flagged
  FROM fd_scoring_service_calls c;

COMMENT ON VIEW fd_scoring_decisions IS
    'Toute decision servie, pipeline et tiers confondus. Une decision est '
    'enregistree une fois, par celui qui detient la donnee qui la rend '
    're-derivable ; cette vue les lit comme une seule.';

-- READ BY THE REPORTS AND THE PAGES, never written. A view over two
-- append-only tables is not updatable here and nothing should try.
GRANT SELECT ON fd_scoring_decisions TO fd_report;
GRANT SELECT ON fd_scoring_decisions TO fd_dashboard;
