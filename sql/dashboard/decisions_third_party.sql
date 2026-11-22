-- The third-party calls of a calendar period, newest first.
-- Block: B4.
-- Lot: B4-L5.
-- Competency: B4-C3.
-- Covers: B4-EC16, B4-EC19.
--
-- WHY ONLY THE THIRD-PARTY SIDE HAS A LIST. The pipeline's decisions are
-- already shown, payment by payment, by the fraud alerts and the morning
-- report. The calls a third party makes were persisted by B4-L5 and displayed by
-- nothing -- B4-EC19 asks that each call and its answer be kept, and a journal
-- nobody reads is a journal nobody can check.
--
-- NO FIELD OF THE CALLER APPEARS, and none is available to: the view carries
-- the decision and its identity, never the cardholder's pseudonym, the
-- merchant, the amount or the history features. A page that showed them would
-- need the table, which fd_dashboard cannot read.
--
-- Parameters: window_start, window_end, listed.
SELECT call_id, decided_at, model_version, recipe_digest,
       score, threshold, flagged
  FROM fd_scoring_decisions
 WHERE source = 'tiers'
   AND decided_at >= %(window_start)s
   AND decided_at <  %(window_end)s
 ORDER BY decided_at DESC, call_id DESC
 LIMIT %(listed)s;
