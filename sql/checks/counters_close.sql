-- Counter closure. MUST return zero rows.
-- Block: B3, B4.
-- Lot: B3-L2, B4-L0.
-- Competency: B3-C5.
-- Covers: B3-EC20, B3-EC22.

-- count(DISTINCT line_no) and not count(*): one line can fail several checks
-- and therefore hold several rows in fd_quarantine, each with its own reason
-- code. Counting rows would make the sum exceed n_read and fail this check for
-- a reason that has nothing to do with the data.

SELECT r.batch_id,
       r.n_read,
       (SELECT count(*) FROM fd_transactions t
         WHERE t.batch_id = r.batch_id)                   AS stored,
       (SELECT count(DISTINCT q.line_no) FROM fd_quarantine q
         WHERE q.batch_id = r.batch_id)                   AS quarantined_lines
FROM   fd_pipeline_runs r
WHERE  r.batch_id IS NOT NULL
  AND  r.n_read <> (SELECT count(*) FROM fd_transactions t
                     WHERE t.batch_id = r.batch_id)
                 + (SELECT count(DISTINCT q.line_no) FROM fd_quarantine q
                     WHERE q.batch_id = r.batch_id);
