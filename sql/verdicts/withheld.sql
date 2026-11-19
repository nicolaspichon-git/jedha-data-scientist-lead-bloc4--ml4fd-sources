-- How many verdicts are written but not yet due, and how many have fallen due.
-- Covers: EI03, EI12, EC14   Lot: N6   Competency: C4
--
-- THE MEASUREMENT THAT MAKES THE WITHHOLDING VISIBLE. A guarantee nobody can
-- count is a promise; this query is how N8 will check that the blind band
-- exists and has the declared width.
--
-- not_yet_due is the blind band: rows whose verdict is WRITTEN -- the truth is
-- not invented later -- and which no consumer may use, because every consumer
-- filters on label_ts <= cut_off.

SELECT count(*)                                                 AS verdicts,
       count(*) FILTER (WHERE g.label_ts >  now())              AS not_yet_due,
       count(*) FILTER (WHERE g.label_ts <= now())              AS due,
       min(g.label_ts - t.txn_ts)                               AS min_delay,
       max(g.label_ts - t.txn_ts)                               AS max_delay,
       count(DISTINCT g.label_ts - t.txn_ts)                    AS distinct_delays
FROM   fd_ground_truth g
JOIN   fd_transactions t ON t.txn_id = g.txn_id
WHERE  g.source = %(source)s;
