-- The quarantine of a calendar period, in total. Read by the dashboard (R7).
-- Block: B3, B4.
-- Lot: B3-R7, B3-R8, B4-L0.
-- Competency: B3-C4.
-- Covers: B3-EC15.
--
-- Read here rather than summed from rejects_of_day.sql: a line rejected for
-- two reasons is ONE line, and only a count over the whole day says so.
--
-- Parameters: window_start, window_end.
SELECT count(*)                            AS motifs,
       count(DISTINCT (batch_id, line_no)) AS lignes,
       count(DISTINCT batch_id)            AS lots
  FROM fd_quarantine
 WHERE rejected_at >= %(window_start)s
   AND rejected_at <  %(window_end)s;
