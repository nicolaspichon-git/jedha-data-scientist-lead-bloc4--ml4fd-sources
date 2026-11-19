-- The quarantine of a calendar period, by class and by reason. Read by the
-- dashboard (S6, lot R7). Restitutes F07.3.
-- Covers: EC15, EC22   Lot: R7, R8   Competency: C4
--
-- COUNTS ONLY. Neither `payload` -- the raw rejected record, the raw zone's
-- personal data -- nor `reason_detail` -- which quotes a raw value, the
-- trans_num itself for a duplicate -- is read, and fd_dashboard holds no grant
-- on either: 140_dashboard_role.sql opens five columns of this table by name.
--
-- TWO COUNTS, because a line can be rejected for several reasons at once:
-- UNIQUE (batch_id, line_no, reason_code). `motifs` counts reasons, `lignes`
-- counts the distinct lines they fell on. Summing `lignes` over the reasons
-- would count a line once per reason; the day's total is therefore read by
-- sql/dashboard/rejects_total_of_day.sql, never added up from these rows.
--
-- The window is report.span_for()'s: whole calendar days in the declared zone,
-- the same days as the morning reports'.
--
-- Parameters: window_start, window_end.
SELECT reason_class,
       reason_code,
       count(*)                            AS motifs,
       count(DISTINCT (batch_id, line_no)) AS lignes,
       count(DISTINCT batch_id)            AS lots
  FROM fd_quarantine
 WHERE rejected_at >= %(window_start)s
   AND rejected_at <  %(window_end)s
 GROUP BY reason_class, reason_code
 ORDER BY motifs DESC, reason_code;
