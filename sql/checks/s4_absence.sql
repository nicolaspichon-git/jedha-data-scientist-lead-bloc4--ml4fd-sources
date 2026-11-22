-- The mornings with no report. MUST RETURN ZERO ROWS.
-- Block: B3, B4.
-- Lot: B3-P3, B4-L0.
-- Competency: B3-C4.
-- Covers: B3-EC14.
--
-- A wrong report is seen; an absent one is not. This is EC14 -- detect the
-- absence -- applied to the one deliverable of this project that a human reads.
--
-- IT STARTS AT THE FIRST RECORDED REPORT AND NOT THIRTY DAYS AGO, and that is
-- the difference between a control and an alarm. A pipeline three days old
-- would otherwise list twenty-seven missing mornings that nobody ever expected,
-- and a check that fails on reality teaches its reader to ignore it -- the
-- lesson of criterion 13 of S3, paid for once already.
--
-- So the span runs from the first report (or thirty days ago, whichever is
-- later, to keep the answer readable) to YESTERDAY. Two kinds of gap are
-- therefore caught, and the second matters more:
--
--   a hole BETWEEN two reports   one morning failed and was never replayed
--   a hole AFTER the last one    the DAG stopped, and has been stopped since
--
-- AN EMPTY TABLE RETURNS NOTHING, by the HAVING below: before the first report
-- ever produced, no morning is missing. "No report has ever been produced" is a
-- different finding, and it belongs to the acceptance criteria of P5 rather than
-- here -- a control that fires on a system not yet started is noise.

WITH span AS (
    SELECT greatest(min(report_date), current_date - 30) AS first_day,
           current_date - 1                              AS last_day
      FROM fd_reports
    HAVING count(*) > 0
)
SELECT day::date                                          AS journee_sans_rapport,
       current_date - day::date                           AS il_y_a_jours
  FROM span,
       generate_series(span.first_day, span.last_day, interval '1 day') AS day
 WHERE NOT EXISTS (SELECT 1 FROM fd_reports r
                    WHERE r.report_date = day::date)
 ORDER BY day;
