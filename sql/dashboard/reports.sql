-- The morning reports on record, most recent first. Read by the dashboard.
-- Covers: EE03   Lot: R1   Competency: C4
--
-- The counts are read HERE and never extracted from the document's prose: the
-- decision of the S4 plan, so that a reworded sentence cannot change a number.
--
-- `path` is returned and NOT followed: it is a path of the Airflow container.
-- ml4fd/dashboard.py compares it with report.path_for() and reports any
-- difference -- which would mean the two containers do not mount reports/ at
-- the same place.
--
-- Parameter: limit.
SELECT report_date,
       knowledge_date,
       produced_at,
       path,
       n_payments,
       n_alerts,
       n_verdicts,
       n_pending,
       model_version,
       threshold,
       content_digest
  FROM fd_reports
 ORDER BY report_date DESC
 LIMIT %(limit)s;
