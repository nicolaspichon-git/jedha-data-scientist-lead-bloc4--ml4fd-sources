-- The per-column drift figures of the most recent monitoring run (S6/R11).
-- Covers: EC39, EC49   Lot: L4   Competency: C6
--
-- WHY THESE ROWS DESERVE A TABLE OF THEIR OWN. On 2026-11-20 input_drift had
-- been in alert since 09:34 and the verdict said "3 of 5 columns drifted" --
-- which is true and says nothing about WHICH. The per-column figures said it:
-- f_tx_count at 6.857 against a limit of 0.100, while amt, the only column the
-- injection touched, sat under its own limit. The verdict was unreadable
-- without these rows, and they were in the table all along.
--
-- THE LIMIT IS THE TOOL'S OWN, recorded from what Evidently reported rather
-- than applied by us: drift_measurements judges the share and records the
-- columns. The state of these rows is therefore always ok, and the page must
-- not read it as "this column is fine".
--
-- Parameters: none.
-- KEYED ON measured_at AND NOT ON run_id, which is nullable. now() is the
-- TRANSACTION's timestamp in PostgreSQL, and monitor.record writes every
-- measurement of a run in one transaction, so the rows of one run share that
-- instant exactly. Grouping by a null run_id would have matched the
-- column rows of every run that carried none, and silently mixed two runs
-- into one table.
WITH last_run AS (
    SELECT max(m.measured_at) AS measured_at
      FROM fd_monitoring m
     WHERE strpos(m.metric, 'column_drift:') > 0
)
SELECT substr(m.metric, length('column_drift:') + 1) AS column_name,
       m.value,
       m.threshold,
       m.detail,
       m.measured_at,
       m.n_rows
  FROM fd_monitoring m
  JOIN last_run r ON r.measured_at = m.measured_at
 WHERE strpos(m.metric, 'column_drift:') > 0
 ORDER BY m.value DESC;
