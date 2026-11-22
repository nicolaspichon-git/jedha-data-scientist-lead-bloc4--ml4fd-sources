-- The line numbers of a batch that went to the quarantine.
-- Block: B3, B4.
-- Lot: B3-N7, B4-L0.
-- Competency: B3-C3.
-- Covers: B3-EC14, B3-EC23.
--
-- WHY IT IS READ BACK RATHER THAN CARRIED. Within one process the ingestion
-- knew which lines it had rejected and passed the set along. The DAG of N7 has
-- no such luxury: Airflow passes JSON between tasks, and a set of line numbers
-- that crossed that boundary would be a second record of something the
-- quarantine table already holds -- and a second record is one too many.
--
-- DISTINCT, because the quarantine holds one row per line AND PER REASON:
-- UNIQUE (batch_id, line_no, reason_code). A line with two defects appears
-- twice, and skipping it twice would be harmless while counting it twice would
-- not.

SELECT DISTINCT line_no
FROM   fd_quarantine
WHERE  batch_id = %(batch_id)s
ORDER  BY line_no;
