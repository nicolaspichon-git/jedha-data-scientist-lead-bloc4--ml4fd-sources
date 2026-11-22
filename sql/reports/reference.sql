-- What M5 measured on the test set, for the comparison of section 3 and of the
-- performance page (S6/R10). The latest recipe, as before R10.
-- Block: B3, B4.
-- Lot: B3-R10, B4-L0.
-- Competency: B3-C4.
-- Covers: B3-EE04, B3-EI02.
--
-- READ FROM THE DATABASE, no longer from run.json: the dashboard runs in a
-- container that does not mount the datasets, and fd_dashboard reads these
-- columns of fd_datasets and no others (140_dashboard_role.sql).
SELECT version, base_rate_test, test_recall, test_precision,
       test_alerts_per_day, test_threshold, measured_at
  FROM fd_datasets
 ORDER BY created_at DESC
 LIMIT 1;
