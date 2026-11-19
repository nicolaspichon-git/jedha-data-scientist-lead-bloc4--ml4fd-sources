-- Copy what M5 measured on the test set into the dataset's row (S6/R10).
-- Covers: EE04, EC23   Lot: R10   Competency: C4
--
-- ONE WRITER: registry.record_reference(), called by dag_train.fit_model and
-- by tools/backfill_reference.py, both from a run.json payload.
-- The casts: psycopg sends untyped parameters (see sql/ops/journal_notification.sql).
UPDATE fd_datasets
   SET test_recall         = %(recall)s::numeric,
       test_precision      = %(precision)s::numeric,
       test_alerts_per_day = %(alerts_per_day)s::numeric,
       test_threshold      = %(threshold)s::numeric,
       measured_at         = %(measured_at)s::timestamptz
 WHERE recipe_digest = %(recipe_digest)s;
