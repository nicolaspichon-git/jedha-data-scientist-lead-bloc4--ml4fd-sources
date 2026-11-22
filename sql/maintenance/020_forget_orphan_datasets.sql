-- Forget the datasets whose Parquet files no longer exist.
-- Block: B3, B4.
-- Lot: B3-M8, B4-L0.
-- Competency: B3-C4.
-- Covers: B3-EC23.
--
-- WHY THIS EXISTS. datasets.root was relative, so an Airflow task resolved it
-- against /opt/airflow instead of the project: the rows were registered here
-- while the files were written inside the container, outside the mounted
-- volume, and vanished with it. fd_datasets then described datasets that no
-- longer exist anywhere.
--
-- build_or_reuse() already rebuilds when the files are missing, so this is not
-- strictly required -- but a registry that lists what is not there is a registry
-- nobody can trust, and the point of M8 is that it can be trusted.
--
-- Replayable: deleting a row only means the next run rebuilds and re-registers.

DELETE FROM fd_datasets
WHERE  created_at < '2026-11-16 00:40:00+00'::timestamptz
RETURNING version, recipe_digest, created_at,
          'oublie : fichiers hors du volume monte' AS reason;
