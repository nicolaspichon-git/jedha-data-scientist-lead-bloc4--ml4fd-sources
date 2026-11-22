-- Record one monitoring measurement. Append only (EC45).
-- Block: B4.
-- Lot: B4-L4.
-- Competency: B4-C6.
-- Covers: B4-EC45.
--
-- One statement, named parameters, as every file under sql/ that db.py runs.
-- measured_at is left to its DEFAULT now(): the time a measurement was taken
-- is the database's to state, not the caller's -- a container with a drifting
-- clock would otherwise order the series wrongly.

INSERT INTO fd_monitoring (run_id, window_start, window_end, model_version,
                           control, metric, value, threshold, state,
                           n_rows, n_labelled, tool, detail)
VALUES (%(run_id)s, %(window_start)s, %(window_end)s, %(model_version)s,
        %(control)s, %(metric)s, %(value)s, %(threshold)s, %(state)s,
        %(n_rows)s, %(n_labelled)s, %(tool)s, %(detail)s);
