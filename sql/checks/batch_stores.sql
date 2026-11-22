-- What one batch holds, store by store. Needs fd_ingest: fd_purge may not read
-- fd_transactions, by design.
-- Block: B3, B4.
-- Lot: B3-Q7, B4-L0.
-- Competency: B3-C4, B3-C5.
-- Covers: B3-EC18, B3-EC23.
--
-- SCALAR SUBQUERIES, NO JOIN. batch_id is a key of none of these tables; a
-- join on it is one-to-many, and an aggregate taken after one counts rows per
-- run instead of rows. sql/checks/raw_batches.sql was wrong that way and
-- reported 1,667,157 lines for 555,719.
--
-- EVERY COMPARISON CARRIES ITS CAST, and that is not decoration. A named
-- parameter reused in one statement is ONE parameter with ONE inferred type:
-- PostgreSQL read $1 as uuid from `fd_raw_events.batch_id = $1`, and the
-- verdicts' comparison then became `text = uuid`, which has no operator.
-- fd_ground_truth carries the batch in a TEXT column of its own, so the two
-- types genuinely coexist here.
--
-- Parameter: batch_id (text; cast at each use).
SELECT (SELECT count(*) FROM fd_raw_events   WHERE batch_id = %(batch_id)s::uuid) AS raw_rows,
       (SELECT count(*) FROM fd_quarantine   WHERE batch_id = %(batch_id)s::uuid) AS quarantine_rows,
       -- The one thing that makes a batch no debris at all: it reached the
       -- exploited layer, and its raw zone is what allows a replay (EC24).
       (SELECT count(*) FROM fd_transactions WHERE batch_id = %(batch_id)s::uuid) AS txn_rows,
       -- The verdicts, which carry the batch as TEXT in their own `source`
       -- column and not as a batch_id (ml4fd/truth.py, dag_load_history's
       -- write_ground_truth). A batch can own verdicts without owning a single
       -- transaction, and forgetting it would orphan them: the fact has to be
       -- read here, not inferred from the transactions.
       (SELECT count(*) FROM fd_ground_truth
          WHERE source = %(batch_id)s::text)                                      AS truth_rows,
       (SELECT max(ingested_at) FROM fd_raw_events
          WHERE batch_id = %(batch_id)s::uuid)                                    AS last_at,
       (SELECT string_agg(source, ', ' ORDER BY source)
          FROM (SELECT DISTINCT source FROM fd_raw_events
                 WHERE batch_id = %(batch_id)s::uuid) s)                          AS sources;
