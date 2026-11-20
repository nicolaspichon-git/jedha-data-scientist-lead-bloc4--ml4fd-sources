-- The training set, at a cut-off, with its temporal split.
-- Covers: EI02, EI04, EE07   Lot: M3 (revu en L4, 20/11/2026)
-- Competency: C2, C5
--
-- THE FILTER THAT MATTERS: g.label_ts <= %(cut_off)s, and NOT t.txn_ts.
--
-- A set filtered on the TRANSACTION date would hold verdicts that were not
-- known at the cut-off. The model would then benefit from later information,
-- the measured recall would be optimistic, and the whole deferred-verdict
-- mechanism would be neutralised. The blind band of one verdict delay before
-- the cut-off exists for this reason: it is not a loss, it is the honesty of
-- the arrangement.
--
-- TWO SOURCES, TWO LABELLING REGIMES, since 2026-11-20. Until then this query
-- bounded every row by t.txn_ts < %(test_end)s, which is a date of 2020, while
-- a streamed transaction carries current_time -- so not one of them had ever
-- entered a training set, the recipe digest never moved, and the retraining
-- refitted on the very same Parquet files. governance datasets.include_stream
-- records the decision and the arithmetic that limits it.
--
--   * the HISTORICAL corpus is trustworthy only up to labels.horizon: past it
--     its rows carry label = 0 because nobody labelled them, not because they
--     are not fraud. That bound used to be carried by the cut-off, which was
--     clamped to the horizon; the cut-off is now free to be the present, so
--     the bound is stated here, where it can apply to one source and not the
--     other.
--   * the STREAM is labelled as it goes, by dag_ingest_verdicts, so the
--     cut-off above is the only bound it needs.
--
-- BY SOURCE AND NEVER BY A DATE. Telling the two apart by txn_ts would work
-- today and break the day a corpus is replayed with recent timestamps.
-- fd_raw_events.source is what the ingestion itself wrote.
--
-- AND THE STREAM GOES TO train, NEVER TO test. The gate of L3 compares a
-- candidate to the champion on one test set; a test set that moved with every
-- retraining would make the published figures incomparable from one version to
-- the next, which is what the promotion journal exists to prevent.
--
-- THE CTE IS CHEAP AND THE ANTI-PATTERN AVOIDED. One scan of fd_raw_events
-- gives the batches of the stream -- a few thousand rows, one per ingestion
-- run -- and the two EXISTS below are semi-joins against that set. A NOT IN
-- over this table is what cost a plan of 4.6e11 in the reconciliation of
-- 19 November; EXISTS against a materialised set is the shape that holds.
--
-- The features are read AS STORED. Nothing is recomputed from history here:
-- S1 persisted them on purpose (F04.1) and L4 proved both computation modes
-- agree, so training reads the very values scoring will read.
--
-- ORDER BY txn_ts, txn_id makes the output deterministic, so two builds of one
-- recipe produce the same rows in the same order.

WITH stream_batches AS (
    SELECT DISTINCT r.batch_id
    FROM   fd_raw_events r
    WHERE  r.source = %(stream_source)s
)
SELECT t.txn_id,
       t.txn_ts,
       t.amt,
       t.f_tx_count,
       t.f_avg_amount,
       t.f_home_distance_km,
       extract(hour   FROM t.txn_ts)::int  AS txn_hour,
       extract(isodow FROM t.txn_ts)::int  AS txn_weekday,
       t.category,
       g.label,
       CASE WHEN EXISTS (SELECT 1 FROM stream_batches b
                          WHERE b.batch_id = t.batch_id) THEN 'train'
            WHEN t.txn_ts < %(train_end)s                THEN 'train'
            ELSE 'test'
       END AS split,
       -- REPORTED AND NOT WRITTEN. ml4fd/build.py counts it and drops it
       -- before the Parquet: "retrained on fresh data" is a claim that needs a
       -- denominator, and 7,200 rows a day against 1,164,585 is six
       -- thousandths of the set a day. The per-cent SIGN is spelled out
       -- here on purpose: psycopg formats this file, so a bare one would
       -- break the substitution from inside a comment.
       -- A reader of the manifest sees the share rather than the adjective.
       EXISTS (SELECT 1 FROM stream_batches b
                WHERE b.batch_id = t.batch_id) AS from_stream
FROM   fd_transactions t
JOIN   fd_ground_truth g ON g.txn_id = t.txn_id
WHERE  g.label_ts <= %(cut_off)s
  AND  CASE
         WHEN EXISTS (SELECT 1 FROM stream_batches b
                       WHERE b.batch_id = t.batch_id)
           THEN %(include_stream)s
         ELSE t.txn_ts < %(test_end)s AND g.label_ts <= %(horizon)s
       END
ORDER  BY t.txn_ts, t.txn_id;
