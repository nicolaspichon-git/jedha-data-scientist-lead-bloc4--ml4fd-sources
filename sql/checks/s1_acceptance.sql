-- The acceptance criteria of S1, as one table. Every verdict must read OK.
-- Covers: EC01, EC12, EC17, EC20, EC23, EE08, EI01, EI04   Lot: L8
-- Competency: C1, C2, C4, C5
--
-- The expected values are MEASURED on the source, not chosen: 555,719 rows,
-- 2,145 frauds, 924 distinct cardholders, amounts totalling 38,562,903.11. A
-- figure that stops matching means either the source changed or the pipeline
-- lost something -- and either way it is worth knowing which.
--
-- Everything is cast to text so that one column can hold counts, sums and
-- intervals side by side. Comparing text is enough: these are exact values, not
-- approximations.

-- THE VERDICT MUST REACH THE SHELL, and until N9 it did not. psql exits 0
-- whatever the column below says, and Invoke-Step reads only the exit code: so
-- `fd-manager check` walked past seven criteria of S1 reading ECHEC and would
-- have printed "Tous les criteres passent". A green light that cannot turn red
-- is worse than no light, because somebody trusts it.
--
-- The rows therefore go to a temp table, are DISPLAYED first -- the operator
-- must see the table, not only an error -- and are then counted: RAISE
-- EXCEPTION makes psql exit non-zero under ON_ERROR_STOP=1.
--
-- TEMP and not dropped first: fd-manager runs each of these files through its
-- own `psql -f`, so there is one session per file and nothing to clean up. Two
-- files pasted into ONE session would fail on "already exists", which names
-- itself.

CREATE TEMP TABLE acceptance AS
-- WHAT "HISTORICAL" MEANS HERE, and it has to be said rather than assumed.
--
-- THE DEFECT THIS REPAIRS. These criteria counted the WHOLE tables, and they
-- were written when nothing else wrote to them. S3 now ingests the stream into
-- the same raw zone, the same fd_transactions and the same fd_ground_truth, so
-- seven criteria read ECHEC on a pipeline that was working perfectly: 555,857
-- typed transactions instead of 555,719 -- exactly the 138 transactions of the
-- stream -- and 555,743 verdicts instead of 555,719, exactly its 24. The
-- amounts were off by 9,540.86 EUR over those 138 lines.
--
-- s3_acceptance.sql has always scoped itself, by its stream_runs and stream_txn
-- CTEs. This file did not, and the asymmetry is the whole bug: a process that
-- measures a shared table must say which rows are its own.
--
-- THE FILTER IS NEGATIVE, and that is forced by the data. S1 writes the CSV's
-- FILE NAME as the source (dag_load_history: raw.write_batch(..., path.name))
-- and its verdicts carry the BATCH UUID, so there is no single positive value
-- to match. The stream has one, ml4fd/stream.py: SOURCE. Everything that is
-- not the stream belongs to the historical load -- and the literal is written
-- ONCE below, so a test can hold it to ml4fd/stream.py.
WITH stream_source AS (
    SELECT 'current-transactions'::text AS name
),
historical_batch AS (
    -- DISTINCT batch_id and not the rows: the EXISTS below then probes a
    -- handful of identifiers instead of nineteen million raw lines.
    SELECT DISTINCT batch_id
      FROM fd_raw_events
     WHERE source <> (SELECT name FROM stream_source)
),
historical_raw AS (
    SELECT * FROM fd_raw_events
     WHERE source <> (SELECT name FROM stream_source)
),
historical_quarantine AS (
    SELECT q.* FROM fd_quarantine q
     WHERE EXISTS (SELECT 1 FROM historical_batch b
                    WHERE b.batch_id = q.batch_id)
),
historical_txn AS (
    SELECT t.* FROM fd_transactions t
     WHERE EXISTS (SELECT 1 FROM historical_batch b
                    WHERE b.batch_id = t.batch_id)
),
historical_truth AS (
    -- Its own source column, written as the batch UUID by S1 and as the
    -- stream's name by S3, so the same filter applies directly.
    SELECT * FROM fd_ground_truth
     WHERE source <> (SELECT name FROM stream_source)
),
historical_runs AS (
    -- By the DAG and not by the source: a run is S1's because dag_load_history
    -- made it. tools/ingest_once.py journals under its own name and the
    -- stream's DAG under its, so neither is counted here.
    SELECT * FROM fd_pipeline_runs WHERE dag_id = 'dag_load_history'
),
criteria AS (

    -- L1 -- raw zone
    --
    -- PER BATCH and not in total: fd_raw_events is append-only, so every load
    -- adds a batch and the table legitimately grows. Comparing the total with
    -- 555,719 was wrong -- three loads give 1,667,157 and that is the raw zone
    -- working as designed, not a defect.
    SELECT 1 AS ord, 'L1 lignes par lot en zone brute' AS criterion,
           '555719' AS expected,
           CASE WHEN min(n) = 555719 AND max(n) = 555719 THEN '555719'
                ELSE min(n)::text || '..' || max(n)::text END AS actual
    FROM  (SELECT count(*) AS n FROM historical_raw GROUP BY batch_id) b
    UNION ALL
    SELECT 2, 'L1 lots historiques en zone brute (informatif)',
           (SELECT count(*)::text FROM historical_batch),
           (SELECT count(*)::text FROM historical_batch)

    -- L2 -- validation
    UNION ALL
    SELECT 3, 'L2 lignes en quarantaine (informatif)',
           (SELECT count(*)::text FROM historical_quarantine),
           (SELECT count(*)::text FROM historical_quarantine)
    UNION ALL
    SELECT 4, 'L2 fermeture des compteurs',
           '0 ecart', (SELECT count(*)::text || ' ecart'
                       FROM historical_runs r
                       WHERE r.n_read IS NOT NULL
                         AND r.n_read <> coalesce(r.n_valid, 0)
                                       + coalesce(r.n_rejected, 0))

    -- L3 -- typing and pseudonymisation
    UNION ALL
    SELECT 5, 'L3 transactions typees', '555719', count(*)::text
    FROM   historical_txn
    UNION ALL
    SELECT 6, 'L3 porteurs distincts', '924', count(DISTINCT client_key)::text
    FROM   historical_txn
    UNION ALL
    SELECT 7, 'L3 somme des montants', '38562903.11', coalesce(sum(amt), 0)::text
    FROM   historical_txn
    UNION ALL
    SELECT 8, 'L3 colonnes nominatives dans la couche exploitee',
           '0', count(*)::text
    FROM   information_schema.columns
    WHERE  table_name = 'fd_transactions'
      AND  column_name IN ('first', 'last', 'street', 'city', 'state', 'zip',
                           'lat', 'long', 'job', 'dob', 'cc_num', 'gender',
                           'city_pop')
    UNION ALL
    SELECT 9, 'L3 distance domicile renseignee', '555719', count(*)::text
    FROM   historical_txn WHERE f_home_distance_km IS NOT NULL

    -- L4 -- features
    UNION ALL
    SELECT 10, 'L4 lignes sans features', '0', count(*)::text
    FROM   historical_txn WHERE f_tx_count IS NULL
    UNION ALL
    SELECT 11, 'L4 premieres transactions a 0 et NULL', '924', count(*)::text
    FROM   historical_txn WHERE f_tx_count = 0 AND f_avg_amount IS NULL

    -- L5 -- ground truth
    UNION ALL
    SELECT 12, 'L5 verdicts', '555719', count(*)::text
    FROM   historical_truth
    UNION ALL
    SELECT 13, 'L5 fraudes', '2145', count(*)::text
    FROM   historical_truth WHERE label
    UNION ALL
    SELECT 14, 'L5 delai de verdict uniforme',
           'oui', CASE WHEN min(label_ts - txn_ts) = max(label_ts - txn_ts)
                       THEN 'oui' ELSE 'non' END
    FROM   historical_truth
    UNION ALL
    SELECT 15, 'L5 dissociation du verdict',
           '0', count(*)::text
    FROM   information_schema.columns
    WHERE  table_name = 'fd_transactions'
      AND  column_name IN ('label', 'is_fraud', 'fraud')

    -- L6 -- idempotence
    --
    -- The most telling line of the table. Several loads of the same file pile
    -- up in the raw zone, and the exploited layer still holds exactly one set
    -- of transactions: the upsert on txn_id did its work. No test states EC23
    -- as plainly as this ratio does.
    UNION ALL
    SELECT 16, 'L6 idempotence : N lots ingeres, un seul jeu de transactions',
           'ok',
           CASE WHEN (SELECT count(*) FROM historical_txn) = 555719
                 AND (SELECT count(*) FROM historical_raw) >= 555719
                THEN 'ok' ELSE 'non' END

    -- L7 -- journal
    UNION ALL
    SELECT 17, 'L7 executions reussies (>= 1)',
           'ok', CASE WHEN count(*) >= 1 THEN 'ok' ELSE 'aucune' END
    FROM   historical_runs WHERE state = 'success'
)
SELECT ord,
       criterion,
       expected,
       actual,
       CASE WHEN expected = actual THEN 'OK' ELSE '<<< ECHEC' END AS verdict
FROM   criteria;

SELECT criterion, expected, actual, verdict
FROM   acceptance
ORDER  BY ord;

DO $$
DECLARE
    failed int;
    names  text;
BEGIN
    SELECT count(*), string_agg(criterion, ' | ' ORDER BY ord)
      INTO failed, names
      FROM acceptance
     WHERE verdict LIKE '%ECHEC%';
    IF failed > 0 THEN
        RAISE EXCEPTION 'S1 : % critere(s) en echec : %', failed, names
            USING HINT = 'Le tableau ci-dessus donne attendu et constate pour chacun.';
    END IF;
END $$;
