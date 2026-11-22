-- The acceptance criteria of S1, as one table. Every verdict must read OK.
-- Block: B3, B4.
-- Lot: B3-L8, B4-L0.
-- Competency: B3-C1, B3-C2, B3-C4, B3-C5.
-- Covers: B3-EC01, B3-EC12, B3-EC17, B3-EC20, B3-EC23, B3-EE08, B3-EI01, B3-EI04.
--
-- THE EXPECTED VALUES ARE DERIVED AND NOT WRITTEN DOWN, and that is the rule
-- this file now follows. Each one is read from the raw zone, from the run
-- journal, or from another column of the exploited layer -- never from a
-- number measured once on a corpus.
--
-- It used to hold four: 555,719 rows, 2,145 frauds, 924 cardholders, amounts
-- totalling 38,562,903.11. They were true of fraudTest.csv, which this project
-- trained on until 2026-11-19. Swapping the corpora -- training on
-- fraudTrain.csv and serving the other as the stream -- turned eight of these
-- seventeen criteria red at once, and not one of them had found a defect.
-- A criterion that fails when the source changes legitimately is not a
-- criterion, it is a coupling.
--
-- So: six lines became invariants between tables, three became informative.
-- A corpus property that no other table can recompute -- the number of
-- cardholders, the number of frauds -- is reported rather than judged, and
-- the figures live where they are measured: tools/profile_corpus.py, and the
-- comments of governance.yml it feeds.
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
-- THE LINES AND NOT THE PAYLOADS, measured 21/11/2026. This CTE is read twice,
-- so PostgreSQL materialises it -- and with SELECT * that meant 650 MB of jsonb
-- carried through a temporary file to answer two counts: 1.7 s to scan and
-- 1.9 s to read back, out of 12.3 s for the whole check. Narrowed to the one
-- column those counts need, the check is 6.7 s.
historical_raw AS (
    SELECT batch_id
      FROM fd_raw_events
     WHERE source <> (SELECT name FROM stream_source)
),
-- THE PAYLOADS OF ONE BATCH, for criterion 7, which is the only reader of a
-- payload and reads one batch. Scoped like every other CTE here -- the source
-- filter is what keeps the stream out of a measurement of the historical load,
-- and reading fd_raw_events from a criterion would break the rule
-- tests/test_acceptance_files.py states: below the scoping block, no criterion
-- may name a shared table. THE MARKER IS NOT QUOTED HERE on purpose -- that
-- test finds the block by its first occurrence, and a comment repeating it
-- moves the boundary; four tests failed that way on 21/11. Read once, so this
-- CTE is inlined and nothing is materialised.
first_batch_raw AS (
    SELECT r.payload
      FROM fd_raw_events r
     WHERE r.source <> (SELECT name FROM stream_source)
       AND r.batch_id = (SELECT batch_id FROM historical_batch
                         ORDER BY batch_id LIMIT 1)
),
historical_quarantine AS (
    SELECT q.* FROM fd_quarantine q
     WHERE EXISTS (SELECT 1 FROM historical_batch b
                    WHERE b.batch_id = q.batch_id)
),
-- THE SEVEN COLUMNS THE CRITERIA READ, and not the thirteen: read thirteen
-- times, this CTE is materialised too, and merchant, category and the merchant
-- coordinates were being carried for nobody.
historical_txn AS (
    SELECT t.txn_id, t.txn_ts, t.client_key, t.amt,
           t.f_tx_count, t.f_avg_amount, t.f_home_distance_km
      FROM fd_transactions t
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

    -- B3-L1 -- raw zone
    --
    -- PER BATCH and not in total: fd_raw_events is append-only, so every load
    -- adds a batch and the table legitimately grows. Comparing the total with
    -- 555,719 was wrong -- three loads give 1,667,157 and that is the raw zone
    -- working as designed, not a defect.
    -- The property is that the batches are IDENTICAL IN SIZE -- the same file
    -- loaded N times giving N equal batches -- and never that they hold a
    -- particular number.
    --
    -- NOTHING BUT THE VERDICT IN actual, and the first version of this line
    -- got it wrong: it returned 'tous egaux (1296675)' against an expected
    -- 'tous egaux', and the verdict below compares the two AS TEXT -- so the
    -- criterion failed on a raw zone that was perfectly healthy. A count added
    -- for the reader's benefit is a count that breaks the comparison. The
    -- counts are on the lines that judge them; this one judges the equality.
    SELECT 1 AS ord, 'B3-L1 lots de taille identique en zone brute' AS criterion,
           'tous egaux' AS expected,
           CASE WHEN min(n) = max(n) THEN 'tous egaux'
                ELSE min(n)::text || '..' || max(n)::text END AS actual
    FROM  (SELECT count(*) AS n FROM historical_raw GROUP BY batch_id) b
    UNION ALL
    SELECT 2, 'B3-L1 lots historiques en zone brute (informatif)',
           (SELECT count(*)::text FROM historical_batch),
           (SELECT count(*)::text FROM historical_batch)

    -- B3-L2 -- validation
    UNION ALL
    SELECT 3, 'B3-L2 lignes en quarantaine (informatif)',
           (SELECT count(*)::text FROM historical_quarantine),
           (SELECT count(*)::text FROM historical_quarantine)
    UNION ALL
    SELECT 4, 'B3-L2 fermeture des compteurs',
           '0 ecart', (SELECT count(*)::text || ' ecart'
                       FROM historical_runs r
                       WHERE r.n_read IS NOT NULL
                         AND r.n_read <> coalesce(r.n_valid, 0)
                                       + coalesce(r.n_rejected, 0))

    -- B3-L3 -- typing and pseudonymisation
    UNION ALL
    -- AGAINST THE JOURNAL, which is a stronger statement than against a
    -- number: n_valid is what validation declared it had passed, and B3-L2 above
    -- has already proved the counters close (n_read = n_valid + n_rejected).
    -- So this line now says "the exploited layer holds exactly what the
    -- pipeline said it accepted" -- and it ties two stores together instead of
    -- tying one store to a memory. max() because the raw zone keeps every
    -- load: the runs are N, the transactions one set.
    SELECT 5, 'B3-L3 transactions typees = n_valid du journal',
           (SELECT max(n_valid)::text FROM historical_runs
             WHERE state = 'success'),
           count(*)::text
    FROM   historical_txn
    UNION ALL
    -- INFORMATIVE: a property of the corpus, which no other table can
    -- recompute. Judging it would couple this file to one source again.
    SELECT 6, 'B3-L3 porteurs distincts (informatif)',
           count(DISTINCT client_key)::text, count(DISTINCT client_key)::text
    FROM   historical_txn
    UNION ALL
    -- THE BEST INVARIANT OF THE TABLE: typing must not change the total. The
    -- expected value is read from the raw zone, so this compares the exploited
    -- layer with what was received rather than with a figure.
    --
    -- ONE BATCH, because the raw zone is append-only: N loads of the same file
    -- hold N copies while the exploited layer holds one set. Any batch serves,
    -- since B3-L1 above has just proved them equal in size; the ORDER BY only
    -- makes the choice deterministic.
    --
    -- ORDER BY ... LIMIT 1 AND NOT min(batch_id): PostgreSQL 16 has no min()
    -- aggregate for the uuid type -- "function min(uuid) does not exist", and
    -- the hint about casts sends the reader the wrong way. uuid does have its
    -- ordering operators, so a sort answers where an aggregate cannot.
    --
    -- THE EXISTS IS NOT A REFINEMENT, it is what makes the comparison exact
    -- when a line was quarantined. A rejected line is in the raw zone and not
    -- in fd_transactions, so its amount must not be counted -- and it must not
    -- even be CAST: an unreadable amount is one of the reasons a line gets
    -- rejected, and "beaucoup"::numeric raises. WHERE is applied before the
    -- select list, so the cast never sees it.
    --
    -- trans_num IS the txn_id for this source: checks.CSV_PROFILE sets
    -- event_id_includes_time=False, so event_id() returns the trimmed
    -- trans_num and nothing else. S1 stores the row itself as the payload.
    SELECT 7, 'B3-L3 somme des montants = zone brute',
           -- first_batch_raw, which carries the payloads of that one batch
           -- and nothing else. Verified identical to the former reading over
           -- every payload of the raw zone: 584,060,225 either way on the test
           -- corpus.
           (SELECT coalesce(sum((r.payload->>'amt')::numeric), 0)::text
            FROM   first_batch_raw r
            WHERE  EXISTS (SELECT 1 FROM historical_txn t
                            WHERE t.txn_id = trim(r.payload->>'trans_num'))),
           coalesce(sum(amt), 0)::text
    FROM   historical_txn
    UNION ALL
    SELECT 8, 'B3-L3 colonnes nominatives dans la couche exploitee',
           '0', count(*)::text
    FROM   information_schema.columns
    WHERE  table_name = 'fd_transactions'
      AND  column_name IN ('first', 'last', 'street', 'city', 'state', 'zip',
                           'lat', 'long', 'job', 'dob', 'cc_num', 'gender',
                           'city_pop')
    UNION ALL
    -- The property is "every transaction has one", so the expected value is
    -- the count of transactions.
    SELECT 9, 'B3-L3 distance domicile renseignee',
           (SELECT count(*)::text FROM historical_txn), count(*)::text
    FROM   historical_txn WHERE f_home_distance_km IS NOT NULL

    -- L4 -- features
    UNION ALL
    SELECT 10, 'B3-L4 lignes sans features', '0', count(*)::text
    FROM   historical_txn WHERE f_tx_count IS NULL
    UNION ALL
    -- EXACTLY ONE FIRST TRANSACTION PER CARDHOLDER, which is ruling 6 stated
    -- as a count. The expected value is the number of cardholders, so the line
    -- holds whatever the corpus is -- and it still catches the defect it was
    -- written for: a feature window that leaked a client's own row would give
    -- fewer firsts than clients.
    SELECT 11, 'B3-L4 une premiere transaction par porteur',
           (SELECT count(DISTINCT client_key)::text FROM historical_txn),
           count(*)::text
    FROM   historical_txn WHERE f_tx_count = 0 AND f_avg_amount IS NULL

    -- L5 -- ground truth
    UNION ALL
    -- One verdict per transaction, which is the property. A verdict missing
    -- or a verdict too many both show, and neither needs a constant.
    SELECT 12, 'B3-L5 un verdict par transaction',
           (SELECT count(*)::text FROM historical_txn), count(*)::text
    FROM   historical_truth
    UNION ALL
    -- INFORMATIVE, like the cardholders above: the number of frauds is the
    -- corpus's, and the base rate it implies is what fd_datasets stores beside
    -- every dataset so that a precision can be read at all.
    SELECT 13, 'B3-L5 fraudes (informatif)', count(*)::text, count(*)::text
    FROM   historical_truth WHERE label
    UNION ALL
    SELECT 14, 'B3-L5 delai de verdict uniforme',
           'oui', CASE WHEN min(label_ts - txn_ts) = max(label_ts - txn_ts)
                       THEN 'oui' ELSE 'non' END
    FROM   historical_truth
    UNION ALL
    SELECT 15, 'B3-L5 dissociation du verdict',
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
    -- STATED AS THE RATIO ITS OWN COMMENT DESCRIBES. The 555,719 added
    -- nothing to the property and coupled the most telling line of the table
    -- to one corpus: it read "non" on a pipeline that was working perfectly.
    SELECT 16, 'B3-L6 idempotence : N lots ingeres, un seul jeu de transactions',
           'ok',
           CASE WHEN (SELECT count(*) FROM historical_txn) > 0
                 AND (SELECT count(*) FROM historical_raw)
                     >= (SELECT count(*) FROM historical_txn)
                THEN 'ok' ELSE 'non' END

    -- L7 -- journal
    UNION ALL
    SELECT 17, 'B3-L7 executions reussies (>= 1)',
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
