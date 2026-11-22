-- The acceptance criteria of S2, lots M0 to M4. Every verdict must read OK.
-- Block: B3, B4.
-- Lot: B3-M8, B4-L0.
-- Competency: B3-C1, B3-C2, B3-C5.
-- Covers: B3-EC05, B3-EC23, B3-EI02, B3-EI04.
--
-- The decisive one is the sixth: the registered counts must NOT EXCEED the rows
-- whose verdict was attributable to the cut-off. If the build had filtered on
-- the transaction date instead of label_ts, it would have used rows labelled
-- later -- and the recall of the model trained on them would be optimistic by
-- exactly the difference.
--
-- BY SOURCE SINCE L5 (21/11/2026), and an inequality on one side. Until then
-- this file recomputed ONE count, split by date, and demanded equality. Three
-- criteria then failed on the first dataset built with the stream admitted,
-- and none of the three was a defect of the data:
--
--   * sql/datasets/training_set.sql sends EVERY stream row to train, bounded
--     only by the cut-off, and bounds the HISTORICAL rows by the horizon as
--     well. A recomputation that splits by txn_ts puts the whole of November
--     2026 on the test side, which is how the test count read 74,343 against
--     47,624 registered. Two rules, not two datasets.
--   * the verdict ROW is written by a daily DAG while its label_ts is the
--     instant the verdict FELL DUE. So "the rows whose verdict was known at
--     the cut-off" is not a stable set: it grows after a build, as
--     dag_ingest_verdicts catches up, with label_ts values already in the
--     past. A recomputation hours later legitimately finds more rows than the
--     build could see.
--   * the horizon literal below was fraudTest.csv's, the corpus this project
--     abandoned at the swap. It was never updated.
--
-- Hence: the TEST side is pure corpus and frozen, so it stays an equality. The
-- TRAIN side mixes corpus and stream and fd_datasets records no breakdown of
-- it, so it is bounded on both sides -- it must hold the whole labelled corpus
-- and must not exceed corpus + attributable stream. A build that used later
-- information breaks the upper bound; a build that lost the corpus breaks the
-- lower one. Equality there would mean demanding that no verdict has been
-- recorded since the build, which nothing guarantees and nothing should.
--
-- test_end is derived from the split rule string rather than duplicated: the
-- rule is the recipe's identity, so reading it back is the only honest source.
--
-- The horizon is written here as a literal, mirroring labels.horizon in
-- governance.yml. A duplication, and a deliberate one: an acceptance check that
-- read its expected value from the thing it checks would verify nothing. A
-- literal that outlives a corpus swap is the cost of it, paid once here.

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
WITH latest AS (
    SELECT *,
           (split_part(split_rule, 'test<=', 2)::date + 1)::timestamptz  AS test_end,
           (split_part(split_part(split_rule, 'train<=', 2), ',', 1)::date + 1)::timestamptz
                                                                         AS train_end
    FROM   fd_datasets
    ORDER  BY created_at DESC
    LIMIT  1
),
-- The batches of the stream, named once because `labelled` below needs the
-- source PER ROW and not as a filter. Reading fd_raw_events is not the cost
-- here: measured, that scan is 73 ms, and an index on (source, batch_id) made
-- it SLOWER -- a bitmap heap scan over rows carrying a jsonb payload instead of
-- a parallel sequential scan. What this query depends on is the index on
-- fd_transactions (batch_id), added to the DDL on 21/11/2026: without it the
-- planner rescans the whole table once per batch.
stream_batches AS (
    SELECT DISTINCT batch_id
    FROM   fd_raw_events
    WHERE  source = 'current-transactions'
),
-- BY SOURCE AND NEVER BY A DATE, for the reason training_set.sql gives: telling
-- the two apart by txn_ts works today and breaks the day a corpus is replayed
-- with recent timestamps.
--
-- THE WHERE IS training_set.sql's WHERE, clause for clause, and that is the
-- point: the recomputation must restrict the same rows the build restricted.
--
-- A LEFT JOIN AND NOT AN EXISTS, measured. Two earlier versions of this CTE
-- cost more than the whole check: the first carried no predicate at all and
-- materialised the 1.3 M x 1.2 M join before discarding anything; the second
-- pushed the predicates into the join and was no better, because label_ts <=
-- cut_off with a 2026 cut-off discards nothing from a corpus labelled in 2020.
-- The cost was never the join -- that is 460 ms -- it was the EXISTS, evaluated
-- once per row and TWICE per row here, since the CASE below needs it too:
-- over 100 seconds against 688 ms for the join below. One hash build of 748
-- batch identifiers, one probe per row, and from_stream falls out of the same
-- join instead of being asked for a second time.
labelled AS (
    SELECT t.txn_ts,
           g.label_ts,
           (b.batch_id IS NOT NULL) AS from_stream
    FROM   latest l
    JOIN   fd_transactions t ON true
    JOIN   fd_ground_truth g ON g.txn_id = t.txn_id
    LEFT   JOIN stream_batches b ON b.batch_id = t.batch_id
    WHERE  g.label_ts <= l.cut_off
      -- The stream is labelled as it goes, so the cut-off above is the only
      -- bound it needs. The corpus is trustworthy only up to the horizon, and
      -- the recipe's own test_end bounds it besides.
      AND  (b.batch_id IS NOT NULL
            OR (t.txn_ts < l.test_end
                AND g.label_ts <= '2020-06-21 03:59:46+00'::timestamptz))
),
known AS (
    SELECT
      count(*) FILTER (WHERE NOT r.from_stream
                         AND r.txn_ts <  l.train_end)   AS corpus_train,
      count(*) FILTER (WHERE NOT r.from_stream
                         AND r.txn_ts >= l.train_end)   AS corpus_test,
      -- All to train, never to test: the gate of L3 compares a candidate to the
      -- champion on ONE test set, and a test set that moved with every
      -- retraining would make the published figures incomparable.
      count(*) FILTER (WHERE r.from_stream)              AS stream_train
    FROM   latest l, labelled r
),
criteria AS (
    SELECT 1 AS ord, 'M0 jeux enregistres (>= 1)' AS criterion,
           'ok' AS expected,
           CASE WHEN count(*) >= 1 THEN 'ok' ELSE 'aucun' END AS actual
    FROM   fd_datasets
    UNION ALL
    SELECT 2, 'M2 empreinte de recette sur 64 hexa', '64',
           length(recipe_digest)::text FROM latest
    UNION ALL
    -- ASKED OF THE DATA AND NOT OF THE SETTING. The rule
    -- datasets.assert_cut_off_usable applies since 20/11 is not "the cut-off is
    -- within the horizon" but "a cut-off beyond the horizon is admissible
    -- BECAUSE the stream is labelled as it goes". Expressed here on the set
    -- itself, so it also catches the failure of 20/11 that no setting showed: a
    -- present-day cut-off on a dataset holding not one stream row -- the
    -- retraining that refitted the very same Parquet files.
    SELECT 3, 'M2 coupure au-dela de l''horizon admise par le flux', 'oui',
           CASE WHEN l.cut_off <= '2020-06-21 03:59:46+00'::timestamptz
                  THEN 'oui'
                WHEN (SELECT stream_train FROM known) > 0
                  THEN 'oui'
                ELSE 'NON : coupure ' || l.cut_off::text
                     || ' et aucune ligne de flux dans le jeu'
           END
    FROM   latest l
    UNION ALL
    -- PURE CORPUS AND FROZEN, so an equality. The stream never reaches the test
    -- side, and the corpus stopped being written in S1.
    -- FROM latest, so the three bounds VANISH when no dataset is registered
    -- rather than reading ECHEC on nulls: criterion 1 already names that cause,
    -- and three more lines saying it differently help nobody.
    SELECT 4, 'M3 cote test = corpus etiquete du cote test',
           (SELECT corpus_test::text FROM known), n_test::text
    FROM   latest
    UNION ALL
    -- THE LOWER BOUND: the labelled corpus must be there whole. A build that
    -- lost part of it would train on less than it claims.
    SELECT 5, 'M3 cote train contient tout le corpus etiquete', 'oui',
           CASE WHEN l.n_train >= (SELECT corpus_train FROM known)
                THEN 'oui'
                ELSE 'NON : ' || l.n_train::text || ' < '
                     || (SELECT corpus_train::text FROM known)
           END
    FROM   latest l
    UNION ALL
    -- THE UPPER BOUND, AND THE DECISIVE CRITERION. Exceeding corpus + stream
    -- attributable to the cut-off means the build used rows whose verdict was
    -- not knowable then -- the optimistic recall this whole file exists for.
    SELECT 6, 'M3 cote train n''excede pas corpus + flux attribuable', 'oui',
           CASE WHEN l.n_train <= (SELECT corpus_train + stream_train FROM known)
                THEN 'oui'
                ELSE 'NON : ' || l.n_train::text || ' > '
                     || (SELECT (corpus_train + stream_train)::text FROM known)
           END
    FROM   latest l
    UNION ALL
    SELECT 7, 'M4 regle de decoupage temporelle', 'oui',
           CASE WHEN split_rule LIKE 'temporal:%' THEN 'oui'
                ELSE 'NON : ' || split_rule END
    FROM   latest
    UNION ALL
    SELECT 8, 'M4 positifs du cote entrainement (> 0)', 'ok',
           CASE WHEN n_train_pos > 0 THEN 'ok' ELSE 'AUCUN' END FROM latest
    UNION ALL
    SELECT 9, 'M4 positifs du cote test (> 0)', 'ok',
           CASE WHEN n_test_pos > 0 THEN 'ok' ELSE 'AUCUN' END FROM latest
    UNION ALL
    SELECT 10, 'M4 taux de base enregistres', 'ok',
           CASE WHEN base_rate_train > 0 AND base_rate_test > 0 THEN 'ok'
                ELSE 'MANQUANT' END FROM latest
    UNION ALL
    SELECT 11, 'M4 aucune transaction des deux cotes', '0',
           CASE WHEN train_end <= test_end THEN '0' ELSE 'CHEVAUCHEMENT' END
    FROM   latest
    -- Informative, and the figures that must accompany any metric later.
    UNION ALL
    SELECT 12, 'info version du jeu', version, version FROM latest
    UNION ALL
    SELECT 13, 'info coupure', cut_off::text, cut_off::text FROM latest
    UNION ALL
    SELECT 14, 'info taux de base entrainement',
           round(100 * base_rate_train, 4)::text || ' %',
           round(100 * base_rate_train, 4)::text || ' %' FROM latest
    UNION ALL
    SELECT 15, 'info taux de base test',
           round(100 * base_rate_test, 4)::text || ' %',
           round(100 * base_rate_test, 4)::text || ' %' FROM latest
    UNION ALL
    SELECT 16, 'info features', array_length(features, 1)::text || ' colonnes',
           array_length(features, 1)::text || ' colonnes' FROM latest
    -- The arithmetic of criteria 4 to 6, in one line: without it an operator
    -- reading a bound cannot see which side moved.
    UNION ALL
    -- FROM latest, like the information lines above it, and not a scalar
    -- subquery: with no dataset registered the row must VANISH rather than
    -- appear holding nulls, which compare unequal and read as a failure. Found
    -- on an empty cluster -- the only place the difference shows.
    SELECT 17, 'info recompte par source (corpus train / test / flux)',
           (SELECT corpus_train || ' / ' || corpus_test || ' / ' || stream_train
              FROM known),
           (SELECT corpus_train || ' / ' || corpus_test || ' / ' || stream_train
              FROM known)
    FROM   latest
    UNION ALL
    SELECT 18, 'info enregistre (train / test)',
           n_train || ' / ' || n_test, n_train || ' / ' || n_test
    FROM   latest
)
SELECT ord, criterion, expected, actual,
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
        RAISE EXCEPTION 'S2 : % critere(s) en echec : %', failed, names
            USING HINT = 'Le tableau ci-dessus donne attendu et constate pour chacun.';
    END IF;
END $$;
