-- The acceptance criteria of S2, lots M0 to M4. Every verdict must read OK.
-- Covers: EC05, EC23, EI02, EI04   Lot: M8   Competency: C1, C2, C5
--
-- The decisive one is the fourth: n_train + n_test must equal the number of
-- rows whose VERDICT WAS KNOWN at the cut-off. If the build had filtered on the
-- transaction date instead of label_ts, that count would be larger -- and the
-- recall of the model trained on it would be optimistic by exactly the
-- difference. The check compares the registered counts with what the data says.
--
-- test_end is derived from the split rule string rather than duplicated: the
-- rule is the recipe's identity, so reading it back is the only honest source.
--
-- The horizon is written here as a literal, mirroring labels.horizon in
-- governance.yml. A duplication, and a deliberate one: an acceptance check that
-- read its expected value from the thing it checks would verify nothing.

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
known AS (
    SELECT count(*) AS n,
           count(*) FILTER (WHERE t.txn_ts <  l.train_end) AS n_train,
           count(*) FILTER (WHERE t.txn_ts >= l.train_end) AS n_test
    FROM   latest l
    JOIN   fd_transactions t ON t.txn_ts < l.test_end
    JOIN   fd_ground_truth g ON g.txn_id = t.txn_id AND g.label_ts <= l.cut_off
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
    SELECT 3, 'M2 coupure dans l''horizon d''etiquetage', 'oui',
           CASE WHEN cut_off <= '2020-12-22 23:13:39+00'::timestamptz
                THEN 'oui' ELSE 'NON : ' || cut_off::text END
    FROM   latest
    UNION ALL
    SELECT 4, 'M3 n_train + n_test = lignes au verdict CONNU',
           (SELECT n::text FROM known),
           (SELECT (l.n_train + l.n_test)::text FROM latest l)
    UNION ALL
    SELECT 5, 'M3 repartition train conforme a la regle',
           (SELECT n_train::text FROM known), n_train::text FROM latest
    UNION ALL
    SELECT 6, 'M3 repartition test conforme a la regle',
           (SELECT n_test::text FROM known), n_test::text FROM latest
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
