-- Acceptance criteria of L5: what the database answers about the scoring service.
-- Covers: EC16, EC17, EC19, EC20   Lot: L5   Competency: C3, C5
--
-- The criterion of L5, as the development plan states it, has three clauses:
--   1. a third party obtains a score over HTTP knowing nothing of the stack;
--   2. THE CALL IS RETRIEVABLE IN THE DATABASE;
--   3. a promotion changes the version the health endpoint announces, with no
--      restart.
-- Clause 2 is here. Clauses 1 and 3 need HTTP and a container, so they live in
-- tools/check_l5.py.
--
-- READ IN THE CATALOGUE, NOT IN THE DDL, for the same reason as S6:
-- 145_scoring_service_role.sql says what the role was MEANT to be; this says
-- what PostgreSQL will let it do, including a grant added by hand to make a
-- page work. has_any_column_privilege() rather than has_table_privilege():
-- a SELECT on one column of fd_quarantine is a door, and a table-level test
-- would not see it.
--
-- THE CONNECT PRIVILEGE IS CRITERION 2, and not a detail of plumbing. On
-- 2026-11-20 the service answered 500 on /score -- permission denied for
-- database -- because 150_database_connect.sql grants CONNECT under an
-- IF EXISTS and the role was then created at 185, after it. The DDL numbering
-- is a dependency; this is the check that would have named the cause in one
-- line.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fd_scoring_service') THEN
        RAISE EXCEPTION 'L5 : le role fd_scoring_service n''existe pas dans cette base'
            USING HINT = 'Le DDL n''y a pas ete rejoue depuis L5 :  .\fd-manager.ps1 init';
    END IF;
END $$;

CREATE TEMP TABLE acceptance AS
WITH role AS (
    SELECT * FROM pg_roles WHERE rolname = 'fd_scoring_service'
),
-- The tables the service must not touch AT ALL. fd_monitoring is in the list
-- on purpose: a service that saw the verdicts could be asked whether it was
-- right, and that question belongs to the surveillance, over a window.
forbidden AS (
    SELECT unnest(ARRAY['fd_raw_events', 'fd_quarantine', 'fd_ground_truth',
                        'fd_datasets', 'fd_monitoring', 'fd_ops_state',
                        'fd_retrain_requests', 'fd_promotions']) AS relname
),
reachable AS (
    SELECT f.relname
      FROM forbidden f
     WHERE to_regclass('public.' || f.relname) IS NOT NULL
       AND (has_any_column_privilege('fd_scoring_service', f.relname, 'SELECT')
         OR has_any_column_privilege('fd_scoring_service', f.relname, 'INSERT')
         OR has_any_column_privilege('fd_scoring_service', f.relname, 'UPDATE')
         OR has_table_privilege('fd_scoring_service', f.relname, 'DELETE'))
),
-- Which columns of fd_transactions are readable. Three are granted by
-- 145_scoring_service_role.sql, and fd_client_features needs exactly those.
readable_txn AS (
    SELECT a.attname
      FROM pg_attribute a
     WHERE a.attrelid = 'public.fd_transactions'::regclass
       AND a.attnum > 0 AND NOT a.attisdropped
       AND has_column_privilege('fd_scoring_service', a.attrelid, a.attname, 'SELECT')
),
criteria AS (
    SELECT 1 AS ord, 'L5 role fd_scoring_service peut se connecter' AS criterion,
           'oui' AS expected,
           CASE WHEN (SELECT rolcanlogin FROM role) THEN 'oui' ELSE 'NON' END AS actual
    UNION ALL
    -- The 500 of 2026-11-20, in one criterion.
    SELECT 2, 'L5 privilege CONNECT sur cette base', 'oui',
           CASE WHEN has_database_privilege('fd_scoring_service',
                                            current_database(), 'CONNECT')
                THEN 'oui'
                ELSE 'NON : 150_database_connect.sql a tourne avant la creation du role'
           END
    UNION ALL
    SELECT 3, 'L5 pas d''heritage des privileges', 'oui',
           CASE WHEN (SELECT rolinherit FROM role) THEN 'NON' ELSE 'oui' END
    UNION ALL
    SELECT 4, 'L5 pas de superutilisateur ni de creation', 'oui',
           CASE WHEN (SELECT rolsuper OR rolcreatedb OR rolcreaterole FROM role)
                THEN 'NON' ELSE 'oui' END
    UNION ALL
    SELECT 5, 'L5 limite de connexions declaree', '20',
           (SELECT rolconnlimit::text FROM role)
    UNION ALL
    SELECT 6, 'L5 colonnes de fd_transactions lisibles', 'amt, client_key, txn_ts',
           (SELECT string_agg(attname, ', ' ORDER BY attname) FROM readable_txn)
    UNION ALL
    SELECT 7, 'L5 aucune ecriture sur fd_transactions', 'oui',
           CASE WHEN has_any_column_privilege('fd_scoring_service',
                                              'fd_transactions', 'INSERT')
                  OR has_any_column_privilege('fd_scoring_service',
                                              'fd_transactions', 'UPDATE')
                  OR has_table_privilege('fd_scoring_service',
                                         'fd_transactions', 'DELETE')
                THEN 'NON' ELSE 'oui' END
    UNION ALL
    SELECT 8, 'L5 aucune ecriture sur fd_scores', 'oui',
           CASE WHEN has_any_column_privilege('fd_scoring_service',
                                              'fd_scores', 'INSERT')
                  OR has_any_column_privilege('fd_scoring_service',
                                              'fd_scores', 'UPDATE')
                THEN 'NON' ELSE 'oui' END
    UNION ALL
    SELECT 9, 'L5 tables interdites atteignables', 'aucune',
           coalesce((SELECT string_agg(relname, ', ' ORDER BY relname)
                       FROM reachable), 'aucune')
    UNION ALL
    SELECT 10, 'L5 journal fd_scoring_service_calls present', 'oui',
           CASE WHEN to_regclass('public.fd_scoring_service_calls') IS NOT NULL
                THEN 'oui' ELSE 'NON' END
    UNION ALL
    -- Append only: the service may add a call and may not revise one.
    SELECT 11, 'L5 journal en ajout seul pour le service', 'oui',
           CASE WHEN to_regclass('public.fd_scoring_service_calls') IS NULL THEN 'indecidable'
                WHEN has_any_column_privilege('fd_scoring_service',
                                              'fd_scoring_service_calls', 'UPDATE')
                  OR has_table_privilege('fd_scoring_service',
                                         'fd_scoring_service_calls', 'DELETE')
                THEN 'NON' ELSE 'oui' END
    UNION ALL
    SELECT 12, 'L5 le service peut ecrire son journal', 'oui',
           CASE WHEN to_regclass('public.fd_scoring_service_calls') IS NULL THEN 'indecidable'
                WHEN has_any_column_privilege('fd_scoring_service',
                                              'fd_scoring_service_calls', 'INSERT')
                THEN 'oui' ELSE 'NON' END
    UNION ALL
    -- The invariant fd_scores carries too: a decision and its threshold cannot
    -- disagree, whichever journal holds them.
    SELECT 13, 'L5 contrainte flagged = (score >= threshold)', 'oui',
           CASE WHEN EXISTS (
                    SELECT 1 FROM pg_constraint
                     WHERE conrelid = to_regclass('public.fd_scoring_service_calls')
                       AND contype = 'c'
                       AND pg_get_constraintdef(oid) LIKE '%flagged%'
                       AND pg_get_constraintdef(oid) LIKE '%threshold%')
                THEN 'oui' ELSE 'NON' END
    UNION ALL
    SELECT 14, 'L5 vue fd_scoring_decisions presente', 'oui',
           CASE WHEN to_regclass('public.fd_scoring_decisions') IS NOT NULL
                THEN 'oui' ELSE 'NON' END
    UNION ALL
    -- The view is what readers must query. A reader without the grant would
    -- go back to the tables, and the convergence to one journal would break it.
    SELECT 15, 'L5 vue lisible par fd_report et fd_dashboard', 'oui, oui',
           CASE WHEN to_regclass('public.fd_scoring_decisions') IS NULL THEN 'indecidable'
                ELSE concat_ws(', ',
                    CASE WHEN has_table_privilege('fd_report', 'fd_scoring_decisions',
                                                  'SELECT') THEN 'oui' ELSE 'NON' END,
                    CASE WHEN has_table_privilege('fd_dashboard', 'fd_scoring_decisions',
                                                  'SELECT') THEN 'oui' ELSE 'NON' END)
           END
    UNION ALL
    -- Nothing is lost and nothing is counted twice when the two journals are
    -- read as one. The arithmetic is the whole claim of option 2.
    SELECT 16, 'L5 la vue compte les deux journaux sans doublon', 'oui',
           CASE WHEN to_regclass('public.fd_scoring_decisions') IS NULL THEN 'indecidable'
                WHEN (SELECT count(*) FROM fd_scoring_decisions)
                   = (SELECT count(*) FROM fd_scores)
                   + (SELECT count(*) FROM fd_scoring_service_calls)
                THEN 'oui' ELSE 'NON' END
    UNION ALL
    -- Which side a row came from must be readable without a join: a pipeline
    -- row carries txn_id and no call_id, a third-party row the reverse.
    SELECT 17, 'L5 chaque ligne de la vue est attribuable a une source', 'oui',
           CASE WHEN to_regclass('public.fd_scoring_decisions') IS NULL THEN 'indecidable'
                WHEN EXISTS (SELECT 1 FROM fd_scoring_decisions
                              WHERE (source = 'pipeline'
                                     AND (txn_id IS NULL OR call_id IS NOT NULL))
                                 OR (source = 'tiers'
                                     AND (call_id IS NULL OR txn_id IS NOT NULL)))
                THEN 'NON' ELSE 'oui' END
    UNION ALL
    -- A decision whose model nobody can name is not traceable to its training
    -- rows, which is what M8 exists for. Asked of the SCHEMA and not of the
    -- rows: the two columns are NOT NULL, so a scan could not fail and would
    -- read as a check while testing nothing. What can still be lost is the
    -- constraint itself, in a migration.
    SELECT 18, 'L5 modele et recette obligatoires dans le journal', 'oui, oui',
           CASE WHEN to_regclass('public.fd_scoring_service_calls') IS NULL THEN 'indecidable'
                ELSE (SELECT string_agg(CASE WHEN a.attnotnull THEN 'oui' ELSE 'NON' END,
                                        ', ' ORDER BY a.attname)
                        FROM pg_attribute a
                       WHERE a.attrelid = 'public.fd_scoring_service_calls'::regclass
                         AND a.attname IN ('model_version', 'recipe_digest'))
           END
    UNION ALL
    -- THE VIEW IS THE ONLY WAY IN, and this is asked of the catalogue because
    -- a file test would only say what the DDL writes. A view runs with its
    -- OWNER's privileges unless security_invoker says otherwise, so a reader
    -- needs nothing on the two journals -- and holding one anyway walks around
    -- the narrowing the view performs: it exposes the nine columns of a
    -- decision, while fd_scoring_service_calls also holds client_key,
    -- merchant, category, amt and the three history features.
    --
    -- Measured 21/11/2026: both grants were there, and s6_acceptance.sql is
    -- what refused them.
    SELECT 19, 'L5 aucun lecteur ne tient le journal des appels en direct', 'aucun',
           CASE WHEN to_regclass('public.fd_scoring_service_calls') IS NULL
                THEN 'indecidable'
                ELSE coalesce((SELECT string_agg(r, ', ' ORDER BY r)
                                 FROM (SELECT unnest(ARRAY['fd_report',
                                                           'fd_dashboard']) AS r) x
                                WHERE has_any_column_privilege(
                                          x.r, 'fd_scoring_service_calls',
                                          'SELECT')),
                              'aucun')
           END
    UNION ALL
    SELECT 20, 'info appels tiers enregistres', 'mesure',
           coalesce((SELECT count(*)::text FROM fd_scoring_service_calls), '0')
    UNION ALL
    SELECT 21, 'info decisions vues comme un seul journal', 'mesure',
           coalesce((SELECT string_agg(source || ' ' || n, ', ' ORDER BY source)
                       FROM (SELECT source, count(*)::text AS n
                               FROM fd_scoring_decisions GROUP BY source) s),
                    'aucune')
    UNION ALL
    SELECT 22, 'info versions de modele presentes dans le journal des appels', 'mesure',
           coalesce((SELECT string_agg(DISTINCT model_version, ', ')
                       FROM fd_scoring_service_calls), 'aucune')
)
SELECT ord, criterion, expected, actual,
       CASE WHEN expected = 'mesure' THEN 'info'
            WHEN expected = actual THEN 'OK'
            ELSE '<<< ECHEC' END AS verdict
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
        RAISE EXCEPTION 'L5 : % critere(s) en echec : %', failed, names
            USING HINT = 'Le tableau ci-dessus donne attendu et constate pour chacun.';
    END IF;
END $$;
