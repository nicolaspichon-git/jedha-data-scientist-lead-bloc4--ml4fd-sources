-- Acceptance criteria of S6: what fd_dashboard IS, read in the catalogue.
-- Covers: EC19, EC27   Lot: R5   Competency: C4, C5
--
-- READ IN THE CATALOGUE, NOT IN THE DDL. 140_dashboard_role.sql says what the
-- role was meant to be; this says what PostgreSQL will actually let it do --
-- including what PUBLIC grants, a column grant added by hand, or a membership
-- somebody added to "fix" a page. has_any_column_privilege() is used rather
-- than has_table_privilege() for that reason: a SELECT granted on one column
-- of fd_quarantine -- its payload, say -- is a door, and a table-level test
-- would not see it.
--
-- Same form as the acceptance of S1 to S5: a temporary table, displayed, then
-- an exception if any criterion fails, so psql exits non-zero (it exits 0
-- whatever a verdict column says).

-- Without the role, every privilege function below would raise on an unknown
-- name. Say so once, with the remedy.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fd_dashboard') THEN
        RAISE EXCEPTION 'S6 : le role fd_dashboard n''existe pas dans cette base'
            USING HINT = 'Le DDL n''y a pas ete rejoue depuis R0 :  .\fd-manager.ps1 init';
    END IF;
END $$;

CREATE TEMP TABLE acceptance AS
WITH role AS (
    SELECT * FROM pg_roles WHERE rolname = 'fd_dashboard'
),
tables AS (
    SELECT c.oid, c.relname
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'public'
       AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
),
readable AS (
    SELECT relname FROM tables
     WHERE has_any_column_privilege('fd_dashboard', oid, 'SELECT')
),
writable AS (
    SELECT relname FROM tables
     WHERE has_table_privilege('fd_dashboard', oid,
                               'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
        OR has_any_column_privilege('fd_dashboard', oid,
                                    'INSERT, UPDATE, REFERENCES')
),
assumable AS (
    -- Roles fd_dashboard could become with SET ROLE. Itself excluded.
    SELECT r.rolname FROM pg_roles r
     WHERE r.rolname <> 'fd_dashboard'
       AND pg_has_role('fd_dashboard', r.oid, 'MEMBER')
),
sequences AS (
    -- THE CASE IS NOT DECORATION. PostgreSQL does not promise the order in
    -- which a WHERE clause is evaluated: the first version put the relkind
    -- filter and has_sequence_privilege() side by side, and the planner called
    -- the function on pg_statistic first -- `"pg_statistic" is not a
    -- sequence`. Only CASE guarantees that the test runs before the call.
    SELECT c.relname FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'public'
       AND CASE WHEN c.relkind = 'S'
                THEN has_sequence_privilege('fd_dashboard', c.oid,
                                            'USAGE, SELECT, UPDATE')
                ELSE false END
),
criteria AS (
    SELECT 1 AS ord, 'R0 fd_dashboard peut se connecter (LOGIN)' AS criterion,
           'oui' AS expected,
           (SELECT CASE WHEN rolcanlogin THEN 'oui' ELSE 'non' END FROM role) AS actual
    UNION ALL
    SELECT 2, 'R0 aucun attribut d''administration', 'aucun',
           (SELECT coalesce(nullif(concat_ws(', ',
                    CASE WHEN rolsuper       THEN 'SUPERUSER'   END,
                    CASE WHEN rolcreatedb    THEN 'CREATEDB'    END,
                    CASE WHEN rolcreaterole  THEN 'CREATEROLE'  END,
                    CASE WHEN rolreplication THEN 'REPLICATION' END,
                    CASE WHEN rolbypassrls   THEN 'BYPASSRLS'   END), ''),
                    'aucun') FROM role)
    UNION ALL
    SELECT 3, 'R0 membre d''aucun role', '0',
           (SELECT count(*)::text FROM pg_auth_members m
              JOIN role ON m.member = role.oid)
    UNION ALL
    SELECT 4, 'R0 mot de passe SCRAM applique (sinon il ne peut pas se connecter)',
           'oui',
           (SELECT CASE WHEN a.rolpassword LIKE 'SCRAM-SHA-256$%' THEN 'oui'
                        WHEN a.rolpassword IS NULL THEN 'AUCUN : .\fd-manager.ps1 init'
                        ELSE 'autre format' END
              FROM pg_authid a WHERE a.rolname = 'fd_dashboard')
    UNION ALL
    SELECT 5, 'R0 tables lisibles, colonnes comprises',
           'fd_datasets, fd_frauds, fd_ground_truth, fd_ops_notifications, fd_ops_state, fd_pipeline_runs, fd_quarantine, fd_reports, fd_scores, fd_transactions',
           (SELECT coalesce(string_agg(relname, ', ' ORDER BY relname), 'aucune')
              FROM readable)
    UNION ALL
    SELECT 6, 'R0 tables modifiables, colonnes comprises', 'aucune',
           (SELECT coalesce(string_agg(relname, ', ' ORDER BY relname), 'aucune')
              FROM writable)
    UNION ALL
    SELECT 7, 'R0 acces a la zone brute', 'aucun',
           (SELECT coalesce(string_agg(relname, ', ' ORDER BY relname), 'aucun')
              FROM readable
             WHERE relname = 'fd_raw_events')
    UNION ALL
    -- R6, R7: the quarantine is readable BY COLUMN -- the volume invariant
    -- and the breakdown by reason.
    -- `payload` (the raw rejected record) and `reason_detail` (which may quote a
    -- raw value) must never be among them. Listed by name, so a column added
    -- by hand shows up as a difference rather than passing unseen.
    SELECT 8, 'R6 colonnes lisibles de la quarantaine',
           'batch_id, line_no, reason_class, reason_code, rejected_at',
           (SELECT coalesce(string_agg(a.attname, ', ' ORDER BY a.attname), 'aucune')
              FROM pg_attribute a
             WHERE a.attrelid = 'fd_quarantine'::regclass
               AND a.attnum > 0 AND NOT a.attisdropped
               AND has_column_privilege('fd_dashboard', a.attrelid, a.attnum,
                                        'SELECT'))
    UNION ALL
    -- R10: the catalogue BY COLUMN. Not the paths of the Parquet files nor the
    -- feature list; listed by name for the same reason as the quarantine.
    SELECT 8.5, 'R10 colonnes lisibles du catalogue des jeux',
           'base_rate_test, created_at, measured_at, recipe_digest, test_alerts_per_day, test_precision, test_recall, test_threshold, version',
           (SELECT coalesce(string_agg(a.attname, ', ' ORDER BY a.attname), 'aucune')
              FROM pg_attribute a
             WHERE a.attrelid = 'fd_datasets'::regclass
               AND a.attnum > 0 AND NOT a.attisdropped
               AND has_column_privilege('fd_dashboard', a.attrelid, a.attnum,
                                        'SELECT'))
    UNION ALL
    SELECT 9, 'R0 roles qu''il peut endosser (SET ROLE)', 'aucun',
           (SELECT coalesce(string_agg(rolname, ', ' ORDER BY rolname), 'aucun')
              FROM assumable)
    UNION ALL
    SELECT 10, 'R0 creation dans le schema public', 'non',
           CASE WHEN has_schema_privilege('fd_dashboard', 'public', 'CREATE')
                THEN 'oui' ELSE 'non' END
    UNION ALL
    SELECT 11, 'R0 sequences utilisables', 'aucune',
           (SELECT coalesce(string_agg(relname, ', ' ORDER BY relname), 'aucune')
              FROM sequences)
    UNION ALL
    SELECT 12, 'info limite de connexions', 'mesure',
           (SELECT rolconnlimit::text FROM role)
    UNION ALL
    -- Recorded in the plan of S6 (7.4) rather than revoked: PUBLIC holds TEMP
    -- on the database, revoking it touches every role of every step, and a
    -- temporary table opens no data.
    SELECT 13, 'info tables temporaires permises (via PUBLIC, consigne)', 'mesure',
           CASE WHEN has_database_privilege('fd_dashboard', current_database(), 'TEMP')
                THEN 'oui' ELSE 'non' END
    UNION ALL
    SELECT 14, 'info connexions ouvertes par fd_dashboard en ce moment', 'mesure',
           (SELECT count(*)::text FROM pg_stat_activity
             WHERE usename = 'fd_dashboard')
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
        RAISE EXCEPTION 'S6 : % critere(s) en echec : %', failed, names
            USING HINT = 'Le tableau ci-dessus donne attendu et constate pour chacun.';
    END IF;
END $$;
