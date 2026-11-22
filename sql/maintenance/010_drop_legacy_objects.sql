-- One-off cleanup: the roles and tables left by the pre-fd_ naming.
-- Block: B3, B4.
-- Lot: B3-L0, B4-L0.
-- Competency: B3-C4.
-- Covers: U/D.
--
-- Run it with:  .\fd-manager.ps1 cleanup
--
-- A role cannot be dropped while anything depends on it, and privileges count as
-- dependencies. The documented order is REASSIGN OWNED (hand over what the role
-- owns), then DROP OWNED (remove the privileges granted TO it), then DROP ROLE.
-- Skipping the first two gives
--   ERROR: role "ingest" cannot be dropped because some objects depend on it
--
-- This script is idempotent and REFUSES to drop a table that holds rows. An
-- empty legacy table is a leftover; a populated one is data, and deleting data
-- is not a cleanup.
--
-- NOTE: DROP OWNED BY only acts on the CURRENT database. If the DDL was ever
-- applied elsewhere, run this there too.

\echo '--- 1. What still depends on the legacy roles'

SELECT r.rolname,
       (SELECT count(*) FROM pg_class c
         WHERE c.relowner = r.oid)                       AS owned_objects,
       (SELECT count(*) FROM information_schema.table_privileges p
         WHERE p.grantee = r.rolname)                    AS granted_privileges
FROM   pg_roles r
WHERE  r.rolname IN ('ingest', 'report')
ORDER  BY r.rolname;

\echo '--- 2. Legacy tables, and whether they hold anything'

SELECT c.relname,
       (xpath('/row/c/text()',
              query_to_xml(format('SELECT count(*) AS c FROM public.%I', c.relname),
                           false, true, '')))[1]::text::bigint AS rows
FROM   pg_class c
JOIN   pg_namespace n ON n.oid = c.relnamespace
WHERE  n.nspname = 'public' AND c.relkind = 'r'
  AND  c.relname IN ('raw_events', 'quarantine', 'transactions',
                     'ground_truth', 'pipeline_runs')
ORDER  BY c.relname;

\echo '--- 3. Dropping the legacy tables, only if empty'

DO $$
DECLARE
    t       text;
    n       bigint;
    -- ground_truth first: it carries a foreign key to transactions, so the
    -- other order fails on a dependency rather than on a missing table.
    legacy  text[] := ARRAY['ground_truth', 'transactions', 'quarantine',
                            'raw_events', 'pipeline_runs'];
BEGIN
    FOREACH t IN ARRAY legacy LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_class c
                       JOIN pg_namespace ns ON ns.oid = c.relnamespace
                       WHERE ns.nspname = 'public' AND c.relname = t
                         AND c.relkind = 'r') THEN
            RAISE NOTICE 'table % already absent', t;
            CONTINUE;
        END IF;
        EXECUTE format('SELECT count(*) FROM public.%I', t) INTO n;
        IF n > 0 THEN
            RAISE WARNING 'table % holds % row(s): KEPT. Deleting data is not a '
                          'cleanup -- empty it yourself if that is what you want.',
                          t, n;
        ELSE
            EXECUTE format('DROP TABLE public.%I', t);
            RAISE NOTICE 'table % dropped (was empty)', t;
        END IF;
    END LOOP;
END
$$;

\echo '--- 4. Dropping the legacy roles'

DO $$
DECLARE
    r text;
BEGIN
    FOREACH r IN ARRAY ARRAY['ingest', 'report'] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
            RAISE NOTICE 'role % already absent', r;
            CONTINUE;
        END IF;
        -- Hand over anything the role owns, then strip its privileges, then
        -- drop it. Each step removes one class of dependency.
        EXECUTE format('REASSIGN OWNED BY %I TO %I', r, current_user);
        EXECUTE format('DROP OWNED BY %I', r);
        EXECUTE format('DROP ROLE %I', r);
        RAISE NOTICE 'role % dropped', r;
    END LOOP;
END
$$;

\echo '--- 5. What remains'

SELECT rolname FROM pg_roles
WHERE  rolname IN ('ingest', 'report', 'fd_ingest', 'fd_report')
ORDER  BY rolname;

SELECT c.relname FROM pg_class c
JOIN   pg_namespace n ON n.oid = c.relnamespace
WHERE  n.nspname = 'public' AND c.relkind = 'r'
ORDER  BY c.relname;
