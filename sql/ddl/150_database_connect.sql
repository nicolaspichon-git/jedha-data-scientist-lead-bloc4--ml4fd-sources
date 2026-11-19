-- WHO MAY CONNECT TO THE BUSINESS DATABASE. Closed by default, opened by name.
-- Covers: EC18, EC22   Lot: L1   Competency: C3
--
-- PostgreSQL grants CONNECT on a new database to PUBLIC, that is to every
-- role the cluster will ever hold. fd_dashboard relied on that default: its
-- own file (140) grants it SELECT on eight tables but never CONNECT. Closing
-- the door therefore means naming everyone who goes through it, in this file,
-- after 140 has created the role -- which the numbering guarantees.
--
-- WHY NOT IN 140. That file deliberately avoids anything touching roles other
-- than fd_dashboard: its header says so about REVOKE ALL ON SCHEMA public.
-- A revocation aimed at PUBLIC belongs to the database, not to a role.
--
-- WHY NOT IN sql/initdb. The service databases are closed there, at first
-- boot, because they are created there. This one must wait for fd_dashboard to
-- exist, and `init` is where the roles are built.
--
-- NO LITERAL DATABASE NAME. The name comes from POSTGRES_DB, and the hyphen in
-- "ml4fd-1" is not a valid unquoted identifier. current_database() with
-- format('%I') quotes whatever the name turns out to be -- so this file does
-- not have to be edited when it changes, and it cannot be defeated by a name
-- that needs quoting.
--
-- Replayable: `init` plays every file of sql/ddl on an existing database, and
-- a REVOKE of an absent privilege is not an error.

DO $$
BEGIN
    -- First, nothing: every replay starts from a closed database, so a grant
    -- added by hand does not survive it.
    EXECUTE format('REVOKE CONNECT ON DATABASE %I FROM PUBLIC', current_database());

    -- The owner, explicitly. It holds the right by ownership and, as a
    -- superuser, bypasses the check entirely -- but an acceptance control
    -- reads grants, not implications.
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO %I',
                   current_database(), current_user);

    -- The dashboard, the only role of this stack that logs in on its own
    -- (140_dashboard_role.sql). The guard keeps this file playable on a
    -- database built before 140 ever ran.
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fd_dashboard') THEN
        EXECUTE format('GRANT CONNECT ON DATABASE %I TO fd_dashboard',
                       current_database());
    END IF;

    -- The two service roles are refused here, and their own databases refuse
    -- everyone but them (sql/initdb/010_databases.sh). Restated on every
    -- replay: this is the separation L1 exists for, and it should not depend
    -- on a file that runs once at first boot.
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fd_airflow') THEN
        EXECUTE format('REVOKE CONNECT ON DATABASE %I FROM fd_airflow',
                       current_database());
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fd_mlflow') THEN
        EXECUTE format('REVOKE CONNECT ON DATABASE %I FROM fd_mlflow',
                       current_database());
    END IF;
END $$;

\echo 'CONNECT : ferme a PUBLIC, ouvert au proprietaire et a fd_dashboard'
