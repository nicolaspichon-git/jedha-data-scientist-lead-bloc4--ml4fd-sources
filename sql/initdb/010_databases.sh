#!/bin/bash
# The two service databases and their owners, created AT FIRST BOOT.
# Covers: EC18, EC22   Lot: L1   Competency: C3
#
# WHY HERE AND NOT IN sql/ddl. Everything in sql/ddl is replayed by
# `fd-manager init`, which runs AFTER `up`. But airflow-init migrates its
# schema during `up`, and it can only do that if its database already exists.
# /docker-entrypoint-initdb.d runs before the server accepts its first outside
# connection, which is the only hook early enough.
#
# WHY A SHELL SCRIPT AND NOT A .sql. The entrypoint plays a .sql without
# arguments, so a .sql cannot receive a password. This script passes the two
# secrets as psql variables, and :'name' makes psql quote them as literals --
# the same mechanism as sql/roles/dashboard_password.sql.
#
# RUNS ONCE, AND ONLY ONCE: the entrypoint plays this directory only when the
# data directory is empty. On an already initialised volume it is ignored, and
# the two databases have to be created by hand. Replayable all the same -- the
# guards below make a second run a no-op.
#
# LF LINE ENDINGS ARE LOAD-BEARING. A CRLF file here fails inside the container
# with "bad interpreter: /bin/bash^M", and the cluster is then initialised
# WITHOUT the two databases. git must not convert it (see .gitattributes).
set -euo pipefail

# Refusing beats a half-built cluster. Without the passwords the roles would
# exist unable to log in, airflow-init would fail during `up`, and the volume
# would already be initialised -- so this script would never run again.
# fd-manager's Assert-Env refuses earlier, for the same reason.
if [ -z "${AIRFLOW_DB_PASSWORD:-}" ] || [ -z "${MLFLOW_DB_PASSWORD:-}" ]; then
    echo "initdb 010_databases.sh : AIRFLOW_DB_PASSWORD ou MLFLOW_DB_PASSWORD est vide." >&2
    echo "  Lancez  .\\fd-manager.ps1 setup  puis supprimez le volume avant de reessayer :" >&2
    echo "  docker compose down -v" >&2
    exit 1
fi

psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v airflow_password="$AIRFLOW_DB_PASSWORD" \
     -v mlflow_password="$MLFLOW_DB_PASSWORD" <<'EOSQL'

-- fd_airflow: owner of the airflow database, and of nothing else.
--
-- LOGIN, unlike fd_ingest, fd_report and fd_purge: those are group roles the
-- application endorses with SET ROLE while connected as the owner. Airflow's
-- scheduler connects on its own, from another container, so it needs to log in.
--
-- OWNER of its database rather than a role granted rights on it: since
-- PostgreSQL 15 the public schema belongs to pg_database_owner, whose only
-- implicit member is the database owner. Making the role the owner is
-- therefore what lets `airflow db migrate` create its tables -- and it spares
-- the explicit GRANT ALL ON SCHEMA public that Airflow's own documentation
-- prescribes for a non-owner role.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fd_airflow') THEN
        CREATE ROLE fd_airflow LOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fd_mlflow') THEN
        CREATE ROLE fd_mlflow LOGIN;
    END IF;
END $$;

-- Stated in full on every run, so a change made by hand does not survive.
ALTER ROLE fd_airflow LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
                      NOINHERIT NOREPLICATION NOBYPASSRLS;
ALTER ROLE fd_mlflow  LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
                      NOINHERIT NOREPLICATION NOBYPASSRLS;

ALTER ROLE fd_airflow PASSWORD :'airflow_password';
ALTER ROLE fd_mlflow  PASSWORD :'mlflow_password';

-- CREATE DATABASE takes no IF NOT EXISTS and runs in no transaction, hence
-- \gexec: the SELECT builds the statement, \gexec executes what it returned,
-- and an existing database returns no row at all. %I quotes the identifier,
-- which also settles the hyphen in the application database's name.
SELECT format('CREATE DATABASE %I OWNER %I', 'airflow', 'fd_airflow')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'airflow')
\gexec

SELECT format('CREATE DATABASE %I OWNER %I', 'mlflow', 'fd_mlflow')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'mlflow')
\gexec

-- Each service database is closed to everyone but its owner. Revoking from
-- PUBLIC does not touch the owner, who holds its rights by ownership.
REVOKE CONNECT ON DATABASE airflow FROM PUBLIC;
REVOKE CONNECT ON DATABASE mlflow  FROM PUBLIC;

-- Neither service role may reach the business database. Stated rather than
-- left to the default, so that sql/ddl/150_database_connect.sql has something
-- to read rather than an absence to infer.
DO $$
BEGIN
    EXECUTE format('REVOKE CONNECT ON DATABASE %I FROM fd_airflow', current_database());
    EXECUTE format('REVOKE CONNECT ON DATABASE %I FROM fd_mlflow',  current_database());
END $$;

\echo 'initdb : bases airflow et mlflow creees, roles fd_airflow et fd_mlflow en place'
EOSQL
