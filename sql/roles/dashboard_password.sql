-- Give fd_dashboard its password, from .env. Run by `fd-manager init`.
-- Block: B3, B4.
-- Lot: B3-R0, B4-L0.
-- Competency: B3-C5.
-- Covers: B3-EC19.
--
-- Kept out of sql/ddl on purpose: everything there is replayed by anything that
-- builds a schema, and this file needs a secret it must not find in the
-- repository. The value arrives as a psql variable:
--
--     psql -v dashboard_password=<hex> -f sql/roles/dashboard_password.sql
--
-- :'dashboard_password' makes psql QUOTE it as a literal, so a value can never
-- be read as SQL. fd-manager generates 64 hexadecimal characters, which also
-- keeps it clear of PowerShell's handling of quotes on a native command line.
--
-- The server stores a SCRAM-SHA-256 verifier, not the value (PostgreSQL 16's
-- default password_encryption). The statement itself carries the value in
-- clear; it would reach the server log only with log_statement = ddl or all,
-- which this stack does not set.
\if :{?dashboard_password}
ALTER ROLE fd_dashboard PASSWORD :'dashboard_password';
\echo 'fd_dashboard : mot de passe applique'
\else
\echo 'fd_dashboard : aucun mot de passe fourni, le role ne peut pas se connecter'
\endif
