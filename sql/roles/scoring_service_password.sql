-- Give fd_scoring_service its password, from .env. Run by `fd-manager init`.
-- Covers: EC22   Lot: L5   Competency: C3, C5
--
-- The same arrangement as dashboard_password.sql, for the same reasons, and
-- that file carries the full reasoning: kept out of sql/ddl because everything
-- there is replayed by anything that builds a schema and must not need a
-- secret; the value arrives as a psql variable; :'name' makes psql quote it as
-- a literal so a value can never be read as SQL.
--
-- WHY THE SERVICE HAS A PASSWORD AT ALL, when fd_purge and fd_report do not:
-- those are used by a process the owner starts, through SET ROLE. This one
-- answers a network. Whoever reaches the container must not be one SET ROLE
-- away from the owner -- see 145_scoring_service_role.sql.
\if :{?scoring_service_password}
ALTER ROLE fd_scoring_service PASSWORD :'scoring_service_password';
\echo 'fd_scoring_service : mot de passe applique'
\else
\echo 'fd_scoring_service : aucun mot de passe fourni, le role ne peut pas se connecter'
\endif
