-- Carry the journal of the scoring service across its rename.
-- Covers: EC19   Lot: L5 (20/11/2026)   Competency: C4
--
-- WHY A MIGRATION AND NOT JUST A NEW NAME IN THE DDL. Every DDL of this
-- project is replayable, which it achieves with CREATE TABLE IF NOT EXISTS --
-- and that is exactly what makes a rename dangerous there. On a database where
-- `init` had already run, the renamed DDL would find no fd_scoring_service_calls,
-- create an EMPTY one, and leave fd_api_calls beside it with every recorded
-- call in it. Two tables, one of them read by nothing, and no error.
--
-- IT MUST RUN BEFORE THE DDL LOOP, and fd-manager's init places it there. The
-- order is the whole point: rename first, then let the DDL find the table it
-- expects and be a no-op on it.
--
-- EVERY DEPENDENT NAME TRAVELS TOO, and this is the part a rename usually
-- forgets. ALTER TABLE ... RENAME TO does NOT rename the sequence a bigserial
-- owns, nor the indexes, nor the constraints. Left alone, a migrated database
-- would carry fd_api_calls_call_id_seq while a freshly built one carries
-- fd_scoring_service_calls_call_id_seq -- and the GRANT of the DDL names the
-- sequence explicitly, so it would succeed on one and fail on the other. Two
-- databases built from the same repository must be indistinguishable.
--
-- IDEMPOTENT, like every file here: each rename is guarded on the old name
-- still existing, so a second run does nothing and says so.

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE c.relname = 'fd_api_calls' AND n.nspname = 'public') THEN

        ALTER TABLE public.fd_api_calls RENAME TO fd_scoring_service_calls;
        RAISE NOTICE 'table fd_api_calls renommee en fd_scoring_service_calls';

        -- The sequence of the bigserial, which the table rename leaves behind.
        IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'fd_api_calls_call_id_seq') THEN
            ALTER SEQUENCE public.fd_api_calls_call_id_seq
                  RENAME TO fd_scoring_service_calls_call_id_seq;
        END IF;

        -- The two indexes. Their names are declared in the DDL, so a database
        -- that kept the old ones would also gain the new ones on the next
        -- replay: two indexes on the same columns, paid for twice on every
        -- insert.
        IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'fd_api_calls_by_instant') THEN
            ALTER INDEX public.fd_api_calls_by_instant
                  RENAME TO fd_scoring_service_calls_by_instant;
        END IF;
        IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'fd_api_calls_by_client') THEN
            ALTER INDEX public.fd_api_calls_by_client
                  RENAME TO fd_scoring_service_calls_by_client;
        END IF;

        -- The CHECK that ties the decision to the score. Renamed rather than
        -- dropped and recreated: dropping it would leave the table, for an
        -- instant, able to hold a row the whole project says cannot exist.
        IF EXISTS (SELECT 1 FROM pg_constraint
                    WHERE conname = 'fd_api_calls_decision_follows_the_score') THEN
            ALTER TABLE public.fd_scoring_service_calls
                  RENAME CONSTRAINT fd_api_calls_decision_follows_the_score
                  TO fd_scoring_service_calls_decision_follows_the_score;
        END IF;
    ELSE
        RAISE NOTICE 'aucune table fd_api_calls : rien a renommer';
    END IF;
END
$$;
