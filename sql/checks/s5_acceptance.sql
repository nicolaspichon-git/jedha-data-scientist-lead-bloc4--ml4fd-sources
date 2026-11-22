-- The acceptance criteria of S5, lots Q0 to Q6. Every verdict must read OK.
-- Block: B3, B4.
-- Lot: B3-Q6, B4-L0.
-- Competency: B3-C1, B3-C4, B3-C5.
-- Covers: B3-EC14, B3-EC16, B3-EC18, B3-EC19, B3-EC23, B3-EC27.
--
-- THE DECISIVE ONE IS CRITERION 4, and it is not a count: fd_purge holds NO
-- privilege on the exploited layer. "The purge did not overflow" is therefore
-- not something this file has to measure after the fact -- it is something the
-- session cannot do. A criterion that reads a privilege rather than a
-- consequence cannot be satisfied by luck.
--
-- AND THE SECOND MOST USEFUL IS 2, WHICH IS NOT WHAT THIS PLAN FIRST SAID. The
-- plan of S5 asked to compare the digest of fd_transactions before and after a
-- purge. That comparison cannot hold here: the stream writes a transaction
-- every minute, so the digest legitimately differs between any two instants,
-- and a criterion that fails for a good reason gets switched off. What IS
-- checkable is that nothing survives the cutoff the last purge actually
-- applied -- read from fd_purges rather than restated as a duration.
--
-- WHY THE DURATIONS ARE NOT WRITTEN HERE AS LITERALS, unlike s1 to s4. Those
-- files mirror a governance value to check an observation against it. Here the
-- observation IS a boundary the purge recorded (fd_purges.cutoff), so the
-- useful question is whether anything survived it. That the recorded boundary
-- matches the declared duration is a separate question, asked by
-- tools/check_s5.py, which can read governance.yml.
--
-- 8 = the margin on the weekly cadence (purge.schedule, Sunday): a purge that
--     has not run for more than eight days has stopped running.
--
-- RETENTION PURGES ONLY, AND THE FILTER IS batch_id IS NULL (S6, after Q7).
-- fd_purges holds two kinds of rows since Q7: the weekly purge, which applies a
-- BOUNDARY to a whole store, and the targeted forget, which removes ONE named
-- batch (batch_id set). A forget's cutoff is its batch's own last row -- not a
-- boundary -- and it journals only the stores that held rows of that batch.
-- Read as purges, four forgets made this file fail twice on a sound stack:
-- 556,560 rows "survived" a cutoff nobody applied to them, and each execution
-- journalled one store instead of two. Forgets are reported apart (ord 16).
--
-- What SQL cannot reach is in tools/check_s5.py: the DAG and its cadence, the
-- declared durations against the applied cutoffs, and the files of the
-- training sets on the disk.

CREATE TEMP TABLE acceptance AS
WITH declared AS (
    SELECT 8 AS cadence_margin_days
),
-- The batches of the raw zone and whether a fingerprint was ever recorded for
-- them. A batch without one is UNPURGEABLE: the purge refuses it, by design,
-- so this criterion is what makes that refusal visible before a Sunday.
batches AS (
    SELECT r.batch_id,
           max(r.ingested_at) AS last_at,
           EXISTS (SELECT 1 FROM fd_pipeline_runs p
                    WHERE p.digests #>> '{raw_batch,batch_id}'
                          = r.batch_id::text) AS has_digest
    FROM   fd_raw_events r
    GROUP  BY r.batch_id
),
-- Every batch a purge has touched, from the marks of Q3, and whether the
-- fingerprint it held is still there. THE EVIDENCE MUST OUTLIVE THE DATA.
purged AS (
    SELECT p.run_id,
           p.digests #>> '{raw_batch,batch_id}' AS batch_id,
           p.digests #>> '{raw_batch,digest}'   AS digest,
           jsonb_object_keys(p.digests -> 'purge') AS store
    FROM   fd_pipeline_runs p
    WHERE  p.digests ? 'purge'
),
-- The last cutoff each store was actually purged up to.
applied AS (
    SELECT store, max(cutoff) AS cutoff, max(ran_at) AS ran_at
    FROM   fd_purges
    WHERE  batch_id IS NULL                 -- retention purges, not forgets
    GROUP  BY store
),
survivors AS (
    SELECT (SELECT count(*) FROM fd_raw_events r
             WHERE r.ingested_at < (SELECT cutoff FROM applied
                                     WHERE store = 'fd_raw_events')) AS raw_rows,
           (SELECT count(*) FROM fd_quarantine q
             WHERE q.rejected_at < (SELECT cutoff FROM applied
                                     WHERE store = 'fd_quarantine')) AS qrt_rows
),
-- What fd_purge is allowed to do, read from the catalogue rather than assumed.
privileges AS (
    SELECT count(*) FILTER (
               WHERE table_name IN ('fd_transactions', 'fd_ground_truth',
                                    'fd_scores', 'fd_frauds', 'fd_reports',
                                    'fd_datasets')) AS on_exploited,
           count(*) FILTER (
               WHERE table_name IN ('fd_pipeline_runs', 'fd_purges')
                 AND privilege_type IN ('DELETE', 'TRUNCATE')) AS can_erase,
           count(*) FILTER (
               WHERE table_name = 'fd_raw_events'
                 AND privilege_type = 'DELETE') AS can_purge_raw,
           count(*) FILTER (
               WHERE table_name = 'fd_quarantine'
                 AND privilege_type = 'DELETE') AS can_purge_quarantine
    FROM   information_schema.table_privileges
    WHERE  grantee = 'fd_purge'
),
runs_journalled AS (
    -- ONE EXECUTION = ONE (run, instant it was for). On as_of and not on
    -- ran_at: as_of is passed once by the caller and is identical for every
    -- store of one purge, whereas ran_at defaults to now() and is therefore
    -- identical only as long as the two inserts share a transaction. The
    -- criterion must not rest on a coupling nobody declared.
    SELECT count(DISTINCT (coalesce(run_id, ''), as_of)) AS executions,
           count(*) AS rows_,
           count(DISTINCT store) AS stores,
           max(ran_at) AS last_at,
           sum(n_deleted) AS deleted,
           count(*) FILTER (WHERE before_date IS NOT NULL) AS overridden
    FROM   fd_purges
    WHERE  batch_id IS NULL                 -- retention purges, not forgets
),
-- The targeted forgets of Q7, kept apart: a figure, not a criterion.
forgets AS (
    SELECT count(DISTINCT batch_id) AS batches,
           coalesce(sum(n_deleted), 0) AS deleted
    FROM   fd_purges
    WHERE  batch_id IS NOT NULL
),
criteria AS (
    SELECT 1 AS ord,
           'Q1 lots de la zone brute sans empreinte relevee' AS criterion,
           '0' AS expected,
           (SELECT count(*)::text FROM batches WHERE NOT has_digest) AS actual
    UNION ALL
    SELECT 2, 'Q3 lignes survivant a la derniere borne appliquee (zone brute)',
           '0', (SELECT raw_rows::text FROM survivors)
    UNION ALL
    SELECT 3, 'Q4 lignes survivant a la derniere borne appliquee (quarantaine)',
           '0', (SELECT qrt_rows::text FROM survivors)
    UNION ALL
    SELECT 4, 'Q3 droits de fd_purge sur la couche exploitee',
           '0', (SELECT on_exploited::text FROM privileges)
    UNION ALL
    SELECT 5, 'Q3 droits de fd_purge pour effacer un journal',
           '0', (SELECT can_erase::text FROM privileges)
    UNION ALL
    SELECT 6, 'Q3 fd_purge peut supprimer dans la zone brute',
           'oui', (SELECT CASE WHEN can_purge_raw > 0 THEN 'oui' ELSE 'NON' END
                     FROM privileges)
    UNION ALL
    SELECT 7, 'Q4 fd_purge peut supprimer dans la quarantaine',
           'oui', (SELECT CASE WHEN can_purge_quarantine > 0 THEN 'oui'
                               ELSE 'NON' END FROM privileges)
    UNION ALL
    SELECT 8, 'Q6 empreinte conservee pour chaque lot purge',
           '0 perdue(s)',
           (SELECT count(*)::text || ' perdue(s)' FROM purged
             WHERE digest IS NULL)
    UNION ALL
    SELECT 9, 'Q6 chaque execution journalise les deux magasins',
           (SELECT CASE WHEN executions = 0 THEN 'mesure' ELSE 'oui' END
              FROM runs_journalled),
           (SELECT CASE
                     WHEN executions = 0 THEN 'aucune purge enregistree'
                     WHEN rows_ = executions * 2 AND stores = 2 THEN 'oui'
                     ELSE 'NON : ' || rows_::text || ' ligne(s) pour '
                          || executions::text || ' execution(s)'
                   END FROM runs_journalled)
    UNION ALL
    SELECT 10, 'Q6 une purge a eu lieu dans les 8 derniers jours',
           (SELECT CASE WHEN last_at IS NULL THEN 'mesure' ELSE 'oui' END
              FROM runs_journalled),
           (SELECT CASE
                     WHEN last_at IS NULL THEN 'aucune purge enregistree'
                     WHEN last_at > now() - (SELECT cadence_margin_days
                                               FROM declared) * interval '1 day'
                       THEN 'oui'
                     ELSE 'NON : derniere le ' || last_at::date::text
                   END FROM runs_journalled)
    UNION ALL
    SELECT 11, 'info lignes dans la zone brute et la quarantaine', 'mesure',
           (SELECT (SELECT count(*) FROM fd_raw_events)::text || ' / '
                   || (SELECT count(*) FROM fd_quarantine)::text)
    UNION ALL
    SELECT 12, 'info plus ancienne ligne conservee (zone brute)', 'mesure',
           (SELECT coalesce(min(ingested_at)::date::text, 'zone vide')
              FROM fd_raw_events)
    UNION ALL
    SELECT 13, 'info bornes appliquees par magasin', 'mesure',
           (SELECT coalesce(string_agg(store || ' jusqu''au '
                                       || cutoff::date::text, ', '
                                       ORDER BY store),
                            'aucune purge enregistree') FROM applied)
    UNION ALL
    SELECT 14, 'info lignes supprimees a ce jour', 'mesure',
           (SELECT coalesce(deleted::text, '0') FROM runs_journalled)
    UNION ALL
    SELECT 15, 'info purges a borne surchargee (demonstration)', 'mesure',
           (SELECT overridden::text FROM runs_journalled)
    UNION ALL
    SELECT 16, 'info oublis cibles (forget), hors retention', 'mesure',
           (SELECT batches::text || ' lot(s), ' || deleted::text || ' ligne(s)'
              FROM forgets)
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
        RAISE EXCEPTION 'S5 : % critere(s) en echec : %', failed, names
            USING HINT = 'Le tableau ci-dessus donne attendu et constate pour chacun.';
    END IF;
END $$;
