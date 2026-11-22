-- Le plan reel de la grande requete de s3_acceptance.sql.
-- A jeter apres lecture : ce fichier ne fait partie d'aucune chaine.
--
--   .\fd-manager.ps1 psql    puis   \i /work/sql/checks/_explain_s3.sql
--
-- LES COUTS SONT GARDES, et c'est la lecon du 21/11 : une premiere version
-- portait COSTS OFF, ce qui masque l'estimation a cote du reel. Or une
-- jointure en boucle imbriquee sur un million de lignes n'est jamais choisie
-- par le cout : elle est choisie par une ESTIMATION fausse, et c'est l'ecart
-- entre "rows=" estime et "actual rows=" qui la nomme.
-- Rejouable DANS LA MEME SESSION, contrairement aux fichiers
-- d'acceptation : ceux-la tournent chacun dans son propre psql -f et
-- n'ont rien a nettoyer, celui-ci est colle a la main et deux fois de
-- suite -- d'ou l'erreur "relation acceptance_plan already exists".
DROP TABLE IF EXISTS acceptance_plan;

EXPLAIN (ANALYZE, BUFFERS)
CREATE TEMP TABLE acceptance_plan AS
WITH stream_runs AS (
    SELECT * FROM fd_pipeline_runs WHERE dag_id = 'dag_ingest'
),
stream_txn AS (
    SELECT t.*
    FROM   fd_transactions t
    WHERE  EXISTS (SELECT 1 FROM fd_raw_events r
                   WHERE  r.batch_id = t.batch_id
                     AND  r.source = 'current-transactions')
),
band AS (
    SELECT count(*)                                            AS verdicts,
           count(*) FILTER (WHERE g.label_ts > now())           AS not_due,
           count(DISTINCT g.label_ts - t.txn_ts)                AS distinct_delays,
           min(g.label_ts - t.txn_ts)                           AS delay
    FROM   fd_ground_truth g
    JOIN   stream_txn t ON t.txn_id = g.txn_id
),
scored AS (
    SELECT count(*)                                            AS rows_scored,
           count(*) FILTER (WHERE s.model_version IS NULL
                              OR s.model_version = '')          AS without_version,
           count(DISTINCT s.model_version)                      AS versions,
           count(*) FILTER (WHERE s.flagged)                    AS flagged
    FROM   fd_scores s
    JOIN   stream_txn t ON t.txn_id = s.txn_id
),
alerts AS (
    SELECT count(*)                                            AS raised,
           count(*) FILTER (WHERE notified_at IS NOT NULL)      AS notified,
           count(*) FILTER (WHERE notified_at IS NULL)          AS pending,
           count(*) FILTER (WHERE notified_at > now() - interval '24 hours')
                                                                AS notified_24h,
           count(DISTINCT txn_id)                               AS distinct_txn
    FROM   fd_frauds
),
window_24h AS (
    SELECT count(*)                                            AS txn_24h,
           min(txn_ts)                                          AS oldest,
           max(txn_ts)                                          AS newest
    FROM   stream_txn
    WHERE  txn_ts > now() - interval '24 hours'
),
criteria AS (
    -- N1: the collector and the raw zone
    SELECT 1 AS ord, 'N1 lots temps reel en zone brute' AS criterion,
           'au moins 1' AS expected,
           CASE WHEN (SELECT count(DISTINCT batch_id) FROM fd_raw_events
                      WHERE source = 'current-transactions') > 0
                THEN 'au moins 1' ELSE 'AUCUN' END AS actual
    UNION ALL
    SELECT 2, 'N1 une execution par lot, jamais deux',
           '0 lot partage',
           CASE WHEN NOT EXISTS (
                    SELECT batch_id FROM stream_runs WHERE batch_id IS NOT NULL
                    GROUP BY batch_id HAVING count(*) > 1)
                THEN '0 lot partage' ELSE 'PARTAGE' END
    -- N2: the counters close. The CHECK constraint already refuses otherwise,
    -- so this reads as a confirmation that the constraint was exercised.
    UNION ALL
    SELECT 3, 'N2 compteurs fermes sur chaque execution',
           '0 ecart',
           CASE WHEN NOT EXISTS (
                    SELECT 1 FROM stream_runs
                    WHERE n_read IS NOT NULL
                      AND n_read <> n_valid + n_rejected)
                THEN '0 ecart' ELSE 'OUVERT' END
    UNION ALL
    -- THE SAME RULE AS sql/checks/abandoned_runs.sql, which is the point: two
    -- readings of one rule must not disagree. Before Q6 this criterion rested
    -- on an age of five minutes while the tool that closes these runs asked
    -- Airflow, so `abandoned` could report nothing to do while this failed and
    -- an operator had no way out. Q6 aligned both on dag_run. B4-L1 then gave
    -- Airflow a database of its own, which put dag_run out of reach from here
    -- and broke the two of them at once -- consistently, at least. Both now
    -- rest on the age alone, and abandoned_runs.sql carries the reasoning.
    SELECT 4, 'N7 aucune execution que personne ne peut fermer',
           '0',
           (SELECT count(*)::text FROM stream_runs r
             WHERE r.state = 'running'
               AND r.started_at < now() - interval '1 hour')
    -- AND WHAT IS OPEN ON EVERY DAG, reported and not enforced: this one is not
    -- restricted to dag_ingest, so a historical load or a hand-run tool that
    -- left a row behind shows up here even though criterion 4 cannot see it.
    UNION ALL
    SELECT 5, 'info executions ouvertes depuis plus d''une heure', 'mesure',
           (SELECT CASE WHEN count(*) = 0 THEN 'aucune'
                        ELSE count(*)::text || ' : '
                             || string_agg(dag_id, ', ' ORDER BY dag_id)
                   END
              FROM fd_pipeline_runs
             WHERE state = 'running'
               AND started_at < now() - interval '1 hour')
    -- N3: no identifying column reaches the exploited layer. Checked against
    -- the catalogue, not against a list written here.
    UNION ALL
    SELECT 6, 'N3 aucune colonne nominative dans fd_transactions',
           '0',
           (SELECT count(*)::text FROM information_schema.columns
            WHERE table_name = 'fd_transactions'
              AND column_name IN ('first','last','street','city','state','zip',
                                  'job','dob','gender','cc_num','lat','long',
                                  'is_fraud','label'))
    UNION ALL
    SELECT 7, 'N3 aucune transaction du flux sans features',
           '0',
           (SELECT count(*)::text FROM stream_txn WHERE f_tx_count IS NULL)
    -- N4: every scored row carries the version that scored it
    UNION ALL
    SELECT 8, 'N4 transactions scorees sans version de modele',
           '0', (SELECT without_version::text FROM scored)
    UNION ALL
    SELECT 9, 'N4 decision stockee conforme au seuil stocke',
           '0',
           (SELECT count(*)::text FROM fd_scores
            WHERE flagged <> (score >= threshold))
    -- N6: THE BLIND BAND, measured in production
    UNION ALL
    SELECT 10, 'N6 verdicts du flux deja echus',
           '0', (SELECT (verdicts - not_due)::text FROM band)
    UNION ALL
    SELECT 11, 'N6 un seul delai de verdict',
           CASE WHEN (SELECT verdicts FROM band) = 0 THEN '0' ELSE '1' END,
           (SELECT distinct_delays::text FROM band)
    -- N5: one alert per transaction, and the ceiling respected
    UNION ALL
    SELECT 12, 'N5 alertes en double sur un txn_id',
           '0', (SELECT (raised - distinct_txn)::text FROM alerts)
    UNION ALL
    SELECT 13, 'N5 notifications sur 24 h sous la capacite',
           'oui',
           CASE WHEN (SELECT notified_24h FROM alerts) <= 60
                THEN 'oui' ELSE 'DEPASSEE' END
    -- Informative. The figures any metric of S2 must be read beside.
    UNION ALL
    SELECT 14, 'info alertes/jour observees contre 38.5 prevues par M6',
           'mesure',
           CASE WHEN (SELECT txn_24h FROM window_24h) = 0 THEN 'pas de flux'
                ELSE round(86400.0 * (SELECT notified_24h + pending FROM alerts)
                           / GREATEST(1, extract(epoch FROM
                               (SELECT newest - oldest FROM window_24h))), 1)::text
                     || ' /jour' END
    UNION ALL
    SELECT 15, 'info transactions du flux sur 24 h', 'mesure',
           (SELECT txn_24h::text FROM window_24h)
    UNION ALL
    SELECT 16, 'info part signalee par le modele', 'mesure',
           CASE WHEN (SELECT rows_scored FROM scored) = 0 THEN 'rien de score'
                ELSE round(100.0 * (SELECT flagged FROM scored)
                           / (SELECT rows_scored FROM scored), 2)::text || ' %'
           END
    UNION ALL
    SELECT 17, 'info versions de modele ayant score le flux', 'mesure',
           (SELECT versions::text FROM scored)
    UNION ALL
    SELECT 18, 'info alertes levees / notifiees / en attente', 'mesure',
           (SELECT raised::text || ' / ' || notified::text || ' / '
                   || pending::text FROM alerts)
    UNION ALL
    SELECT 19, 'info bande aveugle : verdicts non echus', 'mesure',
           (SELECT not_due::text || ' sur ' || verdicts::text FROM band)
    UNION ALL
    SELECT 20, 'info delai de verdict constate', 'mesure',
           (SELECT COALESCE(delay::text, 'aucun verdict') FROM band)
    -- N9: stage (3c), the controls and the operations channel. A control
    -- recorded under a name the code no longer knows would never transition
    -- again -- it would sit in the table as a state nobody computes, and the
    -- channel would go quiet about whatever that control used to watch.
    UNION ALL
    --
    -- EIGHT AND NOT FOUR since B4-L4, 2026-11-22. dag_monitor records its four
    -- families in the same table, deliberately: fd_ops_state is the one place
    -- where a control's last state lives, so the channel and the dashboard
    -- read both kinds alike. The four pipeline controls are health.py's, the
    -- four monitoring ones ml4fd/monitor.py's (and the CHECK of
    -- sql/ddl/170_monitoring.sql). Counting the second as unknown failed this
    -- criterion as soon as one of them changed state, and differently each
    -- time another did.
    SELECT 21, 'N9 controles enregistres hors des huit declares',
           '0 inconnu',
           (SELECT count(*)::text || ' inconnu'
              FROM fd_ops_state
             WHERE control NOT IN ('freshness', 'empty_run',
                                   'rejection_rate', 'volume_invariant',
                                   'input_drift', 'prediction_drift',
                                   'performance', 'data_quality'))
    -- Informative and NOT a criterion, for the same reason as 13: a pipeline
    -- legitimately in alert would otherwise fail its own acceptance, and an
    -- acceptance check that fails on reality teaches people to ignore it.
    UNION ALL
    SELECT 22, 'info controles en alerte', 'mesure',
           (SELECT CASE WHEN count(*) = 0 THEN 'aucun'
                        ELSE string_agg(control, ', ' ORDER BY control) END
              FROM fd_ops_state WHERE state = 'alert')
    UNION ALL
    SELECT 23, 'info dernier changement d''etat notifie', 'mesure',
           (SELECT COALESCE(max(notified_at)::timestamp(0)::text,
                            'aucun : aucun controle n''a change d''etat')
              FROM fd_ops_state)
)
SELECT ord, criterion, expected, actual,
       CASE WHEN expected = 'mesure' THEN 'info'
            WHEN expected = actual THEN 'OK'
            ELSE '<<< ECHEC' END AS verdict
FROM   criteria;
