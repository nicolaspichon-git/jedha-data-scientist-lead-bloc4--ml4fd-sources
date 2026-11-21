-- Le plan reel de la grande requete de s4_acceptance.sql.
-- A jeter apres lecture : ce fichier ne fait partie d'aucune chaine.
--
--   .\fd-manager.ps1 psql    puis   \i /work/sql/checks/_explain_s4.sql
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
WITH declared AS (
    SELECT 30 AS delay_days
),
-- Transactions of the stream only: the historical load carries its own
-- verdicts and answers a different question.
stream_txn AS (
    SELECT t.txn_id, t.txn_ts
      FROM fd_transactions t
     WHERE EXISTS (SELECT 1 FROM fd_raw_events r
                    WHERE r.batch_id = t.batch_id
                      AND r.source = 'current-transactions')
),
-- Payments old enough that their verdict is due, and whose verdict is still
-- not known. THE CRITERION ONLY TIME MAKES POSSIBLE.
overdue AS (
    SELECT count(*) AS waiting
      FROM stream_txn t, declared d
     WHERE t.txn_ts < now() - (d.delay_days * interval '1 day')
       AND NOT EXISTS (SELECT 1 FROM fd_ground_truth g
                        WHERE g.txn_id = t.txn_id
                          AND g.label_ts <= now())
),
-- The days that should carry a report and do not. Same reasoning as
-- sql/checks/s4_absence.sql: from the FIRST recorded report, never from thirty
-- days ago, or a pipeline three days old would list twenty-seven absences
-- nobody ever expected.
span AS (
    SELECT greatest(min(report_date), current_date - 30) AS first_day,
           current_date - 1                              AS last_day
      FROM fd_reports
    HAVING count(*) > 0
),
missing AS (
    SELECT count(*) AS days
      FROM span,
           generate_series(span.first_day, span.last_day, interval '1 day') AS day
     WHERE NOT EXISTS (SELECT 1 FROM fd_reports r
                        WHERE r.report_date = day::date)
),
journal AS (
    SELECT count(*)                                   AS rows_recorded,
           count(DISTINCT report_date)                AS days_recorded,
           count(*) FILTER (WHERE coalesce(path, '') = '')   AS without_path,
           count(*) FILTER (WHERE knowledge_date < report_date) AS known_too_early,
           min(report_date)                           AS first_day,
           max(report_date)                           AS last_day,
           round(avg(n_alerts), 1)                    AS alerts_per_report,
           sum(n_alerts)                              AS alerts_total
      FROM fd_reports
),
latest AS (
    SELECT * FROM fd_reports ORDER BY report_date DESC LIMIT 1
),
-- Section 3 of the report, recomputed over the last 30 days of FALLEN
-- verdicts: the only measurement of the model's real behaviour.
fallen AS (
    SELECT g.label,
           EXISTS (SELECT 1 FROM fd_frauds a WHERE a.txn_id = g.txn_id) AS alerted
      FROM fd_ground_truth g
      JOIN stream_txn t ON t.txn_id = g.txn_id
     WHERE g.label_ts >= now() - interval '30 days'
       AND g.label_ts <= now()
),
observed AS (
    SELECT count(*)                                            AS verdicts,
           count(*) FILTER (WHERE label)                       AS frauds,
           count(*) FILTER (WHERE alerted AND label)           AS caught,
           count(*) FILTER (WHERE alerted)                     AS alerts,
           round(count(*) FILTER (WHERE alerted AND label)::numeric
                 / nullif(count(*) FILTER (WHERE label), 0), 4) AS recall,
           round(count(*) FILTER (WHERE alerted AND label)::numeric
                 / nullif(count(*) FILTER (WHERE alerted), 0), 4) AS precision_,
           round(100.0 * count(*) FILTER (WHERE label)
                 / nullif(count(*), 0), 4)                      AS base_rate
      FROM fallen
),
criteria AS (
    SELECT 1 AS ord, 'P3 rapports enregistres (>= 1)' AS criterion,
           'ok' AS expected,
           CASE WHEN (SELECT rows_recorded FROM journal) >= 1
                THEN 'ok' ELSE 'AUCUN' END AS actual
    UNION ALL
    -- The primary key makes this structurally impossible; stating it is how a
    -- reader learns the key is there for idempotence and not for tidiness.
    SELECT 2, 'P3 aucune journee en double', '0 doublon',
           (SELECT (rows_recorded - days_recorded)::text || ' doublon'
              FROM journal)
    UNION ALL
    SELECT 3, 'P3 aucune journee sans rapport', '0 journee',
           (SELECT coalesce((SELECT days FROM missing), 0)::text || ' journee')
    UNION ALL
    SELECT 4, 'P3 chaque ligne nomme son document', '0 sans chemin',
           (SELECT without_path::text || ' sans chemin' FROM journal)
    UNION ALL
    -- THE DECISIVE ONE.
    SELECT 5, 'P5 aucun paiement au-dela du delai sans verdict connu', '0',
           (SELECT waiting::text FROM overdue)
    UNION ALL
    SELECT 6, 'P2 la connaissance ne precede jamais la journee couverte', '0',
           (SELECT known_too_early::text FROM journal)
    UNION ALL
    SELECT 7, 'info rapports enregistres', 'mesure',
           (SELECT rows_recorded::text FROM journal)
    UNION ALL
    SELECT 8, 'info premiere et derniere journee couverte', 'mesure',
           (SELECT coalesce(first_day::text || ' -> ' || last_day::text,
                            'aucun rapport') FROM journal)
    UNION ALL
    SELECT 9, 'info alertes par rapport contre 41.6 prevues par M6', 'mesure',
           (SELECT coalesce(alerts_per_report::text, 'aucun rapport')
              FROM journal)
    UNION ALL
    SELECT 10, 'info dernier rapport : paiements/alertes/verdicts/attente',
           'mesure',
           (SELECT coalesce(n_payments::text || ' / ' || n_alerts::text || ' / '
                            || n_verdicts::text || ' / ' || n_pending::text,
                            'aucun rapport') FROM latest)
    UNION ALL
    SELECT 11, 'info rappel observe sur 30 j contre 0.7512 mesure par M5',
           'mesure',
           (SELECT coalesce(recall::text, 'aucun verdict echu') FROM observed)
    UNION ALL
    SELECT 12, 'info precision observee sur 30 j contre 0.5292 de M5', 'mesure',
           (SELECT coalesce(precision_::text, 'aucune alerte') FROM observed)
    UNION ALL
    SELECT 13, 'info taux de base observe contre 0.4072 pour-cent du jeu de test',
           'mesure',
           (SELECT coalesce(base_rate::text || ' pour-cent', 'rien a mesurer')
              FROM observed)
    UNION ALL
    SELECT 14, 'info verdicts echus sur 30 j, dont fraudes', 'mesure',
           (SELECT verdicts::text || ', dont ' || frauds::text FROM observed)
)
SELECT ord, criterion, expected, actual,
       CASE WHEN expected = 'mesure' THEN 'info'
            WHEN expected = actual THEN 'OK'
            ELSE '<<< ECHEC' END AS verdict
FROM   criteria;
