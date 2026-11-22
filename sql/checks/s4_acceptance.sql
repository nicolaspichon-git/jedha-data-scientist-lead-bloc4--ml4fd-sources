-- The acceptance criteria of S4, lots P1 to P5. Every verdict must read OK.
-- Block: B3, B4.
-- Lot: B3-P5, B4-L0.
-- Competency: B3-C1, B3-C4, B3-C5.
-- Covers: B3-EE04, B3-EI02, B3-EI03, B3-EI04, B3-EI08, B3-EC14, B3-EC23, B3-EC27.
--
-- THE DECISIVE ONE IS CRITERION 5, and it is the only criterion in this project
-- that ONLY TIME MAKES POSSIBLE: past the declared verdict delay, no
-- transaction may still be waiting. A remainder above zero means the writing of
-- the verdicts failed -- silently, since nothing else would notice -- and the
-- blind band would then never close for those payments.
--
-- AND IT DOES NOT READ fd_reports.n_pending, which is what this plan's own
-- wording suggested. That column records what was pending AT THE KNOWLEDGE DATE
-- of its report, so a row from forty days ago legitimately holds 7,200 for
-- ever: it was true that morning and it stays true as a record. The question
-- "is anything still waiting?" is about TODAY, so it is asked of
-- fd_transactions and fd_ground_truth.
--
-- THE MOST USEFUL ONE IS 11, and it is informative on purpose: the recall and
-- the precision observed on the verdicts that have fallen, beside the figures
-- M5 measured on the test set. An observed recall far from the measured one
-- does not mean the pipeline is broken -- it means the stream does not resemble
-- the test set, and therefore that S2's published metrics do not describe what
-- is happening. That is the single most valuable signal this file carries, so
-- it is reported rather than enforced, exactly as criterion 13 of S3 is.
--
-- The expected values that come from governance.yml are written here as
-- LITERALS, mirroring the file. A duplication, and a deliberate one: an
-- acceptance check that read its expected value from the thing it checks would
-- verify nothing. The same reasoning as s1, s2 and s3.
--
-- 30 = verdicts.delay_days, in nominal mode
--
-- What SQL cannot reach is in tools/check_s4.py: the document on the disk, its
-- digest against content_digest, and the four counts recomputed from the four
-- queries over the window the row itself names.

CREATE TEMP TABLE acceptance AS
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
        RAISE EXCEPTION 'S4 : % critere(s) en echec : %', failed, names
            USING HINT = 'Le tableau ci-dessus donne attendu et constate pour chacun.';
    END IF;
END $$;
