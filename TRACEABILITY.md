# Matrice de tracabilite

> Fichier **genere** par `tools/build_traceability.py`. Ne pas editer a la main.
> Les emplacements viennent des marques `Covers:` du code.
> Perimetre : les exigences de competence (EC) de `step-0/ml4fd-project--d0-requirements.md`.

> **A lire avant de se fier a la colonne Code.** Les marques `Covers:` des lots anciens ont ete ecrites contre le referentiel dont ce projet est issu, que le document d'exigences a depuis renumerote : `ml4fd/stream.py` marque EC01, que le document enonce comme *produire un cahier des charges*. Les marques de C5 et C6 ont ete ecrites contre le document actuel. Le tableau *Marques en desaccord* plus bas est le point de depart du repointage ; tant qu'il n'est pas vide, la couverture de C1 a C4 est a verifier une marque a la fois.

> Nature : L = livrable, D = documentaire, DM = demonstration, A = architectural, F = fonctionnel.

| Exigence | Comp. | Nature | Enonce | Code |
| --- | --- | --- | --- | --- |
| **EC01** | C1 | D | Produire un cahier des charges écrit et versionné, opposable à la réalisation (1) | `ml4fd/stream.py:3` (!)<br/>`ml4fd/stream.py:249 fetch_one()` (!)<br/>`sql/checks/s1_acceptance.sql:2`<br/>`sql/checks/volumetry.sql:3`<br/>`tools/measure_stores.py:7` |
| **EC02** | C1 | D | Énoncer le besoin de l'organisation : activité, problème métier, décision que la solution d'IA informe, population des | - |
| **EC03** | C1 | D | Chiffrer les besoins techniques : nature et volumétrie des données, débit d'appels attendu, latence maximale de répons | `sql/checks/volumetry.sql:3`<br/>`tools/measure_stores.py:7` |
| **EC04** | C1 | D | Chiffrer les besoins économiques : coût d'exploitation visé, gain attendu, et seuil de performance à partir duquel la  | - |
| **EC05** | C1 | D | Fixer des critères d'acceptation mesurables : métriques du modèle, seuils, et mode de mesure de chacun (3) | `ml4fd/build.py:3`<br/>`ml4fd/build.py:68 build()` (!)<br/>`ml4fd/datasets.py:3`<br/>`ml4fd/datasets.py:82 without()`<br/>`ml4fd/db.py:3` (!)<br/>`sql/checks/s2_acceptance.sql:2`<br/>`sql/ddl/070_datasets.sql:2`<br/>`tests/test_dag_train.py:3`<br/>`tools/profile_corpus.py:3`<br/>`airflow/dags/dag_train.py:161 resolve_features()`<br/>`airflow/dags/dag_train.py:240 build_dataset()` |
| **EC06** | C1 | D | Nommer le référentiel d'accessibilité retenu et le niveau visé, et énumérer les surfaces de la solution qui y sont sou | `ml4fd/db.py:3` (!)<br/>`tests/test_fd_manager.py:3` (!)<br/>`tests/test_module_scope.py:3` (!) |
| **EC07** | C1 | D, F | Spécifier les exigences d'accessibilité applicables à chaque surface restituant un résultat de la solution à un utilis | `tests/test_acceptance_files.py:3` (!)<br/>`tests/test_traceability.py:3`<br/>`tools/build_traceability.py:7`<br/>`tools/build_traceability.py:289 disagreements()` |
| **EC08** | C2 | D | Caractériser le jeu de données d'entraînement : volume, schéma, déséquilibre des classes, antériorité disponible, fuit | `airflow/dags/dag_load_history.py:3` (!)<br/>`airflow/dags/dag_train.py:3` (!) |
| **EC09** | C2 | D | Justifier le choix de la famille d'algorithme par les caractéristiques du jeu d'entraînement (5) | `ml4fd/guard.py:3` (!)<br/>`ml4fd/guard.py:98 verdict()` (!)<br/>`ml4fd/notify.py:3` (!)<br/>`ml4fd/notify.py:243 notify()` (!)<br/>`ml4fd/threshold.py:3`<br/>`ml4fd/threshold.py:158 choose()`<br/>`sql/checks/s3_acceptance.sql:2` (!)<br/>`sql/frauds/notified_last_day.sql:2` (!)<br/>`tests/test_check_s3.py:3` (!)<br/>`tests/test_dag_daily.py:3` (!)<br/>`tests/test_dag_ingest.py:3` (!)<br/>`tests/test_dag_purge.py:3` (!)<br/>`tests/test_notify.py:3` (!)<br/>`tests/test_rechoose_threshold.py:3` (!)<br/>`tools/check_s3.py:3` (!)<br/>`tools/rechoose_threshold.py:3`<br/>`airflow/dags/dag_daily.py:3` (!)<br/>`airflow/dags/dag_ingest.py:3`<br/>`airflow/dags/dag_ingest.py:337 raise_and_notify_fraud()` (!)<br/>`airflow/dags/dag_purge.py:3` (!)<br/>`airflow/dags/dag_purge.py:177 check_share()` (!)<br/>`airflow/dags/dag_train.py:3` (!)<br/>`airflow/dags/dag_train.py:218 guard()` (!) |
| **EC10** | C2 | D | Documenter le protocole d'entraînement et d'évaluation : découpage, validation, métriques, hyperparamètres retenus | - |
| **EC11** | C2 | D | Évaluer le modèle contre les critères d'acceptation du cahier des charges, et conclure explicitement sur la conformité | - |
| **EC12** | C2 | A | Rendre l'entraînement reproductible : dépendances figées, graine fixée, version du jeu de données référencée | `ml4fd/runs.py:3` (!)<br/>`sql/checks/raw_batches.sql:2` (!)<br/>`sql/checks/s1_acceptance.sql:2`<br/>`sql/ddl/050_pipeline_runs.sql:2` (!)<br/>`tests/test_batches_listing.py:3` (!)<br/>`tests/test_runs_journal.py:3` (!)<br/>`airflow/dags/dag_load_history.py:3` (!)<br/>`airflow/dags/dag_load_history.py:69 preflight()` (!)<br/>`airflow/dags/dag_load_history.py:432 close_run()` (!) |
| **EC13** | C2 | A | Produire le modèle comme un artefact persistant, versionné et retrouvable par le service de prédiction et par le réent | - |
| **EC14** | C2 | A | Partager le même code de préparation des *features* entre l'entraînement et le service de prédiction (7) | `ml4fd/dashboard.py:3` (!)<br/>`ml4fd/datasets.py:240 assert_cut_off_usable()` (!)<br/>`ml4fd/guard.py:3` (!)<br/>`ml4fd/guard.py:98 verdict()` (!)<br/>`ml4fd/health.py:3`<br/>`ml4fd/health.py:459 check()` (!)<br/>`ml4fd/health.py:483 notify_ops()` (!)<br/>`ml4fd/notify.py:3` (!)<br/>`ml4fd/notify.py:243 notify()` (!)<br/>`ml4fd/quarantine.py:89 rejected_lines()` (!)<br/>`ml4fd/report.py:3`<br/>`ml4fd/report.py:649 record()`<br/>`ml4fd/stream.py:289 poll()`<br/>`ml4fd/verdicts.py:3` (!)<br/>`sql/checks/abandoned_runs.sql:2` (!)<br/>`sql/checks/pipeline_health.sql:2` (!)<br/>`sql/checks/quarantined_lines.sql:2` (!)<br/>`sql/checks/s3_acceptance.sql:2` (!)<br/>`sql/checks/s4_absence.sql:2` (!)<br/>`sql/checks/s4_acceptance.sql:2` (!)<br/>`sql/checks/s5_acceptance.sql:2` (!)<br/>`sql/dashboard/ops_notifications.sql:3` (!)<br/>`sql/dashboard/recent_runs.sql:2` (!)<br/>`sql/dashboard/stream_rate_of_day.sql:3` (!)<br/>`sql/ddl/090_frauds.sql:2`<br/>`sql/ddl/100_ops_state.sql:2`<br/>`sql/ddl/110_reports.sql:2`<br/>`sql/ddl/130_purges.sql:2` (!)<br/>`sql/ddl/135_ops_notifications.sql:2` (!)<br/>`sql/frauds/record_failure.sql:2` (!)<br/>`sql/maintenance/030_close_abandoned_runs.sql:1`<br/>`sql/ops/journal_notification.sql:2` (!)<br/>`sql/ops/record_state.sql:2` (!)<br/>`sql/ops/state.sql:2` (!)<br/>`sql/purge/record_purge.sql:1`<br/>`sql/reports/daily_pending.sql:2`<br/>`sql/reports/record_report.sql:2`<br/>`sql/verdicts/batches_awaiting_verdict.sql:2` (!)<br/>`sql/verdicts/known_transactions.sql:2` (!)<br/>`sql/verdicts/withheld.sql:2` (!)<br/>`tests/test_acceptance_files.py:3` (!)<br/>`tests/test_check_s4.py:3` (!)<br/>`tests/test_dag_daily.py:3` (!)<br/>`tests/test_dag_ingest.py:3` (!)<br/>`tests/test_dag_purge.py:3` (!)<br/>`tests/test_dashboard.py:3` (!)<br/>`tests/test_dashboard_morning.py:3` (!)<br/>`tests/test_dashboard_operations.py:3` (!)<br/>`tests/test_dashboard_ops.py:3` (!)<br/>`tests/test_health.py:3` (!)<br/>`tests/test_notify.py:3` (!)<br/>`tests/test_purge_journal.py:3` (!)<br/>`tests/test_report.py:3` (!)<br/>`tests/test_report_journal.py:3` (!)<br/>`tests/test_runs_journal.py:3` (!)<br/>`tools/check_s4.py:3` (!)<br/>`tools/ingest_once.py:3`<br/>`tools/ops_check.py:3` (!)<br/>`airflow/dags/dag_daily.py:3` (!)<br/>`airflow/dags/dag_daily.py:74 _mark_failed()` (!)<br/>`airflow/dags/dag_daily.py:114 open_run()` (!)<br/>`airflow/dags/dag_daily.py:180 close_run()` (!)<br/>`airflow/dags/dag_ingest.py:3`<br/>`airflow/dags/dag_ingest.py:96 _mark_failed()` (!)<br/>`airflow/dags/dag_ingest.py:195 validate()`<br/>`airflow/dags/dag_ingest.py:337 raise_and_notify_fraud()` (!)<br/>`airflow/dags/dag_ingest.py:360 close_run()` (!)<br/>`airflow/dags/dag_ingest.py:389 check()` (!)<br/>`airflow/dags/dag_ingest.py:417 raise_and_notify_ops()` (!)<br/>`airflow/dags/dag_ingest_verdicts.py:3` (!)<br/>`airflow/dags/dag_purge.py:3` (!)<br/>`airflow/dags/dag_purge.py:81 _mark_failed()` (!)<br/>`airflow/dags/dag_purge.py:124 open_run()` (!)<br/>`airflow/dags/dag_purge.py:234 close_run()` (!)<br/>`airflow/dags/dag_train.py:218 guard()` (!)<br/>`dashboard/pages/operations.py:3` (!)<br/>`dashboard/pages/ops.py:3` (!)<br/>`dashboard/pages/report.py:3` (!) |
| **EC15** | C2 | F | Restituer la sortie du modèle sous une forme exploitable par une technologie d'assistance : valeur textuelle, sans dép | `ml4fd/quarantine.py:103 breakdown()` (!)<br/>`ml4fd/serve.py:3`<br/>`ml4fd/serve.py:315 sentence()` (!)<br/>`sql/dashboard/rejects_of_day.sql:3` (!)<br/>`sql/dashboard/rejects_total_of_day.sql:2` (!)<br/>`sql/dashboard/stream_rate_of_day.sql:3` (!)<br/>`tests/test_check_l5.py:4`<br/>`tests/test_dashboard_rejects.py:3` (!)<br/>`tests/test_serve.py:3`<br/>`tools/check_l5.py:3`<br/>`dashboard/pages/rejects.py:3` (!)<br/>`scoring_service/app.py:3` (!)<br/>`scoring_service/app.py:159 post_score()` |
| **EC16** | C3 | A | Exposer la solution d'IA par une API de prédiction déployée et interrogeable par un tiers | `ml4fd/purge.py:3` (!)<br/>`ml4fd/score.py:364 remote_rows()`<br/>`ml4fd/serve.py:3`<br/>`ml4fd/serve.py:280 batch_answer()`<br/>`sql/checks/l5_acceptance.sql:2`<br/>`sql/checks/s5_acceptance.sql:2` (!)<br/>`sql/dashboard/decisions_by_source.sql:3`<br/>`sql/dashboard/decisions_third_party.sql:2`<br/>`sql/ddl/145_scoring_service_role.sql:1`<br/>`sql/purge/020_quarantine.sql:2` (!)<br/>`sql/scoring_service/features_for_call.sql:2`<br/>`tests/test_api_service.py:3`<br/>`tests/test_check_l5.py:4`<br/>`tests/test_dashboard_decisions.py:3`<br/>`tests/test_purge_journal.py:3` (!)<br/>`tests/test_purge_plan.py:3` (!)<br/>`tests/test_retention.py:14` (!)<br/>`tests/test_serve.py:3`<br/>`tools/check_l5.py:3`<br/>`tools/check_s5.py:4` (!)<br/>`airflow/dags/dag_ingest.py:292 score_batch()`<br/>`dashboard/pages/decisions.py:3`<br/>`scoring_service/app.py:3`<br/>`scoring_service/app.py:159 post_score()`<br/>`scoring_service/app.py:201 post_score_batch()` |
| **EC17** | C3 | D | Documenter le contrat de l'API dans une spécification interrogeable : routes, schémas de requête et de réponse, codes  | `ml4fd/dashboard.py:3` (!)<br/>`ml4fd/health.py:3` (!)<br/>`ml4fd/health.py:483 notify_ops()`<br/>`ml4fd/notify.py:3`<br/>`ml4fd/report.py:3` (!)<br/>`ml4fd/serve.py:143 anonymous()`<br/>`ml4fd/transform.py:3` (!)<br/>`ml4fd/transform.py:84 client_key()` (!)<br/>`ml4fd/transform.py:107 home_distance_km()` (!)<br/>`ml4fd/transform.py:144 to_transaction()` (!)<br/>`ml4fd/verdicts.py:3`<br/>`sql/checks/l5_acceptance.sql:2`<br/>`sql/checks/s1_acceptance.sql:2` (!)<br/>`sql/checks/s3_acceptance.sql:2` (!)<br/>`sql/dashboard/ops_notifications.sql:3` (!)<br/>`sql/ddl/030_transactions.sql:2` (!)<br/>`sql/ddl/090_frauds.sql:2`<br/>`sql/ddl/135_ops_notifications.sql:2` (!)<br/>`sql/ddl/145_scoring_service_role.sql:1`<br/>`sql/ddl/190_scoring_service_calls.sql:1`<br/>`sql/ops/journal_notification.sql:2` (!)<br/>`sql/reports/daily_frauds.sql:2` (!)<br/>`tests/test_api_service.py:3`<br/>`tests/test_check_s3.py:3` (!)<br/>`tests/test_dashboard.py:3` (!)<br/>`tests/test_dashboard_frauds.py:3` (!)<br/>`tests/test_dashboard_ops.py:3` (!)<br/>`tests/test_health.py:3` (!)<br/>`tests/test_leak.py:3` (!)<br/>`tests/test_notify.py:3` (!)<br/>`tests/test_observe_api.py:3` (!)<br/>`tests/test_report.py:3` (!)<br/>`tests/test_reports.py:3` (!)<br/>`tools/check_leak.py:3` (!)<br/>`tools/check_s3.py:3` (!)<br/>`airflow/dags/dag_ingest.py:235 type_and_feature()` (!)<br/>`airflow/dags/dag_ingest.py:417 raise_and_notify_ops()`<br/>`airflow/dags/dag_load_history.py:279 type_and_pseudonymize()` (!)<br/>`dashboard/pages/frauds.py:3` (!)<br/>`dashboard/pages/ops.py:3` (!)<br/>`scoring_service/app.py:3` |
| **EC18** | C3 | A | Raccorder l'API aux magasins de l'infrastructure : lecture des données d'entrée, écriture des prédictions (9) | `sql/checks/batch_runs.sql:2` (!)<br/>`sql/checks/batch_stores.sql:3` (!)<br/>`sql/checks/s5_acceptance.sql:2` (!)<br/>`sql/ddl/130_purges.sql:2` (!)<br/>`sql/ddl/150_database_connect.sql:2`<br/>`sql/purge/record_purge.sql:1`<br/>`tests/test_forget.py:3` (!)<br/>`tests/test_purge_journal.py:3` (!)<br/>`tools/check_s5.py:4` (!)<br/>`tools/forget_plan.py:3` (!)<br/>`tools/forget_run.py:3` |
| **EC19** | C3 | F | Persister chaque appel et sa réponse : horodatage, entrée, score, décision, version du modèle servi | `ml4fd/config.py:3` (!)<br/>`ml4fd/db.py:3` (!)<br/>`ml4fd/purge.py:3` (!)<br/>`ml4fd/score.py:364 remote_rows()`<br/>`ml4fd/serve.py:3`<br/>`ml4fd/serve.py:280 batch_answer()`<br/>`sql/checks/l5_acceptance.sql:2`<br/>`sql/checks/s5_acceptance.sql:2` (!)<br/>`sql/checks/s6_acceptance.sql:2` (!)<br/>`sql/dashboard/decisions_by_source.sql:3`<br/>`sql/dashboard/decisions_third_party.sql:2`<br/>`sql/ddl/060_roles_grants.sql:2` (!)<br/>`sql/ddl/120_purge_role.sql:2` (!)<br/>`sql/ddl/140_dashboard_role.sql:2` (!)<br/>`sql/ddl/190_scoring_service_calls.sql:1`<br/>`sql/ddl/195_scoring_decisions.sql:1`<br/>`sql/maintenance/060_rename_scoring_service_objects.sql:2` (!)<br/>`sql/purge/010_raw_events.sql:2`<br/>`sql/purge/020_quarantine.sql:2` (!)<br/>`sql/purge/candidates_quarantine.sql:2` (!)<br/>`sql/purge/candidates_raw.sql:2` (!)<br/>`sql/roles/dashboard_password.sql:2` (!)<br/>`sql/scoring_service/record_call.sql:2`<br/>`tests/test_api_service.py:3`<br/>`tests/test_check_l5.py:4`<br/>`tests/test_check_s6.py:3` (!)<br/>`tests/test_dag_purge.py:3` (!)<br/>`tests/test_dashboard_decisions.py:3`<br/>`tests/test_dashboard_role.py:3` (!)<br/>`tests/test_dashboard_service.py:3`<br/>`tests/test_forget.py:3` (!)<br/>`tests/test_purge_journal.py:3` (!)<br/>`tests/test_purge_plan.py:3` (!)<br/>`tests/test_purge_run.py:3`<br/>`tests/test_retention.py:14` (!)<br/>`tests/test_serve.py:3`<br/>`tools/check_l5.py:3`<br/>`tools/check_s5.py:4` (!)<br/>`tools/check_s6.py:3`<br/>`tools/forget_run.py:3`<br/>`tools/purge_run.py:3`<br/>`airflow/dags/dag_purge.py:3`<br/>`airflow/dags/dag_purge.py:206 delete()`<br/>`dashboard/common.py:3` (!)<br/>`dashboard/pages/decisions.py:3`<br/>`dashboard/probe.py:3` (!)<br/>`scoring_service/app.py:3`<br/>`scoring_service/app.py:159 post_score()`<br/>`scoring_service/app.py:201 post_score_batch()` |
| **EC20** | C3 | F | Valider les données entrantes de l'API et retourner une erreur explicite en cas de requête non conforme | `ml4fd/checks.py:3` (!)<br/>`ml4fd/checks.py:126 SourceProfile()` (!)<br/>`ml4fd/checks.py:277 check_clock()` (!)<br/>`ml4fd/checks.py:309 check_fields()` (!)<br/>`ml4fd/checks.py:456 validate_batch()` (!)<br/>`ml4fd/serve.py:3`<br/>`ml4fd/serve.py:74 problems()`<br/>`sql/checks/counters_close.sql:2` (!)<br/>`sql/checks/l5_acceptance.sql:2`<br/>`sql/checks/s1_acceptance.sql:2` (!)<br/>`tests/test_api_service.py:3`<br/>`tests/test_check_l5.py:4`<br/>`tests/test_serve.py:3`<br/>`tools/check_l5.py:3`<br/>`airflow/dags/dag_load_history.py:215 validate()` (!)<br/>`scoring_service/app.py:3`<br/>`scoring_service/app.py:159 post_score()` |
| **EC21** | C3 | F | Exposer un point de contrôle de santé du service et la version du modèle servi | `ml4fd/checks.py:3` (!)<br/>`ml4fd/checks.py:126 SourceProfile()` (!)<br/>`ml4fd/checks.py:369 check_record()` (!)<br/>`ml4fd/checks.py:415 profile_gap()` (!)<br/>`ml4fd/checks.py:456 validate_batch()` (!)<br/>`ml4fd/serve.py:3`<br/>`ml4fd/serve.py:370 health()`<br/>`tests/test_api_service.py:3`<br/>`tests/test_check_l5.py:4`<br/>`tests/test_serve.py:3`<br/>`tools/check_l5.py:3`<br/>`airflow/dags/dag_load_history.py:215 validate()` (!)<br/>`scoring_service/app.py:3`<br/>`scoring_service/app.py:141 health()` |
| **EC22** | C3 | A | Externaliser les secrets et restreindre l'accès à l'API | `ml4fd/quarantine.py:3` (!)<br/>`sql/checks/counters_close.sql:2` (!)<br/>`sql/dashboard/rejects_of_day.sql:3` (!)<br/>`sql/ddl/020_quarantine.sql:2` (!)<br/>`sql/ddl/145_scoring_service_role.sql:1`<br/>`sql/ddl/150_database_connect.sql:2`<br/>`sql/roles/scoring_service_password.sql:2`<br/>`tests/test_api_service.py:3`<br/>`tests/test_dashboard_rejects.py:3` (!)<br/>`airflow/dags/dag_load_history.py:215 validate()` (!)<br/>`dashboard/pages/rejects.py:3` (!) |
| **EC23** | C3 | DM | Déployer dans une configuration de production, *cloud* ou *on-premise*, hors carnet interactif (10) | `ml4fd/build.py:3` (!)<br/>`ml4fd/build.py:68 build()` (!)<br/>`ml4fd/build.py:150 build_or_reuse()` (!)<br/>`ml4fd/datasets.py:3` (!)<br/>`ml4fd/datasets.py:82 without()` (!)<br/>`ml4fd/datasets.py:286 register()` (!)<br/>`ml4fd/notify.py:209 raise_alerts()`<br/>`ml4fd/quarantine.py:89 rejected_lines()`<br/>`ml4fd/raw.py:3` (!)<br/>`ml4fd/raw.py:49 history_in_raw()`<br/>`ml4fd/raw.py:66 refuse_second_load()`<br/>`ml4fd/registry.py:3`<br/>`ml4fd/registry.py:42 resolved_packages()` (!)<br/>`ml4fd/registry.py:64 manifest()` (!)<br/>`ml4fd/registry.py:160 record_reference()` (!)<br/>`ml4fd/registry.py:305 experiment_id()` (!)<br/>`ml4fd/report.py:649 record()` (!)<br/>`ml4fd/runs.py:117 digest()` (!)<br/>`ml4fd/transform.py:201 load_transactions()` (!)<br/>`ml4fd/truth.py:124 load_ground_truth()` (!)<br/>`ml4fd/verdicts.py:3`<br/>`ml4fd/verdicts.py:153 write_due()`<br/>`sql/checks/batch_digest.sql:2` (!)<br/>`sql/checks/batch_digest_recorded.sql:2` (!)<br/>`sql/checks/batch_runs.sql:2` (!)<br/>`sql/checks/batch_stores.sql:3` (!)<br/>`sql/checks/history_already_loaded.sql:2` (!)<br/>`sql/checks/quarantined_lines.sql:2`<br/>`sql/checks/raw_batches.sql:2` (!)<br/>`sql/checks/s1_acceptance.sql:2` (!)<br/>`sql/checks/s2_acceptance.sql:2` (!)<br/>`sql/checks/s3_acceptance.sql:2` (!)<br/>`sql/checks/s4_acceptance.sql:2` (!)<br/>`sql/checks/s5_acceptance.sql:2` (!)<br/>`sql/checks/table_digest.sql:2` (!)<br/>`sql/checks/undigested_batches.sql:2` (!)<br/>`sql/datasets/record_reference.sql:2` (!)<br/>`sql/ddl/070_datasets.sql:2` (!)<br/>`sql/ddl/070_datasets.sql:52` (!)<br/>`sql/ddl/110_reports.sql:2` (!)<br/>`sql/frauds/mark_notified.sql:2`<br/>`sql/frauds/raise_from_scores.sql:2`<br/>`sql/maintenance/020_forget_orphan_datasets.sql:2` (!)<br/>`sql/maintenance/050_move_runtime_paths.sql:3` (!)<br/>`sql/reports/record_report.sql:2` (!)<br/>`sql/scoring/write_scores.sql:2`<br/>`sql/verdicts/batches_awaiting_verdict.sql:2`<br/>`sql/verdicts/known_transactions.sql:2`<br/>`tests/test_api_service.py:3`<br/>`tests/test_batch_digest.py:3` (!)<br/>`tests/test_batches_listing.py:3` (!)<br/>`tests/test_check_s4.py:3` (!)<br/>`tests/test_dag_ingest.py:3` (!)<br/>`tests/test_dag_train.py:3` (!)<br/>`tests/test_dashboard_performances.py:3` (!)<br/>`tests/test_driver_import.py:3`<br/>`tests/test_forget.py:3` (!)<br/>`tests/test_history_guard.py:3` (!)<br/>`tests/test_indexes.py:3`<br/>`tests/test_migration_guard.py:3` (!)<br/>`tests/test_mounts.py:3` (!)<br/>`tests/test_purge_journal.py:3` (!)<br/>`tests/test_report_journal.py:3` (!)<br/>`tests/test_score.py:3` (!)<br/>`tests/test_staging_table.py:3` (!)<br/>`tools/backfill_reference.py:3` (!)<br/>`tools/check_s2.py:3` (!)<br/>`tools/check_s4.py:3` (!)<br/>`tools/check_s5.py:4` (!)<br/>`tools/digest_batches.py:3` (!)<br/>`tools/forget_plan.py:3` (!)<br/>`tools/forget_run.py:3`<br/>`tools/write_verdicts.py:3`<br/>`airflow/dags/dag_ingest.py:3`<br/>`airflow/dags/dag_ingest.py:139 poll()` (!)<br/>`airflow/dags/dag_ingest.py:360 close_run()` (!)<br/>`airflow/dags/dag_ingest_verdicts.py:3`<br/>`airflow/dags/dag_ingest_verdicts.py:63 write()`<br/>`airflow/dags/dag_load_history.py:165 write_raw()` (!)<br/>`airflow/dags/dag_load_history.py:279 type_and_pseudonymize()` (!)<br/>`airflow/dags/dag_load_history.py:355 write_ground_truth()` (!)<br/>`airflow/dags/dag_load_history.py:432 close_run()` (!)<br/>`airflow/dags/dag_train.py:90 resolve_cut_off()` (!)<br/>`airflow/dags/dag_train.py:161 resolve_features()` (!)<br/>`airflow/dags/dag_train.py:240 build_dataset()` (!)<br/>`scoring_service/app.py:3` |
| **EC24** | C4 | A | Déclarer la chaîne d'intégration et de déploiement continus comme du code versionné dans le dépôt | `ml4fd/clock.py:3` (!)<br/>`ml4fd/config.py:295 observed_per_day()` (!)<br/>`ml4fd/paired.py:3` (!)<br/>`ml4fd/raw.py:3` (!)<br/>`ml4fd/raw.py:49 history_in_raw()`<br/>`ml4fd/raw.py:66 refuse_second_load()`<br/>`ml4fd/runs.py:117 digest()` (!)<br/>`ml4fd/score.py:3` (!)<br/>`ml4fd/score.py:252 load()` (!)<br/>`ml4fd/score.py:364 remote_rows()` (!)<br/>`sql/checks/history_already_loaded.sql:2` (!)<br/>`sql/checks/s3_acceptance.sql:2`<br/>`sql/checks/table_digest.sql:2` (!)<br/>`sql/ddl/010_raw_events.sql:2` (!)<br/>`sql/ddl/080_scores.sql:2`<br/>`sql/maintenance/050_move_runtime_paths.sql:3`<br/>`sql/scoring/write_scores.sql:2` (!)<br/>`tests/test_clock.py:3` (!)<br/>`tests/test_compare_recipes.py:3` (!)<br/>`tests/test_history_guard.py:3`<br/>`tests/test_measure_hour_effect.py:3` (!)<br/>`tests/test_mounts.py:3` (!)<br/>`tests/test_paired.py:3` (!)<br/>`tests/test_score.py:3` (!)<br/>`tools/check_s3.py:3`<br/>`tools/compare_recipes.py:3` (!)<br/>`tools/measure_hour_effect.py:3`<br/>`tools/rechoose_threshold.py:3` (!)<br/>`airflow/dags/dag_ingest.py:3`<br/>`airflow/dags/dag_ingest.py:292 score_batch()` (!)<br/>`airflow/dags/dag_load_history.py:165 write_raw()` (!)<br/>`airflow/dags/dag_load_history.py:432 close_run()` |
| **EC25** | C4 | A | Déclencher l'intégration automatiquement à chaque modification du code : construction, analyse statique, tests | `ml4fd/stream.py:171 decode()` (!)<br/>`ml4fd/stream.py:362 validate_raw()` (!)<br/>`ml4fd/stream.py:406 validate()` (!)<br/>`tools/ingest_once.py:3` (!)<br/>`tools/observe_api.py:3` (!)<br/>`airflow/dags/dag_ingest.py:195 validate()` (!)<br/>`producer/app.py:3` (!) |
| **EC26** | C4 | F | Bloquer le déploiement lorsqu'une étape d'intégration échoue (11) | `ml4fd/checks.py:277 check_clock()` (!)<br/>`ml4fd/purge.py:3` (!)<br/>`ml4fd/stream.py:362 validate_raw()` (!)<br/>`ml4fd/stream.py:406 validate()` (!)<br/>`sql/purge/candidates_quarantine.sql:2` (!)<br/>`sql/purge/candidates_raw.sql:2` (!)<br/>`tests/test_purge_plan.py:3` (!)<br/>`tests/test_purge_run.py:3` (!)<br/>`tools/forget_run.py:3` (!)<br/>`tools/purge_plan.py:3` (!)<br/>`tools/purge_run.py:3` (!)<br/>`airflow/dags/dag_purge.py:148 dry_run()` (!)<br/>`airflow/dags/dag_purge.py:177 check_share()` |
| **EC27** | C4 | A | Déclencher le déploiement sans intervention manuelle, et le tracer jusqu'au *commit* et à la version de modèle déployé | `sql/checks/s4_acceptance.sql:2`<br/>`sql/checks/s5_acceptance.sql:2`<br/>`sql/checks/s6_acceptance.sql:2`<br/>`sql/ddl/130_purges.sql:2`<br/>`tests/test_check_s4.py:3` (!)<br/>`tests/test_check_s6.py:3`<br/>`tests/test_dashboard_service.py:3` (!)<br/>`tests/test_purge_journal.py:3`<br/>`tools/check_s4.py:3`<br/>`tools/check_s5.py:4`<br/>`tools/check_s6.py:3`<br/>`dashboard/probe.py:3` |
| **EC28** | C4 | F | Tester la solution au-delà du code applicatif : contrat de l'API, préparation des *features*, chargement du modèle (12 | `tools/build_traceability.py:7` (!) |
| **EC29** | C4 | A | Gérer les secrets par le magasin de secrets de la chaîne, sans valeur en clair dans le dépôt | - |
| **EC30** | C4 | F | Permettre le retour à la version précédente du service | - |
| **EC31** | C5 | A | Implémenter le réentraînement comme un script exécutable sans intervention manuelle | `airflow/dags/dag_train.py:3` |
| **EC32** | C5 | A | Déclencher le réentraînement par un ordonnanceur, et déclarer sa cadence | `ml4fd/config.py:212 monitoring_schedule()` |
| **EC33** | C5 | F | Déclarer et implémenter la politique de déclenchement : périodique, et/ou sur franchissement d'un seuil de dérive ou d | `ml4fd/config.py:212 monitoring_schedule()`<br/>`ml4fd/monitor.py:782 retraining_due()`<br/>`sql/dashboard/retrain_requests.sql:2`<br/>`sql/ddl/180_retrain_requests.sql:1`<br/>`sql/monitoring/last_retrain_request.sql:2`<br/>`sql/monitoring/record_retrain_request.sql:1`<br/>`tests/test_dag_monitor.py:3`<br/>`tests/test_monitor.py:3`<br/>`airflow/dags/dag_monitor.py:3`<br/>`airflow/dags/dag_train.py:449 challenge_and_promote()` |
| **EC34** | C5 | F | Comparer le modèle candidat au modèle en service sur les critères du cahier des charges avant toute promotion | `ml4fd/challenge.py:3`<br/>`ml4fd/challenge.py:209 judge()`<br/>`ml4fd/promote.py:3`<br/>`ml4fd/registry.py:403 version_of_run()`<br/>`sql/ddl/160_promotions.sql:2`<br/>`sql/promotions/record_decision.sql:2`<br/>`tests/test_promote.py:3`<br/>`tools/promote.py:3`<br/>`airflow/dags/dag_train.py:449 challenge_and_promote()` |
| **EC35** | C5 | A | Versionner chaque modèle produit avec le jeu de données, les paramètres et les métriques qui l'ont produit | `ml4fd/promote.py:3`<br/>`sql/ddl/160_promotions.sql:2`<br/>`tests/test_promote.py:3` |
| **EC36** | C5 | F | Implémenter la promotion du candidat et le retour au modèle précédent sans interruption du service de prédiction | `ml4fd/challenge.py:3`<br/>`ml4fd/challenge.py:209 judge()`<br/>`ml4fd/promote.py:3`<br/>`ml4fd/score.py:226 champion_version()`<br/>`ml4fd/score.py:252 load()`<br/>`tests/test_promote.py:3`<br/>`tools/promote.py:3`<br/>`airflow/dags/dag_train.py:449 challenge_and_promote()` |
| **EC37** | C5 | F | Journaliser chaque exécution de réentraînement : début, fin, état, volumétrie, métriques obtenues, décision de promoti | `ml4fd/challenge.py:3`<br/>`ml4fd/challenge.py:209 judge()`<br/>`sql/ddl/160_promotions.sql:2`<br/>`sql/ddl/180_retrain_requests.sql:1`<br/>`sql/monitoring/record_retrain_request.sql:1`<br/>`sql/promotions/record_decision.sql:2`<br/>`tools/promote.py:3`<br/>`airflow/dags/dag_train.py:449 challenge_and_promote()` |
| **EC38** | C6 | A | Mettre en œuvre un outil de monitoring parmi ceux nommés dans \[R1\] : *Evidently* ou *Aporia* (14) | `ml4fd/monitor.py:3`<br/>`tests/test_monitor.py:3`<br/>`airflow/dags/dag_monitor.py:3` |
| **EC39** | C6 | F | Surveiller la dérive des données d'entrée et la dérive des prédictions | `ml4fd/config.py:174 drift_window()`<br/>`ml4fd/config.py:247 drift_min_rows()`<br/>`ml4fd/monitor.py:3`<br/>`ml4fd/monitor.py:148 drift_columns()`<br/>`ml4fd/monitor.py:224 comparable()`<br/>`ml4fd/monitor.py:294 thin_window()`<br/>`sql/dashboard/monitoring_columns.sql:2`<br/>`sql/monitoring/features_window.sql:2`<br/>`sql/monitoring/flag_rate_window.sql:2`<br/>`tests/test_dag_monitor.py:3`<br/>`tests/test_drift.py:3`<br/>`tests/test_monitor.py:3`<br/>`tools/drift.py:14`<br/>`airflow/dags/dag_monitor.py:3`<br/>`producer/app.py:3` |
| **EC40** | C6 | F | Surveiller la performance du modèle en production contre les critères d'acceptation du cahier des charges (15) | `ml4fd/config.py:192 performance_window()`<br/>`ml4fd/monitor.py:3`<br/>`sql/ddl/170_monitoring.sql:2`<br/>`sql/monitoring/performance_window.sql:2`<br/>`tests/test_monitor.py:3`<br/>`airflow/dags/dag_monitor.py:3` |
| **EC41** | C6 | A | Définir la source et le délai d'obtention de la vérité-terrain en production, et dater le verdict de sa date de connai | `ml4fd/config.py:229 verdict_schedule()`<br/>`sql/monitoring/performance_window.sql:2` |
| **EC42** | C6 | F | Surveiller la qualité des données reçues en production : schéma, valeurs manquantes, valeurs aberrantes, fraîcheur | `ml4fd/monitor.py:3`<br/>`sql/ddl/170_monitoring.sql:2`<br/>`tests/test_monitor.py:3`<br/>`airflow/dags/dag_monitor.py:3` |
| **EC43** | C6 | F | Surveiller la performance technique du service contre les cibles du cahier des charges : latence, taux d'erreur, dispo | - |
| **EC44** | C6 | F | Alerter sur franchissement de seuil, sur un canal nommé, et désigner le destinataire de l'alerte | `ml4fd/monitor.py:665 notify()`<br/>`tests/test_monitor.py:3`<br/>`airflow/dags/dag_monitor.py:3` |
| **EC45** | C6 | A | Conserver l'historique des mesures, afin de pouvoir constater une évolution dans le temps long (17) | `ml4fd/config.py:174 drift_window()`<br/>`ml4fd/config.py:247 drift_min_rows()`<br/>`ml4fd/dashboard.py:923 monitoring_view()`<br/>`ml4fd/monitor.py:3`<br/>`ml4fd/monitor.py:148 drift_columns()`<br/>`ml4fd/monitor.py:294 thin_window()`<br/>`sql/dashboard/monitoring_latest.sql:3`<br/>`sql/dashboard/monitoring_series.sql:3`<br/>`sql/ddl/170_monitoring.sql:2`<br/>`sql/monitoring/record_measurement.sql:2`<br/>`tests/test_dag_monitor.py:3`<br/>`tests/test_dashboard_monitoring.py:3`<br/>`tests/test_monitor.py:3`<br/>`airflow/dags/dag_monitor.py:3`<br/>`dashboard/pages/monitoring.py:3` |
| **EC46** | C6 | L | Livrer une présentation de la solution d'IA répondant au cahier des charges, au format d'un outil de présentation | - |
| **EC47** | C6 | L | Héberger le code sur **GitHub** | - |
| **EC48** | C6 | L | Tenir la présentation du ou des pipelines en 5 minutes, et prévoir 10 minutes de questions (18) | - |
| **EC49** | C6 | L | Produire un rapport ou un tableau de bord de monitoring consultable et présentable devant le jury | `ml4fd/dashboard.py:923 monitoring_view()`<br/>`sql/dashboard/monitoring_columns.sql:2`<br/>`sql/dashboard/monitoring_latest.sql:3`<br/>`sql/dashboard/monitoring_series.sql:3`<br/>`sql/dashboard/retrain_requests.sql:2`<br/>`tests/test_dashboard_monitoring.py:3`<br/>`dashboard/pages/monitoring.py:3` |
| **EC50** | C6 | D | Tenir une matrice de traçabilité compétence → exigence → élément de la solution → emplacement du code (19) | `tools/build_traceability.py:7` |
| **EC51** | C6 | DM | Démontrer la chaîne complète en fonctionnement sur un exemple : appel de l'API, intégration continue, déploiement, rée | `tests/test_drift.py:3`<br/>`tools/drift.py:14` |

## Decompte

- exigences definies : **51**
- portees par du code : **40**
- sans code : **11**, dont de nature fonctionnelle : **2**
- livrables, dont l'existence n'est pas verifiee ici : **4**
- marques a verifier (desaccord de competence) : **306**
- ruptures de tracabilite : **0**

### Fonctionnelles, sans code

Une exigence de nature F sans emplacement est soit non realisee, soit realisee sans marque `Covers:`. Les deux se corrigent, et pas au meme endroit.

**EC30**, **EC43**

### Livrables, hors code

Une exigence de nature L designe une chose qui doit EXISTER -- une presentation, un depot, un rapport -- et non une ligne de code. Cet outil ne peut pas verifier son existence : il la nomme, et le dire vaut mieux qu'un faux rouge.

**EC46**, **EC47**, **EC48**, **EC49**

## Par competence

- C1 : 5/7 portee(s) par du code, manque EC02, EC04
- C2 : 5/8 portee(s) par du code, manque EC10, EC11, EC13
- C3 : 8/8 portee(s) par du code
- C4 : 5/7 portee(s) par du code, manque EC29, EC30
- C5 : 7/7 portee(s) par du code
- C6 : 10/14 portee(s) par du code, manque EC43, EC46, EC47, EC48

### Marques en desaccord (aide au repointage)

Une marque `Covers:` dont la ligne `Competency:` contredit la section sous laquelle le document classe l'exigence. Les deux sont des declarations du meme fait, donc l'une des deux est fausse. Une frontiere reelle en est un cas particulier et se corrige du cote de la marque : EC33 est declenchee par le monitoring (C6) et executee par le reentrainement (C5), et `tests/test_monitor.py` a cesse d'y figurer le jour ou sa ligne `Competency:` a dit les deux. Ces emplacements portent **(!)** dans la colonne Code. Ils ne changent aucun compte : ce tableau trie, il ne juge pas.

| Exigence | Document | Marque | Emplacement |
| --- | --- | --- | --- |
| **EC01** | C1 | C2 | `ml4fd/stream.py:249 fetch_one()` |
| **EC01** | C1 | C2 | `ml4fd/stream.py:3` |
| **EC05** | C1 | C2, C5 | `ml4fd/build.py:68 build()` |
| **EC05** | C1 | C2, C4 | `ml4fd/db.py:3` |
| **EC06** | C1 | C2, C4 | `ml4fd/db.py:3` |
| **EC06** | C1 | C5 | `tests/test_fd_manager.py:3` |
| **EC06** | C1 | C5 | `tests/test_module_scope.py:3` |
| **EC07** | C1 | C5 | `tests/test_acceptance_files.py:3` |
| **EC08** | C2 | C3 | `airflow/dags/dag_load_history.py:3` |
| **EC08** | C2 | C3, C5 | `airflow/dags/dag_train.py:3` |
| **EC09** | C2 | C1, C3, C4 | `airflow/dags/dag_daily.py:3` |
| **EC09** | C2 | C3, C4 | `airflow/dags/dag_ingest.py:337 raise_and_notify_fraud()` |
| **EC09** | C2 | C4 | `airflow/dags/dag_purge.py:177 check_share()` |
| **EC09** | C2 | C3, C4 | `airflow/dags/dag_purge.py:3` |
| **EC09** | C2 | C3, C5 | `airflow/dags/dag_train.py:218 guard()` |
| **EC09** | C2 | C3, C5 | `airflow/dags/dag_train.py:3` |
| **EC09** | C2 | C3, C5 | `ml4fd/guard.py:3` |
| **EC09** | C2 | C5 | `ml4fd/guard.py:98 verdict()` |
| **EC09** | C2 | C3, C4 | `ml4fd/notify.py:243 notify()` |
| **EC09** | C2 | C3, C4 | `ml4fd/notify.py:3` |
| **EC09** | C2 | C1, C4, C5 | `sql/checks/s3_acceptance.sql:2` |
| **EC09** | C2 | C4 | `sql/frauds/notified_last_day.sql:2` |
| **EC09** | C2 | C5 | `tests/test_check_s3.py:3` |
| **EC09** | C2 | C5 | `tests/test_dag_daily.py:3` |
| **EC09** | C2 | C5 | `tests/test_dag_ingest.py:3` |
| **EC09** | C2 | C5 | `tests/test_dag_purge.py:3` |
| **EC09** | C2 | C5 | `tests/test_notify.py:3` |
| **EC09** | C2 | C5 | `tests/test_rechoose_threshold.py:3` |
| **EC09** | C2 | C1, C4, C5 | `tools/check_s3.py:3` |
| **EC12** | C2 | C3 | `airflow/dags/dag_load_history.py:3` |
| **EC12** | C2 | C4, C5 | `airflow/dags/dag_load_history.py:432 close_run()` |
| **EC12** | C2 | C4 | `airflow/dags/dag_load_history.py:69 preflight()` |
| **EC12** | C2 | C4 | `ml4fd/runs.py:3` |
| **EC12** | C2 | C4 | `sql/checks/raw_batches.sql:2` |
| **EC12** | C2 | C4 | `sql/ddl/050_pipeline_runs.sql:2` |
| **EC12** | C2 | C4 | `tests/test_batches_listing.py:3` |
| **EC12** | C2 | C4, C5 | `tests/test_runs_journal.py:3` |
| **EC14** | C2 | C4 | `airflow/dags/dag_daily.py:114 open_run()` |
| **EC14** | C2 | C4 | `airflow/dags/dag_daily.py:180 close_run()` |
| **EC14** | C2 | C1, C3, C4 | `airflow/dags/dag_daily.py:3` |
| **EC14** | C2 | C4 | `airflow/dags/dag_daily.py:74 _mark_failed()` |
| **EC14** | C2 | C3, C4 | `airflow/dags/dag_ingest.py:337 raise_and_notify_fraud()` |
| **EC14** | C2 | C4 | `airflow/dags/dag_ingest.py:360 close_run()` |
| **EC14** | C2 | C4 | `airflow/dags/dag_ingest.py:389 check()` |
| **EC14** | C2 | C3, C4 | `airflow/dags/dag_ingest.py:417 raise_and_notify_ops()` |
| **EC14** | C2 | C4 | `airflow/dags/dag_ingest.py:96 _mark_failed()` |
| **EC14** | C2 | C1, C3, C4 | `airflow/dags/dag_ingest_verdicts.py:3` |
| **EC14** | C2 | C4 | `airflow/dags/dag_purge.py:124 open_run()` |
| **EC14** | C2 | C4 | `airflow/dags/dag_purge.py:234 close_run()` |
| **EC14** | C2 | C3, C4 | `airflow/dags/dag_purge.py:3` |
| **EC14** | C2 | C4 | `airflow/dags/dag_purge.py:81 _mark_failed()` |
| **EC14** | C2 | C3, C5 | `airflow/dags/dag_train.py:218 guard()` |
| **EC14** | C2 | C4 | `dashboard/pages/operations.py:3` |
| **EC14** | C2 | C4 | `dashboard/pages/ops.py:3` |
| **EC14** | C2 | C4 | `dashboard/pages/report.py:3` |
| **EC14** | C2 | C4 | `ml4fd/dashboard.py:3` |
| **EC14** | C2 | C5 | `ml4fd/datasets.py:240 assert_cut_off_usable()` |
| **EC14** | C2 | C3, C5 | `ml4fd/guard.py:3` |
| **EC14** | C2 | C5 | `ml4fd/guard.py:98 verdict()` |
| **EC14** | C2 | C4 | `ml4fd/health.py:459 check()` |
| **EC14** | C2 | C3, C4 | `ml4fd/health.py:483 notify_ops()` |
| **EC14** | C2 | C3, C4 | `ml4fd/notify.py:243 notify()` |
| **EC14** | C2 | C3, C4 | `ml4fd/notify.py:3` |
| **EC14** | C2 | C3 | `ml4fd/quarantine.py:89 rejected_lines()` |
| **EC14** | C2 | C3, C4, C5 | `ml4fd/verdicts.py:3` |
| **EC14** | C2 | C4 | `sql/checks/abandoned_runs.sql:2` |
| **EC14** | C2 | C4 | `sql/checks/pipeline_health.sql:2` |
| **EC14** | C2 | C3 | `sql/checks/quarantined_lines.sql:2` |
| **EC14** | C2 | C1, C4, C5 | `sql/checks/s3_acceptance.sql:2` |
| **EC14** | C2 | C4 | `sql/checks/s4_absence.sql:2` |
| **EC14** | C2 | C1, C4, C5 | `sql/checks/s4_acceptance.sql:2` |
| **EC14** | C2 | C1, C4, C5 | `sql/checks/s5_acceptance.sql:2` |
| **EC14** | C2 | C4 | `sql/dashboard/ops_notifications.sql:3` |
| **EC14** | C2 | C4 | `sql/dashboard/recent_runs.sql:2` |
| **EC14** | C2 | C4 | `sql/dashboard/stream_rate_of_day.sql:3` |
| **EC14** | C2 | C4, C5 | `sql/ddl/130_purges.sql:2` |
| **EC14** | C2 | C4 | `sql/ddl/135_ops_notifications.sql:2` |
| **EC14** | C2 | C4 | `sql/frauds/record_failure.sql:2` |
| **EC14** | C2 | C4 | `sql/ops/journal_notification.sql:2` |
| **EC14** | C2 | C4 | `sql/ops/record_state.sql:2` |
| **EC14** | C2 | C4 | `sql/ops/state.sql:2` |
| **EC14** | C2 | C3, C4 | `sql/verdicts/batches_awaiting_verdict.sql:2` |
| **EC14** | C2 | C3 | `sql/verdicts/known_transactions.sql:2` |
| **EC14** | C2 | C4 | `sql/verdicts/withheld.sql:2` |
| **EC14** | C2 | C5 | `tests/test_acceptance_files.py:3` |
| **EC14** | C2 | C5 | `tests/test_check_s4.py:3` |
| **EC14** | C2 | C5 | `tests/test_dag_daily.py:3` |
| **EC14** | C2 | C5 | `tests/test_dag_ingest.py:3` |
| **EC14** | C2 | C5 | `tests/test_dag_purge.py:3` |
| **EC14** | C2 | C4 | `tests/test_dashboard.py:3` |
| **EC14** | C2 | C4 | `tests/test_dashboard_morning.py:3` |
| **EC14** | C2 | C4 | `tests/test_dashboard_operations.py:3` |
| **EC14** | C2 | C4 | `tests/test_dashboard_ops.py:3` |
| **EC14** | C2 | C5 | `tests/test_health.py:3` |
| **EC14** | C2 | C5 | `tests/test_notify.py:3` |
| **EC14** | C2 | C4, C5 | `tests/test_purge_journal.py:3` |
| **EC14** | C2 | C5 | `tests/test_report.py:3` |
| **EC14** | C2 | C5 | `tests/test_report_journal.py:3` |
| **EC14** | C2 | C4, C5 | `tests/test_runs_journal.py:3` |
| **EC14** | C2 | C1, C4, C5 | `tools/check_s4.py:3` |
| **EC14** | C2 | C4, C5 | `tools/ops_check.py:3` |
| **EC15** | C2 | C4 | `dashboard/pages/rejects.py:3` |
| **EC15** | C2 | C4 | `ml4fd/quarantine.py:103 breakdown()` |
| **EC15** | C2 | C3 | `ml4fd/serve.py:315 sentence()` |
| **EC15** | C2 | C3 | `scoring_service/app.py:3` |
| **EC15** | C2 | C4 | `sql/dashboard/rejects_of_day.sql:3` |
| **EC15** | C2 | C4 | `sql/dashboard/rejects_total_of_day.sql:2` |
| **EC15** | C2 | C4 | `sql/dashboard/stream_rate_of_day.sql:3` |
| **EC15** | C2 | C4 | `tests/test_dashboard_rejects.py:3` |
| **EC16** | C3 | C5 | `ml4fd/purge.py:3` |
| **EC16** | C3 | C1, C4, C5 | `sql/checks/s5_acceptance.sql:2` |
| **EC16** | C3 | C2 | `sql/purge/020_quarantine.sql:2` |
| **EC16** | C3 | C4, C5 | `tests/test_purge_journal.py:3` |
| **EC16** | C3 | C5 | `tests/test_purge_plan.py:3` |
| **EC16** | C3 | C1, C5 | `tests/test_retention.py:14` |
| **EC16** | C3 | C1, C4, C5 | `tools/check_s5.py:4` |
| **EC17** | C3 | C2 | `airflow/dags/dag_ingest.py:235 type_and_feature()` |
| **EC17** | C3 | C2, C4 | `airflow/dags/dag_load_history.py:279 type_and_pseudonymize()` |
| **EC17** | C3 | C4 | `dashboard/pages/frauds.py:3` |
| **EC17** | C3 | C4 | `dashboard/pages/ops.py:3` |
| **EC17** | C3 | C4 | `ml4fd/dashboard.py:3` |
| **EC17** | C3 | C1, C2, C4 | `ml4fd/health.py:3` |
| **EC17** | C3 | C2 | `ml4fd/report.py:3` |
| **EC17** | C3 | C4 | `ml4fd/transform.py:107 home_distance_km()` |
| **EC17** | C3 | C2 | `ml4fd/transform.py:144 to_transaction()` |
| **EC17** | C3 | C2, C4 | `ml4fd/transform.py:3` |
| **EC17** | C3 | C4 | `ml4fd/transform.py:84 client_key()` |
| **EC17** | C3 | C1, C2, C4, C5 | `sql/checks/s1_acceptance.sql:2` |
| **EC17** | C3 | C1, C4, C5 | `sql/checks/s3_acceptance.sql:2` |
| **EC17** | C3 | C4 | `sql/dashboard/ops_notifications.sql:3` |
| **EC17** | C3 | C2 | `sql/ddl/030_transactions.sql:2` |
| **EC17** | C3 | C4 | `sql/ddl/135_ops_notifications.sql:2` |
| **EC17** | C3 | C4 | `sql/ops/journal_notification.sql:2` |
| **EC17** | C3 | C2 | `sql/reports/daily_frauds.sql:2` |
| **EC17** | C3 | C5 | `tests/test_check_s3.py:3` |
| **EC17** | C3 | C4 | `tests/test_dashboard.py:3` |
| **EC17** | C3 | C4 | `tests/test_dashboard_frauds.py:3` |
| **EC17** | C3 | C4 | `tests/test_dashboard_ops.py:3` |
| **EC17** | C3 | C5 | `tests/test_health.py:3` |
| **EC17** | C3 | C5 | `tests/test_leak.py:3` |
| **EC17** | C3 | C5 | `tests/test_notify.py:3` |
| **EC17** | C3 | C5 | `tests/test_observe_api.py:3` |
| **EC17** | C3 | C5 | `tests/test_report.py:3` |
| **EC17** | C3 | C5 | `tests/test_reports.py:3` |
| **EC17** | C3 | C4, C5 | `tools/check_leak.py:3` |
| **EC17** | C3 | C1, C4, C5 | `tools/check_s3.py:3` |
| **EC18** | C3 | C4 | `sql/checks/batch_runs.sql:2` |
| **EC18** | C3 | C4, C5 | `sql/checks/batch_stores.sql:3` |
| **EC18** | C3 | C1, C4, C5 | `sql/checks/s5_acceptance.sql:2` |
| **EC18** | C3 | C4, C5 | `sql/ddl/130_purges.sql:2` |
| **EC18** | C3 | C2, C4, C5 | `tests/test_forget.py:3` |
| **EC18** | C3 | C4, C5 | `tests/test_purge_journal.py:3` |
| **EC18** | C3 | C1, C4, C5 | `tools/check_s5.py:4` |
| **EC18** | C3 | C2, C4 | `tools/forget_plan.py:3` |
| **EC19** | C3 | C4 | `dashboard/common.py:3` |
| **EC19** | C3 | C4, C5 | `dashboard/probe.py:3` |
| **EC19** | C3 | C4 | `ml4fd/config.py:3` |
| **EC19** | C3 | C2, C4 | `ml4fd/db.py:3` |
| **EC19** | C3 | C5 | `ml4fd/purge.py:3` |
| **EC19** | C3 | C1, C4, C5 | `sql/checks/s5_acceptance.sql:2` |
| **EC19** | C3 | C4, C5 | `sql/checks/s6_acceptance.sql:2` |
| **EC19** | C3 | C4 | `sql/ddl/060_roles_grants.sql:2` |
| **EC19** | C3 | C4 | `sql/ddl/120_purge_role.sql:2` |
| **EC19** | C3 | C2, C5 | `sql/ddl/140_dashboard_role.sql:2` |
| **EC19** | C3 | C4 | `sql/maintenance/060_rename_scoring_service_objects.sql:2` |
| **EC19** | C3 | C2 | `sql/purge/020_quarantine.sql:2` |
| **EC19** | C3 | C5 | `sql/purge/candidates_quarantine.sql:2` |
| **EC19** | C3 | C5 | `sql/purge/candidates_raw.sql:2` |
| **EC19** | C3 | C5 | `sql/roles/dashboard_password.sql:2` |
| **EC19** | C3 | C4, C5 | `tests/test_check_s6.py:3` |
| **EC19** | C3 | C5 | `tests/test_dag_purge.py:3` |
| **EC19** | C3 | C2, C5 | `tests/test_dashboard_role.py:3` |
| **EC19** | C3 | C2, C4, C5 | `tests/test_forget.py:3` |
| **EC19** | C3 | C4, C5 | `tests/test_purge_journal.py:3` |
| **EC19** | C3 | C5 | `tests/test_purge_plan.py:3` |
| **EC19** | C3 | C1, C5 | `tests/test_retention.py:14` |
| **EC19** | C3 | C1, C4, C5 | `tools/check_s5.py:4` |
| **EC20** | C3 | C5 | `airflow/dags/dag_load_history.py:215 validate()` |
| **EC20** | C3 | C5 | `ml4fd/checks.py:126 SourceProfile()` |
| **EC20** | C3 | C5 | `ml4fd/checks.py:277 check_clock()` |
| **EC20** | C3 | C5 | `ml4fd/checks.py:3` |
| **EC20** | C3 | C5 | `ml4fd/checks.py:309 check_fields()` |
| **EC20** | C3 | C5 | `ml4fd/checks.py:456 validate_batch()` |
| **EC20** | C3 | C5 | `sql/checks/counters_close.sql:2` |
| **EC20** | C3 | C1, C2, C4, C5 | `sql/checks/s1_acceptance.sql:2` |
| **EC21** | C3 | C5 | `airflow/dags/dag_load_history.py:215 validate()` |
| **EC21** | C3 | C5 | `ml4fd/checks.py:126 SourceProfile()` |
| **EC21** | C3 | C5 | `ml4fd/checks.py:3` |
| **EC21** | C3 | C5 | `ml4fd/checks.py:369 check_record()` |
| **EC21** | C3 | C1, C5 | `ml4fd/checks.py:415 profile_gap()` |
| **EC21** | C3 | C5 | `ml4fd/checks.py:456 validate_batch()` |
| **EC22** | C3 | C5 | `airflow/dags/dag_load_history.py:215 validate()` |
| **EC22** | C3 | C4 | `dashboard/pages/rejects.py:3` |
| **EC22** | C3 | C5 | `ml4fd/quarantine.py:3` |
| **EC22** | C3 | C5 | `sql/checks/counters_close.sql:2` |
| **EC22** | C3 | C4 | `sql/dashboard/rejects_of_day.sql:3` |
| **EC22** | C3 | C5 | `sql/ddl/020_quarantine.sql:2` |
| **EC22** | C3 | C4 | `tests/test_dashboard_rejects.py:3` |
| **EC23** | C3 | C2 | `airflow/dags/dag_ingest.py:139 poll()` |
| **EC23** | C3 | C4 | `airflow/dags/dag_ingest.py:360 close_run()` |
| **EC23** | C3 | C2 | `airflow/dags/dag_load_history.py:165 write_raw()` |
| **EC23** | C3 | C2, C4 | `airflow/dags/dag_load_history.py:279 type_and_pseudonymize()` |
| **EC23** | C3 | C2 | `airflow/dags/dag_load_history.py:355 write_ground_truth()` |
| **EC23** | C3 | C4, C5 | `airflow/dags/dag_load_history.py:432 close_run()` |
| **EC23** | C3 | C1, C5 | `airflow/dags/dag_train.py:161 resolve_features()` |
| **EC23** | C3 | C1, C2, C5 | `airflow/dags/dag_train.py:240 build_dataset()` |
| **EC23** | C3 | C5 | `airflow/dags/dag_train.py:90 resolve_cut_off()` |
| **EC23** | C3 | C5 | `ml4fd/build.py:150 build_or_reuse()` |
| **EC23** | C3 | C1, C2, C5 | `ml4fd/build.py:3` |
| **EC23** | C3 | C2, C5 | `ml4fd/build.py:68 build()` |
| **EC23** | C3 | C1 | `ml4fd/datasets.py:286 register()` |
| **EC23** | C3 | C1, C5 | `ml4fd/datasets.py:3` |
| **EC23** | C3 | C1, C5 | `ml4fd/datasets.py:82 without()` |
| **EC23** | C3 | C2, C5 | `ml4fd/raw.py:3` |
| **EC23** | C3 | C4 | `ml4fd/registry.py:160 record_reference()` |
| **EC23** | C3 | C1, C4 | `ml4fd/registry.py:305 experiment_id()` |
| **EC23** | C3 | C4 | `ml4fd/registry.py:42 resolved_packages()` |
| **EC23** | C3 | C4 | `ml4fd/registry.py:64 manifest()` |
| **EC23** | C3 | C2 | `ml4fd/report.py:649 record()` |
| **EC23** | C3 | C5 | `ml4fd/runs.py:117 digest()` |
| **EC23** | C3 | C2, C5 | `ml4fd/transform.py:201 load_transactions()` |
| **EC23** | C3 | C2, C5 | `ml4fd/truth.py:124 load_ground_truth()` |
| **EC23** | C3 | C4 | `sql/checks/batch_digest.sql:2` |
| **EC23** | C3 | C4 | `sql/checks/batch_digest_recorded.sql:2` |
| **EC23** | C3 | C4 | `sql/checks/batch_runs.sql:2` |
| **EC23** | C3 | C4, C5 | `sql/checks/batch_stores.sql:3` |
| **EC23** | C3 | C2 | `sql/checks/history_already_loaded.sql:2` |
| **EC23** | C3 | C4 | `sql/checks/raw_batches.sql:2` |
| **EC23** | C3 | C1, C2, C4, C5 | `sql/checks/s1_acceptance.sql:2` |
| **EC23** | C3 | C1, C2, C5 | `sql/checks/s2_acceptance.sql:2` |
| **EC23** | C3 | C1, C4, C5 | `sql/checks/s3_acceptance.sql:2` |
| **EC23** | C3 | C1, C4, C5 | `sql/checks/s4_acceptance.sql:2` |
| **EC23** | C3 | C1, C4, C5 | `sql/checks/s5_acceptance.sql:2` |
| **EC23** | C3 | C5 | `sql/checks/table_digest.sql:2` |
| **EC23** | C3 | C4 | `sql/checks/undigested_batches.sql:2` |
| **EC23** | C3 | C4 | `sql/datasets/record_reference.sql:2` |
| **EC23** | C3 | C1, C5 | `sql/ddl/070_datasets.sql:2` |
| **EC23** | C3 | C2, C4 | `sql/ddl/070_datasets.sql:52` |
| **EC23** | C3 | C2, C4 | `sql/ddl/110_reports.sql:2` |
| **EC23** | C3 | C4 | `sql/maintenance/020_forget_orphan_datasets.sql:2` |
| **EC23** | C3 | C4 | `sql/maintenance/050_move_runtime_paths.sql:3` |
| **EC23** | C3 | C2 | `sql/reports/record_report.sql:2` |
| **EC23** | C3 | C4 | `tests/test_batch_digest.py:3` |
| **EC23** | C3 | C4 | `tests/test_batches_listing.py:3` |
| **EC23** | C3 | C5 | `tests/test_check_s4.py:3` |
| **EC23** | C3 | C5 | `tests/test_dag_ingest.py:3` |
| **EC23** | C3 | C1, C5 | `tests/test_dag_train.py:3` |
| **EC23** | C3 | C2, C4 | `tests/test_dashboard_performances.py:3` |
| **EC23** | C3 | C2, C4, C5 | `tests/test_forget.py:3` |
| **EC23** | C3 | C2, C4 | `tests/test_history_guard.py:3` |
| **EC23** | C3 | C4, C5 | `tests/test_migration_guard.py:3` |
| **EC23** | C3 | C5 | `tests/test_mounts.py:3` |
| **EC23** | C3 | C4, C5 | `tests/test_purge_journal.py:3` |
| **EC23** | C3 | C5 | `tests/test_report_journal.py:3` |
| **EC23** | C3 | C5 | `tests/test_score.py:3` |
| **EC23** | C3 | C2, C5 | `tests/test_staging_table.py:3` |
| **EC23** | C3 | C4 | `tools/backfill_reference.py:3` |
| **EC23** | C3 | C4, C5 | `tools/check_s2.py:3` |
| **EC23** | C3 | C1, C4, C5 | `tools/check_s4.py:3` |
| **EC23** | C3 | C1, C4, C5 | `tools/check_s5.py:4` |
| **EC23** | C3 | C4 | `tools/digest_batches.py:3` |
| **EC23** | C3 | C2, C4 | `tools/forget_plan.py:3` |
| **EC24** | C4 | C3 | `airflow/dags/dag_ingest.py:292 score_batch()` |
| **EC24** | C4 | C2 | `airflow/dags/dag_load_history.py:165 write_raw()` |
| **EC24** | C4 | C5 | `ml4fd/clock.py:3` |
| **EC24** | C4 | C5 | `ml4fd/config.py:295 observed_per_day()` |
| **EC24** | C4 | C5 | `ml4fd/paired.py:3` |
| **EC24** | C4 | C2, C5 | `ml4fd/raw.py:3` |
| **EC24** | C4 | C5 | `ml4fd/runs.py:117 digest()` |
| **EC24** | C4 | C3, C5 | `ml4fd/score.py:252 load()` |
| **EC24** | C4 | C2, C3 | `ml4fd/score.py:3` |
| **EC24** | C4 | C3 | `ml4fd/score.py:364 remote_rows()` |
| **EC24** | C4 | C2 | `sql/checks/history_already_loaded.sql:2` |
| **EC24** | C4 | C5 | `sql/checks/table_digest.sql:2` |
| **EC24** | C4 | C2 | `sql/ddl/010_raw_events.sql:2` |
| **EC24** | C4 | C3 | `sql/scoring/write_scores.sql:2` |
| **EC24** | C4 | C5 | `tests/test_clock.py:3` |
| **EC24** | C4 | C5 | `tests/test_compare_recipes.py:3` |
| **EC24** | C4 | C5 | `tests/test_measure_hour_effect.py:3` |
| **EC24** | C4 | C5 | `tests/test_mounts.py:3` |
| **EC24** | C4 | C5 | `tests/test_paired.py:3` |
| **EC24** | C4 | C5 | `tests/test_score.py:3` |
| **EC24** | C4 | C2, C5 | `tools/compare_recipes.py:3` |
| **EC24** | C4 | C2, C5 | `tools/rechoose_threshold.py:3` |
| **EC25** | C4 | C2, C5 | `airflow/dags/dag_ingest.py:195 validate()` |
| **EC25** | C4 | C5 | `ml4fd/stream.py:171 decode()` |
| **EC25** | C4 | C5 | `ml4fd/stream.py:362 validate_raw()` |
| **EC25** | C4 | C5 | `ml4fd/stream.py:406 validate()` |
| **EC25** | C4 | C2, C6 | `producer/app.py:3` |
| **EC25** | C4 | C2, C5 | `tools/ingest_once.py:3` |
| **EC25** | C4 | C5 | `tools/observe_api.py:3` |
| **EC26** | C4 | C5 | `airflow/dags/dag_purge.py:148 dry_run()` |
| **EC26** | C4 | C5 | `ml4fd/checks.py:277 check_clock()` |
| **EC26** | C4 | C5 | `ml4fd/purge.py:3` |
| **EC26** | C4 | C5 | `ml4fd/stream.py:362 validate_raw()` |
| **EC26** | C4 | C5 | `ml4fd/stream.py:406 validate()` |
| **EC26** | C4 | C5 | `sql/purge/candidates_quarantine.sql:2` |
| **EC26** | C4 | C5 | `sql/purge/candidates_raw.sql:2` |
| **EC26** | C4 | C5 | `tests/test_purge_plan.py:3` |
| **EC26** | C4 | C2, C3 | `tests/test_purge_run.py:3` |
| **EC26** | C4 | C2, C3 | `tools/forget_run.py:3` |
| **EC26** | C4 | C5 | `tools/purge_plan.py:3` |
| **EC26** | C4 | C2, C3 | `tools/purge_run.py:3` |
| **EC27** | C4 | C5 | `tests/test_check_s4.py:3` |
| **EC27** | C4 | C3 | `tests/test_dashboard_service.py:3` |
| **EC28** | C4 | C1, C6 | `tools/build_traceability.py:7` |

### Hors perimetre pour l'instant

Le code porte aussi des marques EE et EI, heritees du referentiel d'exigences dont le cahier des charges complet est en cours. Elles sont listees ici pour ne pas etre perdues, et ne comptent pas comme ruptures.

**EE01** (6), **EE02** (8), **EE03** (14), **EE04** (22), **EE05** (4), **EE06** (5), **EE07** (19), **EE08** (14), **EE09** (16), **EI01** (12), **EI02** (17), **EI03** (22), **EI04** (46), **EI05** (15), **EI06** (13), **EI08** (5), **EI10** (2), **EI11** (7), **EI12** (16), **EI13** (6), **EI14** (1)
