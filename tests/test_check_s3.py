"""The acceptance checker of S3: what it enforces and what it only reports.

Block: B3, B4.
Lot: B3-N8, B4-L0.
Competency: B3-C5.
Covers: B3-EC09, B3-EC17.

The point of testing an acceptance checker is that a check which cannot fail
proves nothing. So the alert file is built here in three states -- clean, leaky,
absent -- and the verdict of each is asserted.
"""
from __future__ import annotations

import importlib.util
import json
import types
from pathlib import Path

from ml4fd import config

MODULE = Path(__file__).resolve().parents[1] / "tools" / "check_s3.py"


def _module():
    spec = importlib.util.spec_from_file_location("check_s3", MODULE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


cs3 = _module()

CLEAN = {"txn_id": "abc:1794817599732", "txn_ts": "2026-11-16T14:13:01+00:00",
         "merchant": "fraud_Dach-Borer", "category": "grocery_net",
         "amt": 371.94, "score": 0.97, "threshold": 0.89,
         "model_version": "2"}


def _rows(root, criteria_root=None, **overrides):
    """Run the criteria with a settings object we control.

    criteria_root IS THE ROOT criteria() ITSELF IS GIVEN, and it defaults to
    the repository -- which is the flaw that let both DAGs read ABSENT in
    production for a whole lot. `root` only places the files the settings
    point at; the criteria kept reading the real repo, where every path
    happens to exist. A test about a LAYOUT has to hand over that layout.
    """
    settings = types.SimpleNamespace(
        calls_per_run=5, realtime_schedule="* * * * *",
        drop_features=("txn_hour", "txn_weekday"),
        alert_channel="file", alert_file=str(root / "ws" / "alerts.jsonl"),
        alert_delivery="at-least-once", enforce_capacity=True,
        # N9. A hand-built Settings that lacks a field the tool reads fails
        # with AttributeError, which is how these sixteen tests announced the
        # operations channel's arrival -- the same lesson as the header of
        # tests/test_config.py, from the other end.
        ops_channel="file", ops_file=str(root / "ws" / "ops.jsonl"),
        observed_per_day=lambda: 7200)
    for key, value in overrides.items():
        setattr(settings, key, value)
    saved = cs3.criteria
    from ml4fd import config
    original_load = config.load
    config.load = lambda *a, **k: settings
    try:
        return {criterion: (expected, actual)
                for _, criterion, expected, actual
                in cs3.criteria(criteria_root
                                or Path(__file__).resolve().parents[1])}
    finally:
        config.load = original_load
        cs3.criteria = saved


def _write_ops(root, payloads):
    path = root / "ws" / "ops.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(p, sort_keys=True) + "\n"
                            for p in payloads), encoding="utf-8")
    return path


CLEAN_OPS = {"control": "freshness", "became": "alert",
             "detail": "derniere execution reussie il y a 23 min",
             "run_id": "scheduled__2026-11-16T16:00:00+00:00",
             "dag_id": "dag_ingest", "window_minutes": 60,
             "raised_at": "2026-11-16T16:00:04+00:00",
             "facts": {"runs_ended": 60, "runs_empty": 0}}


def _write(root, payloads):
    path = root / "ws" / "alerts.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(p, sort_keys=True) + "\n"
                            for p in payloads), encoding="utf-8")
    return path


# --- the alert file, the one thing that leaves this system ---------------------

def test_a_clean_alert_file_passes(tmp_path):
    _write(tmp_path, [CLEAN, dict(CLEAN, txn_id="def:1")])
    rows = _rows(tmp_path)
    assert rows["B3-N5 aucun champ nominatif dans une alerte"] == ("aucun",
                                                                "aucun")
    # THE VERDICT, and not the pair: this test once asserted
    # ("0 illisible", "0") and passed while the report said ECHEC.
    for name in ("B3-N5 aucun champ nominatif dans une alerte",
                 "B3-N5 chaque ligne est du JSON",
                 "B3-N5 chaque alerte porte sa tracabilite"):
        assert cs3.verdict(*rows[name]) == "OK", (name, rows[name])


def test_a_leaked_name_is_caught(tmp_path):
    """THE criterion of this file. fd_transactions holds no identifying column,
    so a leak cannot happen by accident -- and that is an argument, not a
    measurement. This reads the file."""
    _write(tmp_path, [CLEAN, dict(CLEAN, first="Michael", last="Williams")])
    rows = _rows(tmp_path)
    expected, actual = rows["B3-N5 aucun champ nominatif dans une alerte"]
    assert expected == "aucun"
    assert "first" in actual and "last" in actual


def test_the_pseudonym_counts_as_a_leak(tmp_path):
    """client_key is an HMAC: a stable pseudonym over a cardholder's whole
    history. A set of them groups transactions by person without a name."""
    _write(tmp_path, [dict(CLEAN, client_key="deadbeef")])
    _, actual = _rows(tmp_path)["B3-N5 aucun champ nominatif dans une alerte"]
    assert "client_key" in actual


def test_the_label_counts_as_a_leak(tmp_path):
    """An alert carrying is_fraud would hand the verdict to whoever receives
    the notification -- and N6 spent a lot keeping it out."""
    _write(tmp_path, [dict(CLEAN, is_fraud="1")])
    _, actual = _rows(tmp_path)["B3-N5 aucun champ nominatif dans une alerte"]
    assert "is_fraud" in actual


def test_an_unreadable_line_is_counted(tmp_path):
    path = _write(tmp_path, [CLEAN])
    with open(path, "a", encoding="utf-8") as handle:
        handle.write("pas du json\n")
    row = _rows(tmp_path)["B3-N5 chaque ligne est du JSON"]
    assert row == ("0 illisible", "1 illisible")
    assert cs3.verdict(*row) == "<<< ECHEC"


def test_an_alert_without_its_traceability_is_counted(tmp_path):
    """Without the model version, "which model raised this?" has no answer --
    and that is the first question of the first incident."""
    partial = {k: v for k, v in CLEAN.items() if k != "model_version"}
    _write(tmp_path, [CLEAN, partial])
    row = _rows(tmp_path)["B3-N5 chaque alerte porte sa tracabilite"]
    assert row == ("0 incomplete", "1 incomplete")
    assert cs3.verdict(*row) == "<<< ECHEC"


def test_an_absent_file_is_reported_as_unverified_and_not_as_passed(tmp_path):
    """A check that reads "OK" because there was nothing to read is worse than
    no check: it says the property holds when nobody looked."""
    rows = _rows(tmp_path)          # nothing written
    expected, actual = rows["info aucun champ nominatif dans une alerte"]
    assert expected == "mesure"
    assert "NON VERIFIE" in actual


def test_an_empty_file_is_also_unverified(tmp_path):
    _write(tmp_path, [])
    _, actual = _rows(tmp_path)["info aucun champ nominatif dans une alerte"]
    assert "NON VERIFIE" in actual


# --- the declarations ---------------------------------------------------------

def test_the_cadence_is_checked_against_the_announced_rate(tmp_path):
    rows = _rows(tmp_path)
    assert rows["B3-N7 appels/jour contre le debit annonce"] == ("7200", "7200")


def test_a_cadence_that_undershoots_the_rate_fails(tmp_path):
    rows = _rows(tmp_path, observed_per_day=lambda: 1440)
    expected, actual = rows["B3-N7 appels/jour contre le debit annonce"]
    assert expected != actual


def test_the_declared_recipe_must_still_be_trainable(tmp_path):
    rows = _rows(tmp_path)
    assert rows["B3-N4 la recette declaree reste entrainable"][1] == "5 features"


def test_a_recipe_still_reading_the_clock_is_named(tmp_path):
    rows = _rows(tmp_path, drop_features=())
    expected, actual = rows["B3-N4 la recette declaree ne lit plus la pendule"]
    assert "txn_hour" in actual and "txn_weekday" in actual
    assert expected != actual


def test_an_unknown_channel_fails(tmp_path):
    rows = _rows(tmp_path, alert_channel="webhoook")
    assert rows["B3-N5 canal de notification declare"] == ("connu", "INCONNU")


def test_a_delivery_semantics_that_can_lose_a_notification_fails(tmp_path):
    rows = _rows(tmp_path, alert_delivery="at-most-once")
    expected, actual = rows["B3-N5 semantique de livraison declaree"]
    assert expected == "at-least-once"
    assert actual == "at-most-once"


def test_a_capacity_nobody_enforces_fails(tmp_path):
    rows = _rows(tmp_path, enforce_capacity=False)
    assert rows["B3-N5 plafond de capacite applique"] == ("oui", "NON")


def test_the_verdict_dag_must_not_run_every_minute(tmp_path):
    """EI06, and N6's fourth guarantee: a cadence of its own."""
    rows = _rows(tmp_path)
    assert rows["B3-N6 cadence de dag_ingest_verdicts distincte de la minute"] \
        == ("oui", "oui")


# --- N9: the operations channel ------------------------------------------------

def test_a_clean_operations_file_passes(tmp_path):
    _write(tmp_path, [CLEAN])
    _write_ops(tmp_path, [CLEAN_OPS])
    rows = _rows(tmp_path)
    assert rows["B3-N9 aucune transaction dans un message d'exploitation"] \
        == ("aucune", "aucune")
    for name in ("B3-N9 aucune transaction dans un message d'exploitation",
                 "B3-N9 chaque ligne est du JSON",
                 "B3-N9 chaque message nomme son controle et son changement"):
        assert cs3.verdict(*rows[name]) == "OK", (name, rows[name])


def test_a_transaction_in_an_operations_message_is_caught(tmp_path):
    """NOT a leak of personal data -- these fields are not personal. A leak of
    the ALERT STREAM into a channel that has no capacity policy and that the
    analyst does not read."""
    _write(tmp_path, [CLEAN])
    _write_ops(tmp_path, [dict(CLEAN_OPS, txn_id="abc:1794817599732",
                               amt=371.94, merchant="fraud_Dach-Borer")])
    expected, actual = _rows(tmp_path)[
        "B3-N9 aucune transaction dans un message d'exploitation"]
    assert expected == "aucune"
    for name in ("txn_id", "amt", "merchant"):
        assert name in actual


def test_a_personal_field_in_an_operations_message_is_caught_too(tmp_path):
    _write(tmp_path, [CLEAN])
    _write_ops(tmp_path, [dict(CLEAN_OPS, cc_num="4532...")])
    assert "cc_num" in _rows(tmp_path)[
        "B3-N9 aucune transaction dans un message d'exploitation"][1]


def test_an_absent_operations_file_is_unverified_and_not_passed(tmp_path):
    """No control has changed state, which is the normal state of a healthy
    pipeline. What cannot be said is that no message ever carried a
    transaction: there is nothing to read."""
    _write(tmp_path, [CLEAN])
    expected, actual = _rows(tmp_path)[
        "info aucune transaction dans un message d'exploitation"]
    assert expected == "mesure"
    assert "NON VERIFIE" in actual


def test_two_equal_destinations_fail_the_acceptance(tmp_path):
    """THE ONE THING THE TWO CHANNELS MUST NOT SHARE. config.load() refuses it,
    so this criterion guards the case where somebody reaches past it."""
    _write(tmp_path, [CLEAN])
    same = str(tmp_path / "ws" / "alerts.jsonl")
    rows = _rows(tmp_path, ops_file=same)
    assert rows["B3-N9 destinations distinctes des deux canaux"] \
        == ("distinctes", "IDENTIQUES")


def test_an_unknown_operations_channel_fails(tmp_path):
    _write(tmp_path, [CLEAN])
    rows = _rows(tmp_path, ops_channel="pigeon")
    assert rows["B3-N9 canal d'exploitation declare"] == ("connu", "INCONNU")


def test_the_four_controls_are_counted(tmp_path):
    """A control dropped the way the whole stage once was must fail here too,
    and not only in tests/test_health.py."""
    _write(tmp_path, [CLEAN])
    assert _rows(tmp_path)["B3-N9 les quatre controles de la vue 3.3 sont la"] \
        == ("4", "4")


def test_the_criteria_after_the_alert_file_are_still_reached(tmp_path):
    """THE DEFECT THIS REFACTOR REPAIRED. The alert-file block ended in an
    early `return` inside criteria() itself, so every criterion added after it
    was silently skipped -- which is what happened when N9 tried to add three.
    The block is a generator of its own now, and these two cases are the ones
    that used to swallow the rest: no file at all, and a channel that is not a
    file."""
    rows = _rows(tmp_path)                      # no alert file written at all
    assert "B3-N9 canal d'exploitation declare" in rows

    rows = _rows(tmp_path, alert_channel="log")
    assert "B3-N9 canal d'exploitation declare" in rows


# --- where the DAG files are, which is not where the repo keeps them ----------

def test_the_dag_directory_of_the_container_is_the_one_compose_mounts():
    """THE TEST THAT WOULD HAVE CAUGHT IT, and the only one that can.

    The check looked for the DAGs under root/airflow/dags. Inside the scheduler
    root is /opt/project, and docker-compose.yml mounts ./airflow/dags at
    /opt/airflow/dags -- so both DAGs read ABSENT on every run of
    `fd-manager check`, for a whole lot, while these tests passed because they
    hand criteria() the REPO root, where that path exists. A test that supplies
    a layout the production call never has proves nothing about production.

    So this one reads docker-compose.yml: change the mount and the candidate
    list must change with it.
    """
    compose = (Path(__file__).resolve().parents[1]
               / "docker-compose.yml").read_text(encoding="utf-8")
    assert "./airflow/dags:/opt/airflow/dags" in compose
    assert "/opt/airflow/dags" in config.DAG_DIRS


def test_the_repo_layout_is_found_first(tmp_path):
    (tmp_path / "airflow" / "dags").mkdir(parents=True)
    assert config.dags_dir(tmp_path) == tmp_path / "airflow" / "dags"


def test_an_absolute_candidate_is_used_when_the_root_has_none(tmp_path):
    """The container's case: no airflow/ under the root at all."""
    elsewhere = tmp_path / "opt" / "airflow" / "dags"
    elsewhere.mkdir(parents=True)
    found = config.dags_dir(tmp_path / "project",
                         candidates=("airflow/dags", str(elsewhere)))
    assert found == elsewhere


def test_no_directory_at_all_names_where_it_looked(tmp_path):
    """"ABSENT" without saying where sends the reader to check the DAGs, which
    are fine. The criterion names the candidates instead."""
    assert config.dags_dir(tmp_path) is None
    rows = _rows(tmp_path, criteria_root=tmp_path)
    expected, actual = rows["B3-N7 repertoire des DAG"]
    assert expected == "trouve"
    for candidate in config.DAG_DIRS:
        assert candidate in actual
    # And it does not then blame the DAGs themselves.
    assert "B3-N7 DAG dag_ingest" not in rows


def test_the_cadences_are_read_when_the_directory_is_found(tmp_path):
    """The two criteria that were never verified anywhere: the minute for the
    ingestion, and NOT the minute for the verdicts."""
    dags = tmp_path / "airflow" / "dags"
    dags.mkdir(parents=True)
    (dags / "dag_ingest.py").write_text(
        '    schedule="* * * * *",\n', encoding="utf-8")
    (dags / "dag_ingest_verdicts.py").write_text(
        '    schedule="0 2 * * *",\n', encoding="utf-8")
    # criteria_root, or this would read the REAL DAGs of the repository and
    # pass for the wrong reason -- which is how the defect survived.
    rows = _rows(tmp_path, criteria_root=tmp_path)
    assert rows["B3-N7 cadence de dag_ingest"] == ("* * * * *",
                                                         "* * * * *")
    assert rows["B3-N6 cadence de dag_ingest_verdicts distincte de la minute"] \
        == ("oui", "oui")


# --- every criterion of a healthy project passes ---------------------------------

def test_a_healthy_project_fails_no_criterion(tmp_path):
    """THE TEST THAT WOULD HAVE CAUGHT IT. Four criteria wrote their actual
    without the unit of their expected -- "0" against "0 illisible" -- and the
    per-criterion tests asserted the pair, so the checker failed on clean files
    while every test passed. Here the report's own rule judges every row of a
    clean project: a format mismatch anywhere fails it."""
    _write(tmp_path, [CLEAN, dict(CLEAN, txn_id="def:1")])
    _write_ops(tmp_path, [CLEAN_OPS])
    failed = {name: pair for name, pair in _rows(tmp_path).items()
              if cs3.verdict(*pair) == "<<< ECHEC"}
    assert not failed, failed
