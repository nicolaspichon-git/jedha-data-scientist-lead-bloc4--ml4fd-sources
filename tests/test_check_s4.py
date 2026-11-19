"""The checker of S4: what it finds, and what it refuses to call verified.

Covers: EC14, EC23, EC27
Lot: P5
Competency: C5

A CONTROL THAT CANNOT FAIL PROVES NOTHING, so each finding is built here on
purpose: a document missing from the disk, a document whose bytes no longer
match content_digest, a count that drifted from what the queries return, a
window that cannot be reconstructed, and an empty table -- which must read NON
VERIFIE and never OK.

The SQL half of P5 was executed against a real PostgreSQL 16: criterion 5, the
one only time makes possible, was offered a forty-day-old stream payment in
three states and answered correctly each time -- no verdict row (ECHEC), a
verdict row that has fallen (OK), and a verdict row whose label_ts is still in
the future (ECHEC). That last case is the one that matters: the criterion reads
the DUE DATE and not the existence of a row, which is what the blind band is.
"""
from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import importlib.util
import types
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE = ROOT / "tools" / "check_s4.py"
PARIS = dt.timezone(dt.timedelta(hours=1), "CET")


def _module():
    spec = importlib.util.spec_from_file_location("check_s4", MODULE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


cs4 = _module()


class _Conn:
    """A connection answering the one query the checker runs itself."""

    def __init__(self, rows):
        self.rows = rows

    def cursor(self):
        rows = self.rows

        class _Cursor:
            def __enter__(self): return self
            def __exit__(self, *exc): return False
            def execute(self, sql, params=None): self._rows = rows
            def fetchall(self): return self._rows

        return _Cursor()


def _row(day="2026-11-16", digest=None, payments=7200, alerts=44,
         verdicts=7200, pending=7200, path=None, known_at=None):
    return (dt.date.fromisoformat(day),
            known_at or dt.datetime(2026, 11, 17, 6, tzinfo=PARIS),
            path, digest, payments, alerts, verdicts, pending)


def _rows(tmp_path, rows, counts=None, reference=None, root=ROOT, **over):
    """Run the criteria with a settings object and a connection we control."""
    from ml4fd import config, db, report

    counts = counts or {"n_payments": 7200, "n_alerts": 44,
                        "n_verdicts": 7200, "n_pending": 7200}
    settings = types.SimpleNamespace(
        report_root=str(tmp_path), report_timezone="Europe/Paris",
        report_schedule="0 6 * * *")
    for key, value in over.items():
        setattr(settings, key, value)

    saved = (config.load, db.connect, report.volumes, report.alerts,
             report.verdicts, report.pending, report.reference)
    config.load = lambda *a, **k: settings
    db.connect = lambda *a, **k: contextlib.nullcontext(_Conn(rows))
    report.volumes = lambda conn, window: {
        "total": {"transactions": counts["n_payments"]}}
    report.alerts = lambda conn, window: [None] * counts["n_alerts"]
    report.verdicts = lambda conn, window: {"verdicts": counts["n_verdicts"]}
    report.pending = lambda conn, window: {"pending": counts["n_pending"]}
    report.reference = lambda conn, settings_: reference
    try:
        return {criterion: (expected, actual)
                for _, criterion, expected, actual
                in cs4.criteria(root, 30)}
    finally:
        (config.load, db.connect, report.volumes, report.alerts,
         report.verdicts, report.pending, report.reference) = saved


def _document(tmp_path, day="2026-11-16", text="# rapport\n"):
    path = tmp_path / ("%s.md" % day)
    path.write_text(text, encoding="utf-8")
    return path, hashlib.sha256(text.encode("utf-8")).hexdigest()


# --- a clean morning ----------------------------------------------------------

def test_a_matching_document_and_matching_counts_pass(tmp_path):
    path, digest = _document(tmp_path)
    rows = _rows(tmp_path, [_row(digest=digest, path=str(path))])
    assert rows["P3 chaque ligne a son document sur le disque"] \
        == ("0 absent", "0 absent")
    assert rows["P3 empreinte du document egale content_digest"] \
        == ("0 ecart", "0 ecart")
    assert rows["P2 les decomptes egalent ce que les requetes rendent"] \
        == ("0 divergence", "0 divergence")


# --- each finding, built on purpose -------------------------------------------

def test_a_document_missing_from_the_disk_is_caught(tmp_path):
    """fd_reports would otherwise describe a file nobody can open -- the defect
    M8 paid for with a whole dataset."""
    rows = _rows(tmp_path, [_row(digest="a" * 64,
                                 path=str(tmp_path / "absent.md"))])
    expected, actual = rows["P3 chaque ligne a son document sur le disque"]
    assert expected == "0 absent"
    assert actual.startswith("1 absent")
    assert "2026-11-16" in actual


def test_a_document_that_no_longer_matches_its_digest_is_caught(tmp_path):
    """Somebody edited the morning's document, or the row describes another
    version of it. Either way the registry can no longer be trusted."""
    path, _ = _document(tmp_path)
    rows = _rows(tmp_path, [_row(digest="b" * 64, path=str(path))])
    expected, actual = rows["P3 empreinte du document egale content_digest"]
    assert expected == "0 ecart"
    assert actual.startswith("1 ecart")


def test_a_count_that_drifted_is_caught_and_named(tmp_path):
    """THE ONLY CHECK THAT CATCHES A REPORT BUILT ON THE WRONG WINDOW: the
    figures would be internally consistent and describe another day."""
    path, digest = _document(tmp_path)
    rows = _rows(tmp_path, [_row(digest=digest, path=str(path), alerts=44)],
                 counts={"n_payments": 7200, "n_alerts": 12,
                         "n_verdicts": 7200, "n_pending": 7200})
    expected, actual = rows["P2 les decomptes egalent ce que les requetes rendent"]
    assert expected == "0 divergence"
    assert "n_alerts" in actual
    assert "44" in actual and "12" in actual


def test_a_window_that_cannot_be_rebuilt_is_a_criterion_and_not_a_traceback(
        tmp_path):
    """dag_daily cannot produce such a row -- its knowledge date is six hours
    past the end of the window -- but tools/daily_report.py --knowledge-at can,
    and a checker that dies on one bad row verifies none of the others."""
    path, digest = _document(tmp_path)
    rows = _rows(tmp_path, [_row(
        digest=digest, path=str(path),
        known_at=dt.datetime(2026, 11, 16, 12, tzinfo=PARIS))])
    expected, actual = rows["P3 fenetre coherente avec la journee couverte"]
    assert expected == "0 impossible"
    assert actual.startswith("1 impossible")
    assert "precedes the end" in actual


def test_several_days_are_all_verified(tmp_path):
    first, digest_first = _document(tmp_path, "2026-11-15")
    second, digest_second = _document(tmp_path, "2026-11-16", "# autre\n")
    rows = _rows(tmp_path, [
        _row(day="2026-11-16", digest=digest_second, path=str(second)),
        _row(day="2026-11-15", digest=digest_first, path=str(first),
             known_at=dt.datetime(2026, 11, 16, 6, tzinfo=PARIS))])
    assert rows["info decomptes recalcules"][1] == "2 rapport(s)"


# --- nothing to read is not a pass -------------------------------------------

def test_an_empty_journal_is_unverified_and_never_passed(tmp_path):
    """No report has been produced yet, which is the normal state before
    dag_daily is unpaused. What CANNOT be said is that the documents match
    their rows: there is nothing to read."""
    rows = _rows(tmp_path, [])
    expected, actual = rows["info empreinte du document contre fd_reports"]
    assert expected == "mesure"
    assert "NON VERIFIE" in actual
    assert "P3 empreinte du document egale content_digest" not in rows


# --- the declarations --------------------------------------------------------

def test_the_cadence_of_the_dag_is_held_to_the_declaration(tmp_path):
    rows = _rows(tmp_path, [])
    assert rows["P4 cadence de dag_daily"] == ("0 6 * * *", "0 6 * * *")


def test_a_dag_that_stopped_catching_up_is_caught(tmp_path):
    """catchup=True is the exception of this project and must stay deliberate:
    without it a missed morning is lost for good."""
    rows = _rows(tmp_path, [])
    assert rows["P4 dag_daily rattrape les matins manques"] == ("oui", "oui")


def test_a_cadence_that_drifts_from_governance_fails(tmp_path):
    rows = _rows(tmp_path, [], report_schedule="37 6 * * *")
    expected, actual = rows["P4 cadence de dag_daily"]
    assert expected == "37 6 * * *"
    assert actual == "0 6 * * *"


def test_a_report_root_outside_the_project_is_caught(tmp_path):
    rows = _rows(tmp_path, [], report_root="/tmp/ailleurs")
    expected, actual = rows["P2 racine des rapports declaree"]
    assert expected == "sous le projet"
    assert actual.startswith("AILLEURS")


# --- the reference of M5 -----------------------------------------------------

def test_an_unreadable_manifest_is_reported_and_not_hidden(tmp_path):
    rows = _rows(tmp_path, [], reference=None)
    assert "INDISPONIBLE" in rows["info reference de M5"][1]


def test_the_reference_figures_are_shown_when_the_manifest_reads(tmp_path):
    rows = _rows(tmp_path, [], reference={
        "version": "8813b096e2e4", "recall": 0.751219512195122,
        "precision": 0.5292096219931272, "alerts_per_day": 41.61,
        "base_rate_test": 0.004072})
    assert rows["info recette de reference"][1] == "8813b096e2e4"
    assert rows["info rappel mesure par M5 sur le jeu de test"][1] == "0.7512"
    assert rows["info alertes/jour prevues par M6"][1] == "41.6"


# --- the exit code follows the verdict ---------------------------------------

def test_the_checker_exits_non_zero_on_a_failure(tmp_path, capsys, monkeypatch):
    """fd-manager reads only the exit code: a checker that printed ECHEC and
    returned zero would let the chain conclude "tous les criteres passent"."""
    monkeypatch.setattr(cs4, "criteria",
                        lambda root, days: [(1, "un critere", "ok", "NON")])
    assert cs4.main(["check_s4.py", str(ROOT)]) == 1
    assert "<<< ECHEC" in capsys.readouterr().out


def test_the_checker_exits_zero_when_everything_passes(tmp_path, capsys,
                                                       monkeypatch):
    """The passing case is tested as much as the failing one."""
    monkeypatch.setattr(cs4, "criteria",
                        lambda root, days: [(1, "un critere", "ok", "ok"),
                                            (2, "un chiffre", "mesure", "42")])
    assert cs4.main(["check_s4.py", str(ROOT)]) == 0
    out = capsys.readouterr().out
    assert "0 critere(s) en echec sur 1" in out
    assert "1 chiffre(s) informatif(s)" in out


def test_a_block_that_returns_early_does_not_swallow_the_rest(tmp_path):
    """THE TRAP OF check_s3.py, REPRODUCED HERE AND FIXED THE SAME WAY.

    The document block ends in an early `return` when the journal is empty.
    Before the refactor that `return` returned from criteria() ITSELF, and
    every criterion declared after it was silently skipped -- which is how
    three criteria of N9 went missing in check_s3.py. Found there, fixed there,
    and written again here three hours later by the same hand. Knowing a lesson
    is not applying it; this test is.
    """
    rows = _rows(tmp_path, [], reference={
        "version": "8813b096e2e4", "recall": 0.75, "precision": 0.53,
        "alerts_per_day": 41.6, "base_rate_test": 0.004072})
    # The block that returns early still yields its own three figures...
    assert "info rapports enregistres sur 30 jours" in rows
    # ...and everything declared AFTER it is reached.
    assert "info recette de reference" in rows
    assert "info rappel mesure par M5 sur le jeu de test" in rows


# --- the layout of the container ----------------------------------------------

def _container(tmp_path, monkeypatch, with_dag=True):
    """The scheduler's layout: a project root WITHOUT airflow/, DAGs elsewhere."""
    from ml4fd import config

    project = tmp_path / "opt" / "project"
    project.mkdir(parents=True)
    dags = tmp_path / "opt" / "airflow" / "dags"
    dags.mkdir(parents=True)
    if with_dag:
        source = ROOT / "airflow" / "dags" / "dag_daily.py"
        (dags / "dag_daily.py").write_text(
            source.read_text(encoding="utf-8"), encoding="utf-8")
    monkeypatch.setattr(config, "DAG_DIRS", ("airflow/dags", str(dags)))
    return project, dags


def test_the_dag_is_found_where_the_container_mounts_it(tmp_path, monkeypatch):
    """THE TEST THIS FILE DID NOT HAVE. Every other test here hands criteria()
    the repo root, where airflow/dags exists -- a layout the production call
    never has -- so the checker read dag_daily ABSENT on its first real run
    with this whole file green."""
    project, _ = _container(tmp_path, monkeypatch)
    rows = _rows(tmp_path, [], root=project)
    assert rows["P4 cadence de dag_daily"] == ("0 6 * * *", "0 6 * * *")
    assert rows["P4 dag_daily rattrape les matins manques"] == ("oui", "oui")
    assert "P4 DAG dag_daily" not in rows
    assert "P4 repertoire des DAG" not in rows


def test_a_dag_missing_from_a_found_directory_names_the_directory(
        tmp_path, monkeypatch):
    project, dags = _container(tmp_path, monkeypatch, with_dag=False)
    expected, actual = _rows(tmp_path, [], root=project)["P4 DAG dag_daily"]
    assert expected == "present"
    assert str(dags) in actual


def test_no_dag_directory_names_where_it_looked(tmp_path, monkeypatch):
    from ml4fd import config

    monkeypatch.setattr(config, "DAG_DIRS", ("airflow/dags", "/nowhere/dags"))
    rows = _rows(tmp_path, [], root=tmp_path)
    expected, actual = rows["P4 repertoire des DAG"]
    assert expected == "trouve"
    assert "/nowhere/dags" in actual
    # And it does not then blame the DAG itself.
    assert "P4 DAG dag_daily" not in rows
