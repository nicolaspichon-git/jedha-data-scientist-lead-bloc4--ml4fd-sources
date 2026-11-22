"""R5: the acceptance of S6 -- the catalogue, and the container as it runs.

Block: B3, B4.
Lot: B3-R5, B4-L0.
Competency: B3-C4, B3-C5.
Covers: B3-EC19, B3-EC27.

s6_acceptance.sql was run on PostgreSQL 16 three ways: sound (10 OK, exit 0);
damaged by making fd_dashboard a member of fd_report and granting it the
quarantine's payload column (4 named failures, exit 3); and with no password
(1 failure naming the remedy). It also met a real defect on its first run --
the planner called has_sequence_privilege() on pg_statistic before the relkind
filter -- which only a CASE prevents.

check_s6.py cannot meet Docker here, so its logic is held against `docker
inspect` and probe answers shaped like the real ones: a sound container, and
the one the plan of S6 warns about, built on the x-airflow-common anchor.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
sys.path.insert(0, str(ROOT / "dashboard"))

import check_s6  # noqa: E402
import probe  # noqa: E402

SQL = (ROOT / "sql" / "checks" / "s6_acceptance.sql").read_text(encoding="utf-8")
OWNER = "owner-secret-0123"


def _code(text):
    return "\n".join(line.split("--")[0] for line in text.splitlines())


# --- the catalogue ------------------------------------------------------------

def test_the_expected_tables_are_the_ones_r0_grants():
    """Two places name the readable tables; they must name the same ones --
    whole tables and the table granted by column alike.

    THE WHOLE DDL AND NOT 140 ALONE, since L4. This test read
    140_dashboard_role.sql only, on the premise that a role's privileges are
    declared where the role is. That premise broke the day a table created
    LATER had to be granted: the DDL glob runs in name order, so a GRANT cannot
    precede its table -- the trap 150_database_connect.sql paid for with a 500,
    and the reason 190_scoring_service_calls.sql revokes fd_monitoring where
    the table exists rather than where the role is declared. fd_monitoring
    (170), fd_retrain_requests (180) and the view fd_scoring_decisions (195)
    each grant at their own creation.

    What the test still checks is the thing that matters: the two lists agree.
    A grant added anywhere in the DDL must be admitted in s6_acceptance.sql --
    which is how the grant on fd_scoring_service_calls was caught on
    21/11/2026 and revoked rather than admitted.
    """
    granted, by_column = set(), set()
    for path in sorted((ROOT / "sql" / "ddl").glob("*.sql")):
        ddl = _code(path.read_text(encoding="utf-8"))
        # [^;] AND NOT . -- a dot with DOTALL walks across "TO fd_report;"
        # to reach the next fd_dashboard and captures both statements as one
        # table name. 170, 180 and 195 grant to the two readers on consecutive
        # lines, so this is not hypothetical: it is how this test first failed.
        for whole in re.findall(
                r"GRANT\s+SELECT\s+ON\s+([^;]+?)\s+TO\s+fd_dashboard",
                ddl, re.I | re.S):
            granted |= {name.strip() for name in whole.split(",")}
        for one in re.findall(
                r"GRANT\s+SELECT\s*\([^)]*\)\s*ON\s+(\w+)\s+TO\s+fd_dashboard",
                ddl, re.I):
            by_column.add(one)
        # A grant revoked later is not a readable table: 190 grants nothing to
        # the readers any more, but an older database still had to be told.
        for revoked in re.findall(
                r"REVOKE\s+SELECT\s+ON\s+(\w+)\s+FROM\s+fd_dashboard",
                ddl, re.I):
            granted.discard(revoked)
            by_column.discard(revoked)
    expected = re.search(r"'R0 tables lisibles, colonnes comprises',\s*'([^']+)'",
                         SQL).group(1)
    assert sorted(granted | by_column) == \
        [name.strip() for name in expected.split(",")]


def test_the_quarantine_columns_expected_are_the_ones_granted():
    ddl = _code((ROOT / "sql" / "ddl" / "140_dashboard_role.sql").read_text(
        encoding="utf-8"))
    granted = re.search(r"GRANT\s+SELECT\s*\(([^)]*)\)\s*ON\s+fd_quarantine", ddl,
                        re.I).group(1)
    expected = re.search(r"'R6 colonnes lisibles de la quarantaine',\s*'([^']+)'",
                         SQL).group(1)
    assert sorted(c.strip() for c in granted.split(",")) == \
        [c.strip() for c in expected.split(",")]


def test_column_grants_are_seen_not_only_table_grants():
    """A SELECT on fd_quarantine.payload alone is a door a table-level test
    would not see -- proven on PostgreSQL by granting exactly that."""
    assert "has_any_column_privilege('fd_dashboard', oid, 'SELECT')" in SQL


def test_the_sequence_test_cannot_run_before_its_filter():
    code = _code(SQL)
    assert re.search(r"CASE WHEN c\.relkind = 'S'\s+THEN has_sequence_privilege",
                     code)


def test_a_missing_role_is_named_before_anything_else_fails():
    first_do = SQL.index("DO $$")
    assert SQL.index("fd-manager.ps1 init") < SQL.index("CREATE TEMP TABLE")
    assert first_do < SQL.index("CREATE TEMP TABLE")


def test_psql_exits_non_zero_on_any_failure():
    assert "RAISE EXCEPTION 'S6 : % critere(s) en echec" in SQL


# --- the running container ----------------------------------------------------

def inspect(**changes):
    info = {
        "Config": {"User": "dashboard",
                   "Env": ["POSTGRES_USER=fd_dashboard",
                           "POSTGRES_PASSWORD=dash-secret-4567",
                           "POSTGRES_DB=fraud", "POSTGRES_HOST=postgres",
                           "PYTHONPATH=/opt/project:/opt/project/dashboard"]},
        "HostConfig": {"CapDrop": ["ALL"],
                       "SecurityOpt": ["no-new-privileges:true"],
                       "PortBindings": {"8501/tcp": [{"HostIp": "127.0.0.1",
                                                      "HostPort": "8502"}]}},
        "Mounts": [{"Destination": d, "RW": False}
                   for d in sorted(check_s6.EXPECTED_MOUNTS)],
        "State": {"Health": {"Status": "healthy"}},
    }
    for key, value in changes.items():
        section, _, field = key.partition("__")
        if field:
            info[section][field] = value
        else:
            info[section] = value
    return info


def answers(info, seen=None, running=True):
    seen = seen or {"uid": 10001, "reports_write": "refusee (EROFS)",
                    "ml4fd_write": "refusee (EROFS)",
                    "session_user": "fd_dashboard", "health": "ok",
                    "latest_report": {"day": "2026-11-16", "verdict": "conforme",
                                      "relocated": False}}

    def run(args):
        if args[:4] == ["docker", "compose", "ps", "-q"]:
            return "abc123\n" if running else "\n"
        if args[:2] == ["docker", "inspect"]:
            return json.dumps([info])
        if args == check_s6.PROBE:
            return json.dumps(seen) + "\n"
        raise AssertionError(args)
    return run


def verdicts(run, tmp_path, get=lambda url: "ok"):
    env = tmp_path / ".env"
    env.write_text("POSTGRES_PASSWORD=%s\nFRAUD_PEPPER=pepper\n" % OWNER,
                   encoding="utf-8")
    return {c: (e, a) for c, e, a in check_s6.criteria(run, get, env)}


def failures(rows):
    return sorted(c for c, (e, a) in rows.items() if e != "mesure" and e != a)


def test_a_sound_container_passes_every_criterion(tmp_path):
    rows = verdicts(answers(inspect()), tmp_path)
    assert failures(rows) == []
    assert len(rows) >= 18


def test_the_container_built_on_the_airflow_anchor_is_caught(tmp_path):
    """The trap the plan names: the owner's password and the pepper handed
    over, the owner's role, a writable mount, the port open to the network."""
    bad = inspect(Config__Env=["POSTGRES_USER=aiafraud",
                               "POSTGRES_PASSWORD=%s" % OWNER,
                               "ML4FD_PEPPER=pepper"],
                  HostConfig__PortBindings={"8501/tcp": [{"HostIp": "",
                                                          "HostPort": "8502"}]})
    bad["Mounts"][0]["RW"] = True
    seen = {"uid": 0, "reports_write": "ACCEPTEE", "ml4fd_write": "ACCEPTEE",
            "session_error": "WrongIdentity: SUPER-UTILISATEUR", "health": "ok"}
    failed = failures(verdicts(answers(bad, seen), tmp_path))
    for expected in ("B3-R4 poivre absent de l'environnement",
                     "B3-R4 mot de passe du proprietaire absent de l'environnement",
                     "B3-R4 POSTGRES_USER du service",
                     "B3-R4 port publie sur cette machine seulement",
                     "B3-R4 montages en lecture seule",
                     "B3-R5 processus non root (uid vu de l'interieur)",
                     "B3-R5 ecriture dans reports/ refusee par le noyau",
                     "B3-R5 identite de la session PostgreSQL"):
        assert expected in failed, expected


def test_a_stopped_service_says_how_to_start_it(tmp_path):
    rows = verdicts(answers(inspect(), running=False), tmp_path)
    assert list(rows) == ["B3-R4 service dashboard en marche"]
    assert "fd-manager.ps1 dashboard" in rows["B3-R4 service dashboard en marche"][1]


def test_an_altered_or_relocated_report_fails(tmp_path):
    seen = {"uid": 10001, "reports_write": "refusee (EROFS)",
            "ml4fd_write": "refusee (EROFS)", "session_user": "fd_dashboard",
            "health": "ok", "latest_report": {"day": "2026-11-16",
                                              "verdict": "differente",
                                              "relocated": True}}
    failed = failures(verdicts(answers(inspect(), seen), tmp_path))
    assert "B3-R5 dernier rapport conforme a son empreinte" in failed
    assert "B3-R5 dernier rapport lu au chemin ou Airflow l'a ecrit" in failed


def test_a_page_unreachable_from_this_machine_fails(tmp_path):
    def down(url):
        raise OSError("refused")
    failed = failures(verdicts(answers(inspect()), tmp_path, get=down))
    assert failed == ["B3-R5 page servie, vue de cette machine"]


def test_no_secret_is_ever_printed(tmp_path, capsys, monkeypatch):
    """The owner's password is read from .env only to be COMPARED. Whatever
    the container holds, neither password reaches the output -- including when
    the container DOES hold the owner's, the case where it is compared and
    found."""
    env = tmp_path / ".env"
    env.write_text("POSTGRES_PASSWORD=%s\n" % OWNER, encoding="utf-8")
    run = answers(inspect(Config__Env=["POSTGRES_PASSWORD=%s" % OWNER,
                                       "POSTGRES_USER=fd_dashboard",
                                       "OTHER=dash-secret-4567"]))
    rows = list(check_s6.criteria(run, lambda url: "ok", env))
    monkeypatch.setattr(check_s6, "criteria", lambda: iter(rows))
    check_s6.main()
    printed = capsys.readouterr().out
    assert "PRESENT" in printed, "the leak is reported"
    assert OWNER not in printed and "dash-secret" not in printed


# --- the probe ----------------------------------------------------------------

def test_a_missing_directory_is_not_a_refused_write(tmp_path):
    """ENOENT would otherwise read as "refused" -- a missing mount, the very
    defect looked for, reported as a success."""
    assert probe._write_refused(tmp_path / "absent") == "ABSENT"


def test_a_writable_directory_is_reported_as_such(tmp_path):
    assert probe._write_refused(tmp_path) == "ACCEPTEE"
    assert list(tmp_path.iterdir()) == [], "and the test file is removed"


def test_the_probe_prints_names_not_values():
    source = (ROOT / "dashboard" / "probe.py").read_text(encoding="utf-8")
    assert '"env_names": sorted(os.environ)' in source
    assert "os.environ.items()" not in source and "dict(os.environ)" not in source
