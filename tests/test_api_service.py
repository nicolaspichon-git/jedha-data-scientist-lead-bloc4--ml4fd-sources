"""B4-L5: the scoring service as it is deployed -- its image, its service, its role.

Block: B4.
Lot: B4-L5.
Competency: B4-C3, B4-C4.
Covers: B4-EC16, B4-EC17, B4-EC19, B4-EC20, B4-EC21, B4-EC22, B4-EC23, B4-EC28.

READ FROM THE FILES rather than exercised, like tests/test_dashboard_service.py
and for the same reason: fastapi, uvicorn and mlflow live in the api image and
nowhere else, so the suite cannot start the app. What it CAN hold is every
property that is a declaration -- a mount, a privilege, a port, a worker count
-- and those are the ones that fail silently.
"""
from __future__ import annotations

import os
import re
import pathlib
from pathlib import Path

import yaml

os.environ.setdefault("POSTGRES_USER", "test_user")
os.environ.setdefault("POSTGRES_PASSWORD", "test_password")
os.environ.setdefault("POSTGRES_DB", "test_db")

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = yaml.safe_load((ROOT / "docker-compose.yml").read_text(
    encoding="utf-8"))
SERVICE_NAME = "scoring-service"
SERVICE = COMPOSE["services"][SERVICE_NAME]
APP = (ROOT / "scoring_service" / "app.py").read_text(encoding="utf-8")
DOCKERFILE = (ROOT / "scoring_service" / "Dockerfile").read_text(
    encoding="utf-8")


def _ddl(pattern: str) -> pathlib.Path:
    """The DDL matching a pattern, found and not spelled out.

    THE NUMBER IS A DEPENDENCY, NOT A NAME. The role's file moved from 185 to
    145 on 2026-11-20 because 150_database_connect.sql must run after it, and
    the module could not even be IMPORTED afterwards: these two lines carried
    the old numbers, so a renumbering produced FileNotFoundError at import
    instead of a test failure naming what was wrong. A file located by what it
    is survives being renumbered; the ordering is then checked where it
    belongs, by the two tests at the end of this file."""
    found = sorted((ROOT / "sql" / "ddl").glob(pattern))
    assert len(found) == 1, "%s matches %s" % (pattern, [p.name for p in found])
    return found[0]


ROLE = _ddl("*_scoring_service_role.sql").read_text(encoding="utf-8")
CALLS = _ddl("*_scoring_service_calls.sql").read_text(encoding="utf-8")


# --- the privilege, which is what the decoupling actually rests on -----------

def test_the_service_cannot_write_the_exploited_layer():
    """THE DECOUPLING IS A PRIVILEGE, NOT AN INTENTION. The service scores; it
    does not ingest. If this were only a matter of the code not calling an
    INSERT, a later hand could add one and nothing would refuse."""
    granted = re.findall(r"GRANT\s+([^;]*?)\s+ON\s+([^;]*?)\s+TO\s+fd_scoring_service",
                         ROLE, re.S)
    for rights, tables in granted:
        if "fd_transactions" in tables:
            assert "SELECT" in rights
            for forbidden in ("INSERT", "UPDATE", "DELETE", "TRUNCATE"):
                assert forbidden not in rights, rights


def test_it_reads_only_the_three_columns_the_feature_function_touches():
    """fd_client_features is LANGUAGE sql and STABLE, not SECURITY DEFINER, so
    it runs with the caller's privileges: the grant has to name the columns the
    function reads, and nothing more."""
    found = re.search(r"GRANT SELECT \(([^)]*)\) ON fd_transactions TO fd_scoring_service",
                      ROLE)
    assert found, "the column-level grant is gone"
    columns = {name.strip() for name in found.group(1).split(",")}
    assert columns == {"client_key", "txn_ts", "amt"}


def test_it_cannot_reach_the_raw_zone_or_the_verdicts():
    """The raw zone holds the personal data the exploited layer was built to be
    free of; the verdicts belong to the monitoring, over a window, not to a
    caller over one payment."""
    revoked = re.search(r"REVOKE ALL ON ([^;]*?)FROM fd_scoring_service", ROLE, re.S)
    assert revoked
    for table in ("fd_raw_events", "fd_quarantine", "fd_ground_truth",
                  "fd_datasets"):
        assert table in revoked.group(1), table
    for table in ("fd_raw_events", "fd_quarantine", "fd_ground_truth"):
        assert "ON %s TO fd_scoring_service" % table not in ROLE


def test_the_journal_is_append_only():
    assert "REVOKE UPDATE, DELETE, TRUNCATE ON fd_scoring_service_calls FROM fd_scoring_service" \
        in CALLS


def test_the_decision_cannot_contradict_the_score_in_the_database():
    """The same invariant fd_scores carries, for the measured reason
    ml4fd/score.py states: the comparison happens in Python and again here."""
    assert "CHECK (flagged = (score >= threshold))" in CALLS


# --- its own credentials, never the owner's ----------------------------------

def test_it_receives_no_secret_but_its_own():
    environment = SERVICE["environment"]
    assert environment["POSTGRES_USER"] == "fd_scoring_service"
    assert environment["POSTGRES_PASSWORD"] == "${FD_SCORING_SERVICE_PASSWORD}"
    assert "POSTGRES_PASSWORD" in environment
    body = yaml.safe_dump(SERVICE)
    assert "FD_DASHBOARD_PASSWORD" not in body
    assert "AIRFLOW_DB_PASSWORD" not in body


def test_it_receives_the_pepper_and_that_is_deliberate():
    """The dashboard does not get it; this service must, or its pseudonyms
    would differ from the ingestion path's and a call would stop joining the
    transactions of the same cardholder. The reason is written beside it."""
    assert SERVICE["environment"]["ML4FD_PEPPER"] == "${ML4FD_PEPPER}"
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    # KEYED ON THE SERVICE NAME DECLARED ABOVE and not on a literal: this read
    # "\n  api:" and broke the day the service was renamed, which is the same
    # spelling-pinned assertion B4-L5 already corrected in
    # test_the_manager_knows_the_service.
    block = compose[compose.index("\n  %s:" % SERVICE_NAME):]
    assert "MEME pseudonyme" in block or "SAME pseudonym" in block


def test_the_password_lives_in_the_environment_and_not_in_the_schema():
    """sql/ddl is replayed by anything that builds a schema and must not need a
    secret -- the arrangement dashboard_password.sql argues."""
    assert "PASSWORD" not in ROLE.upper().replace("FD_SCORING_SERVICE_PASSWORD", "")
    applied = (ROOT / "sql" / "roles" / "scoring_service_password.sql").read_text(
        encoding="utf-8")
    assert ":'scoring_service_password'" in applied
    manager = (ROOT / "fd-manager.ps1").read_text(encoding="utf-8")
    assert "scoring_service_password=$($env:FD_SCORING_SERVICE_PASSWORD)" in manager
    assert "FD_SCORING_SERVICE_PASSWORD=" in (ROOT / ".env.example").read_text(
        encoding="utf-8")


# --- the image ---------------------------------------------------------------

def test_it_runs_as_nobody_in_particular():
    assert "USER api" in DOCKERFILE
    assert SERVICE["cap_drop"] == ["ALL"]
    assert "no-new-privileges:true" in SERVICE["security_opt"]


def test_one_worker_and_the_reason_is_the_model_cache():
    """score.load() caches per process, keyed on (name, version). Two workers
    would each hold a copy and resolve the alias separately, which makes
    "which version answered?" harder to reason about for no throughput the
    declared 7,200 calls a day needs."""
    assert "--workers 1" in DOCKERFILE


def test_the_scoring_path_is_mounted_and_never_copied():
    """A copy of ml4fd/ in the image would be a second version of the scoring
    path -- the one thing reusing it was meant to avoid."""
    mounts = {entry.split(":")[1]: entry for entry in SERVICE["volumes"]}
    for target in ("/opt/project/ml4fd", "/opt/project/sql",
                   "/opt/project/governance.yml",
                   # THE MANIFESTS, missing on the first /health of 2026-11-20:
                   # score.load() loads the model from MLflow and reads the
                   # feature list and the threshold from run.json on disk, so a
                   # service without this mount resolves its champion and then
                   # cannot say what to feed it.
                   "/opt/project/runtime/datasets"):
        assert target in mounts, target
        assert mounts[target].endswith(":ro"), mounts[target]
    assert "COPY ml4fd" not in DOCKERFILE
    assert "COPY sql" not in DOCKERFILE


def test_the_python_version_matches_the_image_that_fits_the_model():
    """scikit-learn must unpickle a model fitted in the Airflow image."""
    assert "python:3.11-slim" in DOCKERFILE
    airflow = (ROOT / "airflow" / "Dockerfile").read_text(encoding="utf-8")
    assert "python3.11" in airflow


def test_the_port_is_this_machine_only():
    for mapping in SERVICE["ports"]:
        assert str(mapping).startswith("127.0.0.1:"), mapping


# --- the contract and the routes --------------------------------------------

def test_the_three_routes_exist():
    for route in ('@app.get("/health"', '@app.post("/score"', '@app.get("/"'):
        assert route in APP, route


def test_the_contract_is_generated_and_not_written_beside_the_code():
    """B4-EC17 asks for a queryable specification. FastAPI derives OpenAPI from
    the models, so the document cannot describe a route that does not exist or
    a field the service does not read."""
    assert "openapi.json" in APP
    assert "FastAPI(" in APP and "title=TITLE" in APP
    manager = (ROOT / "fd-manager.ps1").read_text(encoding="utf-8")
    assert "openapi.json" in manager, "the operator is never told where it is"


def test_the_request_is_judged_by_our_own_rules_and_not_only_by_the_schema():
    """pydantic checks shapes; B4-EC20 asks for an explicit error, and "string
    expected" does not say that fifty thousand is the maximum the pipeline
    accepts."""
    assert "serve.problems(" in APP
    assert "status_code=422" in APP


def test_no_model_is_a_503_and_not_a_crash():
    assert "status_code=503" in APP


def test_the_answer_is_recorded_before_it_is_returned():
    """ANSWER, THEN RECORD -- but the commit comes before the response is
    built, so a call that was answered was recorded. A journal entry for a
    decision nobody received would be a lie; a decision not recorded is a gap,
    which is the safer direction."""
    body = APP[APP.index("def post_score"):]
    assert body.index("serve.record(") < body.index("conn.commit()")
    assert body.index("conn.commit()") < body.index("status_code=200")


def test_the_champion_is_resolved_by_the_same_path_as_the_batch():
    """score.load() reads the alias before its cache, so a promotion is picked
    up on the next call. The plan calls the opposite "l'alias qui ne reveille
    pas le cache" and names it the most silent failure of the lot."""
    assert "score.load(" in APP
    assert "latest_version" not in APP
    assert "load_model" not in APP


def test_an_unforeseen_failure_is_logged_and_not_returned():
    """A driver's error names the host, the port and the role, and a caller
    has no use for any of them."""
    body = APP[APP.index("def unexpected"):]
    assert "log.exception" in body
    assert "str(problem)" not in body
    assert "status_code=500" in body


def test_the_response_carries_the_figures_and_the_sentence():
    """B4-EC15 is an addition, never a replacement: a caller that needs the
    number must not have to parse French to find it."""
    model = APP[APP.index("class Decision"):APP.index("def _settings")]
    for field in ("score", "threshold", "flagged", "model_version", "texte"):
        assert field in model, field


def test_the_card_number_is_declared_as_never_kept():
    """The contract is what a caller reads before sending a card number."""
    model = APP[APP.index("class Payment"):APP.index("class Decision")]
    assert "cc_num" in model
    assert "pseudonymise" in model or "Jamais conserve" in model


# --- the rename, and the migration that carries the journal across it --------

MIGRATION = (ROOT / "sql" / "maintenance"
             / "060_rename_scoring_service_objects.sql").read_text(
                 encoding="utf-8")
MANAGER = (ROOT / "fd-manager.ps1").read_text(encoding="utf-8")


def test_nothing_still_names_the_old_role_or_the_old_table():
    """A rename that leaves one reader behind is worse than no rename: the
    reader fails at run time, on a name the repository no longer mentions."""
    # THE TWO FILES ALLOWED TO NAME THE OLD TABLE, and no others: the
    # migration, which renames it, and this file, which checks that nothing
    # else does. Declared rather than inferred -- a rule that exempts whatever
    # happens to break it exempts everything.
    allowed = {"060_rename_scoring_service_objects.sql",
               "test_api_service.py"}
    for folder in ("ml4fd", "sql", "tests", "tools", "airflow", "dashboard",
                   "scoring_service"):
        for path in sorted((ROOT / folder).rglob("*")):
            if path.suffix not in (".py", ".sql", ".yml"):
                continue
            body = path.read_text(encoding="utf-8")
            assert not re.search(r"\bfd_api\b", body), path
            if path.name in allowed:
                continue
            assert not re.search(r"\bfd_api_calls\b", body), path


def test_the_migration_runs_before_the_ddl_loop():
    """THE ORDER IS THE WHOLE POINT. The DDL is replayable through CREATE TABLE
    IF NOT EXISTS, so on a database where init had already run it would create
    an EMPTY table under the new name and leave every recorded call in the old
    one. Two tables, one read by nothing, and no error."""
    migration = MANAGER.index("060_rename_scoring_service_objects.sql")
    ddl_loop = MANAGER.index("Get-ChildItem 'sql/ddl/*.sql'")
    assert migration < ddl_loop


def test_every_dependent_name_travels_with_the_table():
    """ALTER TABLE ... RENAME TO renames neither the sequence a bigserial owns,
    nor the indexes, nor the constraints. Left alone, a migrated database would
    carry fd_api_calls_call_id_seq while a fresh one carries the new name -- and
    the DDL's GRANT names the sequence explicitly, so it would succeed on one
    and fail on the other."""
    for old in ("fd_api_calls_call_id_seq", "fd_api_calls_by_instant",
                "fd_api_calls_by_client",
                "fd_api_calls_decision_follows_the_score"):
        assert old in MIGRATION, old
        assert old.replace("fd_api_calls", "fd_scoring_service_calls") \
            in MIGRATION, old


def test_the_new_names_are_the_ones_the_ddl_declares():
    """The cross-check that makes the test above worth having: a fresh database
    and a migrated one must be indistinguishable."""
    for name in re.findall(r"fd_scoring_service_calls[a-z_]*", MIGRATION):
        if name == "fd_scoring_service_calls":
            continue
        assert name in CALLS, "%s is renamed to a name no DDL declares" % name


def test_the_migration_is_replayable():
    """Like every file in sql/maintenance: each rename is guarded on the old
    name still existing, so a second run does nothing and says so."""
    assert MIGRATION.count("IF EXISTS") >= 4
    assert "RAISE NOTICE" in MIGRATION


def test_the_constraint_is_renamed_and_never_dropped():
    """Dropping and recreating it would leave the table, for an instant, able
    to hold a row the whole project says cannot exist."""
    assert "RENAME CONSTRAINT" in MIGRATION
    assert "DROP CONSTRAINT" not in MIGRATION


# --- the numbering of the DDL is a dependency, so it is checked -------------

DDL = sorted((ROOT / "sql" / "ddl").glob("*.sql"))


def _number(path) -> int:
    return int(path.name.split("_", 1)[0])


def _creates() -> dict:
    """Table or role name -> the number of the DDL that creates it."""
    made = {}
    for path in DDL:
        body = path.read_text(encoding="utf-8")
        for found in re.finditer(r"CREATE TABLE IF NOT EXISTS (\w+)", body):
            made.setdefault(found.group(1), _number(path))
        for found in re.finditer(r"CREATE ROLE (\w+)", body):
            made.setdefault(found.group(1), _number(path))
    return made


def test_a_role_granted_connect_is_created_by_an_earlier_file():
    """THE DEFECT OF 2026-11-20 22:46, and it cost the first real call.
    150_database_connect.sql grants CONNECT under IF EXISTS on the role -- a
    guard that keeps the file playable on an older database. The DDL loop runs
    files in NAME order, so a role created by a LATER file is simply not there
    when the guard looks: the grant is skipped in silence and the service fails
    with "User does not have CONNECT privilege".

    The numbering is the guarantee that file rests on, and its own header says
    so. This ties the two."""
    connect = next(path for path in DDL if "database_connect" in path.name)
    body = connect.read_text(encoding="utf-8")
    made = _creates()
    named = set(re.findall(r"rolname = '(\w+)'", body))
    assert named, "150 names no role: the check has stopped checking"
    for role in sorted(named):
        if role not in made:          # fd_airflow, fd_mlflow: sql/initdb
            continue
        assert made[role] < _number(connect), (
            "%s is created by %03d, after %s: its CONNECT grant is skipped"
            % (role, made[role], connect.name))


def test_a_table_a_role_file_revokes_on_is_created_by_an_earlier_file():
    """The same class, one file along: 145 revokes on nine tables, and naming
    one created later would fail with "relation does not exist" on every fresh
    database. fd_monitoring is created by 170, which is why its revocation
    lives in 190 instead."""
    role_file = next(path for path in DDL
                     if "scoring_service_role" in path.name)
    body = role_file.read_text(encoding="utf-8")
    made = _creates()
    block = re.search(r"REVOKE ALL ON ([^;]*?)FROM fd_scoring_service", body,
                      re.S).group(1)
    tables = {name.strip() for name in block.replace("\n", " ").split(",")
              if name.strip()}
    assert tables, "the revocation names nothing"
    for table in sorted(tables):
        assert table in made, table
        assert made[table] < _number(role_file), (
            "%s is created by %03d, after %s"
            % (table, made[table], role_file.name))


def test_the_monitoring_is_still_revoked_somewhere():
    """Removing it from 145 must not lose it: the monitoring belongs to the
    loop, over a window, and a caller asking about one payment has no business
    in it."""
    found = [path for path in DDL
             if "REVOKE ALL ON fd_monitoring FROM fd_scoring_service"
             in path.read_text(encoding="utf-8")]
    assert found, "the revocation on fd_monitoring was lost in the move"
    monitoring = next(path for path in DDL if "monitoring" in path.name
                      and "scoring" not in path.name)
    assert _number(found[0]) > _number(monitoring)
