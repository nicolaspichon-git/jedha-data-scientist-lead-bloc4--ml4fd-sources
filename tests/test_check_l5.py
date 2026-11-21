"""L5: the acceptance of the scoring service -- the catalogue, and the service
as it answers.

Covers: EC15, EC16, EC19, EC20, EC21
Lot: L5
Competency: C2, C3, C5

check_l5.py cannot meet Docker nor the service here, so its logic is held
against `docker inspect` output and HTTP answers shaped like the real ones: a
sound service, and the three failures L5 actually met on 2026-11-20 -- the
port already allocated, runtime/datasets not mounted, and the CONNECT
privilege skipped by the DDL ordering.

THE CRITERION THESE TESTS EXIST FOR is clause 3, "a promotion changes the
version announced without a restart". It is the one a check can report as met
while proving nothing: a service restarted after the promotion also announces
the right version. Four tests below pin the three outcomes apart -- demonstrated,
not demonstrable because the container is the younger, and no promotion yet.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))

import check_l5  # noqa: E402

SQL = (ROOT / "sql" / "checks" / "l5_acceptance.sql").read_text(encoding="utf-8")
DDL = (ROOT / "sql" / "ddl" / "145_scoring_service_role.sql").read_text(
    encoding="utf-8")
OWNER = "owner-secret-0123"
SERVED = "7"


def _code(text):
    return "\n".join(line.split("--")[0] for line in text.splitlines())


# --- the catalogue ------------------------------------------------------------

def test_the_columns_expected_are_the_ones_the_ddl_grants():
    """Two places name the three readable columns of fd_transactions. They must
    name the same ones, or the check passes on a grant nobody intended."""
    granted = set()
    for group in re.findall(
            r"GRANT\s+SELECT\s*\(([^)]*)\)\s*ON\s+fd_transactions", _code(DDL),
            re.I):
        granted |= {name.strip() for name in group.split(",")}
    expected = re.search(
        r"'L5 colonnes de fd_transactions lisibles',\s*'([^']+)'", SQL).group(1)
    assert {name.strip() for name in expected.split(",")} == granted


def test_the_connect_privilege_is_a_criterion_and_not_a_comment():
    """The 500 of 2026-11-20: 150_database_connect.sql grants CONNECT under an
    IF EXISTS, so a role created after it silently had none. A check that only
    mentioned it in a comment would have let the next renumbering repeat it."""
    assert "has_database_privilege('fd_scoring_service'" in SQL
    assert "CONNECT" in SQL


def test_every_forbidden_table_is_tested_by_column_and_not_by_table():
    """has_table_privilege would miss a SELECT granted on one column, which is
    the door S6 found. The same rule has to hold here."""
    body = SQL[SQL.index("reachable AS"):SQL.index("readable_txn AS")]
    assert body.count("has_any_column_privilege") == 3
    assert "has_table_privilege('fd_scoring_service', f.relname, 'DELETE')" in body


def test_the_monitoring_tables_are_forbidden_to_the_service():
    """A service that saw the verdicts could be asked whether it was right, and
    that question belongs to the surveillance, over a window."""
    body = SQL[SQL.index("forbidden AS"):SQL.index("reachable AS")]
    for table in ("fd_ground_truth", "fd_monitoring", "fd_datasets",
                  "fd_raw_events", "fd_quarantine"):
        assert table in body


def test_the_view_is_counted_against_both_journals():
    """The whole claim of option 2: nothing lost, nothing counted twice."""
    assert "FROM fd_scoring_decisions)" in SQL
    assert "FROM fd_scores)" in SQL
    assert "FROM fd_scoring_service_calls)" in SQL


def test_a_missing_role_is_named_before_anything_else_fails():
    """Every privilege function would raise on an unknown name. Say it once,
    with the remedy."""
    head = SQL[:SQL.index("CREATE TEMP TABLE")]
    assert "n''existe pas" in head and "init" in head


def test_psql_exits_non_zero_on_any_failure():
    """psql exits 0 whatever a verdict column says."""
    tail = SQL[SQL.index("SELECT criterion"):]
    assert "RAISE EXCEPTION" in tail and "ECHEC" in tail


def test_the_schema_is_asked_not_the_rows_where_a_scan_could_not_fail():
    """model_version and recipe_digest are NOT NULL, so a row scan would read
    as a check while testing nothing. The constraint itself is what a migration
    can lose."""
    body = SQL[SQL.index("SELECT 18,"):SQL.index("SELECT 19,")]
    assert "attnotnull" in body
    assert "WHERE model_version IS NULL" not in body


# --- the service as it answers ------------------------------------------------

def inspect(**changes):
    info = {
        "Config": {"User": "10001:10001",
                   "Env": ["POSTGRES_USER=fd_scoring_service",
                           "POSTGRES_PASSWORD=service-secret-4567",
                           "POSTGRES_DB=fraud", "POSTGRES_HOST=postgres",
                           "ML4FD_PEPPER=pepper-value",
                           "MLFLOW_TRACKING_URI=http://mlflow:5000"]},
        "HostConfig": {"CapDrop": ["ALL"],
                       "SecurityOpt": ["no-new-privileges:true"],
                       "PortBindings": {"5000/tcp": [{"HostIp": "127.0.0.1",
                                                      "HostPort": "5001"}]}},
        "Mounts": [{"Destination": d, "RW": False}
                   for d in sorted(check_l5.EXPECTED_MOUNTS)],
        "State": {"Health": {"Status": "healthy"},
                  "StartedAt": "2026-11-20T18:00:00.123456789Z"},
    }
    for key, value in changes.items():
        section, _, field = key.partition("__")
        if field:
            info[section][field] = value
        else:
            info[section] = value
    return info


HEALTH = {"state": "ok", "model_version": SERVED, "recipe_digest": "93d16ede",
          "features": ["amt", "category", "f_tx_count", "f_avg_amount",
                       "f_home_distance_km"],
          "threshold": 0.86, "detail": "modele en service"}
DECISION = {"call_id": 1, "decision": "acceptee", "flagged": False,
            "score": 0.00429507, "threshold": 0.86, "model_version": SERVED,
            "recipe_digest": "93d16ede",
            "texte": "Transaction acceptee sans signalement. Score de fraude "
                     "0.0043 pour un seuil de decision de 0.8600, et aucune "
                     "transaction anterieure pour ce porteur."}
REFUSED = {"detail": [{"loc": ["body", "amt"], "msg": "x"},
                      {"loc": ["body", "merch_lat"], "msg": "x"},
                      {"loc": ["body", "txn_ts"], "msg": "x"}]}


def answers(info, health=None, decision=None, batch=None, running=True,
            alias=SERVED, rows_over_cap=422):
    health = HEALTH if health is None else health
    decision = DECISION if decision is None else decision

    def run(args):
        if args[:4] == ["docker", "compose", "ps", "-q"]:
            return "abc123\n" if running else "\n"
        if args[:2] == ["docker", "inspect"]:
            return json.dumps([info])
        if args[:4] == ["docker", "compose", "exec", "-T"]:
            statement = args[-1]
            if "fd_promotions" in statement:
                return "2026-11-20T22:00:00\n"
            if "fd_scoring_decisions" in statement:
                return "1\n"
        raise AssertionError(args)

    def get(url, body=None, timeout=20):
        if url.endswith("/"):
            return 200, {"service": "x", "contrat": "/openapi.json"}
        if url.endswith("/openapi.json"):
            return 200, {"paths": {p: {} for p in check_l5.EXPECTED_ROUTES}}
        if url.endswith("/health"):
            return 200, health
        if url.endswith("/score"):
            if body is check_l5.MALFORMED or (body or {}).get("amt") == "quarante":
                return 422, REFUSED
            return 200, decision
        if url.endswith("/score/batch"):
            if len(((body or {}).get("rows") or [])) > check_l5_cap():
                return rows_over_cap, {"detail": "trop de lignes"}
            if batch is not None:
                return 200, batch
            return 200, {"model_version": SERVED, "recipe_digest": "93d16ede",
                         "threshold": 0.86,
                         "decisions": [{"txn_id": "check-l5-a", "score": 0.1,
                                        "flagged": False},
                                       {"txn_id": "check-l5-b", "score": 0.2,
                                        "flagged": False}]}
        if "registered-models/alias" in url:
            return 200, {"model_version": {"version": alias}}
        raise AssertionError(url)
    return run, get


def check_l5_cap():
    """The ceiling the service enforces, read from ml4fd.serve and not copied:
    a copy here would keep passing after the ceiling moved."""
    sys.path.insert(0, str(ROOT))
    from ml4fd import serve
    return serve.MAX_BATCH_ROWS


def verdicts(run, get, tmp_path):
    env = tmp_path / ".env"
    env.write_text("POSTGRES_PASSWORD=%s\nPOSTGRES_USER=fraud_owner\n"
                   "POSTGRES_DB=fraud\n" % OWNER, encoding="utf-8")
    return {c: (e, a) for c, e, a in check_l5.criteria(run, get, env)}


def failures(rows):
    return sorted(c for c, (e, a) in rows.items() if e != "mesure" and e != a)


def test_a_sound_service_passes_every_criterion(tmp_path):
    run, get = answers(inspect())
    rows = verdicts(run, get, tmp_path)
    assert failures(rows) == []
    assert len(rows) >= 24


def test_a_stopped_service_says_how_to_start_it(tmp_path):
    run, get = answers(inspect(), running=False)
    rows = verdicts(run, get, tmp_path)
    assert len(rows) == 1
    assert "fd-manager.ps1" in rows["L5 service de scoring en marche"][1]


def test_the_missing_dataset_mount_is_caught(tmp_path):
    """The first /health of 2026-11-20 22:38 read degraded: score.load() loads
    the model from the registry and the manifest from disk, and nothing of
    runtime/ was mounted. Two criteria must fail, not one -- the mount and the
    state."""
    mounts = [{"Destination": d, "RW": False}
              for d in sorted(check_l5.EXPECTED_MOUNTS
                              - {"/opt/project/runtime/datasets"})]
    run, get = answers(inspect(Mounts=mounts),
                       health={"state": "degraded", "model_version": None,
                               "recipe_digest": None,
                               "detail": "no run.json"})
    rows = verdicts(run, get, tmp_path)
    assert "L5 montages attendus, et seulement eux" in failures(rows)
    assert "L5 etat de sante lu de cette machine" in failures(rows)
    assert "L5 le point de sante annonce la version servie" in failures(rows)


def test_a_port_published_to_every_interface_is_caught(tmp_path):
    """The service holds three columns of fd_transactions and the pepper. A
    binding on 0.0.0.0 puts that on the network the host is on."""
    run, get = answers(inspect(HostConfig__PortBindings={
        "5000/tcp": [{"HostIp": "", "HostPort": "5001"}]}))
    assert ("L5 port publie sur cette machine seulement"
            in failures(verdicts(run, get, tmp_path)))


def test_the_owners_password_in_the_environment_is_caught(tmp_path):
    run, get = answers(inspect(Config__Env=[
        "POSTGRES_USER=fd_scoring_service", "POSTGRES_PASSWORD=%s" % OWNER,
        "ML4FD_PEPPER=pepper-value"]))
    rows = verdicts(run, get, tmp_path)
    assert ("L5 mot de passe du proprietaire absent de l'environnement"
            in failures(rows))


def test_the_pepper_is_required_here_unlike_the_dashboard(tmp_path):
    """The one place this service differs from the dashboard, and the reason is
    a join: without the same pepper, a call and a transaction of one cardholder
    stop joining and nothing says so."""
    run, get = answers(inspect(Config__Env=["POSTGRES_USER=fd_scoring_service"]))
    rows = verdicts(run, get, tmp_path)
    assert ("L5 poivre present, car le pseudonyme doit etre le meme"
            in failures(rows))


def test_a_service_that_names_one_bad_field_at_a_time_is_caught(tmp_path):
    """A caller must not discover the contract one round trip at a time."""
    run, get = answers(inspect())
    original = get

    def one_at_a_time(url, body=None, timeout=20):
        if url.endswith("/score") and (body or {}).get("amt") == "quarante":
            return 422, {"detail": [{"loc": ["body", "amt"], "msg": "x"}]}
        return original(url, body, timeout)
    rows = verdicts(run, one_at_a_time, tmp_path)
    assert "L5 les trois champs en cause nommes d'un coup" in failures(rows)


def test_a_decision_without_its_model_identity_is_caught(tmp_path):
    """A score whose model nobody can name cannot be traced back to its
    training rows, which is what M8 exists for."""
    run, get = answers(inspect(),
                       decision={k: v for k, v in DECISION.items()
                                 if k not in ("model_version", "recipe_digest")})
    rows = verdicts(run, get, tmp_path)
    assert ("L5 la reponse porte les chiffres et l'identite du modele"
            in failures(rows))


def test_a_sentence_that_replaces_the_figures_is_not_enough(tmp_path):
    """EC15 asks for the sentence BESIDE the figures. An answer with no
    sentence fails, and so does one that leaks the card number into it."""
    run, get = answers(inspect(), decision=dict(DECISION, texte=""))
    assert ("L5 sortie textuelle presente a cote des chiffres"
            in failures(verdicts(run, get, tmp_path)))

    run, get = answers(inspect(), decision=dict(
        DECISION, texte="Transaction acceptee pour la carte %s, sans suite."
                        % check_l5.OUTSIDE_THE_CORPUS))
    assert ("L5 la phrase ne reproduit pas le numero de carte"
            in failures(verdicts(run, get, tmp_path)))


def test_a_flagged_that_disagrees_with_its_threshold_is_caught(tmp_path):
    """The invariant fd_scores and fd_scoring_service_calls both carry, checked
    in the answer as well: the two cannot disagree anywhere."""
    run, get = answers(inspect(), decision=dict(DECISION, flagged=True))
    assert ("L5 flagged est bien score >= threshold"
            in failures(verdicts(run, get, tmp_path)))


def test_a_batch_answered_out_of_order_is_caught(tmp_path):
    """score.remote_rows refuses a response whose txn_ids differ; the check
    must see the weaker version of the same defect -- the right ids in the
    wrong order."""
    run, get = answers(inspect(), batch={
        "model_version": SERVED, "recipe_digest": "93d16ede", "threshold": 0.86,
        "decisions": [{"txn_id": "check-l5-b", "score": 0.2, "flagged": False},
                      {"txn_id": "check-l5-a", "score": 0.1, "flagged": False}]})
    assert ("L5 les decisions reviennent dans l'ordre envoye"
            in failures(verdicts(run, get, tmp_path)))


def test_a_batch_over_the_ceiling_must_be_refused_whole(tmp_path):
    """Scoring half a batch and then failing would leave fd_scores with a
    partial batch nobody asked for."""
    run, get = answers(inspect(), rows_over_cap=200)
    assert ("L5 un lot au-dela du plafond refuse en entier"
            in failures(verdicts(run, get, tmp_path)))


def test_a_batch_naming_another_model_than_the_health_endpoint_is_caught(tmp_path):
    """fd_scores records the version per row and they must all agree. Two
    entry points announcing two versions is the coupling L5 removed, coming
    back inside the service."""
    run, get = answers(inspect(), batch={
        "model_version": "4", "recipe_digest": "93d16ede", "threshold": 0.86,
        "decisions": [{"txn_id": "check-l5-a", "score": 0.1, "flagged": False},
                      {"txn_id": "check-l5-b", "score": 0.2, "flagged": False}]})
    assert ("L5 le lot nomme le modele une fois pour toutes"
            in failures(verdicts(run, get, tmp_path)))


# --- clause 3, the one a check can claim without proving ----------------------

def test_the_served_version_is_read_from_the_registry_and_not_from_the_service():
    """Asking the service which version it serves and comparing it with itself
    would always agree. alias_version must reach MLflow, not BASE."""
    seen = []

    def get(url, body=None, timeout=20):
        seen.append(url)
        return 200, {"model_version": {"version": "9"}}
    assert check_l5.alias_version(get) == "9"
    assert seen and seen[0].startswith(check_l5.MLFLOW)
    assert not seen[0].startswith(check_l5.BASE)
    assert "registered-models/alias" in seen[0]


def test_an_unreachable_registry_is_said_and_not_guessed():
    assert check_l5.alias_version(lambda *a, **k: (503, "")) == "injoignable"


def test_a_stale_service_serving_the_previous_model_is_caught(tmp_path):
    """The silent failure of the whole project: a loop that decides correctly
    and serves the old model until the next restart."""
    run, get = answers(inspect(), alias="8")
    rows = verdicts(run, get, tmp_path)
    assert ("L5 promotion sans redemarrage, le conteneur est anterieur"
            in failures(rows))


def test_the_clause_is_demonstrated_only_when_the_container_is_the_older(tmp_path):
    """The container started at 18:00, the alias last moved at 22:00: the
    equality of the versions IS the clause."""
    run, get = answers(inspect())
    rows = verdicts(run, get, tmp_path)
    assert ("L5 promotion sans redemarrage, le conteneur est anterieur"
            in rows)
    assert rows["L5 promotion sans redemarrage, le conteneur est anterieur"] \
        == ("oui", "oui")


def test_a_container_younger_than_the_promotion_proves_nothing(tmp_path):
    """A service restarted after the promotion announces the right version too.
    Reporting that as met is the one outcome this file exists to prevent: it
    must read as information, never as a criterion passed."""
    run, get = answers(inspect(State={
        "Health": {"Status": "healthy"},
        "StartedAt": "2026-11-21T06:00:00.000000000Z"}))
    rows = verdicts(run, get, tmp_path)
    assert "L5 promotion sans redemarrage" in rows
    expected, actual = rows["L5 promotion sans redemarrage"]
    assert expected == "mesure"
    assert "ne prouve rien" in actual
    assert ("L5 promotion sans redemarrage, le conteneur est anterieur"
            not in rows)


def test_no_promotion_yet_is_not_a_failure(tmp_path):
    """Before the first promotion the clause has nothing to be demonstrated
    against. An empty journal is not a defect of the service."""
    run, get = answers(inspect())
    original = run

    def no_promotion(args):
        if args[:4] == ["docker", "compose", "exec", "-T"] \
                and "fd_promotions" in args[-1]:
            return "\n"
        return original(args)
    rows = verdicts(no_promotion, get, tmp_path)
    assert rows["L5 promotion sans redemarrage"][0] == "mesure"
    assert "non demontrable" in rows["L5 promotion sans redemarrage"][1]
    assert failures(rows) == []


def test_the_promotion_instant_excludes_the_refusals(tmp_path):
    """fd_promotions journals refusals too, and a refusal moved no alias.
    Taking max(decided_at) without the filter would date the clause from a
    decision that changed nothing."""
    source = (ROOT / "tools" / "check_l5.py").read_text(encoding="utf-8")
    assert "WHERE decision = 'promoted'" in source
    assert "AT TIME ZONE 'UTC'" in source


def test_no_secret_is_ever_printed(tmp_path, capsys, monkeypatch):
    """The owner's password is read from .env only to be compared, in memory.
    main() is run whole, since the printing is where a secret would escape."""
    run, get = answers(inspect())
    env = tmp_path / ".env"
    env.write_text("POSTGRES_PASSWORD=%s\nML4FD_PEPPER=pepper-value\n" % OWNER,
                   encoding="utf-8")
    real = check_l5.criteria
    monkeypatch.setattr(check_l5, "criteria",
                        lambda *a, **k: real(run, get, env))
    assert check_l5.main() == 0
    printed = capsys.readouterr().out
    assert OWNER not in printed
    assert "pepper-value" not in printed
    assert "service-secret-4567" not in printed


def test_main_exits_non_zero_on_any_failure(monkeypatch, tmp_path, capsys):
    run, get = answers(inspect(), alias="8")
    env = tmp_path / ".env"
    env.write_text("POSTGRES_PASSWORD=x\n", encoding="utf-8")
    real = check_l5.criteria
    monkeypatch.setattr(check_l5, "criteria",
                        lambda *a, **k: real(run, get, env))
    assert check_l5.main() == 1
    assert "ECHEC" in capsys.readouterr().out


def test_the_card_number_sent_belongs_to_nobody():
    """The payload must not carry a real cardholder's number, and the number
    chosen is also what makes the no-history branch observable."""
    assert check_l5.PAYMENT["cc_num"] == check_l5.OUTSIDE_THE_CORPUS
    assert check_l5.OUTSIDE_THE_CORPUS.startswith("4000000000")


def test_the_malformed_payload_breaks_three_fields_and_not_one():
    broken = {k for k, v in check_l5.MALFORMED.items()
              if check_l5.PAYMENT.get(k) != v}
    assert broken == {"amt", "merch_lat", "txn_ts"}
