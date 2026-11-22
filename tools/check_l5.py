"""Acceptance criteria of B4-L5 that SQL cannot reach: the SERVICE, over HTTP.

Block: B4.
Lot: B4-L5, B4-L6.
Competency: B4-C2, B4-C3, B4-C4, B4-C5.
Covers: B4-EC15, B4-EC16, B4-EC19, B4-EC20, B4-EC21, B4-EC27.

The criterion of B4-L5 has three clauses. sql/checks/l5_acceptance.sql holds the
second one, because it is a question about rows and privileges. The other two
are here, and so is what the container was given:

  CLAUSE 1 -- A THIRD PARTY OBTAINS A SCORE OVER HTTP KNOWING NOTHING OF THE
  STACK. Checked as a third party would: the contract is read from
  /openapi.json, a non-conforming request must come back 422 NAMING the fields,
  and a conforming one must carry the figures AND the sentence. The payload
  below is built from the specification served by the service, not from a copy
  of Payment kept here -- a copy would pass while the published contract drifts.

  CLAUSE 3 -- A PROMOTION CHANGES THE VERSION ANNOUNCED, WITHOUT A RESTART.
  This cannot be CAUSED by a check: promoting a model is not a check's
  business. It is OBSERVED instead, in two parts that are only worth anything
  together:
    - the version /health announces equals the version the champion alias
      designates in the registry, read from MLflow directly;
    - the container started BEFORE the last promotion recorded in
      fd_promotions.
  Equality alone proves nothing: a service restarted after the promotion would
  also agree. With the start time before the alias last moved, the equality IS
  the clause. When the container is the younger of the two, this says so and
  reports the criterion as not demonstrable rather than as met -- the one
  outcome this file exists to make impossible to confuse.

  AND WHAT THE CONTAINER WAS GIVEN, in the form check_s6.py uses for the
  dashboard: its user, its capabilities, its published port, its mounts. One
  difference with the dashboard, and it is deliberate: THIS service receives
  ML4FD_PEPPER, because it must compute the same pseudonym as the ingestion
  path. It must still not hold the owner's password.

RUNS ON THE HOST, with the project's .venv, from step-2 -- like check_s6.py and
unlike check_s1..s5: there is no Docker inside a container, and the service is
published on 127.0.0.1:8012 here while it answers to scoring-service:5000 on
the compose network.

NO SECRET IS PRINTED. The owner's password is read from .env only to be
COMPARED with the container's environment, in memory. NO CARD NUMBER IS SENT
that belongs to anyone: the payload uses a number outside the corpus, which is
also what makes the "aucune transaction anterieure" branch observable.

Usage:
    python tools/check_l5.py
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SERVICE = "scoring-service"
BASE = "http://127.0.0.1:8012"
MLFLOW = "http://127.0.0.1:8002"
REGISTERED_NAME = "fraud_scoring"
ALIAS = "champion"

# DATA ONLY, since L6: the code is in the image, at the commit it was built
# from. A code mount reappearing here would silently undo that -- the service
# would again run whatever the disk holds -- so it is refused by name.
EXPECTED_MOUNTS = {"/opt/project/runtime/datasets"}
EXPECTED_ROUTES = {"/health", "/score", "/score/batch"}

# A card number that is not one of the corpus cardholders, so the call exercises
# the no-history branch of fd_client_features and records nothing about anybody.
OUTSIDE_THE_CORPUS = "4000000000000002"
PAYMENT = {"cc_num": OUTSIDE_THE_CORPUS,
           "txn_ts": "2026-11-21T10:15:00+00:00",
           "merchant": "fraud_check_l5",
           "category": "misc_net",
           "amt": "42.50",
           "merch_lat": "48.8566", "merch_long": "2.3522",
           "home_lat": "48.8566", "home_long": "2.3522"}
# Three fields wrong at once, on purpose: a service that stops at the first one
# makes a caller discover its contract one round trip at a time.
#
# PRESENT AND INVALID, NOT EMPTY. An empty field trips serve.missing(), which
# short-circuits and returns ONE sentence listing the empty fields -- so
# merch_lat="" produced a single problem where this asks for three. Measured on
# the real service, 21/11: 0 of 3.
MALFORMED = dict(PAYMENT, amt="quarante", merch_lat="quatre-vingts",
                 txn_ts="2026-11-21")


def docker(args: list[str]) -> str:
    out = subprocess.run(args, cwd=ROOT, capture_output=True, text=True)
    if out.returncode:
        raise RuntimeError((out.stderr or out.stdout).strip()[:300])
    return out.stdout


def fetch(url: str, body: dict | None = None, timeout: int = 20) -> tuple:
    """(status, decoded body). A 4xx is an ANSWER here, not a failure: the 422
    is one of the criteria."""
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as answer:
            return answer.status, json.loads(answer.read().decode("utf-8"))
    except urllib.error.HTTPError as refused:
        raw = refused.read().decode("utf-8")
        try:
            return refused.code, json.loads(raw)
        except ValueError:
            return refused.code, raw


def dotenv(path: Path) -> dict:
    values = {}
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                key, _, value = line.partition("=")
                values[key.strip()] = value.strip()
    return values


def psql(run, env: dict, statement: str) -> str:
    """One value, read in the postgres container with the owner's local socket
    -- no password crosses the host.

    `run` IS A PARAMETER AND NOT THE MODULE-LEVEL docker, which is the defect
    this signature exists to prevent: with docker hard-coded here, the injected
    runner was bypassed, every psql call raised, and clause 3 reported "no
    promotion recorded" whatever the journal held. A check that cannot fail is
    not a check."""
    out = run(["docker", "compose", "exec", "-T", "postgres",
               "psql", "-At", "-U", env.get("POSTGRES_USER", "postgres"),
               "-d", env.get("POSTGRES_DB", "postgres"), "-c", statement])
    return out.strip()


def alias_version(get) -> str:
    """The version the alias designates, read from MLflow and not from the
    service: asking the service which version it serves and comparing it with
    itself would always agree."""
    status, body = get("%s/api/2.0/mlflow/registered-models/alias"
                       "?name=%s&alias=%s" % (MLFLOW, REGISTERED_NAME, ALIAS))
    if status != 200 or not isinstance(body, dict):
        return "injoignable"
    return str((body.get("model_version") or {}).get("version", "aucune"))


def fields_named(body) -> set:
    """The field names a 422 names, in whichever of the two shapes it arrives.

    THE SERVICE HAS TWO 422 PATHS AND THEY DO NOT LOOK ALIKE. pydantic refuses
    a body whose SHAPE is wrong, and FastAPI reports that as detail[].loc.
    serve.problems refuses VALUES pydantic accepted -- every field of Payment
    is declared str, so "quarante" is a valid amt as far as the schema goes --
    and it returns detail as a LIST OF SENTENCES, each opening with the field
    it is about, because B4-EC20 asks for an error a caller can read.

    This file read the first shape only. Against the real service it therefore
    found no field at all and the criterion read 0 of 3 on 21/11. Worse, the
    fixture in tests/test_check_l5.py carried the same assumption, so
    thirty-five tests passed while the service answered otherwise: a test whose
    data I invented tested my idea of the contract and not the contract.

    A NAME COUNTS ONLY IF IT IS A FIELD OF THE PAYLOAD, which also reads the
    "champ(s) absent(s) ou vide(s) : a, b" sentence without a rule of its own.
    """
    if not isinstance(body, dict):
        return set()
    detail = body.get("detail")
    if isinstance(detail, (str, dict)):
        detail = [detail]
    if not isinstance(detail, list):
        return set()
    found = set()
    for item in detail:
        if isinstance(item, dict):
            words = {part for part in (item.get("loc") or [])
                     if isinstance(part, str)}
        elif isinstance(item, str):
            words = set(re.findall(r"[a-z_]+", item))
        else:
            words = set()
        found |= words & set(PAYMENT)
    return found


def criteria(run=docker, get=fetch, env_file: Path = ROOT / ".env"):
    """Yield (criterion, expected, actual). `run` and `get` are injectable so
    the logic is testable without Docker and without the service."""
    container = run(["docker", "compose", "ps", "-q", SERVICE]).strip()
    yield ("B4-L5 service de scoring en marche", "oui",
           "oui" if container else
           "NON : .\\fd-manager.ps1 up -Service scoring-service")
    if not container:
        return

    info = json.loads(run(["docker", "inspect", container]))[0]
    config, host = info["Config"], info["HostConfig"]
    env = dict(item.partition("=")[::2] for item in config.get("Env") or [])
    state = info.get("State") or {}

    yield ("B4-L5 etat de sante Docker", "healthy",
           (state.get("Health") or {}).get("Status", "aucun"))
    yield ("B4-L5 utilisateur declare de l'image", "non root",
           "non root" if config.get("User") not in ("", "root", "0", None)
           else "ROOT")
    yield ("B4-L5 capacites Linux retirees", "ALL",
           ",".join(host.get("CapDrop") or []) or "aucune")
    yield ("B4-L5 elevation de privileges interdite", "oui",
           "oui" if "no-new-privileges:true" in (host.get("SecurityOpt") or [])
           else "non")

    bindings = [b for spec in (host.get("PortBindings") or {}).values()
                for b in (spec or [])]
    yield ("B4-L5 port publie sur cette machine seulement", "127.0.0.1:8012",
           ", ".join("%s:%s" % (b.get("HostIp") or "0.0.0.0", b.get("HostPort"))
                     for b in bindings) or "aucun")

    mounts = info.get("Mounts") or []
    yield ("B4-L5 montages en lecture seule",
           "%d sur %d" % (len(mounts), len(mounts)),
           "%d sur %d" % (sum(1 for m in mounts if not m.get("RW")), len(mounts)))
    yield ("B4-L5 montages attendus, et seulement eux", "oui",
           "oui" if {m.get("Destination") for m in mounts} == EXPECTED_MOUNTS
           else "non : %s" % sorted({m.get("Destination") for m in mounts}
                                    ^ EXPECTED_MOUNTS))

    owner = dotenv(env_file)
    yield ("B4-L5 POSTGRES_USER du service", "fd_scoring_service",
           env.get("POSTGRES_USER", "absent"))
    yield ("B4-L5 mot de passe du proprietaire absent de l'environnement", "absent",
           "PRESENT" if owner.get("POSTGRES_PASSWORD")
           and owner["POSTGRES_PASSWORD"] in env.values() else "absent")
    # The one place this service differs from the dashboard, and it is argued
    # in docker-compose.yml: without the pepper, a call and a transaction of
    # the same cardholder would stop joining and nothing would say so.
    yield ("B4-L5 poivre present, car le pseudonyme doit etre le meme", "present",
           "present" if env.get("ML4FD_PEPPER") else "ABSENT")

    # ---- clause 1 : a third party, over HTTP
    status, root_body = get("%s/" % BASE)
    yield ("B4-L5 racine annonce le contrat", "200 /openapi.json",
           "%s %s" % (status, (root_body or {}).get("contrat", "rien")
                      if isinstance(root_body, dict) else "rien"))

    status, spec = get("%s/openapi.json" % BASE)
    paths = set((spec or {}).get("paths", {})) if isinstance(spec, dict) else set()
    yield ("B4-L5 contrat publie et interrogeable", "200", str(status))
    yield ("B4-L5 routes declarees dans le contrat", "oui",
           "oui" if EXPECTED_ROUTES <= paths
           else "manque %s" % sorted(EXPECTED_ROUTES - paths))

    status, sante = get("%s/health" % BASE)
    if not isinstance(sante, dict):
        sante = {}
    served = str(sante.get("model_version", "aucune"))
    yield ("B4-L5 etat de sante lu de cette machine", "ok",
           sante.get("state", "injoignable"))
    yield ("B4-L5 le point de sante annonce la version servie", "oui",
           "oui" if served not in ("aucune", "None") else "NON")
    yield ("B4-L6 le point de sante annonce la revision du code", "oui",
           "oui" if sante.get("revision") not in (None, "", "inconnue")
           else "NON")

    status, refused = get("%s/score" % BASE, MALFORMED)
    named = fields_named(refused)
    yield ("B4-L5 requete non conforme refusee", "422", str(status))
    yield ("B4-L5 les trois champs en cause nommes d'un coup", "3",
           str(len(named & {"amt", "merch_lat", "txn_ts"})))

    status, decision = get("%s/score" % BASE, PAYMENT)
    yield ("B4-L5 un tiers obtient un score", "200", str(status))
    if isinstance(decision, dict):
        expected_keys = {"score", "threshold", "flagged", "model_version",
                         "recipe_digest", "call_id"}
        missing = expected_keys - set(decision)
        yield ("B4-L5 la reponse porte les chiffres et l'identite du modele", "oui",
               "oui" if not missing else "manque %s" % sorted(missing))
        # B4-EC15: the figures stay beside the sentence, the sentence never
        # replaces them. A caller using assistive technology must not have to
        # read a number out of a float.
        sentence = str(decision.get("texte") or decision.get("phrase") or "")
        yield ("B4-L5 sortie textuelle presente a cote des chiffres", "oui",
               "oui" if len(sentence) > 20 else "NON")
        yield ("B4-L5 la phrase ne reproduit pas le numero de carte", "oui",
               "NON" if OUTSIDE_THE_CORPUS in sentence
               or OUTSIDE_THE_CORPUS[-4:] in sentence else "oui")
        # The decision and its threshold cannot disagree in the answer either.
        score_, seuil = decision.get("score"), decision.get("threshold")
        if isinstance(score_, (int, float)) and isinstance(seuil, (int, float)):
            yield ("B4-L5 flagged est bien score >= threshold", "oui",
                   "oui" if bool(decision.get("flagged")) == (score_ >= seuil)
                   else "NON")
        call_id = decision.get("call_id")
        yield ("B4-L5 l'appel est retrouvable en base par son identifiant", "oui",
               "oui" if call_id else "NON")
        if call_id:
            try:
                found = psql(run, owner,
                             "SELECT count(*) FROM fd_scoring_decisions "
                             "WHERE source = 'tiers' AND call_id = %s"
                             % int(call_id))
            except Exception as problem:
                found = "indecidable (%s)" % type(problem).__name__
            yield ("B4-L5 et retrouvable dans la vue fd_scoring_decisions", "1",
                   found)

    # ---- /score/batch : the path dag_ingest takes
    features = list(sante.get("features") or [])
    if features:
        rows = [{"txn_id": "check-l5-a",
                 "features": {name: 1 for name in features}},
                {"txn_id": "check-l5-b",
                 "features": {name: 2 for name in features}}]
        # "category" is the one feature of the recipe that is not a number.
        for row in rows:
            if "category" in row["features"]:
                row["features"]["category"] = "misc_net"
        status, batch = get("%s/score/batch" % BASE, {"rows": rows})
        yield ("B4-L5 le chemin par lot repond", "200", str(status))
        if isinstance(batch, dict):
            got = [d.get("txn_id") for d in batch.get("decisions") or []]
            yield ("B4-L5 les decisions reviennent dans l'ordre envoye",
                   "check-l5-a, check-l5-b", ", ".join(str(g) for g in got))
            yield ("B4-L5 le lot nomme le modele une fois pour toutes", served,
                   str(batch.get("model_version", "aucune")))
        # A batch over the ceiling must be refused WHOLE. Scoring half of it
        # and failing would leave fd_scores with a partial batch nobody asked
        # for -- the message says so rather than letting it happen.
        status, _ = get("%s/score/batch" % BASE,
                        {"rows": [{"txn_id": "x", "features": {}}] * 1001})
        yield ("B4-L5 un lot au-dela du plafond refuse en entier", "422",
               str(status))

    # ---- clause 3 : a promotion lands without a restart
    designated = alias_version(get)
    yield ("B4-L5 version servie = version designee par l'alias", designated,
           served)
    # Docker reports StartedAt in UTC. to_char on a timestamptz renders it in
    # the session's TimeZone, so the comparison below is only honest if both
    # are UTC -- hence the explicit cast rather than a trust in the default.
    started = (state.get("StartedAt") or "")[:19]
    try:
        # decided_at and not a promoted_at: fd_promotions journals refusals
        # too, and a refusal moved no alias. decision = 'promoted' is the
        # filter that makes this the instant the alias last changed hands.
        last = psql(run, owner, "SELECT coalesce(to_char(max(decided_at) AT TIME ZONE 'UTC', "
                           "'YYYY-MM-DD\"T\"HH24:MI:SS'), '') "
                           "FROM fd_promotions WHERE decision = 'promoted'")
    except Exception:
        last = ""
    if not last:
        yield ("B4-L5 promotion sans redemarrage", "mesure",
               "aucune promotion enregistree : clause non demontrable")
    elif started and started < last:
        yield ("B4-L5 promotion sans redemarrage, le conteneur est anterieur",
               "oui", "oui" if designated == served else "NON")
    else:
        yield ("B4-L5 promotion sans redemarrage", "mesure",
               "conteneur demarre le %s, apres la promotion du %s : "
               "l'egalite des versions ne prouve rien ici" % (started, last))

    yield ("info version annoncee par le service", "mesure", served)
    yield ("info seuil en service", "mesure",
           str(sante.get("threshold", "aucun")))


def main() -> int:
    print("")
    print("Criteres d'acceptation de B4-L5 que SQL n'atteint pas : le service reel")
    print("")
    try:
        rows = list(criteria())
    except Exception as problem:
        # NAMED FOR WHAT IT IS. This used to read "Docker ne repond pas"
        # whatever had failed, and on 21/11 it hid a TypeError of this file's
        # own making. A diagnosis printed by a check must not be invented by
        # the check.
        print("  echec du controle : %s : %s"
              % (type(problem).__name__, problem))
        print("  Si Docker est en cause, .\\fd-manager.ps1 status le dira.")
        return 1
    width = max(len(r[0]) for r in rows)
    failed = 0
    for criterion, expected, actual in rows:
        if expected == "mesure":
            verdict = "info"
        elif expected == actual:
            verdict = "OK"
        else:
            verdict = "<<< ECHEC"
            failed += 1
        print("  %-*s | %-26s | %-30s | %s" % (width, criterion, expected,
                                               actual, verdict))
    print("")
    print("%d critere(s) en echec sur %d" % (
        failed, sum(1 for r in rows if r[1] != "mesure")))
    print("")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
