"""Acceptance criteria of S6 that SQL cannot reach: the RUNNING container.

Covers: EC19, EC27
Lot: R5
Competency: C3, C4, C5

sql/checks/s6_acceptance.sql says what fd_dashboard IS in the catalogue. This
says what the dashboard SERVICE is -- not as docker-compose.yml declares it
(tests/test_dashboard_service.py holds the file), but as Docker actually runs
it, read three ways:

  docker inspect     what the container was given: its user, its
                     capabilities, its port, its mounts, its environment;
  dashboard/probe.py what only the container can answer: who PostgreSQL thinks
                     the session is, whether a write into reports/ is refused
                     by the kernel, whether the latest document is the one
                     Airflow wrote and at the same path;
  the host           whether the page is served at 127.0.0.1:8501.

The plan of S6 (7.4) names the trap this exists for: reusing the
x-airflow-common anchor hands the container the owner's password and the
pepper, and nothing on the page would show it.

RUNS ON THE HOST, with the project's .venv, from step-2 -- unlike check_s1..s5,
which run in the scheduler container: there is no Docker inside a container.
NO SECRET IS PRINTED. The owner's password is read from .env only to be
COMPARED with the container's environment, in memory.

Usage:
    python tools/check_s6.py
"""
from __future__ import annotations

import json
import subprocess
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SERVICE = "dashboard"
PROBE = ["docker", "compose", "exec", "-T", SERVICE,
         "python", "/opt/project/dashboard/probe.py"]
HEALTH = "http://127.0.0.1:8501/_stcore/health"
EXPECTED_MOUNTS = {"/opt/project/dashboard", "/opt/project/ml4fd",
                   "/opt/project/sql", "/opt/project/governance.yml",
                   "/opt/project/runtime/reports"}


def docker(args: list[str]) -> str:
    out = subprocess.run(args, cwd=ROOT, capture_output=True, text=True)
    if out.returncode:
        raise RuntimeError((out.stderr or out.stdout).strip()[:300])
    return out.stdout


def fetch(url: str) -> str:
    with urllib.request.urlopen(url, timeout=5) as answer:
        return answer.read().decode("utf-8").strip()


def dotenv(path: Path) -> dict:
    values = {}
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                key, _, value = line.partition("=")
                values[key.strip()] = value.strip()
    return values


def criteria(run=docker, get=fetch, env_file: Path = ROOT / ".env"):
    """Yield (criterion, expected, actual). `run` and `get` are injectable so
    the logic is testable without Docker."""
    container = run(["docker", "compose", "ps", "-q", SERVICE]).strip()
    yield ("R4 service dashboard en marche", "oui",
           "oui" if container else "NON : .\\fd-manager.ps1 dashboard")
    if not container:
        return

    info = json.loads(run(["docker", "inspect", container]))[0]
    config, host = info["Config"], info["HostConfig"]
    env = dict(item.partition("=")[::2] for item in config.get("Env") or [])

    yield ("R4 etat de sante Docker", "healthy",
           ((info.get("State") or {}).get("Health") or {}).get("Status", "aucun"))
    yield ("R4 utilisateur declare de l'image", "non root",
           "non root" if config.get("User") not in ("", "root", "0", None)
           else "ROOT")
    yield ("R4 capacites Linux retirees", "ALL",
           ",".join(host.get("CapDrop") or []) or "aucune")
    yield ("R4 elevation de privileges interdite", "oui",
           "oui" if "no-new-privileges:true" in (host.get("SecurityOpt") or [])
           else "non")

    bindings = [b for spec in (host.get("PortBindings") or {}).values()
                for b in (spec or [])]
    yield ("R4 port publie sur cette machine seulement", "127.0.0.1:8501",
           ", ".join("%s:%s" % (b.get("HostIp") or "0.0.0.0", b.get("HostPort"))
                     for b in bindings) or "aucun")

    mounts = info.get("Mounts") or []
    yield ("R4 montages en lecture seule", "%d sur %d" % (len(mounts), len(mounts)),
           "%d sur %d" % (sum(1 for m in mounts if not m.get("RW")), len(mounts)))
    yield ("R4 montages attendus, et seulement eux", "oui",
           "oui" if {m.get("Destination") for m in mounts} == EXPECTED_MOUNTS
           else "non : %s" % sorted({m.get("Destination") for m in mounts}
                                     ^ EXPECTED_MOUNTS))

    owner = dotenv(env_file)
    yield ("R4 poivre absent de l'environnement", "absent",
           "PRESENT" if "ML4FD_PEPPER" in env else "absent")
    yield ("R4 mot de passe du proprietaire absent de l'environnement", "absent",
           "PRESENT" if owner.get("POSTGRES_PASSWORD")
           and owner["POSTGRES_PASSWORD"] in env.values() else "absent")
    yield ("R4 POSTGRES_USER du service", "fd_dashboard",
           env.get("POSTGRES_USER", "absent"))

    try:
        seen = json.loads(run(PROBE).strip().splitlines()[-1])
    except Exception as problem:
        yield ("R5 sonde interieure", "executee", "ECHEC : %s" % problem)
        return

    yield ("R5 processus non root (uid vu de l'interieur)", "non root",
           "non root" if seen.get("uid") else "ROOT")
    yield ("R5 ecriture dans reports/ refusee par le noyau", "refusee",
           seen.get("reports_write", "?").split(" ")[0])
    yield ("R5 ecriture dans ml4fd/ refusee par le noyau", "refusee",
           seen.get("ml4fd_write", "?").split(" ")[0])
    yield ("R5 identite de la session PostgreSQL", "fd_dashboard",
           seen.get("session_user") or seen.get("session_error", "?")[:80])

    latest = seen.get("latest_report")
    if latest is None and "session_error" not in seen:
        yield ("info dernier rapport", "mesure", "aucun rapport produit")
    elif latest:
        yield ("info dernier rapport", "mesure", latest["day"])
        yield ("R5 dernier rapport conforme a son empreinte", "conforme",
               latest["verdict"])
        yield ("R5 dernier rapport lu au chemin ou Airflow l'a ecrit", "oui",
               "non" if latest["relocated"] else "oui")

    yield ("R5 page servie, vue de l'interieur", "ok", seen.get("health", "?"))
    try:
        outside = get(HEALTH)
    except Exception as problem:
        outside = "injoignable (%s)" % type(problem).__name__
    yield ("R5 page servie, vue de cette machine", "ok", outside)


def main() -> int:
    print("")
    print("Criteres d'acceptation de S6 que SQL n'atteint pas : le conteneur reel")
    print("")
    try:
        rows = list(criteria())
    except Exception as problem:
        print("  Docker ne repond pas : %s" % problem)
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
        print("  %-*s | %-16s | %-24s | %s" % (width, criterion, expected,
                                               actual, verdict))
    print("")
    print("%d critere(s) en echec sur %d" % (
        failed, sum(1 for r in rows if r[1] != "mesure")))
    print("")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
