"""R4: the service -- what the one container a browser talks to is given.

Covers: EC19, EC27
Lot: R4
Competency: C3

The service could not be started where this is developed (Streamlit is not
installable here). What these tests hold is its DECLARATION -- the compose
service, the image, the Streamlit configuration -- read as docker compose and
Streamlit will read them. Measured beside them: ml4fd/config.py loads with
EXACTLY the environment the service declares (fd_dashboard's credentials under
the usual names, no pepper), and refuses to start, naming the variable, when
FD_DASHBOARD_PASSWORD is empty.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
SERVICE = COMPOSE["services"]["dashboard"]
AIRFLOW = COMPOSE["services"]["airflow-scheduler"]


def test_it_does_not_inherit_the_airflow_anchor():
    """x-airflow-common carries the owner's password and the pepper. A merge
    key resolved by PyYAML would show here as those variables appearing."""
    raw = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    block = raw[raw.index("\n  dashboard:"):]
    block = block[:block.index("\nvolumes:")]
    assert "<<:" not in block and "*airflow" not in block


def test_it_receives_no_secret_but_its_own():
    env = SERVICE["environment"]
    assert "ML4FD_PEPPER" not in env
    assert env["POSTGRES_USER"] == "fd_dashboard"
    assert env["POSTGRES_PASSWORD"] == "${FD_DASHBOARD_PASSWORD}"
    assert "${POSTGRES_PASSWORD}" not in str(env), "the OWNER's password"
    assert "MLFLOW_TRACKING_URI" not in env


def test_every_mount_is_read_only():
    for mount in SERVICE["volumes"]:
        assert mount.endswith(":ro"), mount


def test_it_mounts_only_what_it_reads():
    sources = {m.split(":")[0] for m in SERVICE["volumes"]}
    assert sources == {"./dashboard", "./ml4fd", "./sql", "./governance.yml",
                       "./runtime/reports"}
    # THE DOCUMENTS ONLY, and not runtime/ whole: the datasets and the two
    # notification channels live under it and are none of this page's business.
    for absent in ("./ws", "./runtime", "./runtime/datasets",
                   "./runtime/channels", "./.local_cache", "./tools"):
        assert absent not in sources, absent


def test_reports_are_mounted_where_airflow_writes_them():
    """report.path_for() must name the same file in both containers. A
    different mount is the defect that cost 28 observations and a dataset.

    Compared as a MAPPING from host directory to container path, and no longer
    as two identical mount lines: Airflow mounts runtime/ whole while the
    dashboard mounts runtime/reports alone, and the property to hold is that
    the documents answer to the same path on both sides.
    """
    def container_path(service, host):
        """Where `host` (a path relative to the project) lands in `service`."""
        best = None
        for mount in service["volumes"]:
            source, target = mount.split(":")[:2]
            if host == source:
                return target
            if host.startswith(source + "/"):
                best = target + host[len(source):]
        if best is None:
            raise AssertionError("%s not mounted" % host)
        return best
    for host in ("./runtime/reports", "./runtime/reports/2026-11-16.md",
                 "./ml4fd"):
        assert container_path(SERVICE, host) == container_path(AIRFLOW, host), host


def test_the_port_is_this_machine_only():
    assert SERVICE["ports"] == ["127.0.0.1:8501:8501"]


def test_it_holds_no_privilege():
    assert SERVICE["cap_drop"] == ["ALL"]
    assert "no-new-privileges:true" in SERVICE["security_opt"]
    dockerfile = (ROOT / "dashboard" / "Dockerfile").read_text(encoding="utf-8")
    users = re.findall(r"^USER\s+(\S+)", dockerfile, re.M)
    assert users and users[-1] not in ("root", "0")


def test_the_image_carries_nothing_it_does_not_import():
    """ml4fd.dashboard imports psycopg and PyYAML at load time, and nothing
    else of weight -- score and health only inside functions it never calls."""
    wanted = [l.split(">")[0].split("[")[0].split("=")[0].strip().lower()
              for l in (ROOT / "dashboard" / "requirements.txt")
              .read_text(encoding="utf-8").splitlines()
              if l.strip() and not l.startswith("#")]
    assert set(wanted) == {"streamlit", "psycopg", "pyyaml"}


def test_the_dashboard_module_loads_without_mlflow_or_sklearn(monkeypatch):
    """The claim above, measured: import the module with both made
    unimportable."""
    for name in ("mlflow", "sklearn", "airflow", "pandas"):
        monkeypatch.setitem(sys.modules, name, None)
    for name in [m for m in sys.modules if m.startswith("ml4fd")]:
        monkeypatch.delitem(sys.modules, name)
    import importlib
    importlib.import_module("ml4fd.dashboard")


def test_streamlit_shows_no_traceback_and_reports_nothing():
    import tomllib
    conf = tomllib.loads((ROOT / "dashboard" / ".streamlit" / "config.toml")
                         .read_text(encoding="utf-8"))
    # "none", not False: False is deprecated and means "stacktrace" -- the
    # stack is still drawn in the page (seen on 2026-11-17).
    assert conf["client"]["showErrorDetails"] == "none"
    assert conf["browser"]["gatherUsageStats"] is False
    assert conf["server"]["enableXsrfProtection"] is True


def test_the_image_finds_its_configuration_and_its_code():
    """Streamlit reads .streamlit/ from the working directory, and the pages
    import `common` and `fraud`."""
    dockerfile = (ROOT / "dashboard" / "Dockerfile").read_text(encoding="utf-8")
    assert "WORKDIR /opt/project/dashboard" in dockerfile
    assert '"app.py"' in dockerfile
    path = SERVICE["environment"]["PYTHONPATH"].split(":")
    assert "/opt/project" in path and "/opt/project/dashboard" in path


def test_the_manager_knows_the_service():
    text = (ROOT / "fd-manager.ps1").read_text(encoding="utf-8")
    assert "'dashboard')" in text, "status expects it"
    # Always a new process: a mounted ml4fd/ is NOT re-imported by a running
    # Streamlit, and a page newer than its module fails (AttributeError).
    assert "docker compose up -d --build --force-recreate dashboard" in text
    assert "http://localhost:8501" in text


def test_every_compose_service_can_be_named_to_the_manager():
    """THE DEFECT OF 2026-11-17: `fd-manager logs -Service dashboard` was
    refused by PowerShell -- the -Service parameter carries a closed list
    (ValidateSet), and adding a service to docker-compose.yml did not add it
    there. Two lists of the same thing: this ties them, and status's list of
    expected services with them (every long-running one)."""
    text = (ROOT / "fd-manager.ps1").read_text(encoding="utf-8")
    block = re.search(r"\[ValidateSet\(([^)]*)\)\]\s*\[string\]\$Service",
                      text).group(1)
    accepted = set(re.findall(r"'([^']+)'", block))
    assert accepted == set(COMPOSE["services"]), (
        "missing: %s, extra: %s" % (set(COMPOSE["services"]) - accepted,
                                    accepted - set(COMPOSE["services"])))

    expected = re.search(r"\$expected = @\(([^)]*)\)", text).group(1)
    watched = set(re.findall(r"'([^']+)'", expected))
    one_shot = {name for name, s in COMPOSE["services"].items()
                if s.get("restart") == "no"}
    assert watched == set(COMPOSE["services"]) - one_shot
