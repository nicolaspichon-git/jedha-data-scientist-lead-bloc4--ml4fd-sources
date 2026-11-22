"""S5 / Q0: every store of the project has a retention decision.

The defect this guards was found by measurement, not by anyone's intention:
fd_quarantine carries `payload jsonb NOT NULL`, the raw payload of a REJECTED
record -- the same personal data as the raw zone -- and no duration covered it.
A rejected transaction would have been kept for ever where an accepted one was
purged at 90 days: the opposite of limiting retention.

So the decision is held where it can be checked. A table added to sql/ddl, a
volume added to docker-compose.yml, must appear in governance.yml's retention
section -- as a duration or as a store that is kept with its reason -- or a test
here fails. Nobody has to remember.

Block: B3, B4.
Lot: B3-Q0, B4-L0.
Competency: B3-C1, B3-C5.
Covers: B3-EC16, B3-EC19, B3-EI12.
"""
from __future__ import annotations

import os
import re
from pathlib import Path

import pytest
import yaml

os.environ.setdefault("POSTGRES_USER", "test_user")
os.environ.setdefault("POSTGRES_PASSWORD", "test_password")
os.environ.setdefault("POSTGRES_DB", "test_db")

from ml4fd import config  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]

# The tables that are PURGED, and the key that says for how long.
PURGED_TABLES = {
    "fd_raw_events": "raw_events_days",
    "fd_quarantine": "quarantine_days",
}
# The volumes of docker-compose.yml, and the retention entry that governs each.
# pgdata is the database itself: its tables are the ones above.
VOLUMES = {
    "pgdata": None,
    "airflow-logs": "airflow_logs_days",
    "mlruns": "mlflow_registry",
}


def _governance():
    return yaml.safe_load(config.governance_path().read_text(encoding="utf-8"))


def _retention():
    return _governance()["retention"]


def _tables():
    names = set()
    for path in sorted((ROOT / "sql" / "ddl").glob("*.sql")):
        text = "\n".join(line for line in path.read_text(encoding="utf-8")
                         .splitlines() if not line.strip().startswith("--"))
        names.update(re.findall(
            r"CREATE TABLE IF NOT EXISTS\s+(\w+)", text, re.IGNORECASE))
    return names


def _section(**over):
    """A valid retention section, with the given keys replaced."""
    section = {"raw_events_days": 90, "quarantine_days": 90,
               "airflow_logs_days": 30,
               "kept": {"fd_transactions": "pseudonymised; kept for training"}}
    section.update(over)
    return section


# --- the declaration, as decided ---------------------------------------------

def test_the_decided_durations_are_the_ones_declared():
    settings = config.load()
    assert settings.raw_retention_days == 90
    assert settings.quarantine_retention_days == 90
    assert settings.airflow_logs_retention_days == 30
    assert settings.purge_max_share == 0.5


def test_the_quarantine_is_no_longer_than_the_raw_zone_it_quotes():
    settings = config.load()
    assert settings.quarantine_retention_days <= settings.raw_retention_days
    assert settings.airflow_logs_retention_days <= settings.raw_retention_days


# --- no store escapes ----------------------------------------------------------

def test_every_table_of_the_project_has_a_retention_decision():
    """THE TEST THAT WOULD HAVE CAUGHT THE QUARANTINE. Read from the DDL, so a
    table added tomorrow is held to it without anyone remembering to add it."""
    tables = _tables()
    assert "fd_quarantine" in tables, "the DDL scan found nothing: broken scan"
    retention = _retention()
    decided = set(PURGED_TABLES) | set(retention["kept"])
    missing = sorted(tables - decided)
    assert not missing, (
        "no retention decision for %s: declare a duration, or list the store "
        "under retention.kept with the reason it is not purged" % missing)


def test_a_purged_table_is_not_also_declared_kept():
    """Both would mean the declaration contradicts itself, and the purge would
    follow whichever it read first."""
    both = set(PURGED_TABLES) & set(_retention()["kept"])
    assert not both, "declared both purged and kept: %s" % sorted(both)


def test_every_declared_table_exists():
    """The reverse: a stale entry is a decision about a store that is gone."""
    tables = _tables()
    stale = [name for name in _retention()["kept"]
             if name.startswith("fd_") and name not in tables]
    assert not stale, "kept stores that no DDL creates: %s" % stale


def test_every_duration_a_purge_reads_is_declared():
    retention = _retention()
    for table, key in PURGED_TABLES.items():
        assert key in retention, "%s has no %s" % (table, key)


def test_every_volume_of_the_compose_file_is_governed():
    """A volume added to the stack is a new place data can sit. The Airflow logs
    were exactly that: a volume with no rotation, outside every database."""
    compose = yaml.safe_load(
        (ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    volumes = set(compose["volumes"])
    unknown = sorted(volumes - set(VOLUMES))
    assert not unknown, (
        "volume(s) %s with no retention decision: add them to VOLUMES here "
        "and to governance.yml" % unknown)
    retention = _retention()
    for volume, key in VOLUMES.items():
        if key is not None:
            assert key in retention or key in retention["kept"], (
                "volume %s is governed by %s, which is not declared"
                % (volume, key))


def test_the_non_database_stores_are_declared_too():
    """Parquet sets, the registry, the report files, the downloaded source."""
    kept = _retention()["kept"]
    for store in ("datasets_directory", "mlflow_registry", "reports_directory",
                  "local_cache"):
        assert store in kept, store


def test_the_training_sets_are_kept_and_the_reason_says_why():
    """A FIRST VERSION DECLARED "THE FIVE LATEST VERSIONS" AMONG THE PURGED
    STORES, and no lot of S5 deleted a parquet file: a rule of disk hygiene
    dressed as a retention rule, which is how a declared duration becomes a
    duration nobody applies. Measured, these files carry no identifying field,
    so the decision moved to `kept` rather than growing code to enforce a rule
    this policy never required."""
    retention = _retention()
    assert "datasets_versions" not in retention
    assert not hasattr(config.load(), "datasets_versions")
    reason = retention["kept"]["datasets_directory"]
    assert "M8" in reason                    # the lineage it serves
    assert "identifying" in reason or "identifiant" in reason


def test_every_kept_store_carries_a_written_reason():
    for store, reason in _retention()["kept"].items():
        assert isinstance(reason, str) and len(reason.split()) >= 6, (
            "%s: a reason of a few words is a label, not a reason" % store)


# --- the shape is refused where it is read ---------------------------------------

def test_a_valid_section_is_accepted():
    out = config._retention(_section())
    assert out["quarantine_days"] == 90
    assert out["kept"] == ("fd_transactions",)


@pytest.mark.parametrize("key", ["quarantine_days", "airflow_logs_days"])
def test_a_store_that_quotes_the_raw_zone_cannot_outlive_it(key):
    """The inverse of the intention: rejected records kept after the accepted
    ones were purged."""
    with pytest.raises(ValueError, match="longer than retention.raw_events_days"):
        config._retention(_section(**{key: 91}))


@pytest.mark.parametrize("key", ["quarantine_days", "airflow_logs_days"])
def test_the_same_duration_as_the_raw_zone_is_allowed(key):
    assert config._retention(_section(**{key: 90}))[key] == 90


@pytest.mark.parametrize("value", [0, -1, True, False, 1.5, "90", None])
def test_a_duration_that_is_not_a_positive_whole_number_is_refused(value):
    with pytest.raises(ValueError, match="whole number of at least 1"):
        config._retention(_section(raw_events_days=value))


@pytest.mark.parametrize("reason", ["", "   ", None, 5])
def test_a_kept_store_without_a_reason_is_refused(reason):
    with pytest.raises(ValueError, match="has no reason"):
        config._retention(_section(kept={"fd_scores": reason}))


@pytest.mark.parametrize("kept", [None, {}, ["fd_scores"]])
def test_an_absent_or_malformed_kept_list_is_refused(kept):
    with pytest.raises(ValueError, match="must name each store"):
        config._retention(_section(kept=kept))


@pytest.mark.parametrize("value", [0, 0.0, 1, 1.0, -0.1, 2])
def test_the_purge_guard_cannot_be_declared_off(value):
    """At 1 the guard never fires; at 0 every purge is refused."""
    with pytest.raises(ValueError, match="strictly between 0 and 1"):
        config._share_under_one(value, "purge.max_share")


def test_the_run_journal_is_kept_and_says_why():
    """MEASURED, NOT ASSUMED (Q4). The plan foresaw emptying its "detail
    columns" at 365 days, and the table has none: an identifier, a DAG name,
    dates, a state, counters, the fingerprints of Q1. Nothing personal to
    empty -- and its rows must not go either, since those fingerprints are all
    that remains of a purged batch."""
    settings = config.load()
    assert not hasattr(settings, "log_retention_days")
    assert not hasattr(settings, "runs_retention_days")
    reason = _retention()["kept"]["fd_pipeline_runs"]
    assert "empreinte" in reason or "fingerprint" in reason
    # And the role that purges cannot delete from it (sql/ddl/120_purge_role.sql)
    role = (ROOT / "sql" / "ddl" / "120_purge_role.sql").read_text(
        encoding="utf-8")
    assert "REVOKE INSERT, DELETE, TRUNCATE ON fd_pipeline_runs" in role
