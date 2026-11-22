"""Every path governance.yml declares must also be mounted into the container.

Block: B3, B4.
Lot: B3-P2, B4-L0.
Competency: B3-C5.
Covers: B3-EC23, B3-EC24.

THE FAILURE THIS FILE EXISTS FOR, and it is the third time this project has paid
for it:

  N0   28 real observations written to ws/ inside the container, where ws/ was
       not mounted. Five minutes of real API calls, gone with the container.
  M8   a whole dataset -- Parquet, model, manifest -- written to a relative
       datasets/ resolved against /opt/airflow. The rows were registered in
       fd_datasets, every acceptance criterion on those rows passed, and the
       files were not there.
  P2   report.root declared in governance.yml with no mount in
       docker-compose.yml: "PermissionError: [Errno 13]
       /opt/project/runtime/reports",
       because /opt/project is a directory of the IMAGE and not a volume.

DECLARED AND MOUNTED ARE TWO DIFFERENT THINGS. config._under_project makes a
declared path absolute against the project, which fixed the M8 defect -- but an
absolute path inside the container still needs a volume behind it. Nothing held
the two files to each other until now.

This test reads both and compares them. It is a cross-file test, like the one
holding tools/check_s3.py to the DAG mount: a path added to one file and not the
other fails here rather than at six in the morning.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
CONTAINER_ROOT = "/opt/project"

# Every declared path, as (governance key, value read from the file). Read from
# governance.yml and NOT from Settings: config resolves them to absolute host
# paths, and what has to match a mount is the RELATIVE declaration.
KEYS = (("notifications", "file"),
        ("ops", "file"),
        ("report", "root"),
        ("datasets", "root"))


def _governance() -> dict:
    return yaml.safe_load((ROOT / "governance.yml").read_text(encoding="utf-8"))


def _mounts() -> list[str]:
    """The container-side paths docker-compose mounts under /opt/project."""
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(
        encoding="utf-8"))
    found = []
    for section in compose.values():
        if not isinstance(section, dict):
            continue
        for volume in section.get("volumes", []) or []:
            if not isinstance(volume, str) or ":" not in volume:
                continue
            target = volume.split(":")[1]
            if target.startswith(CONTAINER_ROOT):
                found.append(target)
    return found


def _declared():
    governance = _governance()
    for section, key in KEYS:
        yield "%s.%s" % (section, key), str(governance[section][key])


def test_the_compose_file_mounts_something_under_the_project_root():
    """A guard on the reader itself: a test that found no mount at all would
    pass every assertion below for the wrong reason."""
    assert len(_mounts()) >= 4


@pytest.mark.parametrize("key,value", list(_declared()))
def test_every_declared_path_is_mounted(key, value):
    """A path declared in governance.yml and not mounted writes into the image
    -- or, under /opt/project, fails to write at all."""
    assert not Path(value).is_absolute(), (
        "%s is absolute (%s): a declared path is relative to the project, or "
        "no mount can cover it" % (key, value))
    target = "%s/%s" % (CONTAINER_ROOT, value)
    covered = [mount for mount in _mounts()
               if target == mount or target.startswith(mount + "/")]
    assert covered, (
        "%s = %r resolves to %s in the container, which docker-compose.yml "
        "does not mount. Declared is not mounted: add "
        "- ./%s:%s to the airflow volumes."
        % (key, value, target, value.split("/")[0],
           "%s/%s" % (CONTAINER_ROOT, value.split("/")[0])))


def test_the_reports_directory_is_versioned():
    """SO THE BIND MOUNT NEVER CREATES IT. Docker creates a missing host side
    itself, and what it creates belongs to root -- which on a Linux engine
    reproduces exactly the permission error this directory was mounted to fix.
    A file inside it makes the directory exist in a fresh clone.

    The other declared paths are NOT held to this. ws/ is versioned because it
    carries the plans, and datasets/ is generated and legitimately absent from a
    fresh clone: asserting their presence failed in a code-only checkout, which
    is a test crying wolf rather than finding anything. The mount is the
    property worth holding; its host side existing is a precaution, and it is
    taken where it costs one file.
    """
    assert (ROOT / "runtime" / "reports").is_dir()
    assert any((ROOT / "runtime" / "reports").iterdir()), (
        "runtime/reports/ est vide, donc absent d'un depot frais : Docker le "
        "creerait lui-meme, en root")


def test_the_report_root_is_mounted_and_that_is_the_defect_of_P2():
    """Named on its own, because the general test above would pass again the
    day somebody removes the line for another reason."""
    # The mount is runtime/ whole: what has to hold is that the declared path
    # is COVERED by a mount, which is what test_every_declared_path_is_mounted
    # checks. Named here too, because the general test would pass again the day
    # somebody removes the line for another reason.
    assert "/opt/project/runtime" in _mounts()
    assert (ROOT / "runtime" / "reports").is_dir()


def test_the_writer_names_the_mount_when_it_cannot_write(tmp_path):
    """A bare "Permission denied" names neither the declaration nor the mount.
    This failure cost a full round trip; the next one will not."""
    import types

    from ml4fd import report

    blocked = tmp_path / "blocked"
    blocked.write_text("not a directory", encoding="utf-8")
    settings = types.SimpleNamespace(report_root=str(blocked / "reports"))
    import datetime as dt
    with pytest.raises(OSError, match="docker-compose"):
        report.write(settings, dt.date(2026, 11, 16), "# essai\n")


# --- the move under runtime/ (S6/R13) -------------------------------------------

MIGRATION = ROOT / "sql" / "maintenance" / "050_move_runtime_paths.sql"


def test_every_produced_path_is_declared_under_runtime():
    """ONE directory holds what the stack produces, so one mount covers it.
    A declared path that escaped runtime/ would need a mount of its own, and
    the three directories this project lost were each missing exactly that."""
    for key, value in _declared():
        assert value.startswith("runtime/"), "%s = %r" % (key, value)


def test_the_recorded_paths_are_carried_across_the_move():
    """fd_reports.path and fd_datasets.*_path name files on disk. The files
    moved with their directory; the strings did not, and the dashboard reports
    a document whose recorded path differs from the computed one."""
    code = MIGRATION.read_text(encoding="utf-8")
    for table in ("fd_reports", "fd_datasets"):
        assert "UPDATE %s" % table in code, table
    # A PREFIX REWRITE: only what sits at the old location, so a path recorded
    # elsewhere is not silently relocated.
    assert code.count("LIKE '/opt/project/reports/%'") == 1
    assert "LIKE '/opt/project/datasets/%'" in code
    assert "/opt/project/runtime/reports/" in code
    assert "/opt/project/runtime/datasets/" in code


def test_init_applies_the_migration_with_the_ddl():
    """The layout change and the rewrite are one operation: a stack whose
    directories moved without its rows points at files that are not there."""
    manager = (ROOT / "fd-manager.ps1").read_text(encoding="utf-8")
    assert "sql/maintenance/050_move_runtime_paths.sql" in manager


def test_no_container_mounts_the_plans_or_the_project_root():
    """ws/ HOLDS WHAT IS WRITTEN BY HAND -- plans, mockups -- and no container
    reads it since N0's measurement moved under runtime/observations/ (R13).

    The project root is named too: mounting it read-only to let psql read a
    file by path also put .env inside that container, with the pepper and the
    dashboard's password. Every use is /work/sql/..., so sql/ alone is mounted.
    """
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(
        encoding="utf-8"))
    for name, service in compose["services"].items():
        for volume in service.get("volumes", []) or []:
            if not isinstance(volume, str):
                continue
            source = volume.split(":")[0]
            assert source not in ("./ws", "./", "."), "%s mounts %s" % (name, source)


def test_the_measurement_of_n0_is_written_where_runtime_covers_it():
    """Five minutes of real API calls were once lost to a directory that was
    not mounted. The first candidate must sit under the one mount that is."""
    code = (ROOT / "tools" / "observe_api.py").read_text(encoding="utf-8")
    first = code.split("OUT_CANDIDATES = (")[1].split(",")[0]
    assert '"runtime" / "observations"' in first, first
