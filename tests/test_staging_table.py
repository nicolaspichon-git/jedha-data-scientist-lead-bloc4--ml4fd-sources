"""A staging table usable any number of times in one transaction.

Block: B3, B4.
Lot: B3-N6, B4-L0.
Competency: B3-C2, B3-C5.
Covers: B3-EC23.

THE DEFECT OF 2026-11-17, in dag_ingest_verdicts: `relation
"tmp_fd_ground_truth" already exists`. The loaders created their staging table
ON COMMIT DROP -- gone at COMMIT, not at the end of the call -- and
verdicts.write_due() calls the loader once per batch inside ONE transaction.
With one batch awaiting a verdict, as every night until then, nothing showed;
the second batch of an afternoon did.

Reproduced on PostgreSQL 16: the old form fails on the second CREATE; the new
one runs three batches in one transaction, carries no row of a batch into the
next (TRUNCATE), and leaves no table behind after COMMIT.
"""
from __future__ import annotations

import os
import re
from pathlib import Path

import pytest

os.environ.setdefault("POSTGRES_USER", "test_user")
os.environ.setdefault("POSTGRES_PASSWORD", "test_password")
os.environ.setdefault("POSTGRES_DB", "test_db")

from ml4fd import db  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]


class Cursor:
    def __init__(self): self.sql = []
    def execute(self, sql, params=None): self.sql.append(sql)


def test_it_creates_only_if_absent_and_always_empties():
    cur = Cursor()
    db.staging_table(cur, "tmp_fd_ground_truth", "fd_ground_truth")
    db.staging_table(cur, "tmp_fd_ground_truth", "fd_ground_truth")
    creates = [s for s in cur.sql if s.startswith("CREATE")]
    assert all("IF NOT EXISTS" in s and "ON COMMIT DROP" in s for s in creates)
    assert cur.sql.count("TRUNCATE tmp_fd_ground_truth") == 2, (
        "a second batch must not see the first one's rows")


def test_an_identifier_is_never_interpolated_unchecked():
    with pytest.raises(ValueError):
        db.staging_table(Cursor(), "tmp; DROP TABLE fd_transactions", "fd_x")


def test_no_loader_creates_its_own_temporary_table():
    """One definition: a loader that wrote its own CREATE TEMP TABLE would
    carry the old contract back -- callable once per transaction."""
    offenders = []
    for path in (ROOT / "ml4fd").glob("*.py"):
        if path.name == "db.py":
            continue
        if re.search(r"CREATE\s+TEMP", path.read_text(encoding="utf-8"), re.I):
            offenders.append(path.name)
    assert offenders == [], offenders


def test_both_loaders_use_it():
    for name, table in (("truth.py", "tmp_fd_ground_truth"),
                        ("transform.py", "tmp_fd_transactions")):
        text = (ROOT / "ml4fd" / name).read_text(encoding="utf-8")
        assert 'db.staging_table(cur, "%s"' % table in text, name
