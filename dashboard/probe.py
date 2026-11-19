"""What the dashboard container IS, seen from inside it. Prints JSON.

Covers: EC19, EC27
Lot: R5
Competency: C4, C5

Run by tools/check_s6.py on the host:
    docker compose exec -T dashboard python /opt/project/dashboard/probe.py

`docker inspect` tells what the container was GIVEN. Some things only the
container itself can answer: who PostgreSQL thinks the session is, whether a
write into reports/ is really refused, whether the document this container
reads is the one Airflow wrote. This file answers those, and nothing else.

IT PRINTS NO SECRET. Variable NAMES are reported, never their values -- with
the one exception of POSTGRES_USER, a role name. It lives in dashboard/ because
that is what the container mounts; tools/ is deliberately not mounted there.
"""
from __future__ import annotations

import errno
import json
import os
import urllib.request
from pathlib import Path

REPORTS = Path("/opt/project/runtime/reports")
HEALTH = "http://localhost:8501/_stcore/health"


def _write_refused(directory: Path) -> str:
    """Try to create a file. Read-only must be what the KERNEL says.

    A MISSING DIRECTORY IS NOT A REFUSAL. Writing into it fails too, with
    ENOENT, and counting that as "refused" would report a missing mount -- the
    very defect this is meant to catch -- as a success.
    """
    if not directory.is_dir():
        return "ABSENT"
    target = directory / ".probe-write-test"
    try:
        target.write_text("x", encoding="utf-8")
    except OSError as problem:
        return "refusee (%s)" % errno.errorcode.get(problem.errno, problem.errno)
    target.unlink(missing_ok=True)
    return "ACCEPTEE"


def probe() -> dict:
    out = {"uid": os.getuid(),
           "env_names": sorted(os.environ),
           "postgres_user": os.environ.get("POSTGRES_USER"),
           "reports_write": _write_refused(REPORTS),
           "ml4fd_write": _write_refused(Path("/opt/project/ml4fd"))}

    try:
        from ml4fd import config, dashboard
        settings = config.load()
        with dashboard.connect(settings) as conn:
            out["session_user"] = dashboard.assert_identity(conn)
            rows = dashboard.reports(conn, limit=1)
        if rows:
            m = dashboard.morning(settings, rows[0])
            out["latest_report"] = {"day": str(m.day),
                                    "verdict": m.check.verdict,
                                    "relocated": m.relocated}
        else:
            out["latest_report"] = None
    except Exception as problem:                 # reported, not raised
        out["session_error"] = "%s: %s" % (type(problem).__name__, problem)

    try:
        with urllib.request.urlopen(HEALTH, timeout=4) as answer:
            out["health"] = answer.read().decode("utf-8").strip()
    except Exception as problem:
        out["health"] = "injoignable (%s)" % type(problem).__name__
    return out


if __name__ == "__main__":
    print(json.dumps(probe(), ensure_ascii=True))
