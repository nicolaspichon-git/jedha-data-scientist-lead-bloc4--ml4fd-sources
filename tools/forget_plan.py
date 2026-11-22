"""Can this batch be forgotten? READS ONLY -- it has no way to delete.

Block: B3, B4.
Lot: B3-Q7, B4-L0.
Competency: B3-C2, B3-C4.
Covers: B3-EC18, B3-EC23.

WHY A BATCH AND NOT A DATE. The retention purge cuts on an instant, and an
instant cannot separate the interrupted loads of a CSV from the load that
finished: they arrived minutes apart. Their run's outcome separates them, so
this boundary is a batch.

Its destructive counterpart is tools/forget_run.py, a file of its own, for the
reason tools/purge_run.py gives: a command that destroys must not be one flag
away from one that does not.

Usage, from the scheduler container:
    python3 /opt/project/tools/forget_plan.py <batch_id>
"""
from __future__ import annotations

import argparse
import uuid


def _batch(value: str) -> str:
    try:
        return str(uuid.UUID(value))
    except ValueError:
        raise SystemExit(
            "identifiant de lot attendu (uuid), lu %r.\n"
            "  Les lister :  .\\fd-manager.ps1 batches" % value)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("batch_id", help="le lot a examiner")
    args = parser.parse_args(argv)
    batch_id = _batch(args.batch_id)

    from ml4fd import config, db, purge, runs

    try:
        # fd_ingest, and neither fd_report nor fd_purge. fd_report cannot see
        # the raw zone at all; fd_purge, which deletes, holds no grant on
        # fd_transactions -- so the one role that can read every fact the
        # decision rests on is the role that WRITES these tables, and that may
        # delete none of them (the revocation of sql/ddl/060_roles_grants.sql).
        with db.connect("fd_ingest", config.load()) as conn:
            runs.assert_digests(conn)
            plan = purge.forget_plan(conn, batch_id)
    except (db.RoleMissing, runs.MigrationMissing) as missing:
        print(missing)
        return 1

    print(purge.render_forget(plan))
    print()
    if plan["forgettable"]:
        print("Rien n'a ete supprime. Pour le faire :")
        print("  .\\fd-manager.ps1 forget -Batch %s -Apply" % batch_id)
        return 0
    if plan["forgettable_with_force"]:
        print("Rien n'a ete supprime. En confirmant la precaution ci-dessus :")
        print("  .\\fd-manager.ps1 forget -Batch %s -Apply -Force" % batch_id)
        return 3
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
