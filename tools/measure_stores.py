#!/usr/bin/env python3
"""Measure the typology and the storage volume of every store, and write them up.

    python tools/measure_stores.py            # writes TYPOLOGY.md
    python tools/measure_stores.py --print    # to standard output only

Covers: EC01, EC03
Lot: L8
Competency: C1

Why a script rather than a written table: EC01 asks for the typology of each
source and each store, and EC03 for the storage volume per store in steady
state. Neither can be honestly answered before the data is loaded, and a figure
typed once drifts the moment a column is added. Measured, they cannot.

The source is read from the CSV, the stores from the live database. Needs the
database up and loaded; run it after .\\fd-manager.ps1 load.

Code and comments in English; the generated document stays in French, since it
is read alongside D0 and D1.
"""
from __future__ import annotations

import argparse
import csv
import io
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from ml4fd import config, db           # noqa: E402

COLUMN_SQL = """
SELECT c.table_name, c.ordinal_position, c.column_name, c.data_type,
       c.numeric_precision, c.numeric_scale, c.character_maximum_length,
       c.is_nullable
FROM   information_schema.columns c
WHERE  c.table_schema = 'public' AND c.table_name LIKE 'fd\\_%'
ORDER BY c.table_name, c.ordinal_position
"""

SIZE_SQL = """
SELECT c.relname,
       pg_total_relation_size(c.oid)  AS total_bytes,
       pg_indexes_size(c.oid)         AS index_bytes
FROM   pg_class c
JOIN   pg_namespace n ON n.oid = c.relnamespace
WHERE  n.nspname = 'public' AND c.relkind = 'r' AND c.relname LIKE 'fd\\_%'
ORDER BY pg_total_relation_size(c.oid) DESC
"""

TYPE_LABEL = {
    "character varying": "varchar", "timestamp with time zone": "timestamptz",
    "double precision": "float8", "bigint": "int8", "integer": "int4",
}


def human(n: int) -> str:
    for unit in ("o", "Kio", "Mio", "Gio"):
        if n < 1024 or unit == "Gio":
            return "%.1f %s" % (n, unit) if unit != "o" else "%d o" % n
        n /= 1024.0
    return "%d o" % n


def source_typology(path: Path) -> list[tuple]:
    with io.open(path, encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        first = next(reader)
        rows = 2 + sum(1 for _ in reader)
    header = ["(sans nom)" if not h else h for h in header]
    return list(zip(range(1, len(header) + 1), header, first)), rows, path.stat().st_size


def render(settings, source, stores, columns) -> str:
    rows_in_source, csv_bytes, sample = source
    out = ["# Typologie et volumétrie des magasins", "",
           "> Document **généré** par `tools/measure_stores.py`, par la mesure et non "
           "par estimation. Ne pas éditer à la main.",
           "> Couvre **EC01** (typologie de chaque source et de chaque magasin) et "
           "**EC03** (volumétrie de stockage par magasin).", "",
           "## 1. La source", "",
           "| | |", "| --- | --- |",
           "| Fichier | `.local_cache/fraudTest.csv` \\[R3\\] |",
           "| Format | CSV, séparateur virgule, en-tête sur la première ligne |",
           "| Encodage | UTF-8 |",
           "| Lignes de données | **%s** |" % format(rows_in_source, ",").replace(",", " "),
           "| Taille | **%s** |" % human(csv_bytes),
           "| Colonnes | %d |" % len(sample),
           "| Encodage temporel | `trans_date_trans_time` naïf, lu en **UTC** "
           "(hypothèse `checks.SOURCE_TZ`) ; `unix_time` en epoch, **écarté** (H5) |",
           "", "### 1.1. Colonnes de la source", "",
           "| # | Colonne | Exemple |", "| --- | --- | --- |"]
    for pos, name, value in sample:
        out.append("| %d | `%s` | `%s` |" % (pos, name, value[:30]))

    out += ["", "## 2. Les magasins", "",
            "| Magasin | Lignes | Taille totale | Dont index | Octets/ligne |",
            "| --- | --- | --- | --- | --- |"]
    total_bytes = 0
    for name, rows, total, idx, _table in stores:
        total_bytes += total
        per_row = "%.0f o" % (total / rows) if rows else "—"
        out.append("| `%s` | %s | **%s** | %s | %s |"
                   % (name, format(rows, ",").replace(",", " "),
                      human(total), human(idx), per_row))
    out.append("| **Total** | | **%s** | | |" % human(total_bytes))

    out += ["", "### 2.1. Volumétrie en régime établi", "",
            "La volumétrie ci-dessus est celle de l'amorçage. En régime établi, "
            "le débit attendu est de **7 200 transactions par jour** et la durée de "
            "conservation de la zone brute est de **%d jours** "
            "(`retention.raw_events_days`)." % settings.raw_retention_days, ""]
    raw = next((s for s in stores if s[0] == "fd_raw_events"), None)
    if raw and raw[1]:
        per = raw[2] / raw[1]
        steady = per * 7200 * settings.raw_retention_days
        out += ["Mesuré : **%.0f octets par ligne** dans `fd_raw_events`. "
                "En régime établi, la zone brute se stabilise donc autour de "
                "**%s** (7 200 × %d × %.0f o), et non de la taille de l'amorçage."
                % (per, human(steady), settings.raw_retention_days, per), "",
                "> La couche exploitée, elle, n'est pas purgée : elle croît de "
                "7 200 lignes par jour sans borne dans le périmètre actuel.", ""]

    out += ["## 3. Schéma de chaque magasin", ""]
    current = None
    for table, _pos, col, dtype, prec, scale, maxlen, nullable in columns:
        if table != current:
            current = table
            out += ["", "### `%s`" % table, "",
                    "| Colonne | Type | Nullable |", "| --- | --- | --- |"]
        label = TYPE_LABEL.get(dtype, dtype)
        if prec is not None and scale is not None:
            label = "%s(%d,%d)" % (label, prec, scale)
        elif maxlen:
            label = "%s(%d)" % (label, maxlen)
        out.append("| `%s` | `%s` | %s |" % (col, label,
                                             "oui" if nullable == "YES" else "non"))
    out.append("")
    return "\n".join(out)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--print", action="store_true", dest="to_stdout")
    args = ap.parse_args()

    settings = config.load()
    csv_path = ROOT / "data" / "fraudTest.csv"
    if not csv_path.is_file():
        print("%s not found: run the load first" % csv_path, file=sys.stderr)
        return 1
    sample, rows_in_source, csv_bytes = source_typology(csv_path)

    # fd_ingest and not fd_report, deliberately: fd_report has NO access to
    # fd_raw_events, because the raw payloads still carry names, addresses and
    # cardholder numbers. Granting the reporting role access to measure a row
    # count would undo what EC17 is for. The measurement reads counts and sizes,
    # never contents.
    with db.connect("fd_ingest", settings) as conn:
        with conn.cursor() as cur:
            cur.execute(SIZE_SQL)
            sizes = {name: (total, idx) for name, total, idx in cur.fetchall()}
            stores = []
            for name in sorted(sizes, key=lambda n: -sizes[n][0]):
                cur.execute("SELECT count(*) FROM %s" % name)
                rows = cur.fetchone()[0]
                total, idx = sizes[name]
                stores.append((name, rows, total, idx, total - idx))
            cur.execute(COLUMN_SQL)
            columns = cur.fetchall()

    text = render(settings, (rows_in_source, csv_bytes, sample), stores, columns)
    if args.to_stdout:
        print(text)
    else:
        (ROOT / "TYPOLOGY.md").write_text(text, encoding="utf-8")
        print("TYPOLOGY.md written: %d store(s), %d column(s) described"
              % (len(stores), len(columns)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
