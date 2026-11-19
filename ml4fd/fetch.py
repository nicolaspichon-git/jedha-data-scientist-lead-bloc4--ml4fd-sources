"""Acquisition of the historical CSV.

Covers: EI04, EI05
Lot: L1
Competency: C2

Local cache with a checksum: re-downloading 143 MB on every debugging attempt is
slow, and makes attempts irreproducible if the source moves. The checksum is not
hard-coded -- we have no authoritative value for it -- so the first download
records it in a sidecar file and later runs verify against that. An unexpected
change is then reported rather than ingested silently.
"""
from __future__ import annotations

import csv
import hashlib
import io
import logging
import os
from pathlib import Path
from typing import Iterator

import requests

log = logging.getLogger(__name__)

CHUNK = 1 << 20          # 1 MiB
# The CSV's first column has an empty header: it is pandas' index from the export.
# Keeping it costs nothing and gives replay a stable ordinal from the source.
SOURCE_LINE = "source_line"


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with io.open(path, "rb") as fh:
        for block in iter(lambda: fh.read(CHUNK), b""):
            h.update(block)
    return h.hexdigest()


def fetch_csv(url: str, dest_dir: Path, force: bool = False) -> Path:
    """Download the CSV unless it is already cached, then verify its checksum.

    Returns the local path. Logs which of the two paths was taken, so the
    demonstration can show either one.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    target = dest_dir / url.rsplit("/", 1)[-1]
    sidecar = target.with_suffix(target.suffix + ".sha256")

    if target.is_file() and not force:
        digest = sha256_of(target)
        log.info("cache hit: %s (%d bytes, sha256=%s)",
                 target, target.stat().st_size, digest)
        if sidecar.is_file():
            expected = sidecar.read_text(encoding="utf-8").split()[0]
            if digest != expected:
                raise ValueError(
                    "the cached file no longer matches its recorded checksum.\n"
                    "  recorded: %s\n  measured: %s\n"
                    "Delete %s to download it again." % (expected, digest, target)
                )
            log.info("checksum matches the recorded value")
        else:
            sidecar.write_text(digest + "  " + target.name + "\n", encoding="utf-8")
            log.info("checksum recorded in %s", sidecar.name)
        return target

    # Download to a .part file and rename once complete: an interrupted transfer
    # must not leave a truncated file that looks like a valid cache.
    part = target.with_suffix(target.suffix + ".part")
    log.info("downloading %s", url)
    with requests.get(url, stream=True, timeout=(10, 300)) as resp:
        resp.raise_for_status()
        announced = int(resp.headers.get("Content-Length") or 0)
        written = 0
        with io.open(part, "wb") as fh:
            for block in resp.iter_content(CHUNK):
                fh.write(block)
                written += len(block)
                if written % (32 * CHUNK) < CHUNK:
                    log.info("  %d MiB received", written >> 20)
    if announced and written != announced:
        part.unlink(missing_ok=True)
        raise IOError("truncated transfer: %d bytes received, %d announced"
                      % (written, announced))
    os.replace(part, target)

    digest = sha256_of(target)
    sidecar.write_text(digest + "  " + target.name + "\n", encoding="utf-8")
    log.info("downloaded: %s (%d bytes, sha256=%s)", target, written, digest)
    return target


def read_rows(path: Path) -> Iterator[dict[str, str]]:
    """Yield one dict per CSV line, **every value as text**.

    cc_num holds up to 19 digits: read as a number it loses its leading zeros
    and exceeds the exact range of a double, which also destroys its Luhn
    validity. The raw zone keeps what was received, not what was interpreted, so
    no conversion happens here -- typing is L3's business.
    """
    with io.open(path, encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        if not header[0]:
            header[0] = SOURCE_LINE
        width = len(header)
        for row in reader:
            if len(row) != width:
                # Not rejected here: the raw zone takes everything, and L2 is
                # what decides the fate of a malformed record (EC22).
                row = (row + [""] * width)[:width]
            yield dict(zip(header, row))
