"""Acquisition of the historical CSV.

Block: B3, B4.
Lot: B3-L1, B4-L0.
Competency: B3-C2.
Covers: B3-EI04, B3-EI05.

Local cache with a checksum: re-downloading 143 MB on every debugging attempt is
slow, and makes attempts irreproducible if the source moves. The checksum is not
hard-coded -- we have no authoritative value for it -- so the first download
records it in a sidecar file and later runs verify against that. An unexpected
change is then reported rather than ingested silently.

THREE WAYS TO THE SAME CACHE, decided by what governance.yml declares and by
what .env holds. One function, because the cache, the .part file, the rename
and the checksum are the same discipline whichever way the bytes arrive:

  an http(s) URL      fetched with requests, as the program's public mirror was.
  an S3 OBJECT KEY    fetched with boto3 from ML4FD_CORPUS_BUCKET. This is the
                      case since 2026-11-19: both corpora moved to a private
                      bucket, and the declaration is the KEY while the bucket
                      and the credentials live in .env. The bucket name carries
                      an AWS account number, which has no business in a
                      versioned file; the credentials have no business there
                      either, and never in a comment.
  a bare file name    the cache and only the cache, for a file placed by hand.

WHY THE CREDENTIALS ARE NOT READ HERE. boto3 finds AWS_ACCESS_KEY_ID,
AWS_SECRET_ACCESS_KEY and AWS_DEFAULT_REGION in the environment by itself, so
this module never names them, never logs them, and cannot put one in an error
message. A secret this file does not touch is a secret it cannot leak.
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

    # A BARE FILE NAME MEANS THE CACHE, AND ONLY THE CACHE. Both corpora now
    # live in a private bucket, so governance.yml declares the file's NAME --
    # which is what names the cached file -- and there is no URL to fetch.
    # Refused here, with the remedy, rather than left to requests.get: that
    # would raise MissingSchema, an error about a URL when the problem is a
    # missing file. force is ignored, since there is nowhere to download from.
    bucket = os.environ.get("ML4FD_CORPUS_BUCKET", "").strip()
    is_url = "://" in url
    is_key = not is_url and bool(bucket)
    cache_only = not is_url and not is_key
    if cache_only and not target.is_file():
        raise FileNotFoundError(
            "%r names neither a URL nor an object of a declared bucket, and "
            "it is not in the cache (%s). Either set ML4FD_CORPUS_BUCKET in "
            ".env with AWS credentials beside it, or download the file and "
            "place it there by hand." % (url, target))

    if target.is_file() and (cache_only or not force):
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
    if is_key:
        announced, written = _download_s3(bucket, url, part)
    else:
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


def _download_s3(bucket: str, key: str, part: Path) -> tuple[int, int]:
    """Download one object to `part`. Returns (announced, written).

    head_object FIRST, so the announced size is known before a byte is read
    and the truncation check below has something to compare against -- the
    same guarantee Content-Length gives on the http path.

    THE ERROR IS TRANSLATED, because boto3's own is unhelpful here: a wrong key
    and a missing object both surface as a ClientError with a code, and an
    operator reading "An error occurred (403)" cannot tell a typo in the key
    from a credential that expired.
    """
    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as problem:          # pragma: no cover
        raise RuntimeError(
            "boto3 is needed to read s3://%s/%s and is not installed. It is "
            "declared in airflow/requirements.txt; rebuild the image, or "
            "download the file and place it in the cache by hand."
            % (bucket, key)) from problem

    client = boto3.client("s3")
    log.info("downloading s3://%s/%s", bucket, key)
    try:
        head = client.head_object(Bucket=bucket, Key=key)
        announced = int(head.get("ContentLength") or 0)
        written = 0

        def progress(chunk: int) -> None:
            nonlocal written
            before = written
            written += chunk
            if written >> 25 != before >> 25:       # every 32 MiB
                log.info("  %d MiB received", written >> 20)

        with io.open(part, "wb") as fh:
            client.download_fileobj(bucket, key, fh, Callback=progress)
    except ClientError as problem:
        part.unlink(missing_ok=True)
        code = problem.response.get("Error", {}).get("Code", "?")
        raise IOError(
            "s3://%s/%s refused with %s. 404 or NoSuchKey: the key is wrong "
            "or the object is elsewhere. 403 or AccessDenied: the credentials "
            "in .env are wrong, expired, or lack s3:GetObject on this object. "
            "The bucket comes from ML4FD_CORPUS_BUCKET."
            % (bucket, key, code)) from problem
    except BotoCoreError as problem:
        part.unlink(missing_ok=True)
        raise IOError("s3://%s/%s unreachable: %s" % (bucket, key, problem)
                      ) from problem
    return announced, written


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
