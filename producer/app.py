"""The real-time payments producer: the exercise's API, cached and windowed.

Covers: EE02, EC25, EC39   Lot: L4   Competency: C2, C6

DERIVED FROM vendor/realtime-transactions-producer/app.py, which is kept
untouched beside it as the pristine reference. The skeleton is theirs -- the
FastAPI route, the slowapi limiter, the df.sample(), and above all the
df.to_json(orient="split") that produces the response. That last one matters
more than it looks: the shape is not reproduced here, it is PRODUCED BY THE
SAME CALL, so there is nothing left to verify about double encoding, about the
index travelling outside the columns, or about current_time landing in epoch
milliseconds. pandas decides all three, as it did for the original.

THREE DEFECTS OF THE ORIGINAL ARE REPAIRED, and each one had a consequence:

  1. IT RE-READ THE CSV ON EVERY CALL, from the program's public mirror over
     the network. That is where the +5.1 s median clock gap N0 measured comes
     from -- not latency of the network but a 150 MB read per request -- and at
     five calls a minute it is some 750 MB a minute of somebody else's egress.
     Here the corpus is read ONCE, at startup, from the local cache.

  2. IT SAMPLED THE WHOLE FILE, so every call drew from the full June-to-
     December range at once and the periods averaged together. The drift the
     data actually holds -- a base rate falling from 0.4425 % to 0.1849 % --
     was therefore invisible, and C6 had nothing to find. Here the served
     window is declared: PRODUCER_SKIP_ROWS and PRODUCER_MAX_ROWS.

  3. ITS "/" ROUTE ANSWERED 500, because it mounts Jinja2Templates on
     "templates" while shipping "template". Fixed by naming the directory
     correctly rather than by a volume alias.

AND ONE DEFECT LEFT ALONE, deliberately: the original's
~df.columns.isin([..., "fraud", ...]) is a typo for is_fraud, so the label was
never dropped from the response. It is not dropped here either. The API this
project was built against leaks the verdict, N6 was designed around that fact,
and check_leak exists to prove the model does not read it. Removing the leak
now would retire a control that earns its place.

WHAT IT DOES NOT REPRODUCE: the failures. N0 measured about 5 % of calls to the
hosted Space failing -- 2 of 60 with a 502, 1 with a 429 -- and those were the
Space's own cold starts and the S3 read timing out. Both causes are gone with
the cache. The rate limit is kept and is real; the 502 is not simulated,
because faking a failure is inventing data. ops.freshness_minutes and
ops.max_empty_run_share therefore sit green while this serves.

IT FETCHES ITS OWN CORPUS, through ml4fd.fetch.fetch_csv -- the same function
S1 uses, not a second downloader. So the cache, the .part file, the rename and
the sha256 sidecar are one discipline across the project, and the key it
resolves comes from governance.yml's sources.replay_csv rather than from a
second declaration here. The credentials are boto3's business: this module
names none of them.

The first start on a cold cache pulls 150 MB and the service will look like it
is hanging. fetch_csv logs every 32 MiB; after that the cache answers.

Settings, all from the environment so the service needs no file of its own:
    PRODUCER_CORPUS      object key or file name, overriding governance.yml
    PRODUCER_CACHE       where the corpus is cached (default .local_cache)
    PRODUCER_SKIP_ROWS   first row of the served window (default 0)
    PRODUCER_MAX_ROWS    rows held (default 100000, ~14 days of stream)
    PRODUCER_RATE_LIMIT  slowapi rule, "13/minute" as MEASURED; "" disables
    PRODUCER_DRIFT_FILE  the drift switch (default /opt/project/runtime/drift.json)
    PRODUCER_GOVERNANCE  governance.yml, read once for validation.max_amount
"""
import json
import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import uvicorn
import yaml
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-7s %(message)s")
log = logging.getLogger("producer")

CACHE = Path(os.environ.get("PRODUCER_CACHE", "/opt/project/.local_cache"))
CORPUS = os.environ.get("PRODUCER_CORPUS", "").strip()
SKIP_ROWS = int(os.environ.get("PRODUCER_SKIP_ROWS") or 0)
MAX_ROWS = int(os.environ.get("PRODUCER_MAX_ROWS") or 100_000)
# THE MEASURED CEILING AND NOT THE DECLARED ONE. The original's source says
# 5/minute; N0 measured the hosted Space letting twelve through and refusing
# the thirteenth, never below. governance.realtime calibrated the cadence on
# that 13 and check_s3 made it a criterion, so a stand-in that enforces the
# declared rule is STRICTER than what it stands in for -- and the whole point
# of this service is that no module of the ingestion can tell the difference.
# Set to 5 it killed the ingestion outright (2026-11-19 23:41 UTC): five calls
# each retried once on a 429 is up to ten requests a minute against a budget
# of five, and the overflow carried into the next run, refused on its FIRST
# call at 0.0 s. Empty disables the limiter, but then nothing in the project
# can reach EC25's 429 path again.
RATE_LIMIT = os.environ.get("PRODUCER_RATE_LIMIT", "13/minute").strip()
DRIFT_FILE = Path(os.environ.get("PRODUCER_DRIFT_FILE",
                                 "/opt/project/runtime/drift.json"))
GOVERNANCE = Path(os.environ.get("PRODUCER_GOVERNANCE",
                                 "/opt/project/governance.yml"))

# The CSV's own time columns. The stream carries current_time instead, and
# checks.STREAM_PROFILE declares has_two_time_columns=False.
DROPPED = ["trans_date_trans_time", "unix_time"]


def governance(path: Path) -> dict:
    """governance.yml, read once. An empty dict when it cannot be read.

    Not fatal: this service's job is to answer, and both values it takes from
    here have a usable default. What is NOT silent is which default applied --
    both branches say so in the log.
    """
    try:
        with path.open(encoding="utf-8") as handle:
            return yaml.safe_load(handle) or {}
    except Exception as problem:            # noqa: BLE001
        log.warning("governance.yml unreadable (%s)", problem)
        return {}


def amount_cap(declared: dict) -> float:
    """validation.max_amount, so a drifted amount stays inside the bound.

    DECLARED AND NOT WRITTEN HERE: above that bound the transactions go to
    QUARANTINE, and the demonstration would then show a data_quality alert
    where it meant to show an input_drift one -- the wrong control, for the
    right reason.
    """
    try:
        return float(declared["validation"]["max_amount"])
    except (KeyError, TypeError, ValueError):
        log.warning("validation.max_amount not declared: cap at 50000")
        return 50000.0


def corpus(declared: dict) -> Path:
    """The corpus, from the cache or downloaded into it.

    THROUGH ml4fd.fetch AND NOT A SECOND DOWNLOADER. fetch_csv resolves an
    object key against ML4FD_CORPUS_BUCKET with boto3, an http(s) URL with
    requests, and a bare file name against the cache alone -- then caches the
    file with its sha256 whichever way it arrived. One acquisition path for the
    whole project, so what the dossier says about the corpus is true of both
    the training file and the stream.

    sources.replay_csv AND NOT sources.historical_csv: this serves what the
    model was NOT trained on. Serving the training corpus would hand the model
    its own memory, and every figure C6 measures would be flattering and
    meaningless.
    """
    from ml4fd import fetch

    key = CORPUS or (declared.get("sources") or {}).get("replay_csv") or ""
    if not key:
        raise RuntimeError(
            "no corpus to serve: set PRODUCER_CORPUS, or declare "
            "sources.replay_csv in governance.yml.")
    try:
        return fetch.fetch_csv(key, CACHE)
    except PermissionError as problem:
        raise RuntimeError(
            "%s is not writable by this container (it runs unprivileged, uid "
            "10001). Either place the file there from the host, or make the "
            "directory writable." % CACHE) from problem


def load_window(path: Path, skip: int, rows: int) -> pd.DataFrame:
    """The served window, read once.

    index_col=[0] IS THE WHOLE RESPONSE SHAPE. It makes the unnamed first
    column the frame's index, which is what sends it to the payload's "index"
    key instead of among the columns -- exactly as the original did, and what
    stream.decode reads it back from.

    DEFAULT DTYPES, as the original: cc_num and zip come back as integers and
    the response carries numbers where the CSV had text. The ingestion already
    copes -- stream.decode parses with parse_float=str -- and matching the
    original matters more here than tidiness, because this is the shape the
    whole chain was measured against.
    """
    frame = pd.read_csv(path, header=0, index_col=[0],
                        skiprows=range(1, skip + 1) if skip else None,
                        nrows=rows)
    frame = frame.loc[:, ~frame.columns.isin(DROPPED)]
    log.info("%d rows of %s held from row %d, %d columns: %s",
             len(frame), path.name, skip, len(frame.columns),
             ", ".join(frame.columns))
    return frame


def drift_factor(path: Path) -> float:
    """The amount multiplier in force, read on EVERY call.

    A missing file, an unreadable one, or a factor of 1 all mean no drift: an
    operator who deleted the switch meant to stop the drift, and a producer
    whose job is to answer must not fail on its absence.
    """
    try:
        value = float(json.loads(path.read_text(encoding="utf-8"))["factor"])
    except Exception:                       # noqa: BLE001
        return 1.0
    return value if value > 0 else 1.0


description = """
The real-time payments producer of this project, derived from the Jedha Lead
program's own API and cached. One endpoint matters:

* `/current-transactions`: **GET**, one current transaction in
  `.to_json(orient="split")` form.

Rate limited, as the original was. The served window and the amount drift are
declared in the environment and in runtime/drift.json.
"""

app = FastAPI(title="ML4FD - Real-time payments producer",
              description=description, version="1.0")

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

DECLARED = governance(GOVERNANCE)
FRAME = load_window(corpus(DECLARED), SKIP_ROWS, MAX_ROWS)
CAP = amount_cap(DECLARED)
SERVED = {"calls": 0}
log.info("cap %.2f, rate limit %r, drift switch %s",
         CAP, RATE_LIMIT or "none", DRIFT_FILE)


@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


def one_transaction() -> str:
    """One row, drifted if the switch says so, stamped now. Returns the body."""
    row = FRAME.sample().copy()
    factor = drift_factor(DRIFT_FILE)
    if factor != 1.0:
        # CAPPED AT THE DECLARED BOUND and not at the factor: above
        # validation.max_amount the transaction is quarantined, and the run
        # would measure the quality control instead of the drift.
        row["amt"] = min(float(row["amt"].iloc[0]) * factor, CAP - 0.01)
    # datetime.now() EXACTLY AS THE ORIGINAL: naive, so pandas serialises it as
    # epoch milliseconds. An aware timestamp would serialise differently and
    # Reading.clock_gap_s is read off this value.
    row["current_time"] = datetime.now()
    SERVED["calls"] += 1
    # NO ROW IN THE LOG: it carries the cardholder's name, street and date of
    # birth, which governance.yml lists as personal data. A log is a
    # destination like any other.
    log.info("call %d served%s", SERVED["calls"],
             ", amount drifted x%.2f" % factor if factor != 1.0 else "")
    return row.to_json(orient="split")


if RATE_LIMIT:
    @app.get("/current-transactions")
    @limiter.limit(RATE_LIMIT)
    async def current_transactions(request: Request):
        """One current transaction, rate limited as the original is."""
        return one_transaction()
else:
    @app.get("/current-transactions")
    async def current_transactions(request: Request):   # noqa: F811
        """One current transaction, with the limiter disabled."""
        return one_transaction()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT") or 4000))
