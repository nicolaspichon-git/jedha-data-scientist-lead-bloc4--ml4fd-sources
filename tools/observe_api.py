"""N0 --- observe the real-time API before writing a line that depends on it.

Covers: EC25, EI14
Lot: N0
Competency: C5

WHY THIS EXISTS. The v01 of the S3 plan asserted that the stream carried no
label, from a reading of the Space's source that was accurate and an inference
that was wrong: the code drops a column named `fraud` while the CSV column is
named `is_fraud`, so the drop does nothing and the label comes through. One
observed response undid a page of reasoning. Every design decision in N1 to N8
now rests on a figure in the file this script writes.

WHAT IT NEVER WRITES. The response carries first, last, street, city, zip, dob,
job, gender and the card number. Writing those into a measurement file kept in
the repository would breach the project's own governance (EC17) more plainly
than any pipeline could. So this script records SHAPES, not values: field names,
lengths, types, counts. The one exception is the fields that identify nothing --
`index`, `trans_num`, `category`, `amt`, `is_fraud`, `current_time`.

ON PRECISION. The body is parsed with parse_int=str, so no integer is ever
turned into a float: 83 of the 924 cards exceed 2**53 and would silently lose
digits. That is not a precaution here, it is the measurement -- the raw text is
what lets us say whether a 19-digit card survives the wire.

Usage, from the scheduler container which has the network and the dependencies:

    python3 /opt/project/tools/observe_api.py            # 20 calls, 13 s apart
    python3 /opt/project/tools/observe_api.py \
        --endpoint http://producer:4000/current-transactions
    python3 /opt/project/tools/observe_api.py \
        --calls 60 --spacing 0 --burst 0                 # the limiter, tested
    python3 /opt/project/tools/observe_api.py \
        --from-log runtime/observations/n0-observations--<stamp>.jsonl
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import statistics
import sys
import types
import time
from collections import Counter
from pathlib import Path

import requests

# THE ADDRESS IS DECLARED, NOT CODED. This literal was the Hugging Face Space,
# and on 2026-11-19 it stopped answering while governance.yml had already been
# pointed at the local producer: the one script whose whole purpose is to
# measure what the ingestion will read was measuring something else. So the
# default now comes from the same line the ingestion reads,
# realtime.endpoint, and --endpoint overrides it for a one-off probe. The
# literal stays as the last resort, so a broken governance.yml cannot stop a
# measurement.
FALLBACK_ENDPOINT = ("https://sdacelo-real-time-fraud-detection.hf.space"
                     "/current-transactions")


def declared_endpoint() -> str:
    """realtime.endpoint, or the literal above if governance cannot be read."""
    try:
        from ml4fd import config
        return config.load().api_endpoint
    except Exception as problem:                              # noqa: BLE001
        print("governance.yml illisible (%s: %s) ; adresse de repli."
              % (type(problem).__name__, problem))
        return FALLBACK_ENDPOINT

# The documented limiter is 5 per minute per IP, so one call every 13 seconds
# stays under it with a margin that does not depend on clock synchronisation.
# It is a DEFAULT, not a constant: --spacing 0 runs back to back, which is how
# one finds out whether the limiter is enforced at all. Hard-coding it cost a
# 19-minute run that was announced as 6.
DEFAULT_SPACING_SECONDS = 13
TIMEOUT_SECONDS = 30

# THE SERVED WINDOW, MEASURED, and not the corpus. The producer holds rows
# PRODUCER_SKIP_ROWS .. + PRODUCER_MAX_ROWS of fraudTest.csv and samples that
# window, so the base rate a campaign draws against is the window's. Measured
# on 2026-11-20 over rows 0..99,999 (2020-06-21 to 2020-07-26): 402 frauds in
# 100,000 rows. The file as a whole sits at 0.3860 % and June alone at
# 0.4425 %, so quoting either here would be wrong by a tenth of a point, in
# the direction that flatters the sample. Re-measure with
# tools/profile_corpus.py if the window moves.
WINDOW_BASE_RATE = 0.004020

# The rate at which the INGESTION consumes the stream: realtime.schedule every
# minute x realtime.calls_per_run = 7,200 a day. It turns a number of draws
# into a duration. The spacing of this script is a property of the
# measurement and not of the pipeline, so quoting it would answer a question
# nobody asked.
INGESTION_PER_DAY = 7200.0

# Fields whose VALUES may be written to the measurement file. Everything else is
# reported by shape alone. cc_num is absent on purpose: its length and whether
# it survives the wire are measurements; its digits are not.
PUBLISHABLE = ("category", "amt", "is_fraud", "index", "trans_num",
               "current_time", "merchant")

IDENTIFYING = ("cc_num", "first", "last", "gender", "street", "city", "state",
               "zip", "lat", "long", "city_pop", "job", "dob")

# Where the report goes. runtime/observations/ is the right home: this is a
# PRODUCED file, like the documents and the datasets, and ws/ now carries only
# what is written by hand (S6/R13). A measurement that cost five minutes of
# real calls must not be lost to a mkdir, so the candidates are tried in order
# and the one used is announced.
OUT_CANDIDATES = (
    Path(__file__).resolve().parent.parent / "runtime" / "observations",
    Path(__file__).resolve().parent.parent / ".local_cache",
    Path("/tmp"),
)


# A measurement campaign must be ALONE. Three overlapping runs on 2026-11-16
# produced a 12 % error rate and a 429 that were attributed to the Space and
# were in fact our own load: latency, error rate and rate limiting are
# meaningless when another campaign runs from the same address.
LOCK_NAME = "observe_api.lock"
LOCK_STALE_SECONDS = 3600


def take_lock(out: Path, calls: int) -> Path:
    """Refuse to start while another campaign is running.

    A lock older than an hour is treated as abandoned -- a killed process must
    not block the tool for ever -- and the refusal names the running campaign.
    """
    lock = out / LOCK_NAME
    if lock.is_file():
        age = time.time() - lock.stat().st_mtime
        if age < LOCK_STALE_SECONDS:
            raise SystemExit(
                "UNE CAMPAGNE TOURNE DEJA (il y a %d min) :\n"
                "  %s\n  %s\n"
                "Deux campagnes simultanees depuis la meme adresse faussent la\n"
                "latence, le taux d erreur et le limiteur -- c est exactement ce\n"
                "qui s est produit le 16/11 avec trois campagnes superposees.\n"
                "Attendre la fin, ou supprimer le verrou si le processus est mort."
                % (age / 60, lock, lock.read_text(encoding="utf-8").strip()))
        print("Verrou abandonne depuis %d min, ignore." % (age / 60))
    lock.write_text("demarree %s, %d appels\n"
                    % (dt.datetime.now(dt.timezone.utc).isoformat(), calls),
                    encoding="utf-8")
    return lock


def writable_dir() -> Path:
    """The first candidate we can actually write to."""
    for candidate in OUT_CANDIDATES:
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            probe = candidate / ".observe_api_probe"
            probe.write_text("x", encoding="utf-8")
            probe.unlink()
            return candidate
        except OSError:
            continue
    raise OSError("no writable directory among: %s"
                  % ", ".join(str(c) for c in OUT_CANDIDATES))


def call(endpoint: str) -> dict:
    """One call. Returns what happened, never raising.

    The address travels in the record, so the raw log says what it measured
    and --from-log can name it instead of guessing.
    """
    started = time.monotonic()
    # OUR clock, in the same unit as current_time, taken at the moment of the
    # call. Without it there is nothing to compare the server's clock against,
    # and the first version of this script invented a drift of +122 s by
    # comparing an elapsed span with a span that forgot the latency.
    our_clock_ms = int(time.time() * 1000)
    try:
        response = requests.get(endpoint, timeout=TIMEOUT_SECONDS)
    except requests.RequestException as problem:
        return {"ok": False, "status": None, "latency": time.monotonic() - started,
                "our_clock_ms": our_clock_ms, "endpoint": endpoint,
                "error": "%s: %s" % (type(problem).__name__, problem)}

    latency = time.monotonic() - started
    record = {"ok": False, "status": response.status_code, "latency": latency,
              "our_clock_ms": our_clock_ms, "endpoint": endpoint,
              "headers": {k.lower(): v for k, v in response.headers.items()
                          if k.lower().startswith(("x-rate", "retry", "ratelimit"))},
              "bytes": len(response.content)}
    if response.status_code != 200:
        record["error"] = "HTTP %d" % response.status_code
        return record

    # THE DOUBLE DECODE, and parse_int=str so no integer becomes a float.
    try:
        outer = json.loads(response.text)
    except ValueError as problem:
        record["error"] = "outer JSON: %s" % problem
        return record

    if isinstance(outer, str):
        record["double_encoded"] = True
        try:
            inner = json.loads(outer, parse_int=str)
        except ValueError as problem:
            record["error"] = "inner JSON: %s" % problem
            return record
    else:
        # Worth knowing if it ever changes: a single decode would then be right.
        record["double_encoded"] = False
        inner = json.loads(response.text, parse_int=str)

    record["ok"] = True
    record["payload"] = inner
    return record


def shape(inner: dict) -> dict:
    """Describe one payload without copying what must not be copied."""
    columns = list(inner.get("columns") or [])
    rows = inner.get("data") or []
    index = inner.get("index") or []
    described = {"columns": columns, "n_columns": len(columns),
                 "n_rows": len(rows), "index": index,
                 "aligned": bool(rows) and len(rows[0]) == len(columns)}
    if not rows:
        return described

    values = dict(zip(columns, rows[0]))
    described["publishable"] = {k: values.get(k) for k in PUBLISHABLE
                                if k in values}
    described["shapes"] = {}
    for name in IDENTIFYING:
        if name not in values:
            continue
        raw = values[name]
        described["shapes"][name] = {"type": type(raw).__name__,
                                     "length": len(str(raw))}
    return described


def shape_of(record: dict) -> dict:
    """The shape of one observation, live or reread from a log.

    A live record carries the payload; a relogged one carries the shape that
    _append wrote. Refusing a record that has neither is the point: a silently
    empty shape would be reported as a measurement.
    """
    if "shape" in record:
        return record["shape"]
    if "payload" in record and record["payload"] is not None:
        return shape(record["payload"])
    raise ValueError("observation sans charge utile ni forme : %r"
                     % sorted(record))


def tally(counter) -> str:
    """A Counter rendered for a reader, not for a debugger.

    repr(Counter) leaked "Counter({True: 20})" into the first report. A
    measurement file is read by people.
    """
    if not counter:
        return "aucun"
    return ", ".join("%s : %d" % (k, v) for k, v in sorted(
        counter.items(), key=lambda kv: (str(kv[0]))))


def main(argv=None) -> int:
    # A description of its own, NOT the module docstring. The docstring is
    # written for someone reading the file; this is for someone running the
    # command. argparse reflows the text, so six paragraphs of rationale come
    # out as one unreadable block.
    parser = argparse.ArgumentParser(
        description="N0: measure the real-time API before writing N1 to N8. "
                    "Writes a report under runtime/observations/ and a raw "
                    "log, one line per "
                    "call. No personal data is ever recorded.",
        epilog="One campaign at a time, AND THE INGESTION IS A CAMPAIGN: "
               "dag_ingest runs in this same container under the LocalExecutor "
               "and the producer keys its limiter on the client address, so it "
               "and this script share one bucket. Measured on 2026-11-20: 20 "
               "back-to-back calls found 8 slots free of 13, the other 5 being "
               "the running DAG's. Stop it first with `fd-manager realtime "
               "-Stop`, or read the ceiling as what was LEFT. Ctrl+C loses "
               "nothing -- the log is already on disk and --from-log rebuilds "
               "the report.")
    parser.add_argument("--calls", type=int, default=20,
                        help="spaced calls (default 20)")
    parser.add_argument("--burst", type=int, default=8,
                        help="rapid calls to provoke the limiter (default 8)")
    parser.add_argument("--spacing", type=int, default=DEFAULT_SPACING_SECONDS,
                        metavar="S",
                        help="seconds between spaced calls (default %d; 0 runs "
                             "back to back, which tests the limiter)"
                             % DEFAULT_SPACING_SECONDS)
    parser.add_argument("--endpoint", metavar="URL",
                        help="address to measure (default: realtime.endpoint "
                             "from governance.yml)")
    parser.add_argument("--from-log", metavar="PATH",
                        help="rebuild the report from a log, no API call")
    args = parser.parse_args(argv)

    if args.from_log:
        return rebuild(Path(args.from_log))

    args.endpoint = args.endpoint or declared_endpoint()
    print("Point d'entree : %s" % args.endpoint)

    started_at = dt.datetime.now(dt.timezone.utc)
    out = writable_dir()
    lock = take_lock(out, args.calls)
    stamp = started_at.strftime("%Y%m%d-%H%M")

    # THE RAW LOG, written as each call returns. The first version built the
    # report only at the end, and a PermissionError on mkdir threw away five
    # minutes of real API calls. Collection and reporting are now separate: the
    # log survives any failure of the report.
    journal = out / ("n0-observations--%s.jsonl" % stamp)
    print("Journal brut : %s" % journal)
    print("N0 : %d appels espaces de %d s, puis une rafale de %d."
          % (args.calls, args.spacing, args.burst))
    # The latency is counted: ignoring it announced 6 minutes for a run that
    # took 19. Measured median is about 6 s per call.
    seconds = (args.calls - 1) * args.spacing + (args.calls + args.burst) * 6
    print("Duree estimee : %d min %02d s, latence comprise (~6 s/appel)."
          % divmod(seconds, 60))
    print("Ctrl+C a tout moment : le journal brut est deja sur le disque et le")
    print("rapport se reconstruit avec --from-log.\n")

    spaced = []
    for i in range(args.calls):
        if i and args.spacing:
            time.sleep(args.spacing)
        record = call(args.endpoint)
        spaced.append(record)
        _append(journal, "spaced", record)
        mark = "ok " if record["ok"] else "KO "
        print("  %2d/%d  %s HTTP %-4s %6.0f ms"
              % (i + 1, args.calls, mark, record["status"] or "-",
                 1000 * record["latency"]))

    # THE LIMITER, provoked on purpose: N7's margin depends on what it does.
    burst = []
    if args.burst:
        print("\n  rafale pour declencher le limiteur :")
    for i in range(args.burst):
        record = call(args.endpoint)
        burst.append(record)
        _append(journal, "burst", record)
        print("    %d  HTTP %-4s %6.0f ms  %s"
              % (i + 1, record["status"] or "-", 1000 * record["latency"],
                 record.get("headers") or ""))

    # How long until a call succeeds again.
    #
    # EVERY PHASE AND NOT ONLY THE RAFALE, since 2026-11-20. The limiter does
    # not care which loop a call came from: `--calls 20 --spacing 0 --burst 0`
    # fires it from the spaced phase, and the old condition then skipped the
    # reopening entirely -- the report said "non observee" about the very run
    # designed to measure the limiter. The condition now asks what happened,
    # not which loop it happened in.
    reopened_after = None
    if any(r["status"] == 429 for r in spaced + burst):
        print("\n  attente de la reouverture :")
        waited = 0
        while waited < 180:
            time.sleep(10)
            waited += 10
            probe = call(args.endpoint)
            # JOURNALLED LIKE THE REST, and it was not until 2026-11-20. The
            # file promises that the log survives any failure of the report and
            # that --from-log rebuilds it; these probes were the one measurement
            # kept nowhere but a local variable, so a rebuilt report said "non
            # observee" where the campaign had measured a delay. A promise that
            # holds for all but one figure is worse than no promise, because
            # nothing in the rebuilt report says which figure went missing.
            _append(journal, "reopen", dict(probe, waited_s=waited))
            print("    apres %3d s : HTTP %s" % (waited, probe["status"]))
            if probe["ok"]:
                reopened_after = waited
                break

    try:
        report = write_report(started_at, spaced, burst, reopened_after,
                              args, out, stamp)
    except Exception as problem:                              # noqa: BLE001
        # The log is already on disk, so the measurement is not lost: the
        # report can be rebuilt from it without calling the API again.
        print("\nLe RAPPORT a echoue : %s: %s" % (type(problem).__name__, problem))
        print("Les observations sont intactes dans %s" % journal)
        print("Reconstruire sans rappeler l'API :")
        print("  python3 %s --from-log %s" % (__file__, journal))
        lock.unlink(missing_ok=True)
        return 1
    print("\nMesure ecrite : %s" % report)
    print("Journal brut   : %s" % journal)
    lock.unlink(missing_ok=True)
    return 0


def rebuild(journal: Path) -> int:
    """Rebuild the report from a log. Costs nothing and calls nothing."""
    spaced, burst, reopen = [], [], []
    for line in journal.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        # THREE PHASES AND NOT TWO. "anything that is not burst is spaced" put
        # the reopening probes among the spaced calls, where they would have
        # moved the median latency and the availability table -- a rebuilt
        # report quietly describing a campaign that never ran.
        {"burst": burst, "reopen": reopen}.get(
            record.get("phase"), spaced).append(record)
    print("Journal relu : %d appels espaces, %d en rafale, %d sonde(s) de "
          "reouverture." % (len(spaced), len(burst), len(reopen)))

    # SimpleNamespace, not a class body: inside a class body the name being
    # assigned shadows the enclosing function's, so `calls, burst = len(spaced),
    # len(burst)` raises NameError on its own right-hand side.
    # The endpoint is read back from the first record rather than defaulted:
    # a report that names the wrong address is worse than one that names none.
    seen = [r.get("endpoint") for r in spaced + burst if r.get("endpoint")]
    args = types.SimpleNamespace(calls=len(spaced), burst=len(burst),
                                 endpoint=seen[0] if seen else "non releve")

    out = writable_dir()
    stamp = journal.stem.split("--")[-1]
    reopened_after = next((r["waited_s"] for r in reopen
                           if r.get("ok") and "waited_s" in r), None)
    path = write_report(dt.datetime.now(dt.timezone.utc), spaced, burst,
                        reopened_after, args, out, stamp)
    print("Mesure ecrite : %s" % path)
    return 0


def _append(path: Path, phase: str, record: dict) -> None:
    """One observation, appended now. Never raises: losing the log must not
    stop the measurement either.

    WHAT IS WRITTEN, and why it is not the payload. The response carries first,
    last, street, city, zip, dob, job, gender and cc_num. The report has always
    recorded those by shape alone -- but this log was added later, to stop
    losing observations when the report could not be written, and it wrote the
    payload whole. On 2026-11-16 five logs of a campaign held 247 complete records,
    roughly 242 distinct identities, in a directory that is not ignored by git.
    Synthetic identities, so no real person was exposed, and still exactly what
    EC17 forbids putting in the repository.

    The remedy is not a second redaction routine: shape() already describes a
    payload without copying what must not be copied, and write_report has
    always consumed its output and nothing else. So the log stores the shape,
    under a key of its own. NOT under "payload": a shape passed back to shape()
    returns early with no publishable section, which would lose the whole
    measurement without raising -- a wrong answer rather than an error."""
    try:
        keep = {k: v for k, v in record.items() if k != "payload"}
        keep["phase"] = phase
        payload = record.get("payload")
        if payload is not None:
            keep["shape"] = shape(payload)
        with io_open(path) as fh:
            fh.write(json.dumps(keep, ensure_ascii=False) + "\n")
    # Not OSError alone, and that is a consequence of the line above: writing
    # the payload could only fail on the filesystem, but shape() reads into the
    # response, so a malformed one now raises here. The promise of this
    # function is that a lost log never stops a campaign of real calls.
    except Exception as problem:                    # noqa: BLE001
        print("    (journal non ecrit : %s)" % problem)


def io_open(path: Path):
    return open(path, "a", encoding="utf-8")


def _mapping(pairs) -> str:
    """Whether index and trans_num identify the same thing, both ways.

    Comparing two counts of distinct values was the first version, and it cried
    DESACCORD on a perfectly ordinary sample: equal counts prove nothing about
    the pairing. What matters is that no index carries two trans_num and no
    trans_num carries two index.
    """
    if not pairs:
        return "aucune paire observee"
    by_index, by_trans = {}, {}
    for index, trans in pairs:
        by_index.setdefault(index, set()).add(trans)
        by_trans.setdefault(trans, set()).add(index)
    broken = ([i for i, t in by_index.items() if len(t) > 1]
              + [t for t, i in by_trans.items() if len(i) > 1])
    if broken:
        return "DESACCORD sur %d cle(s)" % len(broken)
    return "bijective sur %d paire(s)" % len(by_index)


def write_report(started_at, spaced, burst, reopened_after, args,
                 out: Path, stamp: str) -> Path:
    ok = [r for r in spaced if r["ok"]]
    shapes = [shape_of(r) for r in ok]
    latencies = [1000 * r["latency"] for r in spaced if r["status"]]

    column_sets = Counter(tuple(s["columns"]) for s in shapes)
    indexes = [s["index"][0] for s in shapes if s["index"]]
    trans_nums = [s["publishable"].get("trans_num") for s in shapes
                  if s.get("publishable")]
    pairs = [(s["index"][0], s["publishable"].get("trans_num"))
             for s in shapes if s.get("index") and s.get("publishable")]
    cc_lengths = Counter(s["shapes"]["cc_num"]["length"] for s in shapes
                         if "cc_num" in s.get("shapes", {}))
    cc_types = Counter(s["shapes"]["cc_num"]["type"] for s in shapes
                       if "cc_num" in s.get("shapes", {}))
    labels = Counter(s["publishable"].get("is_fraud") for s in shapes
                     if s.get("publishable"))
    # Paired with OUR clock at the same call: that is the only honest drift.
    # The rank in the FULL sequence travels with each pair. Computing a gap on
    # the successful calls alone skips the failures in between and gives the
    # wrong neighbour -- which made the first diagnosis blame the server for a
    # gap of 2 s that was really 818 s.
    rank_of = {id(r): i for i, r in enumerate(spaced)}
    clocked = [(int(sh["publishable"]["current_time"]), r["our_clock_ms"],
                rank_of.get(id(r), -1), r["latency"])
               for sh, r in zip(shapes, ok)
               if sh.get("publishable", {}).get("current_time") is not None
               and r.get("our_clock_ms")]
    times = [t for t, _, _, _ in clocked]
    # The full timeline, failures included: the only correct source for a gap.
    timeline = [r.get("our_clock_ms") for r in spaced]
    # Kept apart: a log from before our_clock_ms existed still carries
    # current_time, and saying "no current_time" there would be false.
    all_times = [int(sh["publishable"]["current_time"]) for sh in shapes
                 if sh.get("publishable", {}).get("current_time") is not None]

    lines = [
        "# N0 --- mesure de l'API temps reel",
        "",
        "> Produit par `tools/observe_api.py`, %s."
        % started_at.strftime("%d/%m/%Y a %H:%M:%S UTC"),
        "> Point d'entree : `%s`"
        % getattr(args, "endpoint", FALLBACK_ENDPOINT),
        "> %d appels espaces de %d s, rafale de %d."
        % (args.calls, getattr(args, "spacing", DEFAULT_SPACING_SECONDS),
           args.burst),
        "",
        "**Ce fichier ne contient aucune donnee nominative.** La reponse porte",
        "`first`, `last`, `street`, `dob`, `cc_num` et le reste ; seules les",
        "FORMES sont relevees ici (type, longueur, decompte). Ecrire les valeurs",
        "dans un fichier du depot violerait EC17 plus directement qu'aucun",
        "traitement ne le pourrait.",
        "",
        "---",
        "",
        "## 1. Disponibilite",
        "",
        "| | |",
        "|---|---|",
        "| appels espaces | %d |" % len(spaced),
        "| reponses 200 | %d |" % len(ok),
        "| autres codes | %s |" % tally(Counter(
            r["status"] for r in spaced if r["status"] != 200)),
        "| erreurs reseau | %d |" % sum(1 for r in spaced if r["status"] is None),
        "",
        "## 2. Latence (ms)",
        "",
    ]
    if latencies:
        lines += [
            "| | |", "|---|---|",
            "| minimum | %.0f |" % min(latencies),
            "| mediane | %.0f |" % statistics.median(latencies),
            "| maximum | %.0f |" % max(latencies),
        ]
    else:
        lines.append("Aucune reponse mesurable.")

    # EVERY call, spaced and burst alike. The first version looked only at the
    # burst list and announced "aucun 429" on a campaign that had one, at call
    # 31 of the spaced run.
    every = list(spaced) + list(burst)
    refused = [(i, r) for i, r in enumerate(every, 1) if r["status"] == 429]
    failures = [(i, r) for i, r in enumerate(every, 1)
                if r["status"] not in (200, 429)]
    lines += [
        "",
        "## 3. Le limiteur et les echecs",
        "",
        "Le plafond est celui de PRODUCER_RATE_LIMIT (docker-compose.yml), que",
        "ce script ne lit pas : il l eprouve. **Ces chiffres decident de la",
        "marge du lot N7** -- et ils ne valent QUE si une seule campagne",
        "tournait : deux clients simultanes depuis la meme adresse faussent le",
        "limiteur comme le taux d erreur.",
        "",
        "**UNE CAMPAGNE ESPACEE NE MESURE PAS LE PLAFOND.** La rafale part",
        "juste apres la phase espacee, dont les derniers appels sont encore",
        "dans la fenetre du limiteur : le rang du premier 429 melange alors les",
        "deux phases et ne dit plus combien d appels passent par minute. Pour",
        "le plafond seul, une campagne qui n est que rafale :",
        "`--calls 20 --spacing 0 --burst 0`.",
        "",
        "| | |", "|---|---|",
        "| appels au total | %d |" % len(every),
        "| **429 (limiteur)** | **%d** |" % len(refused),
        "| premier 429 au rang | %s |" % (refused[0][0] if refused else "aucun"),
        "| cout median d un 429 | %s |" % (
            "%.0f ms" % (1000 * statistics.median(
                [r["latency"] for _, r in refused])) if refused else "n/a"),
        "| autres echecs | %d |" % len(failures),
        "| cout median d un echec | %s |" % (
            "%.1f s" % statistics.median([r["latency"] for _, r in failures])
            if failures else "n/a"),
        "| reouverture apres | %s |" % (
            "%d s" % reopened_after if reopened_after else "non observee"),
        "",
    ]
    if refused or failures:
        lines += ["| rang | code | latence | en-tetes |", "|---|---|---|---|"]
        for i, r in sorted(refused + failures):
            lines.append("| %d | %s | %.1f s | %s |"
                         % (i, r["status"] or "reseau", r["latency"],
                            r.get("headers") or "aucun"))
        lines.append("")
    lines += [
        "## 4. Forme de la reponse",
        "",
        "| | |", "|---|---|",
        "| jeux de colonnes distincts | %d |" % len(column_sets),
        "| colonnes | %d |" % (len(shapes[0]["columns"]) if shapes else 0),
        "| doublement encode | %s |" % tally(Counter(
            "oui" if r.get("double_encoded") else "non" for r in ok)),
        "| colonnes et donnees alignees | %s |" % tally(Counter(
            "oui" if s["aligned"] else "NON" for s in shapes)),
        "",
    ]
    if len(column_sets) == 1 and shapes:
        lines += ["Colonnes, dans l'ordre :", "",
                  "```", ", ".join(shapes[0]["columns"]), "```", ""]
    elif len(column_sets) > 1:
        lines += ["**La forme a CHANGE entre deux appels.** Les jeux observes :",
                  ""]
        for columns, n in column_sets.items():
            lines.append("- %d fois : `%s`" % (n, ", ".join(columns)))
        lines.append("")

    lines += [
        "## 5. L'etiquette",
        "",
        "| | |", "|---|---|",
        "| `is_fraud` present | %s |" % (
            "oui" if shapes and "is_fraud" in shapes[0]["columns"] else "NON"),
        "| valeurs observees | %s |" % tally(labels),
        "| type recu | %s |" % tally(Counter(
            type(s["publishable"].get("is_fraud")).__name__
            for s in shapes if s.get("publishable"))),
        "",
        "Si elle est presente, **le lot N6 est une garantie contre la fuite**",
        "et non un dispositif de simulation.",
        "",
    ]
    if len(labels) < 2:
        seen = next(iter(labels), "aucune")
        lines += [
            "**Une seule valeur observee (%s), donc cet echantillon ne dit rien"
            % seen,
            "sur la capacite du flux a livrer l'autre.** Le taux de fraude de",
            "la FENETRE SERVIE est de %.4f %% : sur %d tirages, la probabilite"
            % (100 * WINDOW_BASE_RATE, max(len(shapes), 1)),
            "d'en voir au moins une est de %.0f %%. Il en faudrait %d, soit"
            % (100 * (1 - (1 - WINDOW_BASE_RATE) ** max(len(shapes), 1)),
               math.ceil(math.log(0.05) / math.log(1 - WINDOW_BASE_RATE))),
            "%.1f h d'ingestion a %d appels par jour, pour atteindre 95 %%."
            % (24 * math.ceil(math.log(0.05) / math.log(1 - WINDOW_BASE_RATE))
               / INGESTION_PER_DAY, int(INGESTION_PER_DAY)),
            "",
            "**N6 ne doit donc pas attendre d'observer un `is_fraud = 1` reel.**",
            "Son critere injecte les deux valeurs dans la meme charge utile et",
            "exige le meme score : c'est une mesure d'absence de fuite, et elle",
            "ne depend pas de la chance du tirage.",
            "",
        ]

    label_types = {type(s["publishable"].get("is_fraud")).__name__
                   for s in shapes if s.get("publishable")}
    if label_types == {"str"}:
        lines += [
            "Le type recu est `str`, comme attendu : le corps est decode avec",
            "`parse_int=str`, donc `is_fraud` arrive en texte comme tout autre",
            "entier. `truth.to_bool`, ecrit pour S1, refuse plutot que de",
            "deviner -- il convient tel quel.",
            "",
        ]
    elif label_types:
        lines += [
            "**Le type recu n'est pas `str` mais %s.** Le decodage avec"
            % ", ".join(sorted(label_types)),
            "`parse_int=str` devait rendre tout entier en texte : ce resultat",
            "demande une explication avant que N6 ne soit ecrit.",
            "",
        ]

    lines += [
        "## 6. L'horodatage",
        "",
    ]
    if clocked:
        moments = [dt.datetime.fromtimestamp(t / 1000, dt.timezone.utc)
                   for t in times]
        # Per call: server clock minus ours, at the same instant. An elapsed
        # span compared with an expected span measures the latency, not a drift.
        deltas = [(server - ours) / 1000.0 for server, ours, _, _ in clocked]
        lines += [
            "| | |", "|---|---|",
            "| premiere valeur | %d |" % times[0],
            "| lue en ms (UTC) | %s |" % moments[0].isoformat(),
            "| derniere valeur | %s |" % moments[-1].isoformat(),
            "| **ecart serveur - nous, median** | **%+.1f s** |"
            % statistics.median(deltas),
            "| ecart minimum | %+.1f s |" % min(deltas),
            "| ecart maximum | %+.1f s |" % max(deltas),
            "",
            "**L'ecart est mesure appel par appel**, horloge du serveur moins la",
            "notre au meme instant. Comparer une duree ecoulee a une duree",
            "attendue mesurerait la latence et non une derive : la premiere",
            "version de ce script a ainsi annonce une derive de +122 s qui",
            "etait exactement la latence mediane multipliee par le nombre",
            "d'appels.",
            "",
            "Un ecart de l'ordre de la latence est normal : le serveur horodate",
            "au moment ou il repond, nous au moment ou nous appelons.",
        ]
        # DIAGNOSING A LARGE DELTA, and not asserting a cause.
        #
        # The first version blamed the server's clock. It was our own process.
        # On Linux CLOCK_MONOTONIC does not advance across a suspend while
        # CLOCK_REALTIME does, so a process suspended DURING a call shows:
        #   - a short measured latency       (monotonic froze)
        #   - a large delta on that call     (the server answered after resume)
        #   - a large gap to the NEXT call   (our clock jumped)
        #
        # The gap that matters is therefore the one AFTER the call, not before:
        # our clock for the aberrant call was taken before the suspend. Measured
        # on 2026-11-16: delta +818.4 s against a following gap of 818.8 s --
        # they match to 0.4 s, which no server clock jump would do.
        #
        # A genuine server clock jump gives a large delta with an ORDINARY gap
        # after it. The two cases are told apart by comparison, not by guessing.
        aberrant = [(i, d) for i, d in enumerate(deltas) if d > 60]
        if aberrant:
            lines += [
                "",
                "**%d appel(s) presentent un ecart superieur a 60 s**, jusqu a"
                % len(aberrant),
                "%+.0f s. Deux causes sont possibles et elles se distinguent :"
                % max(d for _, d in aberrant),
                "",
                "| rang | ecart | latence | intervalle APRES | cause |",
                "|---|---|---|---|---|",
            ]
            for i, d in aberrant:
                _, _, rank, latency = clocked[i]
                after = (timeline[rank + 1]
                         if rank + 1 < len(timeline) and timeline[rank + 1]
                         else None)
                gap = ((after - timeline[rank]) / 1000.0) if after else 0.0
                # The delta and the following gap matching within 20 % is a
                # suspend: our clock jumped by the same amount the server's
                # timestamp ran ahead. Anything else points at the server.
                suspended = (latency < 60 and gap > 60
                             and abs(gap - d) <= 0.2 * max(gap, d))
                lines.append("| %d | %+.0f s | %.1f s | %.0f s | %s |"
                             % (rank + 1, d, latency, gap,
                                "notre processus suspendu" if suspended
                                else "a instruire"))
            lines += [
                "",
                "Sous Linux `CLOCK_MONOTONIC` ne progresse pas pendant une",
                "suspension alors que `CLOCK_REALTIME` continue. Un processus",
                "suspendu PENDANT un appel montre donc trois choses ensemble :",
                "une latence courte, un ecart large sur cet appel, et un",
                "intervalle large jusqu au SUIVANT -- notre horloge ayant ete",
                "prise avant la suspension. Quand l ecart et cet intervalle",
                "coincident, c est notre machine qui a dormi.",
                "",
                "Un saut de pendule cote serveur donnerait l ecart avec un",
                "intervalle ORDINAIRE. La colonne cause n affirme donc rien",
                "qu elle ne compare.",
                "",
                "Dans les deux cas la consequence pour N1 est la meme, car",
                "`current_time` EST notre `txn_ts` : **borner cette valeur contre",
                "notre propre horloge** et mettre en quarantaine un horodatage",
                "invraisemblable, sans quoi une transaction datee dans le futur",
                "casserait l ordre sur lequel les features par ligne reposent.",
            ]
        lines += [
            "",
            "`current_time` est un **epoch en millisecondes**. Lu en secondes il",
            "est hors plage, lu en microsecondes il donne 1970 : les deux erreurs",
            "sont bruyantes.",
            "",
        ]
    elif all_times:
        lines += [
            "`current_time` est bien present (%d valeurs), **mais aucune n'est"
            % len(all_times),
            "appariee a notre propre horloge** : ce journal est anterieur a la",
            "version du script qui releve `our_clock_ms`. L'ecart ne peut donc",
            "pas etre mesure sur cette campagne. La prochaine le donnera.",
            "",
        ]
    else:
        lines += ["Aucun `current_time` observe.", ""]

    lines += [
        "## 7. Les cles de la ligne source",
        "",
        "| | |", "|---|---|",
        "| `index` observes | %d |" % len(indexes),
        "| `index` distincts | %d |" % len(set(indexes)),
        "| doublons | %d |" % (len(indexes) - len(set(indexes))),
        "| `trans_num` distincts | %d |" % len(set(trans_nums)),
        "| correspondance index <-> trans_num | %s |" % _mapping(pairs),
        "",
        "Un doublon est **attendu A L ECHELLE**, et c est une propriete de la",
        "fenetre et non du hasard : le producer tire avec remise dans les",
        "lignes qu il tient (PRODUCER_MAX_ROWS, 100 000 par defaut) et non",
        "dans tout le corpus, comme le faisait l original. Les collisions",
        "attendues sur n tirages valent n^2 / 2N : negligeables sur une",
        "campagne de dix, environ 260 sur une journee d ingestion de 7 200.",
        "Deux tirages de la meme ligne sont deux evenements distincts, separes",
        "par leur `current_time`.",
        "",
        "## 8. La precision du numero de carte --- LE controle decisif",
        "",
        "83 des 924 cartes de la source depassent 2**53 et perdraient des",
        "chiffres en passant par un flottant. Le corps est decode avec",
        "`parse_int=str`, donc la valeur relevee ici est le **texte** recu.",
        "",
        "| | |", "|---|---|",
        "| types observes | %s |" % tally(cc_types),
        "| longueurs observees | %s |" % ", ".join(
            "%d chiffres : %d" % (k, v) for k, v in sorted(cc_lengths.items())),
        "| carte a 19 chiffres rencontree | %s |" % (
            "oui" if 19 in cc_lengths else "NON sur cet echantillon"),
        "",
    ]
    if 19 in cc_lengths:
        lines += [
            "**Une carte a 19 chiffres est bien arrivee**, et le texte recu",
            "porte ses 19 chiffres : le risque de precision est donc reel sur",
            "le chemin complet, et N1 doit lire `cc_num` en texte des la sortie",
            "du decodeur.",
            "",
        ]
    else:
        lines += [
            "**Aucune carte a 19 chiffres sur cet echantillon.** Avec 83 cartes",
            "sur 924, %d tirages en manquent toutes avec une probabilite de"
            % len(shapes),
            "l'ordre de %.0f %%. **Ne pas en conclure que le risque n'existe"
            % (100 * (1 - 83 / 924) ** max(len(shapes), 1)),
            "pas** : relancer avec `--calls 60`.",
            "",
        ]

    lines += [
        "## 9. Ce que les lots suivants doivent y lire",
        "",
        "| Lot | Chiffre |",
        "|---|---|",
        "| N1 | le type et la longueur de `cc_num`, section 8 |",
        "| N1 | l'alignement colonnes / donnees, section 4 |",
        "| N3 | la liste des colonnes, section 4 |",
        "| N4 | l'absence de `trans_date_trans_time`, section 4 |",
        "| N6 | la presence de `is_fraud`, section 5 |",
        "| N7 | le rang du premier 429 et le delai de reouverture, section 3 |",
        "| N7 | la latence mediane, section 2 |",
        "| N8 | le taux de doublons, section 7 |",
        "",
    ]

    path = out / ("n0-api-measure--%s.md" % stamp)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


if __name__ == "__main__":
    sys.exit(main())
