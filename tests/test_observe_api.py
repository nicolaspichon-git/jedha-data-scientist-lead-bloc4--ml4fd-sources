"""What the N0 measurement log may contain.

Covers: EC17
Lot: N0
Competency: C5

WHY THIS FILE EXISTS. shape() was written to describe a response without
copying what must not be copied, and the report has always consumed its output.
The per-call log was added later, to stop losing observations when the report
could not be written, and it wrote the payload whole: on 2026-11-16 five logs
under ws/ held 247 complete records -- name, street, city, zip, date of birth,
occupation, gender and card number -- in a directory git does not ignore.

The defect was not in shape(). It was in the function that writes the log, so
that is what these tests exercise. A test on shape() alone would have passed
throughout.
"""
from __future__ import annotations

import importlib.util
import io
import json
from pathlib import Path

import pytest

MODULE = Path(__file__).resolve().parents[1] / "tools" / "observe_api.py"


def _module():
    spec = importlib.util.spec_from_file_location("observe_api", MODULE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


oa = _module()

# One sentinel per identifying column, none of which may survive the write.
SENTINELS = {
    "cc_num": "4992346398065154184",      # the 19-digit card measured in N0
    "first": "SENTINEL_PRENOM",
    "last": "SENTINEL_NOM",
    "gender": "SENTINEL_GENRE",
    "street": "SENTINEL_RUE",
    "city": "SENTINEL_VILLE",
    "state": "SENTINEL_ETAT",
    "zip": "SENTINEL_CP",
    "lat": "SENTINEL_LAT",
    "long": "SENTINEL_LONG",
    "city_pop": "SENTINEL_POP",
    "job": "SENTINEL_METIER",
    "dob": "SENTINEL_NAISSANCE",
}

PUBLISHED = {
    "merchant": "fraud_Dach-Borer",
    "category": "grocery_net",
    "amt": 31.62,
    "trans_num": "aca54e7c407bb98811b688f3f055c4c7",
    "is_fraud": "0",
    "current_time": "1794817599732",
}


def _payload() -> dict:
    """A response shaped exactly like the one N0 measured: 21 columns."""
    values = dict(SENTINELS)
    values.update(PUBLISHED)
    values["merch_lat"] = 39.065279
    values["merch_long"] = -76.195157
    columns = list(values)
    return {"columns": columns, "index": ["31923"],
            "data": [[values[c] for c in columns]]}


def _record() -> dict:
    return {"ok": True, "status": 200, "latency": 6.41,
            "our_clock_ms": 1794817593819, "headers": {}, "bytes": 566,
            "double_encoded": True, "payload": _payload()}


def _written(tmp_path) -> str:
    path = tmp_path / "observations.jsonl"
    oa._append(path, "spaced", _record())
    return io.open(path, encoding="utf-8").read()


def test_the_log_carries_no_identifying_value(tmp_path):
    """THE test. It fails against the code as it stood on 2026-11-16."""
    body = _written(tmp_path)
    leaked = sorted(name for name, value in SENTINELS.items()
                    if str(value) in body)
    assert not leaked, "valeurs nominatives ecrites : %s" % leaked


def test_the_log_carries_no_payload_key(tmp_path):
    record = json.loads(_written(tmp_path))
    assert "payload" not in record
    assert "shape" in record


def test_the_log_keeps_what_the_report_reads(tmp_path):
    """Redacting is worthless if it takes the measurement with it."""
    record = json.loads(_written(tmp_path))
    shape = record["shape"]
    assert shape["n_columns"] == 21
    assert shape["aligned"] is True
    assert shape["index"] == ["31923"]
    for name, value in PUBLISHED.items():
        assert shape["publishable"][name] == value
    # Section 8 of the report -- the decisive check on card precision -- reads
    # nothing but this length.
    assert shape["shapes"]["cc_num"] == {"type": "str", "length": 19}


def test_every_identifying_column_is_described_by_shape_alone(tmp_path):
    record = json.loads(_written(tmp_path))
    shape = record["shape"]
    for name in SENTINELS:
        assert name in shape["shapes"], name
        assert set(shape["shapes"][name]) == {"type", "length"}
        assert name not in shape["publishable"]


def test_the_call_metadata_survives(tmp_path):
    record = json.loads(_written(tmp_path))
    for key in ("ok", "status", "latency", "our_clock_ms", "bytes",
                "double_encoded"):
        assert key in record, key
    assert record["phase"] == "spaced"


def test_publishable_and_identifying_do_not_overlap():
    assert not set(oa.PUBLISHABLE) & set(oa.IDENTIFYING)


def test_the_clock_field_stays_publishable():
    """current_time is an epoch in milliseconds, not a personal datum. Moving
    it out of PUBLISHABLE would silently empty section 6 of the report."""
    for name in ("current_time", "index", "trans_num", "amt", "is_fraud",
                 "category", "merchant"):
        assert name in oa.PUBLISHABLE, name


def test_a_relogged_observation_reads_the_same_as_a_live_one(tmp_path):
    live = _record()
    relogged = json.loads(_written(tmp_path))
    assert oa.shape_of(relogged) == oa.shape_of(live)


def test_shape_of_refuses_an_observation_that_carries_neither():
    """A silently empty shape would be reported as a measurement."""
    with pytest.raises(ValueError, match="sans charge utile ni forme"):
        oa.shape_of({"ok": False, "status": 502})


def test_the_shape_is_not_stored_under_the_payload_key(tmp_path):
    """Why the key has a name of its own: shape() fed a shape returns early,
    with no publishable section and without raising -- a wrong answer rather
    than an error, and the whole measurement lost."""
    once = oa.shape(_payload())
    twice = oa.shape(once)
    assert "publishable" not in twice
    assert "shapes" not in twice


def test_a_malformed_response_does_not_stop_the_campaign(tmp_path, capsys):
    """_append reads into the response now, so it can fail where it could not
    before. Sixty real calls must not be lost to one odd body."""
    path = tmp_path / "observations.jsonl"
    broken = _record()
    broken["payload"] = "pas un objet"
    oa._append(path, "spaced", broken)     # must not raise
    assert "journal non ecrit" in capsys.readouterr().out
