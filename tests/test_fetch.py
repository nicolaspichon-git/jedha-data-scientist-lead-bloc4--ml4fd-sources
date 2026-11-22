"""Reading the CSV: the properties that typing must not destroy later.

Block: B3, B4.
Lot: B4-L0.
Competency: ?.
Covers: ?.
"""
import hashlib

import pytest

from ml4fd import fetch

HEADER = ",trans_date_trans_time,cc_num,amt,is_fraud\n"
# Three cardholder numbers that a numeric read would damage: 19 digits beyond
# the exact range of a double, a leading zero, and a plain 16-digit number.
ROWS = (
    "0,2020-06-21 12:14:25,4306723884243089331,2.86,0\n"
    "1,2020-06-21 12:14:33,0379879817157,41.28,0\n"
    "2,2020-06-21 12:14:53,2291163933867244,276.31,1\n"
)


@pytest.fixture
def csv_file(tmp_path):
    p = tmp_path / "sample.csv"
    p.write_text(HEADER + ROWS, encoding="utf-8")
    return p


def test_the_unnamed_first_column_gets_a_name(csv_file):
    """The CSV's first header cell is empty -- pandas' index from the export."""
    row = next(fetch.read_rows(csv_file))
    assert fetch.SOURCE_LINE in row
    assert "" not in row
    assert row[fetch.SOURCE_LINE] == "0"


def test_every_value_is_text(csv_file):
    for row in fetch.read_rows(csv_file):
        assert all(isinstance(v, str) for v in row.values())


def test_a_19_digit_cc_num_survives_intact(csv_file):
    """Read as a number it would be rounded, and its Luhn validity destroyed."""
    rows = list(fetch.read_rows(csv_file))
    assert rows[0]["cc_num"] == "4306723884243089331"
    assert len(rows[0]["cc_num"]) == 19
    # The point of reading as text, stated as an assertion:
    assert str(int(rows[0]["cc_num"])) == rows[0]["cc_num"]      # exact here
    assert int(float(rows[0]["cc_num"])) != int(rows[0]["cc_num"])  # lost as a float


def test_a_leading_zero_is_preserved(csv_file):
    rows = list(fetch.read_rows(csv_file))
    assert rows[1]["cc_num"] == "0379879817157"
    assert rows[1]["cc_num"].startswith("0")


def test_amounts_are_not_converted(csv_file):
    rows = list(fetch.read_rows(csv_file))
    assert rows[2]["amt"] == "276.31"


def test_every_line_is_yielded(csv_file):
    assert len(list(fetch.read_rows(csv_file))) == 3


def test_a_short_row_is_padded_not_dropped(tmp_path):
    """The raw zone takes everything; L2 decides an invalid record's fate (EC22)."""
    p = tmp_path / "short.csv"
    p.write_text(HEADER + "9,2020-06-21 12:00:00\n", encoding="utf-8")
    rows = list(fetch.read_rows(p))
    assert len(rows) == 1
    assert rows[0]["cc_num"] == ""
    assert set(rows[0]) == {fetch.SOURCE_LINE, "trans_date_trans_time",
                            "cc_num", "amt", "is_fraud"}


def test_sha256_of_matches_hashlib(csv_file):
    expected = hashlib.sha256(csv_file.read_bytes()).hexdigest()
    assert fetch.sha256_of(csv_file) == expected


def test_a_cached_file_whose_content_changed_is_refused(tmp_path, monkeypatch):
    """A silently modified cache must be reported, not ingested."""
    target = tmp_path / "fraudTest.csv"
    target.write_text(HEADER + ROWS, encoding="utf-8")
    url = "https://example.invalid/fraudTest.csv"

    fetch.fetch_csv(url, tmp_path)                 # records the checksum
    sidecar = target.with_suffix(target.suffix + ".sha256")
    assert sidecar.is_file()

    target.write_text(HEADER, encoding="utf-8")    # content changes
    with pytest.raises(ValueError, match="checksum"):
        fetch.fetch_csv(url, tmp_path)
