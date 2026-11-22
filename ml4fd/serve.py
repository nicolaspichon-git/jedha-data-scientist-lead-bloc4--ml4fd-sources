"""B4-L5: the scoring service's own logic, kept out of the HTTP layer.

Block: B4.
Lot: B4-L5.
Competency: B4-C2, B4-C3.
Covers: B4-EC15, B4-EC16, B4-EC19, B4-EC20, B4-EC21.

WHY THIS MODULE EXISTS AND scoring_service/app.py IS THIN. The same arrangement the
dashboard uses: everything a caller sees is decided here, where pytest can
hold it, and the HTTP layer only does what needs a web framework -- routing,
the request schema, the status code. fastapi is installed in the service's
image and nowhere else; the suite installs psycopg, pandas, requests and
PyYAML. So nothing here imports it, and one test holds that property in place.

THE SERVICE SCORES, IT DOES NOT INGEST, and the separation is a privilege
rather than an intention: 145_scoring_service_role.sql grants fd_scoring_service three columns of
fd_transactions to read and no right to write it. A payment asked about is not
a payment received.

WHAT IT NEVER KEEPS. The caller sends a card number and a home location. The
number is turned into the same pseudonym ml4fd/transform.py writes
(client_key), the location into a distance, and fd_scoring_service_calls has no column for
either -- B4-EC17 enforced by the schema, not promised in a comment.
"""
from __future__ import annotations

import datetime as dt
import logging
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING

from ml4fd import db, score, transform

if TYPE_CHECKING:
    import psycopg

log = logging.getLogger(__name__)

FEATURES_SQL = "scoring_service/features_for_call.sql"
RECORD_SQL = "scoring_service/record_call.sql"

# The fields a caller must send. Named here and not in the HTTP schema, so the
# refusal below and the published contract come from one declaration.
REQUIRED = ("cc_num", "txn_ts", "merchant", "category", "amt",
            "merch_lat", "merch_long", "home_lat", "home_long")

# THE AMOUNT'S BOUNDS, from governance's validation of the ingestion path: a
# service that accepted what the pipeline rejects would score rows the model
# was never fitted on and answer with a straight face.
AMOUNT_FIELD = "amt"
COORDINATE_FIELDS = (("merch_lat", -90, 90), ("merch_long", -180, 180),
                     ("home_lat", -90, 90), ("home_long", -180, 180))


def missing(payload: dict) -> list[str]:
    """The required fields the request does not carry. Pure."""
    return [name for name in REQUIRED
            if payload.get(name) is None
            or (isinstance(payload.get(name), str)
                and not payload[name].strip())]


def problems(payload: dict, max_amount: float) -> list[str]:
    """Everything wrong with a request, in French, as a caller reads it.

    EVERY PROBLEM AND NOT THE FIRST, the same choice ml4fd/guard.py makes for a
    window: a caller who fixes one field and is then told about the next has to
    guess how many rounds are left.

    EXPLICIT, WHICH IS WHAT B4-EC20 ASKS. "requete non conforme" names nothing;
    these sentences name the field, what was received and what is expected. The
    404 of a wrong route and the 422 of a wrong field are different answers and
    the caller must be able to tell them apart without reading our logs.

    Block: B4.
    Lot: B4-L5.
    Competency: B4-C3.
    Covers: B4-EC20.
    """
    absent = missing(payload)
    if absent:
        return ["champ(s) absent(s) ou vide(s) : %s" % ", ".join(absent)]

    found: list[str] = []
    try:
        amount = Decimal(str(payload[AMOUNT_FIELD]).strip())
    except (InvalidOperation, ValueError):
        found.append("amt : %r n'est pas un montant"
                     % payload[AMOUNT_FIELD])
    else:
        if amount <= 0:
            found.append("amt : %s, un montant doit etre strictement positif"
                         % amount)
        elif float(amount) > max_amount:
            found.append("amt : %s, au-dela du maximum accepte de %s "
                         "(validation.max_amount)" % (amount, max_amount))

    for name, low, high in COORDINATE_FIELDS:
        try:
            value = float(payload[name])
        except (TypeError, ValueError):
            found.append("%s : %r n'est pas une coordonnee"
                         % (name, payload[name]))
            continue
        if not low <= value <= high:
            found.append("%s : %s, hors de l'intervalle [%s, %s]"
                         % (name, value, low, high))

    if instant(payload.get("txn_ts")) is None:
        found.append("txn_ts : %r n'est pas un horodatage ISO 8601 avec "
                     "fuseau" % payload.get("txn_ts"))
    return found


def instant(value) -> dt.datetime | None:
    """The payment's instant, or None when it cannot be read.

    TIMEZONE-AWARE OR REFUSED. The history features are computed with a strict
    bound on this instant (fd_client_features, ruling 1), and a naive timestamp
    would be read in the session's zone -- so the same request would get a
    different history depending on how the container happens to be set.
    """
    if isinstance(value, dt.datetime):
        return value if value.tzinfo is not None else None
    if not isinstance(value, str):
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        moment = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    return moment if moment.tzinfo is not None else None


def anonymous(payload: dict, pepper: str) -> dict:
    """The request with its identity removed, ready to be scored and recorded.

    ONE PLACE, AND IT IS transform's. client_key and home_distance_km are the
    functions the ingestion path uses, so a call and a transaction of the same
    cardholder carry the same pseudonym and the same distance -- which is what
    lets the two be compared at all. A second implementation here would be a
    second definition of the pseudonym, and the first mismatch would be
    invisible.

    Block: B4.
    Lot: B4-L5.
    Competency: B4-C3, B4-C4.
    Covers: B4-EC17.
    """
    return {"txn_ts": instant(payload["txn_ts"]),
            "client_key": transform.client_key(str(payload["cc_num"]).strip(),
                                               pepper),
            "merchant": str(payload["merchant"]).strip(),
            "category": str(payload["category"]).strip(),
            "amt": Decimal(str(payload["amt"]).strip()).quantize(
                transform.CENT),
            "f_home_distance_km": transform.home_distance_km(
                payload["home_lat"], payload["home_long"],
                payload["merch_lat"], payload["merch_long"])}


def features(conn: "psycopg.Connection", call: dict) -> dict:
    """The feature row of one call, computed by the database.

    The history features come from fd_client_features and the calendar ones
    from the same extract() the batch path uses -- see
    sql/scoring_service/features_for_call.sql for why neither is computed here.
    """
    rows, names = db.query_file_columns(
        conn, FEATURES_SQL,
        client_key=call["client_key"], txn_ts=call["txn_ts"],
        amt=call["amt"], category=call["category"],
        f_home_distance_km=call["f_home_distance_km"])
    if not rows:
        raise LookupError(
            "fd_client_features returned no row for this call, which it "
            "cannot do: it is an aggregate over a filtered table and always "
            "yields one row. The query or the function has changed.")
    return dict(zip(names, rows[0]))


# THE LARGEST BATCH THE SERVICE WILL ANSWER. realtime.calls_per_run is 5, so a
# real batch is five rows; a replay or a backfill could be larger, and a
# request with no bound is a request that can exhaust the one worker the image
# declares. Refused with a message rather than truncated: a batch half scored
# is worse than a batch refused, because fd_scores would carry the half.
MAX_BATCH_ROWS = 1000


def frame_for(rows: list, scorer: score.Scorer):
    """One frame carrying exactly the features the champion declares, in order.

    THE CHAMPION'S LIST AND NOT datasets.FEATURES. A recipe amputated of
    txn_hour and txn_weekday is in service today (governance.scoring
    drop_features), and a future one may differ again: the service builds what
    the manifest of the model it loaded asks for. Scorer.scores() refuses a
    missing column, so a divergence raises instead of scoring against the
    wrong features.

    THE CALLER SENDS WHATEVER IT HAS, which is why this selects rather than
    validates a shape. dag_ingest sends the seven columns
    sql/scoring/rows_to_score.sql produces; a third party sends the five the
    feature query computes. Neither needs to know the recipe, and a recipe
    change therefore does not change the contract.
    """
    import pandas as pd

    if not rows:
        raise ValueError("no row to score")
    absent = sorted({name for name in scorer.features
                     for row in rows if name not in row})
    if absent:
        raise ValueError(
            "the rows do not carry %s, which recipe %s requires. The caller "
            "and the recipe have diverged."
            % (", ".join(absent), scorer.recipe_digest))
    return pd.DataFrame([{name: row[name] for name in scorer.features}
                         for row in rows])


def frame_of(row: dict, scorer: score.Scorer):
    """A one-row frame carrying exactly the features the champion declares.

    THE CHAMPION'S LIST AND NOT datasets.FEATURES. A recipe amputated of
    txn_hour and txn_weekday is in service today (governance.scoring
    drop_features), and a future one may differ again: the service builds what
    the manifest of the model it loaded asks for. Scorer.scores() refuses a
    missing column, so a divergence raises instead of scoring against the
    wrong features.
    """
    absent = [name for name in scorer.features if name not in row]
    if absent:
        raise ValueError(
            "the feature query does not produce %s, which recipe %s requires. "
            "sql/scoring_service/features_for_call.sql and the recipe have "
            "diverged." % (", ".join(absent), scorer.recipe_digest))
    return frame_for([row], scorer)


def answer(scorer: score.Scorer, row: dict) -> dict:
    """The decision, rounded where it will be stored. Pure but for the model.

    THE SCORE IS ROUNDED BEFORE THE COMPARISON, once, for the reason
    ml4fd/score.py measured on the batch path: fd_scoring_service_calls carries
    CHECK (flagged = (score >= threshold)) over a numeric(9,8), and a float
    four billionths below the line answers one way in Python and the other in
    PostgreSQL. The INSERT would fail after the caller had been answered.
    """
    raw = scorer.scores(frame_of(row, scorer))[0]
    value = round(float(raw), score.SCORE_SCALE)
    return {"score": value,
            "threshold": scorer.threshold,
            "flagged": scorer.flagged(value),
            "model_version": scorer.model_version,
            "recipe_digest": scorer.recipe_digest}


def batch_answer(scorer: score.Scorer, rows: list) -> dict:
    """Score a whole batch in one pass, and say which model answered it.

    ONE MODEL FOR THE WHOLE BATCH, reported once rather than per row. fd_scores
    records model_version on every row and they must all agree: a batch
    answered by two models would be a lie in that journal. One call, one
    Scorer, one version -- and score.load() resolves the alias before its
    cache, so a promotion lands on the NEXT batch and never in the middle of
    one.

    NOTHING IS JOURNALLED HERE, and that is the decision of the lot rather than
    an omission. The pipeline records its own decisions in fd_scores, where the
    foreign key to fd_transactions guarantees the row exists and the stored
    features make it re-derivable. Recording them again in
    fd_scoring_service_calls would create a second set of figures that could
    disagree with the first -- which is what governance.yml refuses for
    data_quality and what ml4fd/score.py refuses between the manifest and the
    registry. A decision is recorded once, by whoever holds the data that makes
    it re-derivable; sql/ddl/195_scoring_decisions.sql is the view that reads
    the two journals as one.

    THE SAME ROUNDING AS THE SINGLE CALL, through answer(): the score is
    rounded to the scale it will be stored at BEFORE the comparison, so the
    decision this returns and the CHECK on fd_scores cannot differ.

    Block: B4.
    Lot: B4-L5.
    Competency: B4-C3.
    Covers: B4-EC16, B4-EC19.
    """
    if len(rows) > MAX_BATCH_ROWS:
        raise ValueError(
            "%d rows in one request, over the declared maximum of %d. A batch "
            "half scored is worse than a batch refused: fd_scores would carry "
            "the half." % (len(rows), MAX_BATCH_ROWS))
    frame = frame_for(rows, scorer)
    raw = scorer.scores(frame)
    decisions = []
    for row, value in zip(rows, raw):
        rounded = round(float(value), score.SCORE_SCALE)
        decisions.append({"txn_id": row["txn_id"],
                          "score": rounded,
                          "flagged": scorer.flagged(rounded)})
    return {"model_version": scorer.model_version,
            "recipe_digest": scorer.recipe_digest,
            "threshold": scorer.threshold,
            "decisions": decisions}


def sentence(decision: dict, row: dict) -> str:
    """The decision as a sentence, which is what B4-EC15 asks for.

    A SENTENCE AND NOT A NUMBER, and the requirement is about who reads it:
    "restituer la sortie du modele sous une forme exploitable par une
    technologie d'assistance". A score of 0.94 beside a threshold of 0.93 is a
    comparison the reader has to make; a screen reader announces this.

    THE FIGURES STAY IN THE RESPONSE BESIDE IT. The sentence is an addition,
    never a replacement: a caller that needs the number must not have to parse
    French to find it.

    Block: B4.
    Lot: B4-L5.
    Competency: B4-C2.
    Covers: B4-EC15.
    """
    verdict = ("signalee pour verification"
               if decision["flagged"] else "acceptee sans signalement")
    history = ("aucune transaction anterieure pour ce porteur"
               if not row.get("f_tx_count")
               else "%d transaction(s) anterieure(s) pour ce porteur"
                    % int(row["f_tx_count"]))
    return ("Transaction %s. Score de fraude %.4f pour un seuil de decision "
            "de %.4f, par le modele en version %s. %s."
            % (verdict, decision["score"], decision["threshold"],
               decision["model_version"], history))


def record(conn: "psycopg.Connection", call: dict, row: dict,
           decision: dict) -> dict:
    """Write the call and its answer down. ANSWER, THEN RECORD.

    The same order as fd_frauds, fd_ops_state and fd_retrain_requests, for the
    reason sql/ddl/100_ops_state.sql states: the record follows the deed, so a
    crash between the two understates what happened rather than claiming
    something that did not. A caller who received a decision that was not
    recorded is a gap in the journal; a journal entry for a decision nobody
    received is a lie.
    """
    rows, names = db.query_file_columns(
        conn, RECORD_SQL,
        txn_ts=call["txn_ts"], client_key=call["client_key"],
        merchant=call["merchant"], category=call["category"],
        amt=call["amt"], f_tx_count=row.get("f_tx_count"),
        f_avg_amount=row.get("f_avg_amount"),
        f_home_distance_km=call["f_home_distance_km"],
        model_version=decision["model_version"],
        recipe_digest=decision["recipe_digest"],
        score=decision["score"], threshold=decision["threshold"],
        flagged=decision["flagged"])
    return dict(zip(names, rows[0]))


def health(scorer: score.Scorer | None, problem: str | None = None) -> dict:
    """What the health endpoint announces, including the version served.

    THE VERSION IS THE POINT, which is what B4-EC21 asks for: a service that
    answers "ok" without saying what it serves cannot be distinguished from
    one that has been serving last month's model since a promotion nobody
    noticed. score.load() resolves the alias before its cache, so the version
    reported here is the one the next call will use.

    DEGRADED AND NOT DOWN when no model can be loaded. The service is up, the
    database may be reachable, and what is missing is a champion -- which is an
    ordinary state before the first promotion and a different problem from a
    process that has stopped.

    Block: B4.
    Lot: B4-L5.
    Competency: B4-C3.
    Covers: B4-EC21.
    """
    if scorer is None:
        return {"state": "degraded", "model_version": None,
                "recipe_digest": None,
                "detail": problem or "aucun modele charge"}
    return {"state": "ok",
            "model_version": scorer.model_version,
            "recipe_digest": scorer.recipe_digest,
            "features": list(scorer.features),
            "threshold": scorer.threshold,
            "detail": "modele en service"}
