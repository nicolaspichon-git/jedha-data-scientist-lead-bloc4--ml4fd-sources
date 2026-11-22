"""B4-L5: the scoring service. `gunicorn app:app`, behind compose.

Block: B4.
Lot: B4-L5.
Competency: B4-C3.
Covers: B4-EC15, B4-EC16, B4-EC17, B4-EC19, B4-EC20, B4-EC21, B4-EC23.

THIN ON PURPOSE. Every decision lives in ml4fd/serve.py, where pytest can hold
it without a web framework; this file routes, declares the request and the
response, and chooses the status code. The dashboard is arranged the same way
and for the same reason.

THE CONTRACT IS PUBLISHED BY THE FRAMEWORK, which is what B4-EC17 asks for:
FastAPI derives an OpenAPI document from the models below and serves it at
/openapi.json, with a readable rendering at /docs. So the specification cannot
describe a route that does not exist or a field the service does not read --
it is generated from the code that answers, not written beside it.

THE CHAMPION IS RESOLVED BY THE SAME PATH AS B4-L3. score.load() reads the alias
from the registry BEFORE consulting its cache, so a promotion is picked up on
the next call without this service having to be restarted or to remember
anything. That is the defect the plan calls "l'alias qui ne reveille pas le
cache", and it is answered in score.py rather than here.
"""
from __future__ import annotations

import logging
import os
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from ml4fd import config, db, score, serve

logging.basicConfig(level=logging.INFO,
                    format="%(levelname)s %(name)s %(message)s")
log = logging.getLogger("scoring_service")

TITLE = "ML4FD : Fraud Scoring Service"
VERSION = "1.0.0"

app = FastAPI(
    title=TITLE,
    version=VERSION,
    description=("Score une transaction de paiement et dit si elle doit etre "
                 "signalee. Le modele servi est celui qui porte l'alias "
                 "champion dans le registre MLflow ; /health dit lequel."),
)


class Payment(BaseModel):
    """One payment to score. The fields are the producer's own, so a caller
    can forward a record of the stream unchanged."""

    cc_num: str = Field(description="Numero de carte. Jamais conserve : "
                                    "pseudonymise des la reception.")
    txn_ts: str = Field(description="Instant du paiement, ISO 8601 AVEC "
                                    "fuseau. Un horodatage naif est refuse.")
    merchant: str = Field(description="Nom du commercant.")
    category: str = Field(description="Categorie du commercant.")
    amt: str = Field(description="Montant, en chaine pour eviter toute "
                                 "arrondi flottant a la reception.")
    merch_lat: str = Field(description="Latitude du commercant.")
    merch_long: str = Field(description="Longitude du commercant.")
    home_lat: str = Field(description="Latitude du domicile. Consommee pour "
                                      "une distance, jamais conservee.")
    home_long: str = Field(description="Longitude du domicile. Idem.")


class Decision(BaseModel):
    """The answer. The figures and the sentence sit side by side: a caller that
    needs the number must not have to parse French, and a reader using an
    assistive technology must not have to compare two numbers (B4-EC15)."""

    call_id: int
    decision: str
    flagged: bool
    score: float
    threshold: float
    model_version: str
    recipe_digest: str
    texte: str


class BatchRow(BaseModel):
    """One already-prepared row. The pipeline holds a pseudonymised
    transaction with its features computed at ingestion, never a card number:
    it CANNOT build a Payment, and sending raw records over HTTP would make
    B4-EC17 worse rather than better. Hence a second entry point."""

    txn_id: str = Field(description="Identifiant de la transaction.")
    features: dict[str, Any] = Field(
        description="Les features deja calculees, par nom. Le service "
                    "selectionne celles que la recette du champion declare, "
                    "donc l'appelant n'a pas a connaitre la recette et un "
                    "changement de recette ne change pas ce contrat.")


class Batch(BaseModel):
    rows: list[BatchRow] = Field(description="Les lignes a scorer.")


class BatchDecision(BaseModel):
    txn_id: str
    score: float
    flagged: bool


class BatchAnswer(BaseModel):
    """The model identity is reported ONCE for the whole batch: fd_scores
    records it per row and they must all agree."""

    model_version: str
    recipe_digest: str
    threshold: float
    decisions: list[BatchDecision]


def _settings():
    return config.load()


def _scorer():
    """The model in service, or None with the reason. Never raises.

    /health must answer while no champion carries the alias -- an ordinary
    state before the first promotion -- so the failure is returned rather than
    thrown.
    """
    settings = _settings()
    try:
        return score.load(
            settings, tracking_uri=os.environ.get("MLFLOW_TRACKING_URI")), None
    except Exception as problem:            # LookupError, mlflow's own kin
        return None, str(problem)


@app.get("/health", summary="Etat du service et version du modele servi")
def health() -> dict:
    """
    Block: B4.
    Lot: B4-L5.
    Competency: ?.
    Covers: B4-EC21.
    """
    scorer, problem = _scorer()
    return serve.health(scorer, problem)


@app.get("/", include_in_schema=False)
def root() -> dict:
    return {"service": TITLE, "version": VERSION,
            "contrat": "/openapi.json", "documentation": "/docs",
            "etat": "/health"}


@app.post("/score", response_model=Decision,
          summary="Score un paiement et dit s'il doit etre signale",
          responses={422: {"description": "Requete non conforme : la reponse "
                                          "nomme chaque champ en cause."},
                     503: {"description": "Aucun modele en service."}})
def post_score(payment: Payment) -> JSONResponse:
    """
    Block: B4.
    Lot: B4-L5.
    Competency: B4-C2, B4-C3.
    Covers: B4-EC15, B4-EC16, B4-EC19, B4-EC20.
    """
    settings = _settings()
    payload = payment.model_dump()

    # VALIDATED BY US AND NOT ONLY BY THE SCHEMA. pydantic checks shapes; B4-EC20
    # asks for an explicit error, and "amt: string expected" does not say that
    # fifty thousand is the maximum the pipeline accepts. Every problem is
    # returned at once -- see serve.problems.
    found = serve.problems(payload, settings.max_amount)
    if found:
        return JSONResponse(status_code=422,
                            content={"detail": found})

    scorer, problem = _scorer()
    if scorer is None:
        return JSONResponse(status_code=503,
                            content={"detail": serve.health(None, problem)})

    call = serve.anonymous(payload, settings.pepper)
    with db.connect("fd_scoring_service", settings) as conn:
        row = serve.features(conn, call)
        decision = serve.answer(scorer, row)
        # ANSWER, THEN RECORD -- the order the whole project uses. The commit
        # happens before the response is built, so a call that was answered
        # was recorded; the reverse would journal a decision nobody received.
        written = serve.record(conn, call, row, decision)
        conn.commit()

    body = dict(decision)
    body["call_id"] = int(written["call_id"])
    body["decision"] = ("signalee" if decision["flagged"] else "acceptee")
    body["texte"] = serve.sentence(decision, row)
    return JSONResponse(status_code=200, content=body)


@app.post("/score/batch", response_model=BatchAnswer,
          summary="Score un lot de lignes deja preparees",
          responses={422: {"description": "Lot vide, trop grand, ou lignes "
                                          "ne portant pas les features de la "
                                          "recette en service."},
                     503: {"description": "Aucun modele en service."}})
def post_score_batch(batch: Batch) -> JSONResponse:
    """The pipeline's entry point.
    Block: B4.
    Lot: B4-L5.
    Competency: ?.
    Covers: B4-EC16, B4-EC19.

    WHY IT EXISTS BESIDE /score. fd_transactions holds no cc_num and no home
    location -- transform.py consumes them at the raw boundary and never writes
    them to the exploited layer -- so dag_ingest cannot build a Payment. The
    two callers do not have the same input: a third party holds a raw payment,
    the pipeline holds a prepared row.

    NOTHING IS JOURNALLED HERE. The pipeline records its own decisions in
    fd_scores, which carries the foreign key that proves the transaction
    exists. See ml4fd/serve.batch_answer for the argument, and
    sql/ddl/195_scoring_decisions.sql for the view that reads both journals as
    one.
    """
    scorer, problem = _scorer()
    if scorer is None:
        return JSONResponse(status_code=503,
                            content={"detail": serve.health(None, problem)})
    rows = [{"txn_id": row.txn_id, **row.features} for row in batch.rows]
    try:
        answer = serve.batch_answer(scorer, rows)
    except ValueError as refused:
        # A SHAPE WE REFUSE, not a failure: an empty batch, one over the
        # declared maximum, or rows missing a feature the recipe needs. 422
        # rather than 500, so a caller can tell a bad request from a broken
        # service.
        return JSONResponse(status_code=422, content={"detail": str(refused)})
    log.info("lot de %d ligne(s) score par la version %s, seuil %.2f",
             len(answer["decisions"]), answer["model_version"],
             answer["threshold"])
    return JSONResponse(status_code=200, content=answer)


@app.exception_handler(Exception)
def unexpected(request: Request, problem: Exception) -> JSONResponse:
    """A failure the service did not foresee, said rather than leaked.

    The message is LOGGED and not returned: a driver's error names the host,
    the port and the role, and a caller has no use for any of them. The status
    is what a caller acts on.
    """
    log.exception("echec non prevu sur %s", request.url.path)
    return JSONResponse(
        status_code=500,
        content={"detail": "echec interne du service de scoring. "
                           "Le detail est dans le journal du conteneur."})
