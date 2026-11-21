-- Every call to the scoring API, and the answer it received. Covers: EC19, EC17
-- Lot: L5 (ajoute le 20/11/2026)   Competency: C3, C4
--
-- WHY NOT fd_scores, which already holds a score, a threshold and a decision.
-- Its primary key is txn_id and it carries
-- REFERENCES fd_transactions (txn_id): it can only record a score for a
-- transaction the pipeline has already ingested and validated. An API call is
-- the opposite case -- a payment the caller is asking about, which the
-- pipeline has never seen and may never see. The foreign key would refuse
-- every honest call.
--
-- SO THE TWO TABLES ANSWER TWO QUESTIONS. fd_scores: what did the batch decide
-- about the transactions we hold? fd_scoring_service_calls: what did the service answer to
-- whoever asked? A single table would have to drop the foreign key, and with
-- it the guarantee that every scored transaction exists.
--
-- WHAT IS KEPT OF THE INPUT, and EC17 decides it, not convenience. The caller
-- sends a cardholder number and a home location; neither reaches this table.
--   client_key          : the HMAC of the card number with the pepper, the same
--                         pseudonym ml4fd/transform.py writes to
--                         fd_transactions -- so a call and a transaction of the
--                         same cardholder can be joined without either holding
--                         the number.
--   f_home_distance_km  : a distance, which describes a circle around the
--                         merchant and not a point. The home coordinates are
--                         read, consumed, and never written.
-- The merchant, the category and the amount are the payment's own fields and
-- carry no identity.
--
-- THE FEATURES ARE STORED BESIDE THE SCORE, and that is what makes a decision
-- re-derivable months later. The model version says WHICH model answered; the
-- features say WHAT it was shown. Without them a disputed decision can only be
-- re-run against today's history, which has moved.
--
-- APPEND-ONLY, like fd_promotions, fd_monitoring and fd_retrain_requests: a
-- record of what the service answered is worth nothing if it can be edited
-- afterwards.

CREATE TABLE IF NOT EXISTS fd_scoring_service_calls (
    call_id        bigserial    PRIMARY KEY,
    called_at      timestamptz  NOT NULL DEFAULT now(),
    -- The instant the PAYMENT carries, which is not the instant of the call:
    -- a caller may ask about a payment of five minutes ago, and the history
    -- features are computed with a strict bound on this one.
    txn_ts         timestamptz  NOT NULL,
    client_key     text         NOT NULL,
    merchant       text         NOT NULL,
    category       text         NOT NULL,
    amt            numeric(12,2) NOT NULL,
    f_tx_count     bigint,
    f_avg_amount   numeric(12,2),
    f_home_distance_km numeric(8,2),
    model_version  text         NOT NULL,
    recipe_digest  text         NOT NULL,
    score          numeric(9,8) NOT NULL CHECK (score >= 0 AND score <= 1),
    threshold      numeric(9,8) NOT NULL CHECK (threshold > 0 AND threshold < 1),
    flagged        boolean      NOT NULL,
    -- THE SAME INVARIANT AS fd_scores, and for the same measured reason: the
    -- comparison happens once in Python on a float and once here on the stored
    -- numeric. ml4fd/score.py rounds to SCORE_SCALE before comparing so the
    -- two answers cannot differ -- see its note on 0.929999996.
    CONSTRAINT fd_scoring_service_calls_decision_follows_the_score
        CHECK (flagged = (score >= threshold))
);

-- THE ANSWER IS READ BY PERIOD, never by call_id: the dashboard and the
-- reports ask "what did the service answer yesterday", and a scan of an
-- append-only table that grows with every payment is not an answer.
CREATE INDEX IF NOT EXISTS fd_scoring_service_calls_by_instant
    ON fd_scoring_service_calls (called_at DESC);

-- And by cardholder, which is how a disputed decision is found again.
CREATE INDEX IF NOT EXISTS fd_scoring_service_calls_by_client
    ON fd_scoring_service_calls (client_key, txn_ts DESC);

GRANT SELECT, INSERT ON fd_scoring_service_calls TO fd_scoring_service;
GRANT USAGE, SELECT ON SEQUENCE fd_scoring_service_calls_call_id_seq TO fd_scoring_service;
GRANT SELECT ON fd_scoring_service_calls TO fd_report;
GRANT SELECT ON fd_scoring_service_calls TO fd_dashboard;
REVOKE UPDATE, DELETE, TRUNCATE ON fd_scoring_service_calls FROM fd_scoring_service;

-- AND THE ONE REVOCATION THAT COULD NOT LIVE WITH THE ROLE. 145 declares
-- everything this service may not touch, but fd_monitoring is created by
-- 170_monitoring.sql -- after 145 runs -- so naming it there would fail on
-- every fresh database. It is revoked here, where the table exists, and said
-- out loud for the same reason as the rest of that list: so a later hand does
-- not grant it without meeting the argument. The monitoring belongs to the
-- loop, over a window; a caller asking about one payment has no business in
-- it.
REVOKE ALL ON fd_monitoring FROM fd_scoring_service;
