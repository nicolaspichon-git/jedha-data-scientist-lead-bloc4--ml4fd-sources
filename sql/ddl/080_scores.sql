-- The score of a transaction, and the model that produced it.
-- Block: B3, B4.
-- Lot: B3-N4, B4-L0.
-- Competency: B3-C2, B3-C4.
-- Covers: B3-EE09, B3-EI04, B3-EC24.
--
-- THE PRIMARY KEY IS (txn_id, model_version), and that is the whole design.
--
-- A key on txn_id alone would make a retrain overwrite the score the previous
-- model gave, and the first question of the first incident is "which model let
-- this one through?". Keeping one row per model version answers it after the
-- weekly retrain has already replaced the model -- which is exactly when the
-- question gets asked. It also makes the comparison of two versions on the same
-- traffic a query rather than an archaeology exercise.
--
-- threshold is stored WITH the score, not looked up later: the threshold is a
-- property of the model version that was chosen for it, and governance.yml
-- holds the policy, not the value. A score of 0.91 means nothing without the
-- number it was compared against.
--
-- flagged is stored rather than derived on read. It is redundant with
-- score >= threshold by construction, and the CHECK below makes the redundancy
-- safe: the column records the decision that was actually taken, so a later
-- change of comparison -- strict instead of inclusive, say -- cannot silently
-- rewrite history.
--
-- NO verdict here. fd_ground_truth stays the only place a label lives (F02.3),
-- and N6 is where the four guarantees against reading it too early are stated.

CREATE TABLE IF NOT EXISTS fd_scores (
    txn_id        text         NOT NULL REFERENCES fd_transactions (txn_id),
    model_version text         NOT NULL,
    score         numeric(9,8) NOT NULL CHECK (score >= 0 AND score <= 1),
    threshold     numeric(9,8) NOT NULL CHECK (threshold > 0 AND threshold < 1),
    flagged       boolean      NOT NULL,
    recipe_digest text         NOT NULL,
    scored_at     timestamptz  NOT NULL DEFAULT now(),

    CONSTRAINT fd_scores_pkey PRIMARY KEY (txn_id, model_version),
    -- The stored decision must be the one the stored numbers imply. Without
    -- this, a bug in the comparison would be invisible in the data.
    --
    -- It constrains the writer as well as the data: the comparison happens in
    -- Python before it happens here, on a float rather than on numeric(9,8).
    -- Measured, 0.929999996 against a threshold of 0.93 is False in Python and
    -- True after rounding to this scale -- so ml4fd/score.py rounds to
    -- SCORE_SCALE = 8 before comparing, and both sides then see one number.
    CONSTRAINT fd_scores_decision_matches
        CHECK (flagged = (score >= threshold))
);

-- The morning report of S4 reads a day of scores; the alert table of N5 reads
-- the flagged ones. Both scan on time, so the index is on time.
CREATE INDEX IF NOT EXISTS fd_scores_scored_at_idx ON fd_scores (scored_at);
CREATE INDEX IF NOT EXISTS fd_scores_flagged_idx
    ON fd_scores (scored_at) WHERE flagged;

-- No UPDATE and no DELETE for the ingesting role: a score is an observation of
-- what a model said at one instant, and an observation is not amended. Same
-- reasoning as the revocation on fd_raw_events.
GRANT SELECT, INSERT ON fd_scores TO fd_ingest;
GRANT SELECT ON fd_scores TO fd_report;
REVOKE UPDATE, DELETE, TRUNCATE ON fd_scores FROM fd_ingest;
