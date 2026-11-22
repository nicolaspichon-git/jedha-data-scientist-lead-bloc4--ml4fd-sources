-- The journal of the promotions: one row per decision, promoted or refused.
-- Block: B4.
-- Lot: B4-L3.
-- Competency: B4-C5.
-- Covers: B4-EC34, B4-EC35, B4-EC37.
--
-- WHY A TABLE AND NOT A LOG LINE. A promotion changes what the system serves,
-- and a refusal explains why it did not. Both have to be answerable months
-- later -- to a jury, to an auditor, to the next person who wonders why
-- version 7 is in production and version 8 is not. A log that rotates answers
-- neither.
--
-- WHAT IS RECORDED IS THE DECISION, NOT THE MODEL. The chain from a model back
-- to the rows it was fitted on already lives in MLflow and in run.json
-- (ml4fd/registry.py). This table holds the one thing neither of them knows:
-- that somebody, or something, decided to serve it -- and on what grounds.
--
-- THE POLICY IN FORCE IS WRITTEN INTO EACH ROW, in alert_capacity and
-- significance. governance.yml can be changed; a decision taken under the old
-- numbers must stay readable as such, or the journal becomes a set of verdicts
-- nobody can re-derive.
--
-- APPEND ONLY. fd_ingest may insert and nothing else: the process that
-- promotes cannot rewrite the record of what it promoted. The same reasoning
-- as on fd_purges.
--
-- Replayable: `init` plays every file of sql/ddl on an existing database.

CREATE TABLE IF NOT EXISTS fd_promotions (
    promotion_id      bigserial   PRIMARY KEY,
    -- The Airflow run, when there was one. Null when a human ran it by hand
    -- (tools/promote.py), which is a fact worth keeping rather than hiding
    -- behind an invented identifier. Same choice as fd_purges.run_id.
    run_id            text,
    decided_at        timestamptz NOT NULL DEFAULT now(),

    -- The version the decision was ABOUT, and the recipe it came from.
    candidate_version text        NOT NULL,
    candidate_digest  text,
    -- The version that held the alias when the decision was taken. Null means
    -- there was no champion -- the unopposed case, which is a different
    -- statement from having won.
    champion_before   text,

    decision          text        NOT NULL,
    -- In words, and meant to be read: "champion catches 14 frauds the
    -- candidate misses, against 3 the other way, p = 0.013" says more than a
    -- boolean ever will.
    reason            text        NOT NULL,

    -- WHAT DECIDED, side by side. Recalls are kept even though the comparison
    -- does not rest on them: they are what a reader looks for first, and their
    -- absence from the journal would send that reader to another table.
    candidate_recall  numeric(6,4) CHECK (candidate_recall IS NULL
                                          OR (candidate_recall >= 0
                                              AND candidate_recall <= 1)),
    champion_recall   numeric(6,4) CHECK (champion_recall IS NULL
                                          OR (champion_recall >= 0
                                              AND champion_recall <= 1)),
    -- The volume the candidate would produce, against the ceiling that applied.
    alerts_per_day    numeric(9,2) CHECK (alerts_per_day IS NULL
                                          OR alerts_per_day >= 0),
    alert_capacity    integer     NOT NULL CHECK (alert_capacity > 0),

    -- THE TWO DISCORDANT CELLS and the exact p-value of the paired test
    -- (ml4fd/paired.py). The agreements are deliberately not stored: they
    -- carry no information about the difference, and storing them would invite
    -- somebody to recompute an aggregate from this table.
    only_candidate    integer     CHECK (only_candidate IS NULL
                                         OR only_candidate >= 0),
    only_champion     integer     CHECK (only_champion IS NULL
                                         OR only_champion >= 0),
    p_value           numeric(12,10) CHECK (p_value IS NULL
                                            OR (p_value >= 0 AND p_value <= 1)),
    significance      numeric(6,4) NOT NULL CHECK (significance > 0
                                                   AND significance < 1),

    CONSTRAINT fd_promotions_decision_is_known
        CHECK (decision IN ('promoted', 'refused')),
    -- A decision taken with no champion cannot carry a comparison: there was
    -- nothing to compare against, and a p-value here would be invented.
    CONSTRAINT fd_promotions_unopposed_carries_no_comparison
        CHECK (champion_before IS NOT NULL
               OR (p_value IS NULL AND only_candidate IS NULL
                   AND only_champion IS NULL AND champion_recall IS NULL)),
    -- A comparison is the two cells AND its p-value, or none of the three.
    CONSTRAINT fd_promotions_comparison_is_whole
        CHECK ((p_value IS NULL AND only_candidate IS NULL
                AND only_champion IS NULL)
               OR (p_value IS NOT NULL AND only_candidate IS NOT NULL
                   AND only_champion IS NOT NULL)),
    -- A candidate cannot be promoted over itself.
    CONSTRAINT fd_promotions_candidate_is_not_the_champion
        CHECK (champion_before IS NULL OR champion_before <> candidate_version)
);

CREATE INDEX IF NOT EXISTS fd_promotions_decided_at_idx
    ON fd_promotions (decided_at);
CREATE INDEX IF NOT EXISTS fd_promotions_candidate_idx
    ON fd_promotions (candidate_version, decided_at);

GRANT SELECT, INSERT ON fd_promotions TO fd_ingest;
GRANT USAGE, SELECT ON SEQUENCE fd_promotions_promotion_id_seq TO fd_ingest;
GRANT SELECT ON fd_promotions TO fd_report;
REVOKE UPDATE, DELETE, TRUNCATE ON fd_promotions FROM fd_ingest;
