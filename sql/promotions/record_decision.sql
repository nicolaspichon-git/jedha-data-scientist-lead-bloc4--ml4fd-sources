-- Record a promotion decision, promoted or refused.
-- Block: B4.
-- Lot: B4-L3.
-- Competency: B4-C5.
-- Covers: B4-EC34, B4-EC37.
--
-- BOTH OUTCOMES, and that is the point of the table: a journal of promotions
-- alone answers "what is in production" and never "why version 8 is not".
--
-- AFTER THE ALIAS IS SET, like every other journal of this project: recording
-- first would claim a promotion that may not happen. A promotion left without
-- a journal line is caught by comparing the alias with the last promoted row;
-- a journal line without a promotion would be a lie nothing corrects.
--
-- Parameters: run_id, candidate_version, candidate_digest, champion_before,
--             decision, reason, candidate_recall, champion_recall,
--             alerts_per_day, alert_capacity, only_candidate, only_champion,
--             p_value, significance

INSERT INTO fd_promotions (run_id, candidate_version, candidate_digest,
                           champion_before, decision, reason,
                           candidate_recall, champion_recall, alerts_per_day,
                           alert_capacity, only_candidate, only_champion,
                           p_value, significance)
VALUES (%(run_id)s, %(candidate_version)s, %(candidate_digest)s,
        %(champion_before)s, %(decision)s, %(reason)s,
        %(candidate_recall)s, %(champion_recall)s, %(alerts_per_day)s,
        %(alert_capacity)s, %(only_candidate)s, %(only_champion)s,
        %(p_value)s, %(significance)s);
