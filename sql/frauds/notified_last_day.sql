-- Notifications that went out in the last 24 hours, and what is waiting.
-- Block: B3, B4.
-- Lot: B3-N5, B4-L0.
-- Competency: B3-C4.
-- Covers: B3-EI05, B3-EC09.
--
-- This is what turns alert_capacity_per_day from a figure used once to pick a
-- threshold into a limit that is ENFORCED. Until N5 nothing applied it: a bad
-- retrain or a misplaced threshold would have drowned the analyst, which is
-- precisely what the capacity policy exists to prevent.
--
-- A ROLLING window and not a calendar day: a calendar day would let 60 go out
-- at 23:50 and 60 more at 00:10.

SELECT (SELECT count(*) FROM fd_frauds
         WHERE notified_at > now() - interval '24 hours')  AS notified_24h,
       (SELECT count(*) FROM fd_frauds
         WHERE notified_at IS NULL)                        AS pending,
       (SELECT count(*) FROM fd_frauds
         WHERE notified_at IS NULL AND attempts > 0)       AS pending_failed,
       (SELECT count(*) FROM fd_frauds)                    AS total;
