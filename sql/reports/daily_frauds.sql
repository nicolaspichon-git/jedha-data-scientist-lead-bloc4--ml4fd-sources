-- Section 2 of the morning report: yesterday's alerts, on the PAYMENT axis.
-- Block: B3, B4.
-- Lot: B3-P1, B4-L0.
-- Competency: B3-C2.
-- Covers: B3-EE04, B3-EI08, B3-EC17.
--
-- ORDERED BY EXPECTED LOSS, score x amount -- decided in P0 and written into
-- the document, because an analyst who reviews the first 38 of a list of 60
-- must know what he sorted on. Both factors are returned, so the order is
-- checkable line by line rather than taken on trust.
--
-- AND A PREDICTION THIS QUERY MAKES MEASURABLE. Every alert is above the
-- threshold by construction, so the score band is narrow while the amounts
-- span two orders of magnitude: the order will follow the amount for the most
-- part. Returning score, amount and the product separately is what lets that be
-- confirmed or denied on figures after a few weeks, instead of argued.
--
-- times_usual IS THE INTERPRETABLE NUMBER, decided in P0: "this payment is 16.9
-- times the cardholder's habit" reads, where two raw columns ask the reader to
-- divide forty times a morning. f_avg_amount is null for a cardholder's FIRST
-- transaction and the guard is nullif, not a coalesce to zero -- an unknown
-- habit must read as unknown, not as a ratio of zero.
--
-- NO NAME, NO ADDRESS, NO CARD NUMBER, and not by discipline: fd_transactions
-- holds no identifying column at all (EC17, decided in S1), so this query
-- cannot leak one even by mistake. The merchant and the category are triage
-- facts; the cardholder appears only through aggregates of his own history.

SELECT a.txn_id,
       t.txn_ts,
       t.merchant,
       t.category,
       t.amt,
       a.score,
       a.threshold,
       a.model_version,
       a.raised_at,
       a.notified_at IS NOT NULL                        AS notified,
       a.notified_at,                                  -- read by the dashboard (S6/R11)
       a.channel,
       a.attempts,
       a.last_error,                                   -- read by the dashboard (S6/R3)
       t.f_tx_count,
       t.f_avg_amount,
       round(t.amt / nullif(t.f_avg_amount, 0), 1)      AS times_usual,
       round(a.score * t.amt, 2)                        AS expected_loss
  FROM fd_frauds a
  JOIN fd_transactions t ON t.txn_id = a.txn_id
 WHERE t.txn_ts >= %(window_start)s
   AND t.txn_ts <  %(window_end)s
 -- txn_id breaks ties, so the order is total and the document reproducible.
 ORDER BY expected_loss DESC, a.txn_id;
