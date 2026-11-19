-- One alert notified. Written AFTER the channel accepted it.
-- Covers: EI05, EC23   Lot: N5   Competency: C3
--
-- THE ORDER IS THE DECISION. Send, then record. With an external channel the
-- two cannot be made atomic, so the choice is which failure to prefer: a crash
-- between them re-sends the alert on the next run, and the message carries
-- txn_id as an idempotency key so a receiver can discard the duplicate. The
-- reverse order would lose a fraud notification in silence while the table
-- claimed it had gone out.
--
-- attempts is incremented here too: a successful notification is also an
-- attempt, and the constraint fd_frauds_notified_implies_attempted requires it.

UPDATE fd_frauds
SET    notified_at = now(),
       channel     = %(channel)s,
       attempts    = attempts + 1,
       last_error  = NULL
WHERE  txn_id = %(txn_id)s
  AND  notified_at IS NULL;
