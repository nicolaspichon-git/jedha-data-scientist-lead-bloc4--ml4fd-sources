-- Record what a purge did to one store.
-- Block: B3, B4.
-- Lot: B3-Q6, B4-L0.
-- Competency: B3-C4.
-- Covers: B3-EC14, B3-EC18.
--
-- AFTER THE DELETION, like every other journal of this project and for the
-- same reason turned around: recording first would claim a purge that may not
-- happen, and nothing would ever correct the claim. Recording after can leave
-- rows deleted without a journal line -- which the acceptance of S5 CATCHES,
-- by recomputing the oldest row of each store. One failure is silent, the
-- other is loud.
--
-- Parameters: run_id, as_of, cutoff, before_date, store, n_batches,
--             n_deleted, n_refused, n_resumed, share, batch_id
--             (batch_id null for a retention purge: Q7).

INSERT INTO fd_purges (run_id, as_of, cutoff, before_date, store,
                       n_batches, n_deleted, n_refused, n_resumed, share,
                       batch_id)
VALUES (%(run_id)s, %(as_of)s, %(cutoff)s, %(before_date)s, %(store)s,
        %(n_batches)s, %(n_deleted)s, %(n_refused)s, %(n_resumed)s,
        %(share)s, %(batch_id)s);
