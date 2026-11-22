-- The recorded state of each control. A control absent from the table is 'ok'.
-- Block: B3, B4.
-- Lot: B3-N9, B4-L0.
-- Competency: B3-C4.
-- Covers: B3-EC14.
SELECT control, state, since, detail
  FROM fd_ops_state;
