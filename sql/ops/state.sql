-- The recorded state of each control. A control absent from the table is 'ok'.
-- Covers: EC14   Lot: N9   Competency: C4
SELECT control, state, since, detail
  FROM fd_ops_state;
