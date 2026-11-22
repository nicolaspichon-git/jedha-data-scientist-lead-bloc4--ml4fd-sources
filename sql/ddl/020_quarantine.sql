-- Quarantine, with a reason.
-- Block: B3, B4.
-- Lot: B3-L2, B4-L0.
-- Competency: B3-C5.
-- Covers: B3-EC22.
--
-- reason_class makes the separation between the two classes of check required by
-- EC20 and EC21 visible IN THE DATA: 'field' for per-field validation, 'record'
-- for cross-field coherence within one record. F07.3 reports the breakdown over
-- this column.

CREATE TABLE IF NOT EXISTS fd_quarantine (
    quarantine_id bigserial   PRIMARY KEY,
    batch_id      uuid        NOT NULL,
    line_no       integer     NOT NULL,
    reason_class  text        NOT NULL CHECK (reason_class IN ('field', 'record')),
    reason_code   text        NOT NULL,
    reason_detail text,
    payload       jsonb       NOT NULL,
    rejected_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (batch_id, line_no, reason_code)
);

CREATE INDEX IF NOT EXISTS fd_quarantine_reason_idx
    ON fd_quarantine (reason_class, reason_code);
