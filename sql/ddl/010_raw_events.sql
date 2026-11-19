-- Raw zone. The payload exactly as received, one row per source line.
-- Covers: EC24   Lot: L1   Competency: C2
--
-- The (batch_id, line_no) uniqueness carries insert idempotency: re-inserting a
-- batch that is already loaded cannot produce duplicates.

CREATE TABLE IF NOT EXISTS fd_raw_events (
    raw_id      bigserial   PRIMARY KEY,
    batch_id    uuid        NOT NULL,
    source      text        NOT NULL,
    line_no     integer     NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    payload     jsonb       NOT NULL,
    UNIQUE (batch_id, line_no)
);

CREATE INDEX IF NOT EXISTS fd_raw_events_batch_idx ON fd_raw_events (batch_id);
