-- Raw zone. The payload exactly as received, one row per source line.
-- Block: B3, B4.
-- Lot: B3-L1, B4-L0.
-- Competency: B3-C2.
-- Covers: B3-EC24.
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
