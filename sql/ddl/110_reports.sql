-- The journal of the morning reports: one row per covered day.
-- Block: B3, B4.
-- Lot: B3-P3, B4-L0.
-- Competency: B3-C2, B3-C4.
-- Covers: B3-EE04, B3-EC14, B3-EC23.
--
-- A WRONG REPORT IS SEEN. AN ABSENT ONE IS NOT. The analyst who finds no
-- document in his inbox concludes there was nothing to report, which is the
-- worst reading available. This table is what turns that silence into a
-- question: one row per day, and sql/checks/s4_absence.sql lists the days
-- without one. Without it the silent failure of S4 is undetectable, and that
-- is the direct application of EC14 -- detect the absence.
--
-- THE PRIMARY KEY ON report_date IS THE IDEMPOTENCE. A replay of the 3rd of
-- November REPLACES the row of the 3rd of November; it cannot add a second.
-- The document is byte-identical on a replay (ml4fd/report.py carries no
-- production timestamp), so the only column a replay really changes is
-- produced_at -- and that is the one column whose change is meaningless.
--
-- content_digest LETS A FILE BE CHECKED AGAINST THE ROW THAT DESCRIBES IT.
-- The same reasoning as run.json beside a dataset in M8, and for the same
-- reason: a registry that describes what is not there is a registry nobody can
-- trust. P5 compares the digest of the file on disk with this column.
--
-- WHY fd_report MAY WRITE HERE, and it is the only table it may write. The
-- property of that role is that it has NO ACCESS TO THE RAW ZONE -- the raw
-- payloads still carry names, addresses and card numbers -- and not that it
-- writes nothing. The journal of the report belongs to the report, the same way
-- fd_ingest is granted UPDATE on the notification columns of fd_frauds and on
-- nothing else.

CREATE TABLE IF NOT EXISTS fd_reports (
    report_date    date         PRIMARY KEY,
    knowledge_date timestamptz  NOT NULL,
    produced_at    timestamptz  NOT NULL DEFAULT now(),
    path           text         NOT NULL,
    n_payments     bigint       NOT NULL CHECK (n_payments >= 0),
    n_alerts       bigint       NOT NULL CHECK (n_alerts >= 0),
    n_verdicts     bigint       NOT NULL CHECK (n_verdicts >= 0),
    n_pending      bigint       NOT NULL CHECK (n_pending >= 0),
    -- Null on a day that raised no alert: there is then no version and no
    -- threshold to name, and inventing one would describe a model that did
    -- nothing. Several versions in one day travel as a comma-separated list,
    -- which is itself a finding -- the model changed during the day.
    model_version  text,
    threshold      numeric(9,8) CHECK (threshold IS NULL
                                       OR (threshold > 0 AND threshold < 1)),
    content_digest text         NOT NULL,

    -- The alerts and the pending transactions are both subsets of the day's
    -- payments. A count that breaks this did not come from the same window.
    CONSTRAINT fd_reports_alerts_within_payments
        CHECK (n_alerts <= n_payments),
    CONSTRAINT fd_reports_pending_within_payments
        CHECK (n_pending <= n_payments),
    -- A day with alerts names the model that raised them, and a day without
    -- cannot. Without this, a row could claim alerts produced by nothing.
    CONSTRAINT fd_reports_version_with_alerts
        CHECK ((n_alerts = 0) = (model_version IS NULL)),
    CONSTRAINT fd_reports_threshold_with_alerts
        CHECK ((n_alerts = 0) = (threshold IS NULL)),
    -- sha256, lower case hexadecimal. A truncated or upper-case digest would
    -- never match the file and the mismatch would read as a corrupted report.
    CONSTRAINT fd_reports_digest_is_sha256
        CHECK (content_digest ~ '^[0-9a-f]{64}$')
);

-- The absence check walks days; the primary key already orders them, and the
-- table holds one row a day, so no further index earns its place.

GRANT SELECT, INSERT, UPDATE ON fd_reports TO fd_report;
GRANT SELECT ON fd_reports TO fd_ingest;
REVOKE DELETE, TRUNCATE ON fd_reports FROM fd_report;
