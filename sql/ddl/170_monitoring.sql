-- The journal of the monitoring measurements: one row per figure measured.
-- Covers: EC40, EC42, EC45   Lot: L4   Competency: C6
--
-- WHY A TABLE AND NOT A REPORT FILE. Evidently produces an HTML report, and a
-- report answers "is it drifting now". EC45 asks the other question -- has it
-- been drifting -- and that one needs the measurement of six weeks ago to
-- still be comparable with today's. An HTML file per run answers it only to
-- somebody willing to open forty files. So the tool computes, this table
-- remembers, and the report stays an artefact of its own run.
--
-- WHAT A ROW IS. One figure, measured over one window, for one control, by one
-- tool. Not one row per run holding every figure in its own column: the
-- figures a control reports change with the tool version, and a schema that
-- must be altered to record a new one is a schema that will record nothing.
--
-- THE DENOMINATORS TRAVEL WITH THE FIGURE, in n_rows and n_labelled. The same
-- rule as fd_datasets, which stores the base rate beside the dataset: a recall
-- of 0.60 over five labelled frauds and the same figure over two hundred are
-- not the same measurement, and nothing downstream can tell them apart once
-- the denominator is gone.
--
-- AND THE TOOL THAT PRODUCED IT, in tool. EC38 requires a named monitoring
-- tool; a measurement whose provenance is not recorded cannot support that
-- claim six months later. The version belongs in the name on purpose: a drift
-- statistic is comparable only with another computed the same way, and
-- Evidently has changed its defaults between minor versions.
--
-- THE LIMIT IN FORCE IS WRITTEN INTO EACH ROW, as in fd_promotions. The
-- thresholds in governance.yml will be tuned -- that is what tuning is for --
-- and a measurement recorded as an alert under the old limit must stay
-- readable as such.
--
-- APPEND ONLY. fd_ingest may insert and nothing else: a measurement that can
-- be corrected afterwards is evidence of nothing.

CREATE TABLE IF NOT EXISTS fd_monitoring (
    measurement_id bigserial    PRIMARY KEY,

    measured_at    timestamptz  NOT NULL DEFAULT now(),
    run_id         text,

    -- THE WINDOW MEASURED, and never merely "now". A drift figure without the
    -- window it was computed over cannot be placed on a timeline, and the two
    -- axes of time of this project -- the payment, and the knowledge of it --
    -- make "the last hour" ambiguous. fd_reports carries the same distinction.
    window_start   timestamptz  NOT NULL,
    window_end     timestamptz  NOT NULL,

    -- The model in service when the window was measured. A drift of the inputs
    -- is a property of the stream; a drop in performance is a property of this
    -- version. The two are read together, so both need naming.
    model_version  text         NOT NULL,

    -- WHICH CONTROL: the four families of C6. The inputs moving away from the
    -- training reference, the predictions moving, the measured performance
    -- against the specification, and the quality of what the stream delivers.
    control        text         NOT NULL,

    -- The figure itself, named. metric is free text rather than an enum, so a
    -- new figure costs an insert and not a migration.
    metric         text         NOT NULL,
    value          numeric(18,8) NOT NULL,

    -- The limit that applied, and the verdict it produced. threshold is null
    -- for a figure recorded to be looked at rather than judged -- one column's
    -- drift score beside the share that is judged, for instance.
    threshold      numeric(18,8),
    state          text         NOT NULL,

    -- The denominators. n_labelled is null where the figure needs no ground
    -- truth, which is the formal difference between EC39 and EC40: drift is
    -- measurable without a verdict, performance is not.
    n_rows         bigint       NOT NULL CHECK (n_rows >= 0),
    n_labelled     bigint       CHECK (n_labelled IS NULL OR n_labelled >= 0),

    -- The tool and its version, e.g. 'evidently 0.7.11'. Free text: a record
    -- of what ran, not a key.
    tool           text         NOT NULL,
    detail         text,

    CONSTRAINT fd_monitoring_state_is_known
        CHECK (state IN ('ok', 'alert', 'unmeasurable')),
    CONSTRAINT fd_monitoring_control_is_known
        CHECK (control IN ('input_drift', 'prediction_drift',
                           'performance', 'data_quality')),
    -- A window runs forwards. Reversed bounds would make every comparison
    -- over time wrong in one direction, and silently.
    CONSTRAINT fd_monitoring_window_runs_forwards
        CHECK (window_end > window_start),
    -- PERFORMANCE WITHOUT GROUND TRUTH IS NOT A MEASUREMENT. EC41 is the
    -- requirement that makes EC40 implementable, and this constraint is what
    -- keeps a performance figure from being recorded without the verdicts it
    -- was computed from. A count of zero is allowed only for 'unmeasurable',
    -- which is how the gap gets recorded instead of hidden.
    CONSTRAINT fd_monitoring_performance_is_labelled
        CHECK (control <> 'performance'
               OR (n_labelled IS NOT NULL
                   AND (n_labelled > 0 OR state = 'unmeasurable'))),
    -- An alert needs a limit to have been compared against. 'unmeasurable'
    -- is the state that says no comparison took place, so it carries none.
    CONSTRAINT fd_monitoring_alert_had_a_limit
        CHECK (state <> 'alert' OR threshold IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS fd_monitoring_measured_at_idx
    ON fd_monitoring (measured_at);
-- The index EC45 is read through: one control's figure, over the long run.
CREATE INDEX IF NOT EXISTS fd_monitoring_series_idx
    ON fd_monitoring (control, metric, window_end);

GRANT SELECT, INSERT ON fd_monitoring TO fd_ingest;
GRANT USAGE, SELECT ON SEQUENCE fd_monitoring_measurement_id_seq TO fd_ingest;
GRANT SELECT ON fd_monitoring TO fd_report;
GRANT SELECT ON fd_monitoring TO fd_dashboard;
REVOKE UPDATE, DELETE, TRUNCATE ON fd_monitoring FROM fd_ingest;
