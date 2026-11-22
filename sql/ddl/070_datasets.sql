-- Dataset registry. A recipe's identity, and the base rate that reads with it.
-- Block: B3, B4.
-- Lot: B3-M0, B4-L0.
-- Competency: B3-C1, B3-C5.
-- Covers: B3-EC05, B3-EC23, B3-EI10.
--
-- The primary key is the RECIPE DIGEST and not a version number: a version is a
-- label stuck on afterwards, a digest is what lets the same recipe find the
-- dataset it already produced instead of building a twin. That is what makes the
-- construction idempotent (EC23) and versioning useful -- one versions in order
-- to find again, not in order to archive.
--
-- The base rates are stored WITH the dataset because a precision figure cannot
-- be read without them. Measured on this source: the fraud rate runs from
-- 0.185 % to 0.554 % across months, so two runs are comparable only if each
-- carries the base rate of its own window.
--
-- The two CHECK constraints make the December mistake unrecordable: a split
-- whose test side holds no positive cannot be registered. The last nine days of
-- the source are exactly that case -- 38,147 transactions, zero labelled fraud,
-- because the generator stopped labelling on 2020-12-22.

CREATE TABLE IF NOT EXISTS fd_datasets (
    recipe_digest   text         PRIMARY KEY,
    family          text         NOT NULL,
    version         text         NOT NULL,
    cut_off         timestamptz  NOT NULL,
    features        text[]       NOT NULL,
    split_rule      text         NOT NULL,
    schema_version  text         NOT NULL,
    train_path      text         NOT NULL,
    test_path       text         NOT NULL,
    n_train         bigint       NOT NULL,
    n_test          bigint       NOT NULL,
    n_train_pos     bigint       NOT NULL,
    n_test_pos      bigint       NOT NULL,
    base_rate_train numeric(9,6) NOT NULL,
    base_rate_test  numeric(9,6) NOT NULL,
    created_at      timestamptz  NOT NULL DEFAULT now(),

    CONSTRAINT fd_datasets_unique_version   UNIQUE (family, version),
    CONSTRAINT fd_datasets_train_positives  CHECK (n_train_pos > 0),
    CONSTRAINT fd_datasets_test_positives   CHECK (n_test_pos > 0),
    CONSTRAINT fd_datasets_train_nonempty   CHECK (n_train > n_train_pos),
    CONSTRAINT fd_datasets_test_nonempty    CHECK (n_test > n_test_pos),
    CONSTRAINT fd_datasets_features_present CHECK (array_length(features, 1) > 0)
);

CREATE INDEX IF NOT EXISTS fd_datasets_cut_off_idx ON fd_datasets (cut_off);

GRANT SELECT, INSERT ON fd_datasets TO fd_ingest;
GRANT SELECT ON fd_datasets TO fd_report;

-- WHAT M5 MEASURED ON THE TEST SET, IN THE DATABASE (S6/R10).
-- Block: B3, B4.
-- Lot: B3-R10 (S6).
-- Competency: B3-C2, B3-C4.
-- Covers: B3-EE04, B3-EC23.
--
-- Until R10 the figures lived only in the recipe's run.json, beside the
-- dataset: the morning report read the file, and a dashboard in another
-- container would have needed the datasets directory mounted to compare
-- anything. The file stays -- it is M8's record and survives a database
-- rebuild -- and the database holds a COPY of the five figures the comparison
-- uses, written by the same task in the same breath (registry.record_reference,
-- called by dag_train.fit_model right after write_manifest). Datasets registered
-- before R10 are filled once from their run.json: .\fd-manager.ps1 reference.
--
-- Null until the dataset has been fitted: a recipe is registered by
-- build_dataset, measured by fit_model, and the two are separate tasks.
-- A REFIT of the same recipe overwrites them, as it overwrites run.json: the
-- reference is that of the last model trained on this recipe.
ALTER TABLE fd_datasets
    ADD COLUMN IF NOT EXISTS test_recall         numeric(9,6)
        CHECK (test_recall IS NULL OR test_recall BETWEEN 0 AND 1),
    ADD COLUMN IF NOT EXISTS test_precision      numeric(9,6)
        CHECK (test_precision IS NULL OR test_precision BETWEEN 0 AND 1),
    ADD COLUMN IF NOT EXISTS test_alerts_per_day numeric(12,4)
        CHECK (test_alerts_per_day IS NULL OR test_alerts_per_day >= 0),
    -- Null when no threshold policy chose one (run.json "threshold": null).
    ADD COLUMN IF NOT EXISTS test_threshold      numeric(9,8)
        CHECK (test_threshold IS NULL OR (test_threshold > 0 AND test_threshold < 1)),
    -- run.json's written_at: when the figures were measured, not copied.
    ADD COLUMN IF NOT EXISTS measured_at         timestamptz;

-- fd_ingest writes the figures and NOTHING ELSE of an existing row: a column
-- grant, so the recipe, its paths and its counts stay as build_dataset wrote
-- them.
GRANT UPDATE (test_recall, test_precision, test_alerts_per_day, test_threshold,
              measured_at)
      ON fd_datasets TO fd_ingest;
