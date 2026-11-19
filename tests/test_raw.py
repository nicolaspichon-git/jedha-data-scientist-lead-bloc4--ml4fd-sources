"""The batch identifier: what makes a retry harmless (EC23)."""
import uuid

from ml4fd import raw


def test_a_run_id_always_yields_the_same_batch_id():
    """A retried task must not ingest the same lines twice."""
    a = raw.batch_id_for("manual__2026-11-15T20:00:00+00:00")
    b = raw.batch_id_for("manual__2026-11-15T20:00:00+00:00")
    assert a == b


def test_two_run_ids_yield_different_batch_ids():
    a = raw.batch_id_for("manual__2026-11-15T20:00:00+00:00")
    b = raw.batch_id_for("manual__2026-11-15T21:00:00+00:00")
    assert a != b


def test_the_batch_id_is_a_uuid():
    assert isinstance(raw.batch_id_for("whatever"), uuid.UUID)


def test_the_derivation_is_stable_across_processes():
    """Pinned value: changing it would orphan every batch already loaded."""
    assert str(raw.batch_id_for("manual__2026-11-15T20:00:00+00:00")) == \
        str(uuid.uuid5(raw.NAMESPACE,
                       "fraud-detection:raw:manual__2026-11-15T20:00:00+00:00"))
