-- The fingerprint of ONE batch of the raw zone: what the API said, as stored.
-- Covers: EC23, EI12   Lot: Q1   Competency: C4
--
-- ONE DEFINITION. The writers take it right after writing, the backfill takes
-- it for the batches that predate the column, and the purge takes it again
-- before deleting to see that nothing moved: three callers, one file, so the
-- three can never mean different things by "the digest of a batch".
--
-- WHAT IS HASHED: the line number and the payload, in line order. The payload
-- is jsonb, whose text form is canonical (keys ordered, spacing fixed), so the
-- same stored record always gives the same text. The line number is in the hash
-- so that two records swapped between lines are a different batch.
--
-- A hash of per-row hashes, not one string_agg over the payloads: a historical
-- batch is about 555 thousand rows and some 200 MB of JSON, and aggregating the
-- payloads themselves would build that whole string in memory. The per-row
-- md5 makes the aggregate a few tens of MB.
--
-- Parameter: batch_id.

SELECT count(*) AS n_rows,
       md5(coalesce(
           string_agg(md5(line_no::text || ':' || payload::text), ''
                      ORDER BY line_no),
           '')) AS digest
FROM   fd_raw_events
WHERE  batch_id = %(batch_id)s;
