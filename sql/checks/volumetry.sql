-- Measured storage volume per store. Feeds EC03, which can only be satisfied
-- once the data is actually loaded.
-- Block: B3, B4.
-- Lot: B3-L8, B4-L0.
-- Competency: B3-C1.
-- Covers: B3-EC01, B3-EC03.

SELECT c.relname                                     AS store,
       to_char(c.reltuples::bigint, 'FM999G999G999')  AS estimated_rows,
       pg_size_pretty(pg_total_relation_size(c.oid))  AS total_size,
       pg_size_pretty(pg_indexes_size(c.oid))         AS of_which_indexes
FROM   pg_class c
JOIN   pg_namespace n ON n.oid = c.relnamespace
WHERE  n.nspname = 'public'
  AND  c.relkind = 'r'
  AND  c.relname LIKE 'fd\_%'
ORDER BY pg_total_relation_size(c.oid) DESC;
