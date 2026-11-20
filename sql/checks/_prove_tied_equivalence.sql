WITH win AS (
    SELECT txn_id
    FROM  (SELECT txn_id,
                  count(*) OVER (PARTITION BY client_key, txn_ts) AS peers
           FROM   fd_transactions) p
    WHERE peers > 1
),
sem AS (
    SELECT t.txn_id
    FROM   fd_transactions t
    WHERE  EXISTS (SELECT 1 FROM fd_transactions o
                   WHERE  o.client_key = t.client_key
                     AND  o.txn_ts     = t.txn_ts
                     AND  o.txn_id    <> t.txn_id)
)
SELECT (SELECT count(*) FROM win)                                  AS par_fenetrage,
       (SELECT count(*) FROM sem)                                  AS par_semi_jointure,
       (SELECT count(*) FROM (SELECT * FROM win EXCEPT SELECT * FROM sem) a) AS seulement_fenetrage,
       (SELECT count(*) FROM (SELECT * FROM sem EXCEPT SELECT * FROM win) b) AS seulement_semi;
