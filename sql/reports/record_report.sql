-- Record a produced report. WRITTEN AFTER THE DOCUMENT, never before.
-- Covers: EC14, EC23   Lot: P3   Competency: C2
--
-- THE ORDER IS THE SAME AS N5's, AND FOR THE SAME REASON turned around:
-- recording first would leave the table claiming a document that was never
-- written, and the absence check -- which reads this table -- would then report
-- a healthy morning. A crash between the write and the record leaves a document
-- with no row, which the absence check DOES catch. One failure is silent, the
-- other is loud; the order picks the loud one.
--
-- ON CONFLICT ON report_date, so a replay replaces. The document is
-- byte-identical on a replay, so content_digest does not move and produced_at
-- is the only column whose change carries no meaning.
INSERT INTO fd_reports (report_date, knowledge_date, produced_at, path,
                        n_payments, n_alerts, n_verdicts, n_pending,
                        model_version, threshold, content_digest)
VALUES (%(report_date)s, %(knowledge_date)s, now(), %(path)s,
        %(n_payments)s, %(n_alerts)s, %(n_verdicts)s, %(n_pending)s,
        %(model_version)s, %(threshold)s, %(content_digest)s)
ON CONFLICT (report_date) DO UPDATE
   SET knowledge_date = excluded.knowledge_date,
       produced_at    = excluded.produced_at,
       path           = excluded.path,
       n_payments     = excluded.n_payments,
       n_alerts       = excluded.n_alerts,
       n_verdicts     = excluded.n_verdicts,
       n_pending      = excluded.n_pending,
       model_version  = excluded.model_version,
       threshold      = excluded.threshold,
       content_digest = excluded.content_digest;
