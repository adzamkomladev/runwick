-- 0003_worker_detail (16 Changes 2026-11-10): what each worker instance reports about itself with every presence publication
-- (05 §14), returned by ListWorkers. One nullable JSON column; NULL = not reported (an older worker, or a row written before
-- this migration). At most 16 KiB. Schema revision 3.
ALTER TABLE runwick_worker_instances ADD COLUMN detail TEXT CHECK (detail IS NULL OR (json_type(detail) = 'object' AND length(CAST(detail AS BLOB)) <= 16384));
