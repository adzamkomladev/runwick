-- 0002_attempt_usage (16 Changes 2026-11-09): what the worker process used during each attempt, stored with the attempt's
-- Finish. One nullable JSON column; NULL = not recorded (an attempt finished by an older binary, by a worker with
-- RecordResourceUsage off, or closed without running). Schema revision 2.
ALTER TABLE runwick_job_attempts ADD COLUMN usage TEXT CHECK (usage IS NULL OR (json_type(usage) = 'object' AND length(CAST(usage AS BLOB)) <= 1024));
