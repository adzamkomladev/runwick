-- 0020_stats_indexes (16 Changes 2026-11-10): two indexes for the exact management aggregates. Schema revision 20.
--
-- jobs_name_list: ListJobTypes reads the distinct (queue, job name) pairs of stored jobs by skip scan, one index probe per
-- pair; no other index orders jobs by name, so without it the read is a full scan of runwick.jobs.
-- job_attempts_errors: GetJobStats counts the error codes of attempts that finished in a range; no index orders attempts by
-- finish time, so without it the read scans every attempt. Partial: only attempts that recorded an error.
SET LOCAL lock_timeout = '5s';

CREATE INDEX jobs_name_list
    ON runwick.jobs (application, queue, job_name);

CREATE INDEX job_attempts_errors
    ON runwick.job_attempts (application, finished_at)
    WHERE error IS NOT NULL;
