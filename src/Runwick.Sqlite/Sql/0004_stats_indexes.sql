-- 0004_stats_indexes (16 Changes 2026-11-10): two indexes for the exact management aggregates. Schema revision 4.
-- runwick_jobs_name_list: ListJobTypes reads the distinct (queue, job name) pairs by skip scan, one probe per pair.
-- runwick_job_attempts_errors: GetJobStats counts the error codes of attempts that finished in a range (only attempts with an error).
CREATE INDEX runwick_jobs_name_list ON runwick_jobs (application, queue, job_name);
CREATE INDEX runwick_job_attempts_errors ON runwick_job_attempts (application, finished_at) WHERE error IS NOT NULL;
