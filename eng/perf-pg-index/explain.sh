#!/usr/bin/env bash
# Part of the 0029 jobs index diet measurements (docs/reports/fragments/pg-index.md). Server: PORT (default 55631), user postgres, password pg.
# EXPLAIN the retention and stats reads on copies of both templates, after detail expiry of the older half of the jobs.
# usage: explain.sh <template> <stats-form: old|new>
set -euo pipefail
export PGPASSWORD=pg
T=$1; FORM=$2; DB=${T}_x
Q() { psql -h 127.0.0.1 -p "${PORT:-55631}" -U postgres -d "$1" -Atqc "$2"; }
Q postgres "DROP DATABASE IF EXISTS $DB WITH (FORCE)"
Q postgres "CREATE DATABASE $DB TEMPLATE $T STRATEGY file_copy"
# Detail expiry of jobs that ended more than 15 days ago (what CleanupBatch does, minus payload and error rewrite).
Q $DB "UPDATE runwick.jobs SET payload_retained = false, detail_expired_at = terminal_at + interval '7 days' WHERE terminal_at < now() - interval '7 days'"
Q $DB "VACUUM ANALYZE runwick.jobs"
E() { echo "### $1"; psql -h 127.0.0.1 -p "${PORT:-55631}" -U postgres -d $DB -Atqc "EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, SUMMARY OFF) $2"; echo; }
E "purge candidates (17 §3.2)" "
SELECT j.job_id, j.terminal_at FROM runwick.jobs AS j
WHERE j.application = 'bench' AND j.state IN ('succeeded', 'failed', 'cancelled') AND j.terminal_at < now() - interval '20 days'
  AND j.detail_expired_at IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM runwick.dispatch_keys AS d WHERE d.application = j.application AND d.bound_job_id = j.job_id
                    AND (d.key_kind = 'active' OR d.expires_at > now()))
  AND NOT EXISTS (SELECT 1 FROM runwick.concurrency_permits AS pm WHERE pm.application = j.application AND pm.job_id = j.job_id)
ORDER BY j.terminal_at, j.job_id LIMIT 500 FOR UPDATE OF j SKIP LOCKED"
E "detail expiry candidates (17 §3.1)" "
SELECT j.job_id, j.terminal_at, COALESCE(octet_length(p.payload_utf8), 0)
FROM runwick.jobs AS j LEFT JOIN runwick.job_payloads AS p ON p.application = j.application AND p.job_id = j.job_id
WHERE j.application = 'bench' AND j.state = ANY('{succeeded,failed,cancelled}'::text[]) AND j.detail_expired_at IS NULL AND j.terminal_at < now() - interval '7 days' + interval '1 hour'
ORDER BY j.terminal_at, j.job_id LIMIT 500 FOR NO KEY UPDATE OF j SKIP LOCKED"
if [ "$FORM" = old ]; then
  SRC="runwick.jobs AS j WHERE j.application = 'bench' AND j.state IN ('succeeded', 'failed', 'cancelled') AND j.terminal_at >= @F AND j.terminal_at < @T"
else
  SRC="(SELECT * FROM runwick.jobs WHERE application = 'bench' AND detail_expired_at IS NULL AND state IN ('succeeded', 'failed', 'cancelled') AND terminal_at >= @F AND terminal_at < @T
        UNION ALL
        SELECT * FROM runwick.jobs WHERE application = 'bench' AND detail_expired_at IS NOT NULL AND state IN ('succeeded', 'failed', 'cancelled') AND terminal_at >= @F AND terminal_at < @T) AS j WHERE true"
fi
for w in "1 day:now() - interval '1 day':now()" "30 days:now() - interval '30 days':now()"; do
  IFS=: read -r name F T2 <<< "$w"
  S=${SRC//@F/$F}; S=${S//@T/$T2}
  E "job stats ended, $name ($FORM form)" "
WITH t AS MATERIALIZED (
  SELECT j.queue, j.job_name, j.state, j.terminal_at,
         (extract(epoch FROM j.terminal_at - (SELECT coalesce(min(a.authorized_at), min(a.reserved_at)) FROM runwick.job_attempts AS a
              WHERE a.application = j.application AND a.job_id = j.job_id AND a.generation = j.generation)) * 1000)::float8 AS ms
  FROM $S)
SELECT queue, job_name, state, count(*), percentile_cont(0.5) WITHIN GROUP (ORDER BY ms) FROM t GROUP BY 1, 2, 3"
done
Q postgres "DROP DATABASE IF EXISTS $DB WITH (FORCE)"
