#!/usr/bin/env python3
"""Clone succeeded 'bench' jobs (jobs, generations, attempts, payloads) with new ids and times shifted back, until TARGET jobs exist."""
# Copied from the keyed-claim run (perf-round-2 "PostgreSQL claim reads its jobs by key"). usage: seed.py <db> <target-jobs> <port>
import subprocess, sys, os
db, target, port = sys.argv[1], int(sys.argv[2]), sys.argv[3]
env = dict(os.environ, PGPASSWORD='pg')


def psql(sql, tuples=True):
    args = ['psql', '-h', '127.0.0.1', '-p', port, '-U', 'postgres', '-d', db, '-v', 'ON_ERROR_STOP=1', '-q'] + (['-At'] if tuples else [])
    r = subprocess.run(args + ['-c', sql], env=env, capture_output=True, text=True)
    if r.returncode:
        sys.exit(r.stderr)
    return r.stdout.strip()


cols = {}
for line in psql("select table_name, column_name from information_schema.columns where table_schema='runwick' and data_type='timestamp with time zone' and table_name in ('jobs','job_generations','job_attempts','job_payloads')").splitlines():
    t, c = line.split('|')
    cols.setdefault(t, []).append(c)


def shift(t, a):
    return ''.join(f", '{c}', {a}.{c} - m.shift" for c in cols.get(t, []))


template = int(psql("select count(*) from runwick.jobs where application='bench' and state='succeeded'"))
assert template > 0, 'no succeeded job to clone'
have = int(psql("select count(*) from runwick.jobs where application='bench'"))
batch = 50000
while have < target:
    n = min(batch, target - have)
    sql = f"""
BEGIN;
CREATE TEMP TABLE seed_template ON COMMIT DROP AS SELECT job_id FROM runwick.jobs WHERE application='bench' AND state='succeeded' LIMIT 20000;
CREATE TEMP TABLE seed_map ON COMMIT DROP AS SELECT t.job_id AS old_job, gen_random_uuid() AS new_job, make_interval(secs => floor(random()*2592000)+3600) AS shift
  FROM seed_template t CROSS JOIN generate_series(1, {(n + 1999)//2000}) LIMIT {n};
CREATE TEMP TABLE seed_amap ON COMMIT DROP AS SELECT m.new_job, a.attempt_id AS old_att, gen_random_uuid() AS new_att FROM seed_map m JOIN runwick.job_attempts a ON a.application='bench' AND a.job_id=m.old_job;
INSERT INTO runwick.jobs SELECT (jsonb_populate_record(NULL::runwick.jobs, to_jsonb(j) || jsonb_build_object('job_id', m.new_job, 'current_attempt_id', am.new_att{shift('jobs','j')}))).*
  FROM seed_map m JOIN runwick.jobs j ON j.application='bench' AND j.job_id=m.old_job LEFT JOIN seed_amap am ON am.new_job=m.new_job AND am.old_att=j.current_attempt_id;
INSERT INTO runwick.job_generations SELECT (jsonb_populate_record(NULL::runwick.job_generations, to_jsonb(g) || jsonb_build_object('job_id', m.new_job{shift('job_generations','g')}))).*
  FROM seed_map m JOIN runwick.job_generations g ON g.application='bench' AND g.job_id=m.old_job;
INSERT INTO runwick.job_attempts SELECT (jsonb_populate_record(NULL::runwick.job_attempts, to_jsonb(x) || jsonb_build_object('job_id', m.new_job, 'attempt_id', am.new_att{shift('job_attempts','x')}))).*
  FROM seed_amap am JOIN seed_map m ON m.new_job=am.new_job JOIN runwick.job_attempts x ON x.application='bench' AND x.attempt_id=am.old_att;
INSERT INTO runwick.job_payloads SELECT (jsonb_populate_record(NULL::runwick.job_payloads, to_jsonb(p) || jsonb_build_object('job_id', m.new_job{shift('job_payloads','p')}))).*
  FROM seed_map m JOIN runwick.job_payloads p ON p.application='bench' AND p.job_id=m.old_job;
COMMIT;"""
    psql(sql, tuples=False)
    have += n
    print('seeded', have, flush=True)
psql("ANALYZE runwick.jobs, runwick.job_generations, runwick.job_attempts, runwick.job_payloads", tuples=False)
print('done', psql("select count(*) from runwick.jobs where application='bench'"))
