## PostgreSQL claim reads its jobs by key (A/C and worker B recheck)

Written 2026-11-17. Informational: every timing was taken on the shared host while the SP-029 run and other test suites were running (load average 20 to 40 on 8 cores). Arms alternated inside each pair so both saw the same neighbours. The plan comparisons and the read counts do not depend on load.

**Short answer.** The recheck against retained data found a real cost in the worker's claim that the earlier empty-table runs could not see. When a backlog arrives after the table statistics were last taken, the batched claim reads the whole ready backlog twice per call. That is fixed by reading and updating the claimed jobs by their key. In five paired drains on a copy of a 205,000-job retained database the fixed claim drained 1.46 times faster (median, every pair at least 1.23). The change is a new migration (`0025_claim_keyed`, schema revision 25), kept on its branch for review.

### What the nested plans showed

Setup: a PostgreSQL 18.7 server of its own (fsync, synchronous_commit and full_page_writes on), a database holding 205,000 succeeded `bench` jobs (the source drain's 5,000 real jobs, cloned with new identities and times spread over 30 days, then `ANALYZE`), and a copy of it for every run. Each run drained 5,000 new 512-byte jobs with 16 workers while `auto_explain` logged 2% of statements, nested ones included, with buffers and WAL. The claim function's set-based statement (`rw_claim_plain_v1`, inside `reserve_and_begin_v2`) was the only statement reading many rows:

| | Baseline (`v1`) | Keyed (`v2`) |
|---|---:|---:|
| Sampled claim statements | 15 | 19 |
| Mean statement time (loaded host) | 39.1 ms | 13.7 ms |
| Mean shared buffers hit per statement | 2,764 | 403 |
| Scans of the ready index (`jobs_ready`) | 30, up to 4,918 rows each | none |
| One sampled plan | 49.9 ms | 3.6 ms |

Why: the statistics were taken while no job was waiting, so the planner expected one ready job. Its plan scanned the ready index (every waiting job, 4,918 rows near the start of the drain) and, for each, looked in the claim's job list. The claim runs that scan once to read the jobs and again to update them. So a claim cost grows with the backlog, and a retained, bursty store (statistics rarely refreshed, big backlogs) pays most. The earlier A6 and C1 runs used small, fresh tables where the two plans cost almost the same.

The other statements in the claim and finish paths read their jobs by key in this fixture. The attempt-close usage update (the A4 candidate's target) took 4.6 ms and 207 buffers on average over 6 sampled calls, so it is not the first change to make. The finish-path plans showed no application-wide scans on retained data. The earlier `jobs_name_list` scans came from small, stale fixtures.

### The change

`rw_claim_plain_v2` is `rw_claim_plain_v1` with two changes to how rows are reached, none to what is written:

1. The claimed jobs are read through a lateral lookup by (application, job ID) with `OFFSET 0`, and the state test is applied to what it returns. This keeps the ready index out of the plan.
2. The jobs update drops its state test. The row was read in the same statement as pending or retrying, the candidate claim holds its lock, and every change to a job raises its revision, so `revision = s.revision` already means the state is unchanged. A row that no longer matches is not updated, the count check fails and the whole command rolls back, as before.

`reserve_and_begin_v3` is `reserve_and_begin_v2` calling it, with the correlated payload lookup from the earlier C1 candidate (`OFFSET 0` on the creation-time lookup). Released functions stay in place and the worker now calls `v3`. No table or index changes. Spec 04 has the dated change text.

### Tests

- PostgreSQL project: 400 of 400 passed before the final test was added (24 minutes), including the migration kill-and-resume cases with `0025_claim_keyed` expand, validate and activate, the manifest at revision 25, the set-based versus per-job claim equality test, the lost-reply and fault tests, and the plan tests.
- New `A_claim_reaches_its_jobs_by_key_even_when_statistics_have_not_seen_the_backlog` (RAB-12): statistics taken on an empty table, 1,500 jobs dispatched, one claim of eight in a rolled-back transaction. The keyed claim fetched fewer than 300 job rows through indexes (passes). The same test pointed at `reserve_and_begin_v2` fetched 3,024 and failed, as expected.
- Contract project: 491 of 491 passed (20 minutes), including the batched-claim, fallback and worker contracts that hide `reserve_and_begin_v3`.

### Paired drains

Each pair: a fresh copy of the 205,000-job template per arm (both arms start from the same data), 5,000 jobs, 16 workers, order alternating, every arm reconciled all 5,000 jobs with no diagnostics. Baseline is the branch point; candidate adds the migration. WAL is the server's growth after the copy, so it includes the checkpoint after the copy.

| Pair | Baseline jobs/s | Keyed jobs/s | Ratio | Baseline WAL MB | Keyed WAL MB |
|---:|---:|---:|---:|---:|---:|
| 1 | 49.2 | 61.4 | 1.25 | 189.4 | 178.1 |
| 2 | 56.5 | 69.5 | 1.23 | 183.2 | 172.9 |
| 3 | 59.6 | 129.2 | 2.17 | 182.3 | 151.2 |
| 4 | 80.8 | 118.3 | 1.46 | 169.6 | 152.4 |
| 5 | 62.7 | 115.2 | 1.84 | 179.1 | 155.6 |
| Median | 59.6 | 115.2 | 1.46 | 182.3 | 155.6 |

Control on a fresh, empty database (three pairs, no retained data): median 77.9 against 108.3 jobs/s, ratios 1.25, 1.15 and 1.57, WAL equal (63.7 against 63.0 MB). The fixed claim is not slower on a small table either.

### Reading and limits

- The gain should grow with the backlog and with retained data, and shrink when the statistics are fresh. Both arms here start from statistics that show no ready jobs, which is how a store looks right after a burst of enqueues. The runs do not show how often a production store is in that state. Autovacuum may refresh statistics during a long backlog. A quiet-host rerun with a 100,000-job backlog would size the effect.
- Absolute rates are low because the host was loaded. Compare ratios, not rates. The per-pair spread (1.23 to 2.17) is the host's noise.
- The WAL difference is probably the shorter run with fewer checkpoint full-page images, not a change in what the claim writes. The statement's own WAL per sampled call was similar.
- The A4 attempt-close candidate (usage written in the same update as the close) was not rebuilt on the current schema. Its earlier result was 2.8% less WAL with a throughput change inside the noise (median +1.0%), and the plans above show the usage update is small. It stays isolated.
- Not done: index narrowing on `jobs` (every update inserts into the six always-on indexes and the partial ones; the plans did not make any one of them a clear first drop), replay-proof-gated receipt compaction, combined finish and claim, finish batching.

Raw results: `artifacts/perf/round-two/pg-keyed-claim/` (retained plans, paired runs, control, scripts: `plans.sh`, `pairs.sh`, `seed.py`, `summ.py`, `summarize.py`). The `--db-reuse` bench flag runs a bench against an existing database (a copy of a template).
