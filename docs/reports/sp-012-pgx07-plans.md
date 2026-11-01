# SP-012 PGX-07 plans: ReadinessProbe/2, Reserve/1 candidate scan, ListUnsupportedBacklog/1

Recorded by `ReadinessPlanTests.Probe_reserve_and_diagnostic_plans_use_the_ready_index_with_bounded_buffers_at_a_million_rows`
(2026-11-01, Testcontainers `postgres:18`, shared 8 vCPU host; run with `RUNWICK_PLAN_DIR`). Output of
`EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF)` of the shipped statements, read from the product source at run time.
Data: 80 % succeeded history, 20 % pending over 4 queues × 3 lanes, 1 % of the pending rows with an unsupported
tuple; `VACUUM (ANALYZE)` after the load. The application name in the index conditions is the per-test namespace.
The diagnostic ran with no live worker, so its capability lookups are not exercised (see `sp-012.md`).

## probe at 100,000 rows: 108 shared buffers, 0.85 ms

```text
Nested Loop Left Join (actual rows=17.00 loops=1)
  Buffers: shared hit=108
  CTE gate
    ->  LockRows (actual rows=1.00 loops=1)
          Buffers: shared hit=3
          ->  Index Scan using store_metadata_pk on store_metadata s_1 (actual rows=1.00 loops=1)
                Index Cond: (singleton_id = 1)
                Index Searches: 1
                Buffers: shared hit=2
  CTE lanes
    ->  Nested Loop (actual rows=12.00 loops=1)
          Join Filter: ((q_1.queue)::text = (l_2.queue)::text)
          Rows Removed by Join Filter: 36
          Buffers: shared hit=2
          ->  Bitmap Heap Scan on queue_controls q_1 (actual rows=4.00 loops=1)
                Recheck Cond: ((application)::text = 'pgx07_69565939'::text)
                Filter: (NOT paused)
                Heap Blocks: exact=1
                Buffers: shared hit=2
                ->  Bitmap Index Scan on queue_controls_pk (actual rows=4.00 loops=1)
                      Index Cond: ((application)::text = 'pgx07_69565939'::text)
                      Index Searches: 1
                      Buffers: shared hit=1
          ->  Function Scan on l_2 (actual rows=12.00 loops=4)
  CTE tuples
    ->  Function Scan on t_2 (actual rows=7.00 loops=1)
  ->  CTE Scan on gate g (actual rows=1.00 loops=1)
        Storage: Memory  Maximum Storage: 17kB
        Buffers: shared hit=3
  ->  Append (actual rows=17.00 loops=1)
        Buffers: shared hit=105
        ->  Result (actual rows=13.00 loops=1)
              Buffers: shared hit=99
              ->  Append (actual rows=13.00 loops=1)
                    Buffers: shared hit=99
                    ->  Nested Loop (actual rows=12.00 loops=1)
                          Buffers: shared hit=51
                          ->  CTE Scan on lanes l (actual rows=12.00 loops=1)
                                Storage: Memory  Maximum Storage: 17kB
                                Buffers: shared hit=2
                          ->  Limit (actual rows=1.00 loops=12)
                                Buffers: shared hit=49
                                ->  Nested Loop Semi Join (actual rows=1.00 loops=12)
                                      Join Filter: (((t.name)::text = (j.job_name)::text) AND (t.version = j.payload_version) AND ((t.profile_id)::text = (j.serializer_profile_id)::text) AND (t.profile_version = j.serializer_profile_version))
                                      Rows Removed by Join Filter: 7
                                      Buffers: shared hit=49
                                      ->  Index Scan using jobs_ready on jobs j (actual rows=1.08 loops=12)
                                            Index Cond: (((application)::text = 'pgx07_69565939'::text) AND ((queue)::text = l.queue) AND (priority = l.priority) AND (eligible_at <= g.observed_at))
                                            Index Searches: 12
                                            Buffers: shared hit=49
                                      ->  CTE Scan on tuples t (actual rows=7.00 loops=13)
                                            Storage: Memory  Maximum Storage: 17kB
                    ->  Nested Loop Anti Join (actual rows=1.00 loops=1)
                          Join Filter: ((t_1.name = (j_1.job_name)::text) AND (t_1.version = j_1.payload_version) AND (t_1.profile_id = (j_1.serializer_profile_id)::text) AND (t_1.profile_version = j_1.serializer_profile_version))
                          Rows Removed by Join Filter: 73
                          Buffers: shared hit=48
                          ->  Nested Loop (actual rows=12.00 loops=1)
                                Buffers: shared hit=48
                                ->  CTE Scan on lanes l_1 (actual rows=12.00 loops=1)
                                      Storage: Memory  Maximum Storage: 17kB
                                ->  Limit (actual rows=1.00 loops=12)
                                      Buffers: shared hit=48
                                      ->  Index Scan using jobs_ready on jobs j_1 (actual rows=1.00 loops=12)
                                            Index Cond: (((application)::text = 'pgx07_69565939'::text) AND ((queue)::text = l_1.queue) AND (priority = l_1.priority) AND (eligible_at <= g.observed_at))
                                            Index Searches: 12
                                            Buffers: shared hit=48
                          ->  CTE Scan on tuples t_1 (actual rows=7.00 loops=12)
                                Storage: Memory  Maximum Storage: 17kB
        ->  Subquery Scan on "*SELECT* 3" (actual rows=4.00 loops=1)
              Buffers: shared hit=6
              ->  Nested Loop Left Join (actual rows=4.00 loops=1)
                    Join Filter: ((s.scope_key)::text = (q.queue)::text)
                    Buffers: shared hit=6
                    ->  Hash Join (actual rows=4.00 loops=1)
                          Hash Cond: ((q.queue)::text = (u.queue)::text)
                          Buffers: shared hit=2
                          ->  Bitmap Heap Scan on queue_controls q (actual rows=4.00 loops=1)
                                Recheck Cond: ((application)::text = 'pgx07_69565939'::text)
                                Heap Blocks: exact=1
                                Buffers: shared hit=2
                                ->  Bitmap Index Scan on queue_controls_pk (actual rows=4.00 loops=1)
                                      Index Cond: ((application)::text = 'pgx07_69565939'::text)
                                      Index Searches: 1
                                      Buffers: shared hit=1
                          ->  Hash (actual rows=4.00 loops=1)
                                Buckets: 1024  Batches: 1  Memory Usage: 9kB
                                ->  HashAggregate (actual rows=4.00 loops=1)
                                      Group Key: u.queue
                                      Batches: 1  Memory Usage: 32kB
                                      ->  Function Scan on unnest u (actual rows=12.00 loops=1)
                    ->  Index Scan using concurrency_scopes_natural_uq on concurrency_scopes s (actual rows=0.00 loops=4)
                          Index Cond: (((application)::text = 'pgx07_69565939'::text) AND (scope_kind = 'queue'::text))
                          Index Searches: 4
                          Buffers: shared hit=4
                          SubPlan 4
                            ->  Aggregate (never executed)
                                  ->  Limit (never executed)
                                        ->  Index Only Scan using permits_live_count on concurrency_permits p (never executed)
                                              Index Cond: ((application = 'pgx07_69565939'::text) AND (scope_id = s.scope_id) AND (lease_expires_at > g.observed_at))
                                              Heap Fetches: 0
                                              Index Searches: 0
Planning:
  Buffers: shared hit=7
Planning Time: 1.585 ms
Execution Time: 1.003 ms
```

## probe at 1,000,000 rows: 110 shared buffers, 2.17 ms

```text
Nested Loop Left Join (actual rows=17.00 loops=1)
  Buffers: shared hit=108
  CTE gate
    ->  LockRows (actual rows=1.00 loops=1)
          Buffers: shared hit=3
          ->  Index Scan using store_metadata_pk on store_metadata s_1 (actual rows=1.00 loops=1)
                Index Cond: (singleton_id = 1)
                Index Searches: 1
                Buffers: shared hit=2
  CTE lanes
    ->  Nested Loop (actual rows=12.00 loops=1)
          Join Filter: ((q_1.queue)::text = (l_2.queue)::text)
          Rows Removed by Join Filter: 36
          Buffers: shared hit=2
          ->  Bitmap Heap Scan on queue_controls q_1 (actual rows=4.00 loops=1)
                Recheck Cond: ((application)::text = 'pgx07_69565939'::text)
                Filter: (NOT paused)
                Heap Blocks: exact=1
                Buffers: shared hit=2
                ->  Bitmap Index Scan on queue_controls_pk (actual rows=4.00 loops=1)
                      Index Cond: ((application)::text = 'pgx07_69565939'::text)
                      Index Searches: 1
                      Buffers: shared hit=1
          ->  Function Scan on l_2 (actual rows=12.00 loops=4)
  CTE tuples
    ->  Function Scan on t_2 (actual rows=7.00 loops=1)
  ->  CTE Scan on gate g (actual rows=1.00 loops=1)
        Storage: Memory  Maximum Storage: 17kB
        Buffers: shared hit=3
  ->  Append (actual rows=17.00 loops=1)
        Buffers: shared hit=105
        ->  Result (actual rows=13.00 loops=1)
              Buffers: shared hit=99
              ->  Append (actual rows=13.00 loops=1)
                    Buffers: shared hit=99
                    ->  Nested Loop (actual rows=12.00 loops=1)
                          Buffers: shared hit=51
                          ->  CTE Scan on lanes l (actual rows=12.00 loops=1)
                                Storage: Memory  Maximum Storage: 17kB
                                Buffers: shared hit=2
                          ->  Limit (actual rows=1.00 loops=12)
                                Buffers: shared hit=49
                                ->  Nested Loop Semi Join (actual rows=1.00 loops=12)
                                      Join Filter: (((t.name)::text = (j.job_name)::text) AND (t.version = j.payload_version) AND ((t.profile_id)::text = (j.serializer_profile_id)::text) AND (t.profile_version = j.serializer_profile_version))
                                      Rows Removed by Join Filter: 7
                                      Buffers: shared hit=49
                                      ->  Index Scan using jobs_ready on jobs j (actual rows=1.08 loops=12)
                                            Index Cond: (((application)::text = 'pgx07_69565939'::text) AND ((queue)::text = l.queue) AND (priority = l.priority) AND (eligible_at <= g.observed_at))
                                            Index Searches: 12
                                            Buffers: shared hit=49
                                      ->  CTE Scan on tuples t (actual rows=7.00 loops=13)
                                            Storage: Memory  Maximum Storage: 17kB
                    ->  Nested Loop Anti Join (actual rows=1.00 loops=1)
                          Join Filter: ((t_1.name = (j_1.job_name)::text) AND (t_1.version = j_1.payload_version) AND (t_1.profile_id = (j_1.serializer_profile_id)::text) AND (t_1.profile_version = j_1.serializer_profile_version))
                          Rows Removed by Join Filter: 73
                          Buffers: shared hit=48
                          ->  Nested Loop (actual rows=12.00 loops=1)
                                Buffers: shared hit=48
                                ->  CTE Scan on lanes l_1 (actual rows=12.00 loops=1)
                                      Storage: Memory  Maximum Storage: 17kB
                                ->  Limit (actual rows=1.00 loops=12)
                                      Buffers: shared hit=48
                                      ->  Index Scan using jobs_ready on jobs j_1 (actual rows=1.00 loops=12)
                                            Index Cond: (((application)::text = 'pgx07_69565939'::text) AND ((queue)::text = l_1.queue) AND (priority = l_1.priority) AND (eligible_at <= g.observed_at))
                                            Index Searches: 12
                                            Buffers: shared hit=48
                          ->  CTE Scan on tuples t_1 (actual rows=7.00 loops=12)
                                Storage: Memory  Maximum Storage: 17kB
        ->  Subquery Scan on "*SELECT* 3" (actual rows=4.00 loops=1)
              Buffers: shared hit=6
              ->  Nested Loop Left Join (actual rows=4.00 loops=1)
                    Join Filter: ((s.scope_key)::text = (q.queue)::text)
                    Buffers: shared hit=6
                    ->  Hash Join (actual rows=4.00 loops=1)
                          Hash Cond: ((q.queue)::text = (u.queue)::text)
                          Buffers: shared hit=2
                          ->  Bitmap Heap Scan on queue_controls q (actual rows=4.00 loops=1)
                                Recheck Cond: ((application)::text = 'pgx07_69565939'::text)
                                Heap Blocks: exact=1
                                Buffers: shared hit=2
                                ->  Bitmap Index Scan on queue_controls_pk (actual rows=4.00 loops=1)
                                      Index Cond: ((application)::text = 'pgx07_69565939'::text)
                                      Index Searches: 1
                                      Buffers: shared hit=1
                          ->  Hash (actual rows=4.00 loops=1)
                                Buckets: 1024  Batches: 1  Memory Usage: 9kB
                                ->  HashAggregate (actual rows=4.00 loops=1)
                                      Group Key: u.queue
                                      Batches: 1  Memory Usage: 32kB
                                      ->  Function Scan on unnest u (actual rows=12.00 loops=1)
                    ->  Index Scan using concurrency_scopes_natural_uq on concurrency_scopes s (actual rows=0.00 loops=4)
                          Index Cond: (((application)::text = 'pgx07_69565939'::text) AND (scope_kind = 'queue'::text))
                          Index Searches: 4
                          Buffers: shared hit=4
                          SubPlan 4
                            ->  Aggregate (never executed)
                                  ->  Limit (never executed)
                                        ->  Index Only Scan using permits_live_count on concurrency_permits p (never executed)
                                              Index Cond: ((application = 'pgx07_69565939'::text) AND (scope_id = s.scope_id) AND (lease_expires_at > g.observed_at))
                                              Heap Fetches: 0
                                              Index Searches: 0
Planning:
  Buffers: shared hit=5
Planning Time: 1.315 ms
Execution Time: 0.972 ms
```

## reserve at 100,000 rows: 6 shared buffers, 1.13 ms

```text
Limit (actual rows=1.00 loops=1)
  Buffers: shared hit=6
  ->  LockRows (actual rows=1.00 loops=1)
        Buffers: shared hit=6
        ->  Nested Loop Semi Join (actual rows=1.00 loops=1)
              Join Filter: (((c.name)::text = (j.job_name)::text) AND (c.version = j.payload_version) AND ((c.profile_id)::text = (j.serializer_profile_id)::text) AND (c.profile_version = j.serializer_profile_version))
              Rows Removed by Join Filter: 6
              Buffers: shared hit=4
              ->  Index Scan using jobs_ready on jobs j (actual rows=1.00 loops=1)
                    Index Cond: (((application)::text = 'pgx07_69565939'::text) AND ((queue)::text = 'q1'::text) AND (priority = 'medium'::text) AND (eligible_at <= statement_timestamp()))
                    Filter: (state = ANY ('{pending,retrying}'::text[]))
                    Index Searches: 1
                    Buffers: shared hit=4
              ->  Function Scan on c (actual rows=7.00 loops=1)
Planning:
  Buffers: shared hit=1
Planning Time: 0.413 ms
Execution Time: 0.126 ms
```

## reserve at 1,000,000 rows: 6 shared buffers, 0.22 ms

```text
Limit (actual rows=1.00 loops=1)
  Buffers: shared hit=6
  ->  LockRows (actual rows=1.00 loops=1)
        Buffers: shared hit=6
        ->  Nested Loop Semi Join (actual rows=1.00 loops=1)
              Join Filter: (((c.name)::text = (j.job_name)::text) AND (c.version = j.payload_version) AND ((c.profile_id)::text = (j.serializer_profile_id)::text) AND (c.profile_version = j.serializer_profile_version))
              Rows Removed by Join Filter: 6
              Buffers: shared hit=4
              ->  Index Scan using jobs_ready on jobs j (actual rows=1.00 loops=1)
                    Index Cond: (((application)::text = 'pgx07_69565939'::text) AND ((queue)::text = 'q1'::text) AND (priority = 'medium'::text) AND (eligible_at <= statement_timestamp()))
                    Filter: (state = ANY ('{pending,retrying}'::text[]))
                    Index Searches: 1
                    Buffers: shared hit=4
              ->  Function Scan on c (actual rows=7.00 loops=1)
Planning Time: 0.548 ms
Execution Time: 0.187 ms
```

## diagnostic at 100,000 rows: 3198 shared buffers, 210.27 ms

```text
Nested Loop Left Join (actual rows=15.00 loops=1)
  Buffers: shared hit=3192
  CTE gate
    ->  LockRows (actual rows=1.00 loops=1)
          Buffers: shared hit=3
          ->  Index Scan using store_metadata_pk on store_metadata s_1 (actual rows=1.00 loops=1)
                Index Cond: (singleton_id = 1)
                Index Searches: 1
                Buffers: shared hit=2
  CTE live
    ->  Nested Loop (actual rows=0.00 loops=1)
          Join Filter: (w.expires_at > g_1.observed_at)
          Buffers: shared hit=2
          ->  Index Scan using worker_instances_expiry on worker_instances w (actual rows=0.00 loops=1)
                Index Cond: ((application)::text = 'pgx07_69565939'::text)
                Filter: (status = ANY ('{starting,ready,degraded}'::text[]))
                Index Searches: 1
                Buffers: shared hit=2
          ->  CTE Scan on gate g_1 (never executed)
                Storage: Memory  Maximum Storage: 17kB
  CTE sample
    ->  Nested Loop (actual rows=3072.00 loops=1)
          Buffers: shared hit=3157
          ->  CTE Scan on gate g_2 (actual rows=1.00 loops=1)
                Storage: Memory  Maximum Storage: 17kB
          ->  Nested Loop (actual rows=3072.00 loops=1)
                Buffers: shared hit=3157
                ->  Bitmap Heap Scan on queue_controls q (actual rows=4.00 loops=1)
                      Recheck Cond: ((application)::text = 'pgx07_69565939'::text)
                      Heap Blocks: exact=1
                      Buffers: shared hit=2
                      ->  Bitmap Index Scan on queue_controls_pk (actual rows=4.00 loops=1)
                            Index Cond: ((application)::text = 'pgx07_69565939'::text)
                            Index Searches: 1
                            Buffers: shared hit=1
                ->  Nested Loop (actual rows=768.00 loops=4)
                      Buffers: shared hit=3155
                      ->  Function Scan on unnest p (actual rows=3.00 loops=4)
                      ->  Limit (actual rows=256.00 loops=12)
                            Buffers: shared hit=3155
                            ->  Index Scan using jobs_ready on jobs j (actual rows=256.00 loops=12)
                                  Index Cond: (((application)::text = 'pgx07_69565939'::text) AND ((queue)::text = (q.queue)::text) AND (priority = p.priority) AND (eligible_at <= g_2.observed_at))
                                  Index Searches: 12
                                  Buffers: shared hit=3155
  InitPlan 4
    ->  Aggregate (actual rows=1.00 loops=1)
          ->  CTE Scan on live (actual rows=0.00 loops=1)
                Storage: Memory  Maximum Storage: 17kB
  ->  CTE Scan on gate g (actual rows=1.00 loops=1)
        Storage: Memory  Maximum Storage: 17kB
        Buffers: shared hit=3
  ->  Sort (actual rows=15.00 loops=1)
        Sort Key: s.queue, s.priority, (min(s.eligible_at))
        Sort Method: quicksort  Memory: 26kB
        Buffers: shared hit=3189
        ->  Hash Join (actual rows=15.00 loops=1)
              Hash Cond: (((s.queue)::text = (lc.queue)::text) AND (s.priority = lc.priority))
              Buffers: shared hit=3189
              ->  HashAggregate (actual rows=15.00 loops=1)
                    Group Key: s.queue, s.priority, s.job_name, s.payload_version, s.serializer_profile_id, s.serializer_profile_version
                    Filter: (NOT EXISTS(SubPlan 7))
                    Batches: 1  Memory Usage: 40kB
                    Buffers: shared hit=32
                    ->  CTE Scan on sample s (actual rows=3072.00 loops=1)
                          Storage: Memory  Maximum Storage: 275kB
                    SubPlan 6
                      ->  Nested Loop (actual rows=0.00 loops=1)
                            Buffers: shared hit=2
                            ->  CTE Scan on live l (actual rows=0.00 loops=1)
                                  Storage: Memory  Maximum Storage: 17kB
                                  Buffers: shared hit=2
                            ->  Index Only Scan using worker_capabilities_pk on worker_capabilities c (never executed)
                                  Index Cond: ((application = 'pgx07_69565939'::text) AND (instance_id = l.instance_id) AND (capability_revision = l.capability_revision))
                                  Heap Fetches: 0
                                  Index Searches: 0
                    SubPlan 7
                      ->  Nested Loop (actual rows=0.00 loops=15)
                            Join Filter: (c_1.instance_id = ws.instance_id)
                            Buffers: shared hit=30
                            ->  Nested Loop (actual rows=0.00 loops=15)
                                  Join Filter: ((c_1.instance_id = l_1.instance_id) AND (l_1.capability_revision = c_1.capability_revision))
                                  Buffers: shared hit=30
                                  ->  Index Only Scan using worker_capabilities_pk on worker_capabilities c_1 (actual rows=0.00 loops=15)
                                        Index Cond: ((application = 'pgx07_69565939'::text COLLATE "C") AND (job_name = (s.job_name)::text) AND (payload_version = s.payload_version) AND (serializer_profile_id = (s.serializer_profile_id)::text) AND (serializer_profile_version = s.serializer_profile_version))
                                        Heap Fetches: 0
                                        Index Searches: 15
                                        Buffers: shared hit=30
                                  ->  CTE Scan on live l_1 (never executed)
                                        Storage: Memory  Maximum Storage: 17kB
                            ->  Index Only Scan using worker_subscriptions_queue on worker_subscriptions ws (never executed)
                                  Index Cond: ((application = 'pgx07_69565939'::text COLLATE "C") AND (queue = (s.queue)::text))
                                  Heap Fetches: 0
                                  Index Searches: 0
              ->  Hash (actual rows=12.00 loops=1)
                    Buckets: 1024  Batches: 1  Memory Usage: 9kB
                    Buffers: shared hit=3157
                    ->  Subquery Scan on lc (actual rows=12.00 loops=1)
                          Buffers: shared hit=3157
                          ->  HashAggregate (actual rows=12.00 loops=1)
                                Group Key: sample.queue, sample.priority
                                Batches: 1  Memory Usage: 40kB
                                Buffers: shared hit=3157
                                ->  CTE Scan on sample (actual rows=3072.00 loops=1)
                                      Storage: Memory  Maximum Storage: 275kB
                                      Buffers: shared hit=3157
Planning:
  Buffers: shared hit=11
Planning Time: 1.616 ms
Execution Time: 49.478 ms
```

## diagnostic at 1,000,000 rows: 3192 shared buffers, 21.20 ms

```text
Nested Loop Left Join (actual rows=15.00 loops=1)
  Buffers: shared hit=3192
  CTE gate
    ->  LockRows (actual rows=1.00 loops=1)
          Buffers: shared hit=3
          ->  Index Scan using store_metadata_pk on store_metadata s_1 (actual rows=1.00 loops=1)
                Index Cond: (singleton_id = 1)
                Index Searches: 1
                Buffers: shared hit=2
  CTE live
    ->  Nested Loop (actual rows=0.00 loops=1)
          Join Filter: (w.expires_at > g_1.observed_at)
          Buffers: shared hit=2
          ->  Index Scan using worker_instances_expiry on worker_instances w (actual rows=0.00 loops=1)
                Index Cond: ((application)::text = 'pgx07_69565939'::text)
                Filter: (status = ANY ('{starting,ready,degraded}'::text[]))
                Index Searches: 1
                Buffers: shared hit=2
          ->  CTE Scan on gate g_1 (never executed)
                Storage: Memory  Maximum Storage: 17kB
  CTE sample
    ->  Nested Loop (actual rows=3072.00 loops=1)
          Buffers: shared hit=3157
          ->  CTE Scan on gate g_2 (actual rows=1.00 loops=1)
                Storage: Memory  Maximum Storage: 17kB
          ->  Nested Loop (actual rows=3072.00 loops=1)
                Buffers: shared hit=3157
                ->  Bitmap Heap Scan on queue_controls q (actual rows=4.00 loops=1)
                      Recheck Cond: ((application)::text = 'pgx07_69565939'::text)
                      Heap Blocks: exact=1
                      Buffers: shared hit=2
                      ->  Bitmap Index Scan on queue_controls_pk (actual rows=4.00 loops=1)
                            Index Cond: ((application)::text = 'pgx07_69565939'::text)
                            Index Searches: 1
                            Buffers: shared hit=1
                ->  Nested Loop (actual rows=768.00 loops=4)
                      Buffers: shared hit=3155
                      ->  Function Scan on unnest p (actual rows=3.00 loops=4)
                      ->  Limit (actual rows=256.00 loops=12)
                            Buffers: shared hit=3155
                            ->  Index Scan using jobs_ready on jobs j (actual rows=256.00 loops=12)
                                  Index Cond: (((application)::text = 'pgx07_69565939'::text) AND ((queue)::text = (q.queue)::text) AND (priority = p.priority) AND (eligible_at <= g_2.observed_at))
                                  Index Searches: 12
                                  Buffers: shared hit=3155
  InitPlan 4
    ->  Aggregate (actual rows=1.00 loops=1)
          ->  CTE Scan on live (actual rows=0.00 loops=1)
                Storage: Memory  Maximum Storage: 17kB
  ->  CTE Scan on gate g (actual rows=1.00 loops=1)
        Storage: Memory  Maximum Storage: 17kB
        Buffers: shared hit=3
  ->  Sort (actual rows=15.00 loops=1)
        Sort Key: s.queue, s.priority, (min(s.eligible_at))
        Sort Method: quicksort  Memory: 26kB
        Buffers: shared hit=3189
        ->  Hash Join (actual rows=15.00 loops=1)
              Hash Cond: (((s.queue)::text = (lc.queue)::text) AND (s.priority = lc.priority))
              Buffers: shared hit=3189
              ->  HashAggregate (actual rows=15.00 loops=1)
                    Group Key: s.queue, s.priority, s.job_name, s.payload_version, s.serializer_profile_id, s.serializer_profile_version
                    Filter: (NOT EXISTS(SubPlan 7))
                    Batches: 1  Memory Usage: 40kB
                    Buffers: shared hit=32
                    ->  CTE Scan on sample s (actual rows=3072.00 loops=1)
                          Storage: Memory  Maximum Storage: 275kB
                    SubPlan 6
                      ->  Nested Loop (actual rows=0.00 loops=1)
                            Buffers: shared hit=2
                            ->  CTE Scan on live l (actual rows=0.00 loops=1)
                                  Storage: Memory  Maximum Storage: 17kB
                                  Buffers: shared hit=2
                            ->  Index Only Scan using worker_capabilities_pk on worker_capabilities c (never executed)
                                  Index Cond: ((application = 'pgx07_69565939'::text) AND (instance_id = l.instance_id) AND (capability_revision = l.capability_revision))
                                  Heap Fetches: 0
                                  Index Searches: 0
                    SubPlan 7
                      ->  Nested Loop (actual rows=0.00 loops=15)
                            Join Filter: (c_1.instance_id = ws.instance_id)
                            Buffers: shared hit=30
                            ->  Nested Loop (actual rows=0.00 loops=15)
                                  Join Filter: ((c_1.instance_id = l_1.instance_id) AND (l_1.capability_revision = c_1.capability_revision))
                                  Buffers: shared hit=30
                                  ->  Index Only Scan using worker_capabilities_pk on worker_capabilities c_1 (actual rows=0.00 loops=15)
                                        Index Cond: ((application = 'pgx07_69565939'::text COLLATE "C") AND (job_name = (s.job_name)::text) AND (payload_version = s.payload_version) AND (serializer_profile_id = (s.serializer_profile_id)::text) AND (serializer_profile_version = s.serializer_profile_version))
                                        Heap Fetches: 0
                                        Index Searches: 15
                                        Buffers: shared hit=30
                                  ->  CTE Scan on live l_1 (never executed)
                                        Storage: Memory  Maximum Storage: 17kB
                            ->  Index Only Scan using worker_subscriptions_queue on worker_subscriptions ws (never executed)
                                  Index Cond: ((application = 'pgx07_69565939'::text COLLATE "C") AND (queue = (s.queue)::text))
                                  Heap Fetches: 0
                                  Index Searches: 0
              ->  Hash (actual rows=12.00 loops=1)
                    Buckets: 1024  Batches: 1  Memory Usage: 9kB
                    Buffers: shared hit=3157
                    ->  Subquery Scan on lc (actual rows=12.00 loops=1)
                          Buffers: shared hit=3157
                          ->  HashAggregate (actual rows=12.00 loops=1)
                                Group Key: sample.queue, sample.priority
                                Batches: 1  Memory Usage: 40kB
                                Buffers: shared hit=3157
                                ->  CTE Scan on sample (actual rows=3072.00 loops=1)
                                      Storage: Memory  Maximum Storage: 275kB
                                      Buffers: shared hit=3157
Planning:
  Buffers: shared hit=10
Planning Time: 3.316 ms
Execution Time: 11.374 ms
```
