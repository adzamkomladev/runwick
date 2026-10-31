### G1-M1

Result directory `final/perf/20261031-000411-G1-M1`; 2026-10-31T00:04:44 → 2026-10-31T00:16:02 UTC; load average at end 20.86 24.13 23.88 44/2874 3815052; correctness violations 0.

| Measurement | rep1 |
|---|---|
| label / database | rep1 / wl_g1_m1_rep1 |
| run failure | none |
| measured window | 599.7 s |
| offered / accepted / failed per s (producer) | — / — / — |
| producer failures by reason | — |
| confirmed finishes/s | 0 |
| backlog start → end (max, slope/s) | 0 → 0 (max 0, 0/s) |
| dispatch p50/p95/p99/max ms | — |
| due-to-Begin p50/p95/p99/max ms | — |
| pipeline end → confirmed Finish p50/p95/p99/max ms | — |
| reserve p50/p95/p99/max ms | — |
| begin p50/p95/p99/max ms | — |
| renew p50/p95/p99/max ms | — |
| finish p50/p95/p99/max ms | — |
| control-I/O wait (sampled) p50/p95/p99/max ms | 0 / 0 / 0 / 0 (n=600) |
| renewal margin min / p01 / p50 ms | — |
| missed renewal margins | 0 |
| lease-safety / ownership losses | 0 |
| pressure stops by reason (stopped s) | 0 (0 s) |
| WAL bytes/job; transactions/s; receipt writes/s | —; 2.73; 0 |
| lock waiters max / mean | 0 / 0 |
| connections max by application:role | wl-worker1:work 2, wl:sampler 1 |
| t_reserve / t_begin / t_finish mean (p99) ms | 0 (0) / 0 (0) / 0 (0) |
| store-time floor; slot-bound ceiling; C_claim | 0 ms; 160/s; —/s |
| worker1 CPU s, GC pause %, alloc/job KiB, RSS MiB, timer lag max ms | 52, 0.14, 23713.5, 101, 1131 |
| worker1 maxima | controlWaitMs 0, entries 0, finishing 0 |
| idle probes (whole run) / measured tx/s | 307 / 2.73 |
| correctness | 0 violation(s); jobs 0, succeeded 0, open 0, invoked more than once 0, >1 execution 0 |

| D-14 threshold | rep1 |
|---|---|
| correctness: zero violations | pass |

### G1-M2

Result directory `final/perf/20261031-001612-G1-M2`; 2026-10-31T00:16:52 → 2026-10-31T00:33:26 UTC; load average at end 21.45 21.54 22.08 64/2609 3851031; correctness violations 0.

| Measurement | rep1 |
|---|---|
| label / database | rep1 / wl_g1_m2_rep1 |
| run failure | none |
| measured window | 895 s |
| offered / accepted / failed per s (producer) | 0 / 0 / 0 |
| producer failures by reason | — |
| confirmed finishes/s | 0.13 |
| backlog start → end (max, slope/s) | 160 → 48 (max 160, -0.130/s) |
| dispatch p50/p95/p99/max ms | — |
| due-to-Begin p50/p95/p99/max ms | 481675 / 849023 / 849367 / 849434 (n=112) |
| pipeline end → confirmed Finish p50/p95/p99/max ms | 534 / 2093 / 2459 / 2656 (n=128) |
| reserve p50/p95/p99/max ms | 215 / 1136 / 1680 / 3262 (n=112) |
| begin p50/p95/p99/max ms | 169 / 1026 / 1731 / 3213 (n=112) |
| renew p50/p95/p99/max ms | 226 / 1482 / 2945 / 4481 (n=897) |
| finish p50/p95/p99/max ms | 274 / 1420 / 2448 / 2581 (n=128) |
| control-I/O wait (sampled) p50/p95/p99/max ms | 0 / 1197 / 2264 / 4235 (n=921) |
| renewal margin min / p01 / p50 ms | 39003 / 41191 / 45065 (n=897) |
| missed renewal margins | 0 |
| lease-safety / ownership losses | 0 |
| pressure stops by reason (stopped s) | controlWait 32 (161 s) |
| WAL bytes/job; transactions/s; receipt writes/s | 20905; 4.66; 0.38 |
| lock waiters max / mean | 0 / 0 |
| connections max by application:role | wl-producer:work 1, wl-worker1:control 4, wl-worker1:work 4, wl:sampler 1 |
| t_reserve / t_begin / t_finish mean (p99) ms | 354 (1680) / 311 (1731) / 423 (2448) |
| store-time floor; slot-bound ceiling; C_claim | 1087 ms; 0.1/s; 2.80/s |
| worker1 CPU s, GC pause %, alloc/job KiB, RSS MiB, timer lag max ms | 136, 0.15, 893.5, 144, 1965 |
| worker1 maxima | controlWaitMs 4235, entries 16, finishing 16 |
| correctness | 0 violation(s); jobs 176, succeeded 112, open 64, invoked more than once 0, >1 execution 0 |

| D-14 threshold | rep1 |
|---|---|
| healthy control I/O p99 ≤ 100 ms incl. admission | **miss** |
| zero lease-safety losses | pass |
| correctness: zero violations | pass |

<details><summary>Query plans at end of rep1 (EXPLAIN ANALYZE, BUFFERS)</summary>

`probe`

```
Limit  (cost=0.28..2.11 rows=1 width=24) (actual time=3.286..3.289 rows=1.00 loops=1)
  Buffers: shared hit=3
  ->  Nested Loop Semi Join  (cost=0.28..29.64 rows=16 width=24) (actual time=3.282..3.284 rows=1.00 loops=1)
        Join Filter: (((c.name)::text = (j.job_name)::text) AND (c.version = j.payload_version) AND ((c.profile_id)::text = (j.serializer_profile_id)::text) AND (c.profile_version = j.serializer_profile_version))
        Buffers: shared hit=3
        ->  Index Scan using jobs_ready on jobs j  (cost=0.27..29.15 rows=16 width=53) (actual time=3.117..3.118 rows=1.00 loops=1)
              Index Cond: (((application)::text = 'wl'::text) AND ((queue)::text = 'load'::text) AND (priority = 'medium'::text))
              Filter: (eligible_at <= clock_timestamp())
              Index Searches: 1
              Buffers: shared hit=3
        ->  Function Scan on c  (cost=0.01..0.02 rows=1 width=72) (actual time=0.155..0.155 rows=1.00 loops=1)
Planning:
  Buffers: shared hit=88
Planning Time: 2.004 ms
Execution Time: 3.444 ms
```

`reserveCandidate`

```
Limit  (cost=0.28..0.92 rows=1 width=126) (actual time=0.134..0.136 rows=1.00 loops=1)
  Buffers: shared hit=4
  ->  LockRows  (cost=0.28..31.08 rows=48 width=126) (actual time=0.133..0.134 rows=1.00 loops=1)
        Buffers: shared hit=4
        ->  Nested Loop Semi Join  (cost=0.28..30.60 rows=48 width=126) (actual time=0.091..0.092 rows=1.00 loops=1)
              Join Filter: (((c.name)::text = (j.job_name)::text) AND (c.version = j.payload_version) AND ((c.profile_id)::text = (j.serializer_profile_id)::text) AND (c.profile_version = j.serializer_profile_version))
              Buffers: shared hit=3
              ->  Index Scan using jobs_ready on jobs j  (cost=0.27..29.15 rows=48 width=59) (actual time=0.033..0.034 rows=1.00 loops=1)
                    Index Cond: (((application)::text = 'wl'::text) AND ((queue)::text = 'load'::text) AND (priority = 'medium'::text) AND (eligible_at <= statement_timestamp()))
                    Filter: (state = ANY ('{pending,retrying}'::text[]))
                    Index Searches: 1
                    Buffers: shared hit=3
              ->  Function Scan on c  (cost=0.01..0.02 rows=1 width=168) (actual time=0.055..0.055 rows=1.00 loops=1)
Planning Time: 0.462 ms
Execution Time: 0.181 ms
```

</details>

### P-02

Result directory `final/perf/20261031-003335-P-02`; 2026-10-31T00:33:57 → 2026-10-31T01:39:23 UTC; load average at end 15.41 18.54 17.79 23/2720 3998029; correctness violations 0.
Summary: sustainableRateSource = low ramp (deviation: the 50/s first step saturated); sustainableRate = 0.5.

Ramp `lowramp` steps:

| nominal/s | offered/s | accepted/s | finishes/s | backlog | saturated |
|---|---|---|---|---|---|
| 0.5 | 0.50 | 0.49 | 0.49 | 0 → 0 | False |
| 1 | 0.96 | 0.87 | 0.86 | 1 → 1 | True |

Ramp `ramp` steps:

| nominal/s | offered/s | accepted/s | finishes/s | backlog | saturated |
|---|---|---|---|---|---|
| 50 | 50.18 | 4.47 | 1.39 | 162 → 530 | True |

| Measurement | confirm1 | confirm2 | confirm3 |
|---|---|---|---|
| label / database | confirm1 / wl_p_02_confirm1 | confirm2 / wl_p_02_confirm2 | confirm3 / wl_p_02_confirm3 |
| run failure | none | none | none |
| measured window | 893.5 s | 895.1 s | 894.8 s |
| offered / accepted / failed per s (producer) | 0.48 / 0.48 / 0.00 | 0.48 / 0.48 / 0.00 | 0.48 / 0.48 / 0.01 |
| producer failures by reason | local_capacity_exceeded 3 | local_capacity_exceeded 3 | local_capacity_exceeded 6 |
| confirmed finishes/s | 0.48 | 0.48 | 0.47 |
| backlog start → end (max, slope/s) | 1 → 0 (max 8, -0.002/s) | 0 → 1 (max 5, 0/s) | 1 → 1 (max 5, 0.001/s) |
| dispatch p50/p95/p99/max ms | 237 / 1374 / 2559 / 4759 (n=431) | 252 / 1560 / 2745 / 4674 (n=433) | 329 / 1869 / 2961 / 3888 (n=428) |
| due-to-Begin p50/p95/p99/max ms | 1393 / 6552 / 19347 / 25777 (n=432) | 1386 / 5335 / 9778 / 11206 (n=433) | 1874 / 5215 / 6846 / 11659 (n=429) |
| pipeline end → confirmed Finish p50/p95/p99/max ms | 168 / 1257 / 2366 / 9297 (n=432) | 184 / 1278 / 2708 / 4635 (n=435) | 215 / 1488 / 2421 / 3226 (n=429) |
| reserve p50/p95/p99/max ms | 150 / 1173 / 2100 / 4575 (n=644) | 166 / 1184 / 2101 / 3013 (n=632) | 231 / 1480 / 2302 / 3318 (n=614) |
| begin p50/p95/p99/max ms | 199 / 1294 / 2051 / 3625 (n=432) | 208 / 1275 / 2143 / 3478 (n=433) | 276 / 1456 / 2350 / 4021 (n=429) |
| renew p50/p95/p99/max ms | — | — | — |
| finish p50/p95/p99/max ms | 167 / 1202 / 2017 / 3061 (n=432) | 183 / 1278 / 2708 / 4635 (n=435) | 215 / 1482 / 2421 / 3226 (n=429) |
| control-I/O wait (sampled) p50/p95/p99/max ms | 0 / 526 / 1867 / 7429 (n=900) | 0 / 584 / 1728 / 3800 (n=904) | 0 / 653 / 1610 / 2697 (n=901) |
| renewal margin min / p01 / p50 ms | — | — | — |
| missed renewal margins | 0 | 0 | 0 |
| lease-safety / ownership losses | 0 | 0 | 0 |
| pressure stops by reason (stopped s) | controlWait 14 (65 s) | controlWait 25 (103 s) | controlWait 15 (52 s) |
| WAL bytes/job; transactions/s; receipt writes/s | 17559; 8.50; 2.15 | 17684; 8.42; 2.15 | 17517; 8.19; 2.11 |
| lock waiters max / mean | 0 / 0 | 0 / 0 | 0 / 0 |
| connections max by application:role |  1, wl-producer:work 2, wl-worker1:control 2, wl-worker1:work 5, wl:sampler 1 | wl-producer:work 2, wl-worker1:control 2, wl-worker1:work 4, wl:sampler 1 |  1, wl-producer:work 2, wl-worker1:control 2, wl-worker1:work 5, wl:sampler 1 |
| t_reserve / t_begin / t_finish mean (p99) ms | 317 (2100) / 364 (2051) / 317 (2017) | 319 (2101) / 362 (2143) / 364 (2708) | 422 (2302) / 455 (2350) / 398 (2421) |
| store-time floor; slot-bound ceiling; C_claim | 998 ms; 23.5/s; 3.20/s | 1044 ms; 22/s; 3.10/s | 1275 ms; 18.8/s; 2.40/s |
| worker1 CPU s, GC pause %, alloc/job KiB, RSS MiB, timer lag max ms | 165, 0.16, 343.7, 126, 2016 | 139, 0.16, 335.1, 120, 1902 | 149, 0.28, 335.7, 119, 1587 |
| worker1 maxima | controlWaitMs 7429, entries 4, finishing 4 | controlWaitMs 4046, entries 4, finishing 2 | controlWaitMs 2697, entries 4, finishing 3 |
| correctness | 0 violation(s); jobs 495, succeeded 495, open 0, invoked more than once 0, >1 execution 0 | 0 violation(s); jobs 495, succeeded 495, open 0, invoked more than once 0, >1 execution 0 | 0 violation(s); jobs 492, succeeded 492, open 0, invoked more than once 0, >1 execution 0 |

| D-14 threshold | confirm1 | confirm2 | confirm3 |
|---|---|---|---|
| healthy control I/O p99 ≤ 100 ms incl. admission | **miss** | **miss** | **miss** |
| zero lease-safety losses | pass | pass | pass |
| correctness: zero violations | pass | pass | pass |

<details><summary>Query plans at end of confirm1 (EXPLAIN ANALYZE, BUFFERS)</summary>

`probe`

```
Limit  (cost=0.14..15.74 rows=1 width=24) (actual time=0.030..0.031 rows=0.00 loops=1)
  Buffers: shared hit=1
  ->  Nested Loop Semi Join  (cost=0.14..15.74 rows=1 width=24) (actual time=0.028..0.029 rows=0.00 loops=1)
        Join Filter: (((c.name)::text = (j.job_name)::text) AND (c.version = j.payload_version) AND ((c.profile_id)::text = (j.serializer_profile_id)::text) AND (c.profile_version = j.serializer_profile_version))
        Buffers: shared hit=1
        ->  Index Scan using jobs_ready on jobs j  (cost=0.13..15.70 rows=1 width=53) (actual time=0.028..0.028 rows=0.00 loops=1)
              Index Cond: (((application)::text = 'wl'::text) AND ((queue)::text = 'load'::text) AND (priority = 'medium'::text))
              Filter: (eligible_at <= clock_timestamp())
              Index Searches: 1
              Buffers: shared hit=1
        ->  Function Scan on c  (cost=0.01..0.02 rows=1 width=72) (never executed)
Planning:
  Buffers: shared hit=88
Planning Time: 0.833 ms
Execution Time: 0.092 ms
```

`reserveCandidate`

```
Limit  (cost=0.14..5.37 rows=1 width=126) (actual time=0.017..0.019 rows=0.00 loops=1)
  Buffers: shared hit=1
  ->  LockRows  (cost=0.14..15.83 rows=3 width=126) (actual time=0.015..0.016 rows=0.00 loops=1)
        Buffers: shared hit=1
        ->  Nested Loop Semi Join  (cost=0.14..15.80 rows=3 width=126) (actual time=0.015..0.016 rows=0.00 loops=1)
              Join Filter: (((c.name)::text = (j.job_name)::text) AND (c.version = j.payload_version) AND ((c.profile_id)::text = (j.serializer_profile_id)::text) AND (c.profile_version = j.serializer_profile_version))
              Buffers: shared hit=1
              ->  Index Scan using jobs_ready on jobs j  (cost=0.13..15.70 rows=3 width=59) (actual time=0.014..0.015 rows=0.00 loops=1)
                    Index Cond: (((application)::text = 'wl'::text) AND ((queue)::text = 'load'::text) AND (priority = 'medium'::text) AND (eligible_at <= statement_timestamp()))
                    Filter: (state = ANY ('{pending,retrying}'::text[]))
                    Index Searches: 1
                    Buffers: shared hit=1
              ->  Function Scan on c  (cost=0.01..0.02 rows=1 width=168) (never executed)
Planning Time: 0.729 ms
Execution Time: 0.087 ms
```

</details>

### P-01

Result directory `final/perf/20261031-015736-P-01`; 2026-10-31T01:57:58 → 2026-10-31T04:08:36 UTC; load average at end 11.25 12.68 12.88 5/2837 153735; correctness violations 0.

| Measurement | rep1 | rep2 | rep3 |
|---|---|---|---|
| label / database | rep1 / wl_p_01_rep1 | rep2 / wl_p_01_rep2 | rep3 / wl_p_01_rep3 |
| run failure | none | none | none |
| measured window | 894.7 s | 895.2 s | 894.8 s |
| offered / accepted / failed per s (producer) | 99.65 / 4.19 / 95.45 | 99.68 / 4.97 / 94.70 | 99.65 / 8.18 / 91.48 |
| producer failures by reason | local_capacity_exceeded 113883 | local_capacity_exceeded 113249 | local_capacity_exceeded 110455 |
| confirmed finishes/s | 1.25 | 1.54 | 3 |
| backlog start → end (max, slope/s) | 1305 → 3937 (max 3937, 2.857/s) | 1292 → 4357 (max 4357, 3.490/s) | 1175 → 5817 (max 5817, 4.803/s) |
| dispatch p50/p95/p99/max ms | 278 / 1555 / 2757 / 7206 (n=3782) | 238 / 1287 / 2113 / 4771 (n=4479) | 148 / 770 / 1497 / 4792 (n=7376) |
| due-to-Begin p50/p95/p99/max ms | 981454 / 1154826 / 1180510 / 1181523 (n=5080) | 941265 / 969798 / 1006853 / 1041940 (n=5743) | 788242 / 1816085 / 1923378 / 1934909 (n=8548) |
| pipeline end → confirmed Finish p50/p95/p99/max ms | 224 / 1123 / 2336 / 8115 (n=5081) | 197 / 1012 / 1873 / 4723 (n=5744) | 196 / 936 / 1841 / 8083 (n=8549) |
| reserve p50/p95/p99/max ms | 194 / 1044 / 1968 / 5974 (n=5080) | 166 / 865 / 1751 / 3861 (n=5743) | 172 / 846 / 1691 / 3858 (n=8548) |
| begin p50/p95/p99/max ms | 171 / 987 / 1928 / 4862 (n=5080) | 146 / 820 / 1657 / 3740 (n=5743) | 151 / 795 / 1655 / 4507 (n=8548) |
| renew p50/p95/p99/max ms | — | — | — |
| finish p50/p95/p99/max ms | 214 / 1083 / 2309 / 4381 (n=5081) | 187 / 950 / 1822 / 4713 (n=5744) | 189 / 918 / 1759 / 4439 (n=8549) |
| control-I/O wait (sampled) p50/p95/p99/max ms | 90 / 1388 / 2617 / 6263 (n=2083) | 95 / 1233 / 2336 / 4434 (n=1943) | 92 / 1162 / 2204 / 6790 (n=2838) |
| renewal margin min / p01 / p50 ms | — | — | — |
| missed renewal margins | 0 | 0 | 0 |
| lease-safety / ownership losses | 0 | 0 | 0 |
| pressure stops by reason (stopped s) | controlWait 106 (492 s) | controlWait 102 (426 s) | controlWait 142 (585 s) |
| WAL bytes/job; transactions/s; receipt writes/s | 25070; 18.39; 7.94 | 24513; 21.76; 9.58 | 22056; 37.12; 17.17 |
| lock waiters max / mean | 0 / 0 | 0 / 0 | 0 / 0 |
| connections max by application:role |  1, wl-producer:work 2, wl-worker1:control 2, wl-worker1:work 5, wl:sampler 1 |  1, wl-producer:work 2, wl-worker1:control 2, wl-worker1:work 5, wl:sampler 1 |  1, wl-producer:work 2, wl-worker1:control 2, wl-worker1:work 5, wl:sampler 1 |
| t_reserve / t_begin / t_finish mean (p99) ms | 321 (1968) / 297 (1928) / 347 (2309) | 275 (1751) / 251 (1657) / 297 (1822) | 272 (1691) / 248 (1655) / 295 (1759) |
| store-time floor; slot-bound ceiling; C_claim | 965 ms; 21.5/s; 3.10/s | 822 ms; 24.7/s; 3.60/s | 815 ms; 24.9/s; 3.70/s |
| worker1 CPU s, GC pause %, alloc/job KiB, RSS MiB, timer lag max ms | 624, 0.29, 233.6, 133, 1802 | 591, 0.36, 232.4, 125, 1696 | 1009, 0.27, 233.2, 134, 1271 |
| worker1 maxima | controlWaitMs 6263, entries 8, finishing 5 | controlWaitMs 4433, entries 6, finishing 4 | controlWaitMs 6790, entries 6, finishing 5 |
| correctness | 0 violation(s); jobs 5779, succeeded 5779, open 0, invoked more than once 0, >1 execution 0 | 0 violation(s); jobs 6315, succeeded 6315, open 0, invoked more than once 0, >1 execution 0 | 0 violation(s); jobs 9096, succeeded 9096, open 0, invoked more than once 0, >1 execution 0 |

| D-14 threshold | rep1 | rep2 | rep3 |
|---|---|---|---|
| throughput ≥ 100 finishes/s, no sustained backlog growth | **miss** | **miss** | **miss** |
| dispatch p95 ≤ 50 ms / p99 ≤ 200 ms | **miss** | **miss** | **miss** |
| due-to-Begin p99 ≤ 2 s | **miss** | **miss** | **miss** |
| pipeline → confirmed Finish p99 ≤ 500 ms | **miss** | **miss** | **miss** |
| healthy control I/O p99 ≤ 100 ms incl. admission | **miss** | **miss** | **miss** |
| zero lease-safety losses | pass | pass | pass |
| correctness: zero violations | pass | pass | pass |

<details><summary>Query plans at end of rep1 (EXPLAIN ANALYZE, BUFFERS)</summary>

`probe`

```
Limit  (cost=0.15..15.09 rows=1 width=24) (actual time=0.014..0.016 rows=0.00 loops=1)
  Buffers: shared hit=1
  ->  Nested Loop Semi Join  (cost=0.15..209.25 rows=14 width=24) (actual time=0.013..0.013 rows=0.00 loops=1)
        Join Filter: (((c.name)::text = (j.job_name)::text) AND (c.version = j.payload_version) AND ((c.profile_id)::text = (j.serializer_profile_id)::text) AND (c.profile_version = j.serializer_profile_version))
        Buffers: shared hit=1
        ->  Index Scan using jobs_ready on jobs j  (cost=0.14..208.82 rows=14 width=53) (actual time=0.012..0.013 rows=0.00 loops=1)
              Index Cond: (((application)::text = 'wl'::text) AND ((queue)::text = 'load'::text) AND (priority = 'medium'::text))
              Filter: (eligible_at <= clock_timestamp())
              Index Searches: 1
              Buffers: shared hit=1
        ->  Function Scan on c  (cost=0.01..0.02 rows=1 width=72) (never executed)
Planning:
  Buffers: shared hit=88
Planning Time: 0.840 ms
Execution Time: 0.070 ms
```

`reserveCandidate`

```
Limit  (cost=0.15..5.28 rows=1 width=126) (actual time=0.019..0.020 rows=0.00 loops=1)
  Buffers: shared hit=1
  ->  LockRows  (cost=0.15..210.47 rows=41 width=126) (actual time=0.017..0.017 rows=0.00 loops=1)
        Buffers: shared hit=1
        ->  Nested Loop Semi Join  (cost=0.15..210.06 rows=41 width=126) (actual time=0.016..0.017 rows=0.00 loops=1)
              Join Filter: (((c.name)::text = (j.job_name)::text) AND (c.version = j.payload_version) AND ((c.profile_id)::text = (j.serializer_profile_id)::text) AND (c.profile_version = j.serializer_profile_version))
              Buffers: shared hit=1
              ->  Index Scan using jobs_ready on jobs j  (cost=0.14..208.82 rows=41 width=59) (actual time=0.016..0.016 rows=0.00 loops=1)
                    Index Cond: (((application)::text = 'wl'::text) AND ((queue)::text = 'load'::text) AND (priority = 'medium'::text) AND (eligible_at <= statement_timestamp()))
                    Filter: (state = ANY ('{pending,retrying}'::text[]))
                    Index Searches: 1
                    Buffers: shared hit=1
              ->  Function Scan on c  (cost=0.01..0.02 rows=1 width=168) (never executed)
Planning Time: 0.488 ms
Execution Time: 0.067 ms
```

</details>

### P-08a

Result directory `final/perf/20261031-040840-P-08a`; 2026-10-31T04:09:03 → 2026-10-31T05:19:55 UTC; load average at end 6.77 7.62 8.36 5/2816 303977; correctness violations 0.

| Measurement | rep1 |
|---|---|
| label / database | rep1 / wl_p_08a_rep1 |
| run failure | none |
| measured window | 598.5 s |
| offered / accepted / failed per s (producer) | 99.16 / 7.84 / 91.33 |
| producer failures by reason | local_capacity_exceeded 65839 |
| confirmed finishes/s | 0.51 |
| backlog start → end (max, slope/s) | 777 → 5164 (max 5164, 7.143/s) |
| dispatch p50/p95/p99/max ms | 158 / 798 / 1542 / 4951 (n=4704) |
| due-to-Begin p50/p95/p99/max ms | 2399716 / 3416112 / 3489579 / 3502867 (n=5487) |
| pipeline end → confirmed Finish p50/p95/p99/max ms | 349 / 1652 / 3050 / 9844 (n=5487) |
| reserve p50/p95/p99/max ms | 325 / 1463 / 2762 / 4978 (n=5489) |
| begin p50/p95/p99/max ms | 291 / 1387 / 2605 / 5310 (n=5487) |
| renew p50/p95/p99/max ms | — |
| finish p50/p95/p99/max ms | 348 / 1636 / 3038 / 5420 (n=5488) |
| control-I/O wait (sampled) p50/p95/p99/max ms | 179 / 2859 / 5018 / 9004 (n=4108) |
| renewal margin min / p01 / p50 ms | — |
| missed renewal margins | 0 |
| lease-safety / ownership losses | 0 |
| pressure stops by reason (stopped s) | controlWait 272 (1565 s) |
| WAL bytes/job; transactions/s; receipt writes/s | 74346; 20.87; 9.38 |
| lock waiters max / mean | 0 / 0 |
| connections max by application:role |  1, wl-producer:work 2, wl-worker1:control 2, wl-worker1:work 4, wl:sampler 1 |
| t_reserve / t_begin / t_finish mean (p99) ms | 489 (2762) / 449 (2605) / 538 (3038) |
| store-time floor; slot-bound ceiling; C_claim | 1475 ms; 14.7/s; 2/s |
| worker1 CPU s, GC pause %, alloc/job KiB, RSS MiB, timer lag max ms | 1058, 0.12, 246.4, 127, 1361 |
| worker1 maxima | controlWaitMs 9004, entries 4, finishing 3 |
| correctness | 0 violation(s); jobs 5647, succeeded 5647, open 0, invoked more than once 1, >1 execution 1 |

| D-14 threshold | rep1 |
|---|---|
| resource counts within declared caps (connections control 6 / work 8; entries ≤ 16) | pass |
| admission pauses | pass |
| correctness: zero violations | pass |

<details><summary>Query plans at end of rep1 (EXPLAIN ANALYZE, BUFFERS)</summary>

`probe`

```
Limit  (cost=0.26..6.07 rows=1 width=24) (actual time=0.221..0.222 rows=0.00 loops=1)
  Buffers: shared hit=3
  ->  Nested Loop Semi Join  (cost=0.26..6.07 rows=1 width=24) (actual time=0.218..0.219 rows=0.00 loops=1)
        Join Filter: (((c.name)::text = (j.job_name)::text) AND (c.version = j.payload_version) AND ((c.profile_id)::text = (j.serializer_profile_id)::text) AND (c.profile_version = j.serializer_profile_version))
        Buffers: shared hit=3
        ->  Index Scan using jobs_ready on jobs j  (cost=0.25..6.03 rows=1 width=53) (actual time=0.218..0.219 rows=0.00 loops=1)
              Index Cond: (((application)::text = 'wl'::text) AND ((queue)::text = 'load'::text) AND (priority = 'medium'::text))
              Filter: (eligible_at <= clock_timestamp())
              Index Searches: 1
              Buffers: shared hit=3
        ->  Function Scan on c  (cost=0.01..0.02 rows=1 width=72) (never executed)
Planning:
  Buffers: shared hit=88
Planning Time: 1.692 ms
Execution Time: 0.300 ms
```

`reserveCandidate`

```
Limit  (cost=0.26..6.08 rows=1 width=126) (actual time=0.030..0.031 rows=0.00 loops=1)
  Buffers: shared hit=3
  ->  LockRows  (cost=0.26..6.08 rows=1 width=126) (actual time=0.027..0.028 rows=0.00 loops=1)
        Buffers: shared hit=3
        ->  Nested Loop Semi Join  (cost=0.26..6.07 rows=1 width=126) (actual time=0.027..0.028 rows=0.00 loops=1)
              Join Filter: (((c.name)::text = (j.job_name)::text) AND (c.version = j.payload_version) AND ((c.profile_id)::text = (j.serializer_profile_id)::text) AND (c.profile_version = j.serializer_profile_version))
              Buffers: shared hit=3
              ->  Index Scan using jobs_ready on jobs j  (cost=0.25..6.03 rows=1 width=59) (actual time=0.026..0.027 rows=0.00 loops=1)
                    Index Cond: (((application)::text = 'wl'::text) AND ((queue)::text = 'load'::text) AND (priority = 'medium'::text) AND (eligible_at <= statement_timestamp()))
                    Filter: (state = ANY ('{pending,retrying}'::text[]))
                    Index Searches: 1
                    Buffers: shared hit=3
              ->  Function Scan on c  (cost=0.01..0.02 rows=1 width=168) (never executed)
Planning Time: 0.447 ms
Execution Time: 0.085 ms
```

</details>

### P-08b

Result directory `final/perf/20261031-051958-P-08b`; 2026-10-31T05:20:45 → 2026-10-31T06:08:13 UTC; load average at end 7.66 8.41 8.29 10/2797 411878; correctness violations 0.

| Measurement | rep1 |
|---|---|
| label / database | rep1 / wl_p_08b_rep1 |
| run failure | none |
| measured window | 595.7 s |
| offered / accepted / failed per s (producer) | 99.16 / 8.75 / 90.46 |
| producer failures by reason | local_capacity_exceeded 65253 |
| confirmed finishes/s | 0.71 |
| backlog start → end (max, slope/s) | 853 → 5639 (max 5639, 7.827/s) |
| dispatch p50/p95/p99/max ms | 144 / 718 / 1325 / 3562 (n=5264) |
| due-to-Begin p50/p95/p99/max ms | 1229868 / 1932252 / 2089976 / 2103784 (n=6085) |
| pipeline end → confirmed Finish p50/p95/p99/max ms | 258 / 999 / 1786 / 4470 (n=6085) |
| reserve p50/p95/p99/max ms | 238 / 918 / 1763 / 3976 (n=6086) |
| begin p50/p95/p99/max ms | 213 / 843 / 1565 / 3701 (n=6086) |
| renew p50/p95/p99/max ms | — |
| finish p50/p95/p99/max ms | 256 / 993 / 1776 / 4469 (n=6086) |
| control-I/O wait (sampled) p50/p95/p99/max ms | 174 / 2948 / 6747 / 26534 (n=2710) |
| renewal margin min / p01 / p50 ms | — |
| missed renewal margins | 0 |
| lease-safety / ownership losses | lost:BeginUnresolved 1 |
| pressure stops by reason (stopped s) | controlWait 99 (713 s) |
| WAL bytes/job; transactions/s; receipt writes/s | 61331; 23.76; 10.90 |
| lock waiters max / mean | 0 / 0 |
| connections max by application:role |  1, wl-producer:work 2, wl-worker1:control 2, wl-worker1:work 4, wl:sampler 1 |
| t_reserve / t_begin / t_finish mean (p99) ms | 333 (1763) / 300 (1565) / 356 (1776) |
| store-time floor; slot-bound ceiling; C_claim | 989 ms; 21.2/s; 3/s |
| worker1 CPU s, GC pause %, alloc/job KiB, RSS MiB, timer lag max ms | 888, 0.16, 240.5, 127, 995 |
| worker1 maxima | controlWaitMs 26533, entries 4, finishing 3 |
| correctness | 0 violation(s); jobs 6233, succeeded 6233, open 0, invoked more than once 1, >1 execution 1 |

| D-14 threshold | rep1 |
|---|---|
| resource counts within declared caps (connections control 6 / work 8; entries ≤ 16) | pass |
| admission pauses | pass |
| correctness: zero violations | pass |

<details><summary>Query plans at end of rep1 (EXPLAIN ANALYZE, BUFFERS)</summary>

`probe`

```
Limit  (cost=0.28..7.01 rows=1 width=24) (actual time=0.048..0.049 rows=0.00 loops=1)
  Buffers: shared hit=4
  ->  Nested Loop Semi Join  (cost=0.28..404.21 rows=60 width=24) (actual time=0.045..0.046 rows=0.00 loops=1)
        Join Filter: (((c.name)::text = (j.job_name)::text) AND (c.version = j.payload_version) AND ((c.profile_id)::text = (j.serializer_profile_id)::text) AND (c.profile_version = j.serializer_profile_version))
        Buffers: shared hit=4
        ->  Index Scan using jobs_ready on jobs j  (cost=0.27..402.40 rows=60 width=53) (actual time=0.044..0.045 rows=0.00 loops=1)
              Index Cond: (((application)::text = 'wl'::text) AND ((queue)::text = 'load'::text) AND (priority = 'medium'::text))
              Filter: (eligible_at <= clock_timestamp())
              Index Searches: 1
              Buffers: shared hit=4
        ->  Function Scan on c  (cost=0.01..0.02 rows=1 width=72) (never executed)
Planning:
  Buffers: shared hit=88
Planning Time: 5.455 ms
Execution Time: 1.210 ms
```

`reserveCandidate`

```
Limit  (cost=0.28..2.54 rows=1 width=126) (actual time=0.033..0.034 rows=0.00 loops=1)
  Buffers: shared hit=4
  ->  LockRows  (cost=0.28..409.63 rows=181 width=126) (actual time=0.031..0.032 rows=0.00 loops=1)
        Buffers: shared hit=4
        ->  Nested Loop Semi Join  (cost=0.28..407.82 rows=181 width=126) (actual time=0.031..0.031 rows=0.00 loops=1)
              Join Filter: (((c.name)::text = (j.job_name)::text) AND (c.version = j.payload_version) AND ((c.profile_id)::text = (j.serializer_profile_id)::text) AND (c.profile_version = j.serializer_profile_version))
              Buffers: shared hit=4
              ->  Index Scan using jobs_ready on jobs j  (cost=0.27..402.38 rows=181 width=59) (actual time=0.030..0.031 rows=0.00 loops=1)
                    Index Cond: (((application)::text = 'wl'::text) AND ((queue)::text = 'load'::text) AND (priority = 'medium'::text) AND (eligible_at <= statement_timestamp()))
                    Filter: (state = ANY ('{pending,retrying}'::text[]))
                    Index Searches: 1
                    Buffers: shared hit=4
              ->  Function Scan on c  (cost=0.01..0.02 rows=1 width=168) (never executed)
Planning Time: 1.397 ms
Execution Time: 0.077 ms
```

</details>

### G1-M3

Result directory `final/perf/20261031-074106-G1-M3`; 2026-10-31T07:41:27 → 2026-10-31T07:58:48 UTC; load average at end 3.20 4.85 6.85 10/2942 661108; correctness violations 0.

| Measurement | rep1 |
|---|---|
| label / database | rep1 / wl_g1_m3_rep1 |
| run failure | none |
| measured window | 894.7 s |
| offered / accepted / failed per s (producer) | 0.48 / 0.48 / 0 |
| producer failures by reason | — |
| confirmed finishes/s | 0.48 |
| backlog start → end (max, slope/s) | 0 → 1 (max 3, 0/s) |
| dispatch p50/p95/p99/max ms | 156 / 986 / 1944 / 3668 (n=434) |
| due-to-Begin p50/p95/p99/max ms | 851 / 2358 / 4773 / 5625 (n=434) |
| pipeline end → confirmed Finish p50/p95/p99/max ms | 120 / 746 / 1443 / 1806 (n=434) |
| reserve p50/p95/p99/max ms | 100 / 668 / 1402 / 3371 (n=829) |
| begin p50/p95/p99/max ms | 118 / 559 / 1303 / 2586 (n=434) |
| renew p50/p95/p99/max ms | — |
| finish p50/p95/p99/max ms | 120 / 745 / 1442 / 1806 (n=434) |
| control-I/O wait (sampled) p50/p95/p99/max ms | 0 / 0 / 428 / 1557 (n=1809) |
| renewal margin min / p01 / p50 ms | — |
| missed renewal margins | 0 |
| lease-safety / ownership losses | 0 |
| pressure stops by reason (stopped s) | controlWait 4 (13 s) |
| WAL bytes/job; transactions/s; receipt writes/s | 19944; 11.61; 2.37 |
| lock waiters max / mean | 2 / 0.04 |
| connections max by application:role | wl-producer:work 2, wl-worker1:control 2, wl-worker1:work 3, wl-worker2:control 2, wl-worker2:work 3, wl:sampler 1 |
| t_reserve / t_begin / t_finish mean (p99) ms | 189 (1402) / 175 (1303) / 190 (1442) |
| store-time floor; slot-bound ceiling; C_claim | 555 ms; 43.8/s; 10.60/s |
| worker1 CPU s, GC pause %, alloc/job KiB, RSS MiB, timer lag max ms | 117, 0.02, 478.6, 117, 843 |
| worker1 maxima | controlWaitMs 1392, entries 2, finishing 2 |
| worker2 CPU s, GC pause %, alloc/job KiB, RSS MiB, timer lag max ms | 117, 0.03, 457.2, 120, 868 |
| worker2 maxima | controlWaitMs 1632, entries 3, finishing 3 |
| correctness | 0 violation(s); jobs 498, succeeded 498, open 0, invoked more than once 0, >1 execution 0 |

| D-14 threshold | rep1 |
|---|---|
| healthy control I/O p99 ≤ 100 ms incl. admission | **miss** |
| correctness: zero violations | pass |

<details><summary>Query plans at end of rep1 (EXPLAIN ANALYZE, BUFFERS)</summary>

`probe`

```
Limit  (cost=0.14..5.94 rows=1 width=24) (actual time=0.027..0.028 rows=0.00 loops=1)
  Buffers: shared hit=1
  ->  Nested Loop Semi Join  (cost=0.14..5.94 rows=1 width=24) (actual time=0.025..0.026 rows=0.00 loops=1)
        Join Filter: (((c.name)::text = (j.job_name)::text) AND (c.version = j.payload_version) AND ((c.profile_id)::text = (j.serializer_profile_id)::text) AND (c.profile_version = j.serializer_profile_version))
        Buffers: shared hit=1
        ->  Index Scan using jobs_ready on jobs j  (cost=0.12..5.90 rows=1 width=53) (actual time=0.024..0.025 rows=0.00 loops=1)
              Index Cond: (((application)::text = 'wl'::text) AND ((queue)::text = 'load'::text) AND (priority = 'medium'::text))
              Filter: (eligible_at <= clock_timestamp())
              Index Searches: 1
              Buffers: shared hit=1
        ->  Function Scan on c  (cost=0.01..0.02 rows=1 width=72) (never executed)
Planning:
  Buffers: shared hit=88
Planning Time: 1.258 ms
Execution Time: 0.103 ms
```

`reserveCandidate`

```
Limit  (cost=0.14..5.95 rows=1 width=126) (actual time=0.019..0.021 rows=0.00 loops=1)
  Buffers: shared hit=1
  ->  LockRows  (cost=0.14..5.95 rows=1 width=126) (actual time=0.017..0.018 rows=0.00 loops=1)
        Buffers: shared hit=1
        ->  Nested Loop Semi Join  (cost=0.14..5.94 rows=1 width=126) (actual time=0.016..0.017 rows=0.00 loops=1)
              Join Filter: (((c.name)::text = (j.job_name)::text) AND (c.version = j.payload_version) AND ((c.profile_id)::text = (j.serializer_profile_id)::text) AND (c.profile_version = j.serializer_profile_version))
              Buffers: shared hit=1
              ->  Index Scan using jobs_ready on jobs j  (cost=0.13..5.91 rows=1 width=59) (actual time=0.016..0.016 rows=0.00 loops=1)
                    Index Cond: (((application)::text = 'wl'::text) AND ((queue)::text = 'load'::text) AND (priority = 'medium'::text) AND (eligible_at <= statement_timestamp()))
                    Filter: (state = ANY ('{pending,retrying}'::text[]))
                    Index Searches: 1
                    Buffers: shared hit=1
              ->  Function Scan on c  (cost=0.01..0.02 rows=1 width=168) (never executed)
Planning Time: 0.482 ms
Execution Time: 0.069 ms
```

</details>

### P-08c

Result directory `final/perf/20261031-075850-P-08c`; 2026-10-31T07:59:10 → 2026-10-31T08:26:42 UTC; load average at end 6.56 8.14 8.75 4/2968 722800; correctness violations 0.

| Measurement | rep1 |
|---|---|
| label / database | rep1 / wl_p_08c_rep1 |
| run failure | none |
| measured window | 598.6 s |
| offered / accepted / failed per s (producer) | 99.34 / 8.24 / 91.10 |
| producer failures by reason | local_capacity_exceeded 66288, store_unavailable 6121 |
| confirmed finishes/s | 3.56 |
| backlog start → end (max, slope/s) | 561 → 3954 (max 3954, 5.538/s) |
| dispatch p50/p95/p99/max ms | 142 / 639 / 1222 / 2848 (n=5556) |
| due-to-Begin p50/p95/p99/max ms | 441288 / 790695 / 834080 / 839130 (n=6119) |
| pipeline end → confirmed Finish p50/p95/p99/max ms | 164 / 724 / 1303 / 5536 (n=6118) |
| reserve p50/p95/p99/max ms | 141 / 606 / 1177 / 3276 (n=6121) |
| begin p50/p95/p99/max ms | 123 / 582 / 1111 / 3285 (n=6120) |
| renew p50/p95/p99/max ms | — |
| finish p50/p95/p99/max ms | 158 / 683 / 1256 / 3916 (n=6120) |
| control-I/O wait (sampled) p50/p95/p99/max ms | 92 / 895 / 1677 / 4925 (n=1517) |
| renewal margin min / p01 / p50 ms | — |
| missed renewal margins | 0 |
| lease-safety / ownership losses | lost:BeginUnresolved 1 |
| pressure stops by reason (stopped s) | controlWait 45, finish 1 (207 s) |
| WAL bytes/job; transactions/s; receipt writes/s | 21496; 42.69; 19.88 |
| lock waiters max / mean | 0 / 0 |
| connections max by application:role |  1, wl-producer:work 2, wl-worker1:control 2, wl-worker1:work 5, wl:sampler 1 |
| t_reserve / t_begin / t_finish mean (p99) ms | 209 (1177) / 189 (1111) / 232 (1256) |
| store-time floor; slot-bound ceiling; C_claim | 630 ms; 30.7/s; 4.80/s |
| worker1 CPU s, GC pause %, alloc/job KiB, RSS MiB, timer lag max ms | 688, 0.26, 236.1, 119, 1024 |
| worker1 restoration → ready | 6.4 s |
| worker1 maxima | controlWaitMs 4924, entries 6, finishing 5 |
| correctness | 0 violation(s); jobs 6384, succeeded 6384, open 0, invoked more than once 2, >1 execution 2 |

| D-14 threshold | rep1 |
|---|---|
| resource counts within declared caps (connections control 6 / work 8; entries ≤ 16) | pass |
| admission pauses | pass |
| readiness ≤ 30 s after restoration | pass |
| correctness: zero violations | pass |

<details><summary>Query plans at end of rep1 (EXPLAIN ANALYZE, BUFFERS)</summary>

`probe`

```
Limit  (cost=0.28..4.85 rows=1 width=24) (actual time=0.016..0.017 rows=0.00 loops=1)
  Buffers: shared hit=3
  ->  Nested Loop Semi Join  (cost=0.28..439.37 rows=96 width=24) (actual time=0.014..0.015 rows=0.00 loops=1)
        Join Filter: (((c.name)::text = (j.job_name)::text) AND (c.version = j.payload_version) AND ((c.profile_id)::text = (j.serializer_profile_id)::text) AND (c.profile_version = j.serializer_profile_version))
        Buffers: shared hit=3
        ->  Index Scan using jobs_ready on jobs j  (cost=0.27..436.48 rows=96 width=53) (actual time=0.014..0.014 rows=0.00 loops=1)
              Index Cond: (((application)::text = 'wl'::text) AND ((queue)::text = 'load'::text) AND (priority = 'medium'::text))
              Filter: (eligible_at <= clock_timestamp())
              Index Searches: 1
              Buffers: shared hit=3
        ->  Function Scan on c  (cost=0.01..0.02 rows=1 width=72) (never executed)
Planning:
  Buffers: shared hit=88
Planning Time: 0.573 ms
Execution Time: 0.060 ms
```

`reserveCandidate`

```
Limit  (cost=0.28..1.84 rows=1 width=126) (actual time=0.024..0.026 rows=0.00 loops=1)
  Buffers: shared hit=3
  ->  LockRows  (cost=0.28..447.40 rows=287 width=126) (actual time=0.023..0.024 rows=0.00 loops=1)
        Buffers: shared hit=3
        ->  Nested Loop Semi Join  (cost=0.28..444.53 rows=287 width=126) (actual time=0.023..0.023 rows=0.00 loops=1)
              Join Filter: (((c.name)::text = (j.job_name)::text) AND (c.version = j.payload_version) AND ((c.profile_id)::text = (j.serializer_profile_id)::text) AND (c.profile_version = j.serializer_profile_version))
              Buffers: shared hit=3
              ->  Index Scan using jobs_ready on jobs j  (cost=0.27..435.91 rows=287 width=59) (actual time=0.022..0.022 rows=0.00 loops=1)
                    Index Cond: (((application)::text = 'wl'::text) AND ((queue)::text = 'load'::text) AND (priority = 'medium'::text) AND (eligible_at <= statement_timestamp()))
                    Filter: (state = ANY ('{pending,retrying}'::text[]))
                    Index Searches: 1
                    Buffers: shared hit=3
              ->  Function Scan on c  (cost=0.01..0.02 rows=1 width=168) (never executed)
Planning Time: 0.401 ms
Execution Time: 0.065 ms
```

</details>

