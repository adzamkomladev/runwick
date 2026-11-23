# Why Redis Sentinel and Cluster looked about 15% slower

## The question

An earlier three-round result put Sentinel at about 85% of standalone throughput and Cluster at about 88%. The budget is no more than 5% slower. The question was how much of that gap comes from Redis itself (replication, or several nodes sharing one host) and how much from Runwick's Sentinel and Cluster client code.

## How it was run

Five setups, each running a 20,000-job drain with 16 workers, 8 calls and always-fsync. Every run used fresh containers. There were 3 rounds, with the order of the setups rotated each round.

- **A:** standalone Redis.
- **B:** standalone Redis plus 2 replicas, with Runwick still in standalone mode. This isolates the cost of replication.
- **C:** Sentinel (B plus 3 sentinels). Compared with B, this is Runwick's Sentinel client path.
- **D:** a one-node Cluster that owns all slots. Compared with A, this is Runwick's Cluster client path.
- **E:** a three-node Cluster. Compared with D, this is three nodes sharing the host.

Results are in `artifacts/perf/round-two/redis-ha-cause/`.

## Throughput (jobs/s)

| Setup | Round 1 | Round 2 | Round 3 | Median |
|---|---|---|---|---|
| A standalone | 452 | 509 | 247 | 452 |
| B + 2 replicas | 433 | 421 | 390 | 421 |
| C Sentinel | 334 | 599 | 369 | 369 |
| D one-node Cluster | 565 | 710 | 511 | 565 |
| E three-node Cluster | 603 | 461 | 521 | 521 |

Ratios of the medians:

| Comparison | Ratio | What it measures |
|---|---|---|
| B / A | 0.93 | replication |
| C / B | 0.88 | Sentinel client path |
| D / A | 1.25 | Cluster client path |
| E / D | 0.92 | three nodes on one host |

## Work on the primaries per job (all primaries added together)

| | A | B | C | D | E |
|---|---|---|---|---|---|
| evalsha | 0.42 | 0.44 | 0.43 | 0.47 | 0.46 |
| hset | 4.83 | 4.90 | 4.84 | 5.01 | 5.01 |
| zadd | 1.69 | 1.75 | 1.70 | 1.87 | 1.87 |
| server busy time (µs) | 909 | 955 | 1,011 | 862 | 912 |

## What we found

- **Replication** costs about 7%. Primary server time per job goes up about 5%, which matches.
- **Sentinel's client path** looks 12% slower, but Sentinel's own three rounds range from 334 to 599 jobs/s, a wider spread than the effect. Its commands per job match B's, apart from a handful of ping, info and replconf calls. No Runwick overhead shows up.
- **Cluster's client path** costs nothing measurable. The one-node Cluster beat standalone in every round. It sends about 8% more scripts and 4% more hash writes per job, and about 17% more finish round trips, because work that standalone sends in one batch is split by slot. That shows up in the command counts but not in throughput.
- **Three Cluster nodes on one host** look 8% slower on medians, but the per-round ratios disagree (1.07, 0.65, 1.02).
- The per-phase trace (claim, then handler to finish) follows throughput. When the host was slower, every phase was slower. No single phase stands out in C or D.

## Caveats

The host was shared with other sessions' benchmarks and builds. The load gate passed before each round (6.9, 3.8, 5.1), but the load when a setup actually started ranged from 2.2 to 10.1. The same setup swung up to 2× between rounds, and the two slowest runs (A round 3, C round 1) were the most loaded.

## What this means

The 15% gap is not explained by Runwick's Sentinel or Cluster client code. Replication accounts for about 7%, and the rest is within the host noise. These three rounds can't settle whether the true cost is under the 5% budget. That needs alternating A/C and A/E pairs with more rounds on a quiet host.
