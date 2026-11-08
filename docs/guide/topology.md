# Which store, which topology

All three stores give the same job behaviour (same states, retries, receipts, uniqueness and rate rules). They differ in where they can run and what they survive.

| | PostgreSQL | SQLite | Redis |
|---|---|---|---|
| Hosts | Many | One machine only | Many, all reaching one primary |
| Server | PostgreSQL 16, 17, 18 (18 is the reference) | A local file; SQLite 3.38 or newer (bundled) | Redis 8.2 or newer, one standalone primary |
| Deployment shapes (embedded, external, mixed workers) | All five | All five, if every process is on that machine | All five |
| Commit a job or completion with your own data | Yes (same database) | Yes (same file) | No; use the outbox |
| New-work wake-up between processes | Polling | Polling | Polling |
| Refused at startup | Replica or proxy profiles are not certified | Network share, in-memory database, weaker-than-FULL sync | Replica, Cluster, Sentinel, wrong AOF settings |

## Limits you should plan around

**PostgreSQL.** Connect to the primary directly. Ownership decisions never read from a replica. High-availability and connection-pooler setups are not certified; if you use one, you are outside what has been tested.

**SQLite.** One file on one machine. There is no multi-host support and no high availability. Durability is whatever your disk and its fsync give you; restoring the file from a backup is a break in history. Only one write happens at a time across all processes, and nothing here says SQLite matches PostgreSQL's throughput. A process being killed is not a power-loss test.

**Redis.** One primary, no high availability. Failover, Sentinel promotion and Cluster are not supported, and a promoted replica may have lost acknowledged jobs. Durability is the primary's disk and `appendfsync always`. One application lives in one hash slot, so its throughput is bounded by one server thread and one very large command can delay everything else for that application. Redis cannot join your business transaction. Results say nothing about speed relative to PostgreSQL, and Redis without `appendfsync always` is not a supported configuration.

## Roles are configuration, not separate programs

A process can be a producer (dispatches jobs), a worker (`AddWorker`), a scheduler (`AddScheduler`) and a management host (`MapRunwickManagement`), in any combination. Typical shapes: one app doing everything; a web app that only dispatches plus a worker service; many replicas each doing both. Rate limits and some local allowances apply per process, not across replicas.

The scheduler and the autoscaler also have limits worth knowing: pausing a schedule stops new jobs but does not pause ones already created; the local autoscaler adjusts concurrency inside one process and does not coordinate across the cluster.
