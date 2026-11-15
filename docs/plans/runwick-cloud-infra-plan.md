# Runwick Cloud: infrastructure, costs and store choice

*Draft for review. Written 2026-11-11 against main at `11fa7f7`. Not approved. It changes no spec: every spec change still goes into `docs/spec/` with a date. It builds on [commercial-strategy-and-seams.md](../commercial-strategy-and-seams.md), which records the product decision (a hosted job store, "Shape C").*

## How to read this

- **Read in the repo** means I opened the file (spec 04, the backlog, the strategy document).
- **From a vendor page** means I fetched the vendor's page on 2026-11-11. Prices are list prices, on demand, compute only, and vendors change them.
- **Not verified** means I could not confirm it. Section 11 lists these. Verify them before committing money.
- Nothing here is built. There is no `Runwick.Cloud` project yet, and SP-031, SP-033 and SP-034 are not started.

## 1. The decision in one page

1. **Job store: real PostgreSQL 16–18 only.** The store uses `SKIP LOCKED` row claims, advisory locks, deferred constraint triggers and partitioned receipts (spec 04). ClickHouse and SingleStore have none of that, so neither can be the job store.
2. **Where to run Postgres: PlanetScale first.** **PlanetScale Postgres** is the lead choice for the shared and dedicated tiers. It is real Postgres, its 3-node HA is priced below Azure's doubling, and its local-NVMe Metal tier may suit the claim-heavy workload. On Azure it exists in three regions only (`centralus`, `eastus2`, `westeurope`), and its Azure price, replication mode and Private Link support are not verified (section 11). Phase 0 must clear those before we commit. **Azure Database for PostgreSQL Flexible Server** is the fallback and covers every other Azure region.
3. **ClickHouse Postgres is out for now.** It is generally available on AWS and in private preview on GCP. I found no Azure support.
4. **ClickHouse Cloud is in, as an analytics copy.** It runs on Azure. It holds dashboard history and metering rollups, fed from the events cursor. It can be rebuilt at any time, and Postgres receipts stay the billing source of truth.
5. **SingleStore: skip.** It would add a third database to run for work ClickHouse already does.
6. **Compute: AKS.** Stateless gRPC pods across three zones.
7. **Enterprise store choice** is three levels (section 5). A host counts as supported only if it passes our driver contract suite and the G-2 benchmark.
8. **Migration to and from self-host** is a new backlog item, SP-035 (section 8). It does not exist today.
9. **Dashboards are three separate things** (section 6): the free self-host dashboard stays as it is, the hosted customer dashboard needs a tenant-aware layer the current one lacks, and we need an internal admin and fleet view.
10. **Telemetry: OpenTelemetry Collector, SigNoz for application data, Azure Monitor for platform logs** (section 7). The tenant gap in our own metrics (section 6.4) matters more than which backend we pick.

## 2. What the system looks like

```text
customer workers/producers ──gRPC──► Azure edge (gateway, TLS, API-key check)
                                          │
                                  AKS: Runwick server pods (3 zones)
                                  = core + Postgres driver + gRPC adapter
                                    + tenancy + metering
                                          │
              ┌───────────────────────────┼───────────────────────────┐
              ▼                           ▼                           ▼
   control-plane Postgres        tenant job stores            event feed (cursor)
   (tenants, keys, billing)      shared / dedicated / BYO            │
                                                                     ▼
                                                           ClickHouse Cloud
                                                    (dashboard history, metering)
```

Nothing in the engine forks. A worker reaches the store only through the named operations of the storage command protocol, so `Runwick.Cloud` is a client of the same interfaces and the server is the existing core behind an RPC layer.

## 3. Component choices

| Piece | Choice | Why | Watch out for |
|---|---|---|---|
| Compute | AKS, 3 zones, Standard control plane | Long-lived push streams need HTTP/2 and gRPC end to end, and AKS gives full control of the ingress. | Check the edge's gRPC support and idle timeouts before choosing it. Not verified for Front Door or Container Apps. |
| Edge | Application Gateway for Containers or an Envoy ingress | Terminates TLS, enforces per-key rate limits. | Keep-alives on push streams. |
| Job store, lead | PlanetScale Postgres, 3-node HA | HA is priced lower than Azure's doubling; Metal local-NVMe may suit the claim-heavy workload. | Three Azure regions; Azure price, replication mode and Private Link not verified. |
| Job store, fallback | Azure Flexible Server, Premium SSD v2, zone-redundant HA | Available in every Azure region, Private Link, customer-managed keys, Azure Marketplace billing for enterprise buyers. | Zone-redundant HA roughly doubles compute. |
| Analytics | ClickHouse Cloud, Scale tier, `eastus2` / `germanywestcentral` | Fast history and rollups; scales compute to zero when idle. | Derived data only. Never read it for ownership or billing decisions. |
| Secrets, images, network | Key Vault, ACR, Private Link, private DNS | Standard. | — |
| Observability | OpenTelemetry Collector, SigNoz for application data, Azure Monitor for platform logs (section 7) | Reuses our telemetry contract; backend can change with a collector setting. | Log volume costs money; set retention early. |

### Connection handling (read in spec 04)

- Normal work uses transaction-local settings and `No Reset On Close`, so a transaction-mode pooler should be safe for claims. This is **inferred**, so test it.
- Migrations need one dedicated physical session holding a session advisory lock. Never route migrations through a pooler.
- The optional wake-up listener needs its own direct connection.

## 4. Costs

All figures are USD per month, East US, on demand, compute only. Reserved prices are about 40% lower for 1 year and 60% lower for 3 years (Azure Postgres).

### Production, lean launch (one region)

| Item | Per month | Source |
|---|---|---|
| AKS: 3 nodes of 4 vCPU plus control plane | ~$500 | estimate |
| Gateway, load balancer, egress | ~$150–250 | estimate |
| Postgres, PlanetScale 16 GiB, 3-node HA (lead) | ~$350 | AWS price; Azure price not verified |
| Or Postgres, Azure 4 vCPU / 16 GB, zone-redundant HA, plus storage (fallback) | ~$580 | $260 compute × 2 + ~$60 storage; from a vendor page |
| ClickHouse Cloud Scale on Azure | ~$450–500 | $0.33 per compute unit-hour, about 2 units always on; storage $25.30/TB |
| Telemetry (SigNoz Cloud for application data, Azure Monitor for platform logs), Key Vault, registry, backups | ~$250–500 | estimate; SigNoz Cloud lists $0.30 per GB of logs and traces |
| **Total** | **~$1.7k–2.3k** | matches the "low thousands" in the strategy document; the low end uses PlanetScale |

A staging environment adds roughly $800–1,000.

### Reference prices (from vendor pages)

- **PlanetScale Postgres, AWS us-east-1, 3-node HA:** 16 GiB $286 (arm) or $349 (x86); 256 GiB $4,529 or $5,599; Metal 200 GiB NVMe $630 or $650.
- **Azure Postgres, East US:** 2 vCPU / 8 GB $130; 4 / 16 $260; 8 / 32 $520; 16 / 64 $1,040. Memory-optimized 4 / 32 $365; 8 / 64 $730.
- **ClickHouse Cloud on Azure (`eastus2`), per compute unit-hour:** Basic $0.241, Scale $0.330, Enterprise $0.431. A unit is 2 vCPU and 8 GiB. Storage $25.30/TB. Internet egress $0.102/GB. No monthly minimum.
- **ClickHouse Postgres:** AWS only for billing (from 2026-11-09). Not offered on Azure that I could find.
- **SingleStore Helios:** the sources disagree ($3.20, $3.60 and $3.96 per credit). Not costed.

### What this means for pricing the product

- A dedicated tenant store costs you about $260–350 a month before storage, backups and egress. Bill it per store-hour at cost plus margin (section 4.2).
- Shared-pool tenants cost almost nothing each until the pool is busy. The risk is noisy neighbours, which the per-tenant limits in the strategy document address.

### 4.1 What the lean setup can handle

**Read this first.** Nobody has run Runwick on this setup. Every number below was measured on one shared 8-vCPU test machine (load 3 to 38). PostgreSQL ran in Docker on the same machine as the worker, the producer and the test runner, with no network delay. The cloud changes three things:
- The server talks to the store over the network, about 1 ms inside one region.
- In HA, a replica must confirm every commit. This usually lowers throughput.
- Workers talk to the server over gRPC, 30–80 ms across the internet. That is why batching (SP-031) exists.

Treat the table as a floor to plan from, not a promise. Phase 0 reruns these tests on the real setup.

| What | Measured | Source |
|---|---|---|
| Adding jobs one call at a time | 63/s from 1 caller, 306/s from 16 callers (quiet host, before the later add improvements). A prototype that sends the wake message after the commit reached 451/s at 16 callers; it is not merged. | `perf-investigation.md` |
| Adding jobs in bulk | About 3,900/s (20,000 jobs in 5.1 s, before set-based adding). The client caps a bulk call at 100 jobs. | `perf-investigation.md`, `perf-plan-2.md` |
| Running jobs (claim, start, finish), no-op jobs | 108–161/s with 16 workers, 283–346/s with 64 workers, on the busy host | `pf-final.md` |
| Steady rate without a growing backlog | 50 jobs/s with instant jobs (P-02). At 100/s arriving with 0.1 s jobs: 87.5/s finished, backlog grew 12/s, start-time p99 23 s (P-01). | `sp-029.md` |
| More worker servers on one store | 1 replica 85.8/s, 3 replicas 171.9/s (P-05). The store is the limit, not the workers. | `sp-029.md` |
| Dashboard load | 20 viewers polling every 5 s cost 13.5% of finish throughput near the limit; list p95 135 ms (P-07) | `sp-029.md` |
| Schedules | 1,000 per-minute schedules (about 17 jobs/s): the last runs up to 52 s late at p99 against the 13 s rule, so P-06 fails. The scheduler reads at most 32 schedules per pass and allows 100 jobs/s by default. | `sp-029.md`, spec 13 |
| Large payloads | 64 KiB jobs 15.7/s; 1 MiB jobs 0.97/s (P-03, one worker) | `sp-029.md` |
| Cost per job | About 13 KB written to the log; memory churn 120 KB to 300 KiB per job (goal 40 KiB) | `sp-029.md`, `pf-final.md` |
| Outbox relay (customer side) | 24–26 messages/s per relay, goal 100 | `sp-029.md` |
| Queues | Not measured | — |

**What that means for the lean setup (my arithmetic, not a test)**
- **Plan on about 50 finished jobs a second per store** until the real-setup run says otherwise. That is about 4.3 million runs a day, or about 130 million a month if traffic were perfectly flat. Real traffic is bursty. If peak is ten times average, one store comfortably serves about 13 million runs a month.
- **Log volume:** 50 jobs/s × 13 KB is about 650 KB/s, or about 56 GB a day. HA replicas and backups carry this, so check replica lag and backup cost.
- **One store means one ceiling.** All shared-tier tenants together share the store's ~50/s. A dedicated store gives that tenant its own ceiling. A bigger Postgres helps the CPU-bound part, but the cost is mostly writes per job (about 10 rows inserted, 7 updated and 55 index entries written). `perf-plan-2.md` aims to cut that; it is unmeasured.
- **Where it breaks first, in order:** (1) many callers adding one job at a time, (2) claim and finish round trips on the store, (3) about 1,000 schedules due in the same second, (4) dashboard polling near the limit, (5) the outbox relay.
- **Queues:** I found no limit in the spec and no test. A pool that serves several queues claims one job at a time instead of in batches (`pf-final.md`, "What is not done"), so many small queues per pool run slower than a few busy ones. The dashboard lists stop at 5,000, and telemetry bounds 1,000 series per instrument.
- **Schedules:** until P-06 passes, do not promise more than the 100 schedules the soak test assumes. This is a placeholder for the shared tier, to settle after Phase 0.

### 4.2 Pay as you go, spending limits, and how many customers fit

**Decision (2026-11-11): everyone pays as they go. There are no plans and no minimum fee. Every workspace has a spending limit.** This replaces the free tier and plan shape in the strategy document's pricing paragraph (§2). I fix no price number here. This section shows how to choose one from cost.

#### What is metered

| Meter | Billed as | Basis |
|---|---|---|
| Runs | Per million runs. A run is the first committed Begin of a generation. Retries inside a generation are free, and so are the first 5 manual retries of a job (strategy §2). | Shared infrastructure cost |
| Concurrency | Per slot-hour for the slot count you set on an environment. This is the existing `application` concurrency scope. | Value, not cost: it is an admission ceiling |
| Dedicated store | Per store-hour, only for customers who choose a dedicated store. | Cost plus margin. Compute is about $0.36–0.48 an hour ($260–350 a month, section 4), plus storage, backups and egress passed through. |
| Retention | Not priced yet. Bytes per job are not measured, so every customer gets the same short retention at launch. Longer retention is priced later. | Unknown |

#### How spending limits work

- A limit is a monthly amount per workspace, and optionally per environment. It resets at the start of the billing period (UTC).
- Alerts go out at 50%, 80% and 100% by email and webhook. The webhook uses the event feed.
- **At 100%:**
  - New dispatches, scheduled occurrences and new generations are refused with the typed `QuotaExceeded` code, which SP-031 already lists. It means "known not applied", so a producer can see it at once and the outbox can retry the same command ID later.
  - Jobs already accepted are not lost. Running jobs finish. Retries inside a generation stay free.
  - Queued jobs that have not begun stay held, because beginning them would count as a run. The dashboard shows "held by spending limit". Raising the limit releases them.
- **Raising the limit** is instant and self-serve up to a ceiling. Above the ceiling needs a check.
- **New accounts** need a card on file before any usage, and start with a low default limit until the first payment succeeds. This guards against runaway loops and unpaid usage.
- **If a payment fails:** dispatch is refused and data is kept for a set number of days. Export and the move to self-host (SP-035) stay allowed, so a customer is never locked in.
- **Precision:** the spend counter is built from receipts and refreshed at least every 5 s. The limit is not exact to the cent. The overshoot is bounded by peak rate × 5 s. At 100 jobs/s that is at most 500 runs, about $0.10 at $200 per million.
- Receipts stay the billing source of truth (strategy §2).

#### Choosing the price per million runs

The shared store costs about $2,000 a month (the middle of section 4) however many runs it carries. The cost of a million runs depends on how full the store is:

| Store fill (50 jobs/s plan, 13 million real runs a month at 100%) | Real runs a month | Cost per million runs |
|---|---|---|
| 25% | 3.25 million | ~$615 |
| 50% | 6.5 million | ~$308 |
| 100% | 13 million | ~$154 |

Break-even is the fixed cost divided by the price:

| Price per million runs | Real runs a month to break even at $2,000 | Fits in the planned 13 million? |
|---|---|---|
| $50 | 40 million | No, needs more than the 100 jobs/s goal |
| $100 | 20 million | No, needs about the 100 jobs/s result (26 million) |
| $200 | 10 million | Yes, at 77% fill |
| $400 | 5 million | Yes, at 38% fill |

- **Anything below about $150 per million only works if the real-setup test beats the 50 jobs/s plan.**
- I did not check competitors' prices, because you stopped that search. Check them before fixing a price. The cost floor here may sit above the market.
- Breaking even needs margin on top. Price = (monthly cost ÷ real runs at your target fill) × (1 + margin), and the margin is your decision.

#### How many customers fit

Customers pay for what they run, so the count depends on their size. On the shared store at the planned 13 million real runs a month:

| Average customer | Runs a month | Customers that fit |
|---|---|---|
| Tiny | 10,000 | 1,300 |
| Small | 100,000 | 130 |
| Medium | 1,000,000 | 13 |
| Large | 10,000,000 | 1 |

A mix that fits: 400 tiny, 60 small and 3 medium (4 + 6 + 3 = 13 million runs). It would bill about $2,600 a month at $200 per million, or $1,300 at $100.

A customer's bill is runs × price. At $200 per million, 100,000 runs cost $20. Concurrency slot-hours and any dedicated store are billed on top.

I cannot say how many customers you will win. This only shows how many the lean setup can carry.

#### Decisions for you

1. The price per million runs, and the margin.
2. Whether any usage is free each month (a small free allowance is still pay as you go). I'd keep a small one to ease sign-up, with a card on file.
3. The default spending limit for a new account.
4. Whether queued jobs are held at the limit. I recommend yes, so a backlog cannot overspend.

**Dedicated-store customers:** their store fee is billed per hour at cost plus your margin. At 50% margin that is about $0.53–0.72 an hour, which is $390–525 a month. They also get their own throughput ceiling of roughly 50 jobs/s, and their runs are billed at the same per-million price.

## 5. Enterprise store choice

| Level | Who runs the store | Where the server runs | Notes |
|---|---|---|---|
| Shared | Us, pooled Postgres | Our Azure region | Schema per tenant, using the `application` key. |
| Dedicated | Us, one Postgres per tenant | Our Azure region, region of the customer's choice | Needs the price floor above. |
| Bring your own store | The customer, their Postgres 16–18 | Next to their store, in their network | Our control plane stays in Azure; the data plane runs in their cloud. |

Rules:

- **A store is supported only if it passes the driver contract suite and the G-2 benchmark.** Run both against each candidate host and record the report.
- **Put the server next to the store.** Reserve holds row locks. A 20–60 ms round trip between server and store makes contention and lease renewals worse.
- **ClickHouse and SingleStore are not store options.** They can be analytics sinks only.
- Redis and SQLite already have drivers. Offering Redis as a cloud store is a separate decision, because the strategy document assumes hosted Postgres.

## 6. Dashboards

There are three different dashboards. Keep them apart.

### 6.1 What exists (read in spec 21 and spec 16)

- One dashboard mount shows **one application in one store**. Every page reads through `IManagementSession` in the same process.
- Sign-in and permissions come from the host. The role presets are Viewer, Operator and Administrator.
- Spec 21 §10 says it must **not claim** "isolation finer than application plus queue grants". It has no tenant isolation.
- Pages poll every 5–10 s, and every count comes from an exact store read. Fleet pages stop at 5,000 workers (503 `too_many`).
- Rate limits are per process, so they do not hold across replicas.
- SQLite and Redis do not support the dashboard reads yet.
- The strategy document kept two seams open: management behind one session with an actor, and `store_id` + `application` as the fleet key. The dashboard must never assume one store per process. I read the spec, not the mount code, so check that the code honours this.

### 6.2 Customer dashboard (hosted, multi-tenant)

It lives in the separate proprietary repository and consumes the OSS packages and the versioned management API (SP-032).

- **One hosted app, not one mount per tenant.** Each request resolves tenant, then environment, then (store, application), and builds a session for that. The dashboard needs a "session factory" seam for this. Today a mount is fixed at registration. That is a spec 21 change.
- **Sign-in:** an OIDC provider for customers (Microsoft Entra External ID is one candidate, not verified). Map customer roles to the existing Viewer, Operator and Administrator grants, per environment. API keys stay for producers, workers and the CLI.
- **Tenant isolation is new work.** The binding lives below the dashboard (tenant and store binding in every command, per-environment key scopes). The dashboard never holds a handle to another tenant's store. Add a cross-tenant isolation fuzz on every dashboard route.
- **Rate limits across replicas** move to the gateway, because the in-process limiter does not hold with several pods.
- **Load on tenant stores.** Every open tab runs exact count queries every 5–10 s. Serve Overview charts and stats from the ClickHouse rollups and keep exact reads for detail pages and controls. Poll more slowly, and give each tenant a query budget. This breaks the current rule "every count is an exact store read", so the hosted Overview must say "as of <time>". It is a product decision, not just an optimisation.
- **Payload reveal** stays audited. Add a tenant setting to switch it off.
- **Pages customers will expect that the free dashboard has no need for:** usage and plan (runs, concurrency, retention), API keys, members and roles, environments, audit export, and store settings with migration status.

### 6.3 Internal dashboards (for us)

| View | What it is | Built with |
|---|---|---|
| Service health | Pods, store hosts, lease margin, collector age, error rates | SigNoz dashboards, plus the shipped Grafana and Prometheus rules (SP-022) if they import cleanly |
| Tenant admin and support | Create or suspend a tenant, plan caps, revoke keys, store-host registry, migration runs, noisy-neighbour view (top tenants by calls, connections, running attempts) | A small internal admin app on the same stack as the dashboard (ASP.NET, Inertia, shadcn), behind Entra ID with MFA and just-in-time role elevation. It reads the control-plane Postgres and ClickHouse. |
| Metering check | Runs counted from receipts against the ClickHouse rollups, per tenant per period | A page in the admin app and a monthly CSV |

Rules for the admin app:
- It never reads payload bytes. Metering reads receipts only.
- "View as customer" is read-only, time-boxed and audited, with payload reveal off.
- It runs on the same AKS, so it adds almost no cost.

### 6.4 A gap in our own telemetry

Spec 18 §3.2–3.3 forbids tenant IDs as metric attributes and caps each instrument at 1,000 series per process. A hosted server process serves many tenants, and queue and job names that are not registered in that process fold into `_other`. (This is inferred from the spec, not tested.)

- Per-tenant numbers cannot come from the `Runwick` meter. They come from the events cursor into ClickHouse, and from gateway metrics that use a bounded attribute such as plan tier.
- The `Runwick` meter still gives per-process engine health.

## 7. Observability: SigNoz or Azure Monitor

**Keep the code backend-neutral.** Runwick emits OpenTelemetry, and everything goes through an OpenTelemetry Collector (OTLP in, fan out). Changing the backend later is then a collector setting.

| | Azure Monitor | SigNoz |
|---|---|---|
| OTLP | Native OTLP ingestion through a collector, generally available June 2026 per the Azure Updates feed (some Learn pages still say preview) | Native |
| Where it runs | Azure, nothing to operate | Cloud: US, EU and India reported, no Azure region found. Self-host: Helm on AKS (ClickHouse and ZooKeeper), and it can use an external ClickHouse |
| Price | Per GB in Log Analytics; I did not fetch it | Cloud: $0.30 per GB of logs and traces, $0.10 per million metric samples (their pricing page, via search) |
| Licence | Proprietary service | MIT outside `ee/` (unconfirmed); SAML single sign-on needs an enterprise licence |
| Fit | AKS, Postgres, gateway and Key Vault send diagnostics here natively | ClickHouse-based, like our analytics copy |

**My recommendation**

1. **Azure platform logs stay in Azure Monitor:** Activity Log, AKS and database diagnostics, gateway and Key Vault audit logs. They arrive automatically and compliance asks for them.
2. **Application telemetry (traces, metrics, logs from the Runwick server) goes to SigNoz.** There is a span for every job, so volume is high and the per-GB price gap matters. Example: 500 GB a month is about $150 on SigNoz Cloud, plus metrics.
3. **Beta: SigNoz Cloud**, which has no servers to run. If an enterprise customer needs telemetry kept in Azure or in their region, **self-host SigNoz on AKS with an external ClickHouse**. Use its own node pool, or its own cluster, so monitoring does not fail together with the thing it watches.
4. **Dead-man's switch:** an Azure Monitor alert that fires when the SigNoz heartbeat stops. It monitors the monitor.
5. **Redaction:** spec 18 §6 keeps payloads out. Still check queue and job names, because those are customer data.
6. **Customers' own telemetry** goes to their own backend through their own exporters. We host nothing for it. The hosted dashboard reads jobs from the store and ClickHouse, so it does not depend on SigNoz.

If you prefer to stay on Azure, that works for the private beta: nothing to operate, and OTLP is generally available. The cost is the per-GB price, and I did not verify it.

Self-hosting costs more than the licence: the SigNoz install guide suggests at least 4 cores and 8 GB for a small install and 8 cores and 16 GB recommended. A production ClickHouse needs more, plus on-call. As a rough guess that is $300–700 a month of extra nodes.

## 8. Moving between cloud and self-host: SP-035 (proposed)

**What exists today (read in the repo):** `runwick migrate` and `doctor` handle schema, not data. The prepared-dispatch envelope moves one job or batch (up to 2 MiB). The test plan defers cross-driver identity (SER-06). No tool moves a whole store.

**Why Postgres to Postgres should be tractable:** the hosted tenant store is the same `runwick` schema that a self-hoster runs. Moving it is mostly moving the rows for one `application`.

**Proposed item**

| ID | Deliverable | Depends on |
|---|---|---|
| SP-035 | `runwick store export` and `runwick store import` for Postgres to Postgres, both directions, with a quiesce step, a verification step and a rollback step. A logical export format for moves between Postgres and SQLite or Redis comes in a second stage. | SP-032, SP-033 |

**Stage 1 procedure (Postgres to Postgres)**

1. Pause the tenant's queues and schedules.
2. Wait for running work to finish or for leases to expire. Never fabricate outcomes.
3. Take a consistent snapshot of that `application`'s rows. For a near-zero-downtime move, use logical replication and cut over at the end.
4. Restore into the target, then run `doctor`. Check the migration ledger checksums and row counts per record class.
5. Rebind the store identity (see the open question below), switch the connection string or the Cloud driver, and resume.
6. Keep the source read-only for a retention window so the move can be reversed.

**Done means:**
- A tenant moves cloud to self-host and back with no job lost and no job run twice.
- Receipts and fence counters carry over, so a worker resolving an old command still gets its original answer.
- Both directions are covered by the cross-tenant isolation fuzz.

**Open question for a spike:** gate rows bind a command to a store identity, and prepared envelopes already reject a different `storeId`. A moved store probably needs a new store ID or an explicit rebind. I have not read how receipts and in-flight commands behave when that happens. This is the first thing to settle.

**History in ClickHouse:** the analytics copy is derived. On a move, history either stays in the source or is rebuilt from the events cursor. The move never depends on ClickHouse.

## 9. Phases

| Phase | What | Gate |
|---|---|---|
| 0. Facts (about a week) | Verify the items in section 11. Run the contract suite and G-2 on PlanetScale in `eastus2` first, then on Azure Flexible Server. Include P-01, P-02, P-05, P-06 and P-07 with HA on, plus a new many-queues test. Run P-02 on every size to build a size ladder (jobs/s, log GB a day, bytes per job). | A written report per host, with numbers. |
| 1. Protocol | SP-031: `ReserveAndBegin/1`, batched Reserve and Renew, `application`-kind concurrency scope, `QuotaExceeded`. | Contract suite passes with the new operations. |
| 2. Wire and driver | SP-033 and SP-034. | Driver report with topology "hosted". |
| 2b. Dashboard seam | Spec 21 change: a session factory so one hosted app serves many tenants; decision on ClickHouse-backed Overview figures (section 6.2). | Spec merged; isolation fuzz on dashboard routes passes. |
| 3. Private beta | One region, shared tier, AKS plus PlanetScale (Flexible Server as fallback) plus ClickHouse. Collector plus SigNoz. Hosted dashboard and a minimal internal admin app. | Soak test, status page, on-call rota. |
| 4. Dedicated and bring-your-own | Dedicated stores, then bring-your-own with the server in the customer's network. | Isolation fuzz across tenants. |
| 5. Migration | SP-035 stage 1, then stage 2. | Round-trip move test (section 8). |
| 6. Billing | Metering from receipts, pay-as-you-go invoices, spending limits with `QuotaExceeded` and held-at-limit jobs (section 4.2). | Per-tenant reconciliation matches receipts; the limit overshoot stays within the stated bound. |

The strategy document's estimate stands: roughly 9–15 months after v1 for a credible hosted service.

## 10. Risks

- **The service is the customer's critical path.** Use zone-redundant HA, a multi-zone API tier, a status page and an SLA. Workers already degrade honestly on disconnect.
- **Durability depends on the store host.** The strategy document assumes synchronous replication. Confirm it for each candidate host before promising anything.
- **Cross-region latency** between server and store. Never split them across clouds in the shared or dedicated tiers.
- **Cost surprises:** egress ($0.102/GB from ClickHouse Cloud, and Azure egress too), log volume and zone-redundant doubling.
- **Vendor lock-in** is limited because any Postgres 16–18 that passes the suite works. The same holds for telemetry while everything goes through a collector.
- **Tenant leakage through the dashboard.** The free dashboard has no tenant isolation by design. The hosted one needs the fuzz in phase 2b before any second tenant is let in.
- **Per-tenant telemetry gap** (section 6.4): without the ClickHouse rollups we cannot see a noisy tenant.
- **Monitoring that fails with the service.** Keep SigNoz off the cluster it watches, and keep the dead-man's switch.

## 11. Not verified (check before committing)

1. PlanetScale Postgres prices on Azure. Their pricing page showed only AWS prices.
2. Whether PlanetScale's 3-node HA replicates synchronously.
3. Whether PlanetScale supports Azure Private Link, and whether Metal NVMe runs on Azure.
4. Whether ClickHouse Postgres will come to Azure, and when.
5. SingleStore Helios prices on Azure.
6. gRPC support and idle-stream timeouts of the edge options (Application Gateway for Containers, Front Door, Container Apps).
7. Whether a transaction-mode pooler is safe for Reserve, Begin and Renew. Inferred from spec 04, never run.
8. How store identity and receipts behave when a store moves (section 8).
9. Whether the dashboard mount code really avoids assuming one store per process (I read the spec, not the code).
10. Whether SigNoz Cloud has an Azure region, and the exact SigNoz licence terms (MIT outside `ee/`, SAML only in the enterprise licence).
11. Azure Monitor's per-GB price, and whether OTLP ingestion is generally available (the Azure Updates feed and some Learn pages disagree).
12. Whether the shipped Grafana dashboard and Prometheus alert rules (SP-022) import into SigNoz without rework.
13. Whether the current dashboard can serve Overview figures from ClickHouse without breaking its "exact counts" rule (a spec decision).
14. Whether Microsoft Entra External ID fits as the customer sign-in.
15. Throughput on the real setup (section 4.1). All current numbers come from a busy shared test machine with local Postgres, and the cost of HA commits and the gRPC hop is unmeasured. How many queues one application can hold is also untested.
16. Which payment and metering provider fits pay-as-you-go with spending limits (card billing, and Azure Marketplace metering for enterprise buyers). Not researched.

## Sources

- [PlanetScale regions](https://planetscale.com/docs/plans/regions)
- [PlanetScale pricing](https://planetscale.com/pricing)
- [ClickHouse Managed Postgres pricing](https://clickhouse.com/docs/products/managed-postgres/pricing)
- [ClickHouse Cloud pricing](https://clickhouse.com/pricing)
- [Azure PostgreSQL price indications (Bytebase)](https://www.bytebase.com/dbcost/azure-flexible-server-pricing/)
- [Premium SSD v2 for Azure Database for PostgreSQL](https://learn.microsoft.com/azure/postgresql/compute-storage/concepts-storage-premium-ssd-v2)
- [SingleStore pricing](https://www.singlestore.com/cloud-pricing/)
- [SigNoz pricing](https://signoz.io/pricing)
- [SigNoz Kubernetes install](https://signoz.io/docs/install/kubernetes/others)
- [SigNoz external ClickHouse](https://signoz.io/docs/operate/clickhouse/external-clickhouse/)
- [Azure Monitor OTLP ingestion (Learn)](https://learn.microsoft.com/en-us/azure/azure-monitor/containers/opentelemetry-protocol-ingestion)
- [Azure Updates: OTLP ingestion generally available](https://mc.merill.net/message/AZ565090)
