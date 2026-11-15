# Runwick Cloud: capacity, cost, pricing and revenue in one place

*Draft for review. Written 2026-11-11 against main at `11fa7f7`. Not approved, and nothing here is built. The long version, with the reasoning, is [docs/plans/runwick-cloud-infra-plan.md](../plans/runwick-cloud-infra-plan.md). This report puts the numbers together.*

## How much to trust each number

- **Measured** means a benchmark in this repository produced it. Every measured speed comes from one shared 8-CPU test machine (load 3 to 38), with PostgreSQL in Docker on the same machine and no network delay. **Nobody has run Runwick on Azure, PlanetScale or AKS.** The cloud adds a network hop, a replica that must confirm every commit, and a gRPC hop between worker and server. Expect those to lower the numbers.
- **Vendor price** means I read it on a vendor page on 2026-11-11. Prices are list prices, on demand, and change.
- **My arithmetic** means I computed it from the other numbers.
- **Assumption** means a guess you should change.

## 1. Short answers

1. **Is the jobs-per-second figure per customer? No.** It is per store. Every customer on the shared store shares it. A customer on a dedicated store gets a whole store of their own (section 3.2).
2. **Runs per month the lean setup can handle:** about **13 million real runs a month** on the shared store, as a planning number. The range is 6.5 million (if the real setup is half as fast) to 26 million (if it reaches the 100 jobs/s goal).
3. **Monthly cost of the lean production setup:** about **$1.7k–2.3k**, before staging, payment fees, support and on-call.
4. **Price:** pay as you go for everyone, with spending limits. The price per million runs is **your decision**. This report shows what each price earns.
5. **Break-even** needs about **10 million real runs a month at $200 per million**, or 20 million at $100. Below about $150 per million, the plan only works if the real-setup tests beat 50 jobs/s.
6. **Customers:** I cannot predict how many you will win. The shared store can carry about 463 customers of mixed size, or 1,300 tiny ones, or 13 medium ones (section 7).

## 2. What the lean production setup is

| Piece | Choice |
|---|---|
| Region | `eastus2` first (the overlap between Azure and PlanetScale's Azure regions), `westeurope` for the EU |
| Compute | AKS: 3 nodes of 4 vCPU across 3 zones, running the Runwick server (core + Postgres driver + gRPC adapter + tenancy + metering) |
| Job store, lead | PlanetScale Postgres, 16 GiB, 3-node HA |
| Job store, fallback | Azure Database for PostgreSQL Flexible Server, 4 vCPU / 16 GB, zone-redundant HA |
| Control-plane database | Tenants, keys and billing (a small database of its own) |
| Analytics copy | ClickHouse Cloud, Scale tier, on Azure. Dashboard history and metering rollups. It can be rebuilt and is never the billing source. |
| Telemetry | OpenTelemetry Collector; SigNoz for application data; Azure Monitor for platform logs |
| Edge | Application Gateway for Containers or an Envoy ingress, with TLS, API-key checks and per-key rate limits |

ClickHouse and SingleStore cannot be the job store. The store needs real PostgreSQL 16 to 18 (`SKIP LOCKED` row claims, advisory locks, deferred constraint triggers). ClickHouse's own managed Postgres is not offered on Azure that I could find.

## 3. How much it can handle

### 3.1 The measured inputs

| What | Measured | Source |
|---|---|---|
| Adding jobs, one call at a time | 63/s from 1 caller, 306/s from 16 callers | `perf-investigation.md` |
| Adding jobs in bulk | about 3,900/s | same |
| Running jobs (claim, start, finish), no-op jobs | 108–161/s with 16 workers; 283–346/s with 64 workers | `pf-final.md` |
| Steady rate without a growing backlog | **50 jobs/s** with instant jobs (P-02) | `sp-029.md` |
| 100/s arriving, 0.1 s jobs | 87.5/s finished, backlog grew 12/s, start-time p99 23 s | `sp-029.md` |
| Three worker servers on one store | 171.9/s against 85.8/s for one: the store is the limit | `sp-029.md` |
| 20 dashboard viewers polling every 5 s | cost 13.5% of finish throughput near the limit | `sp-029.md` |
| 1,000 per-minute schedules | last one up to 52 s late at p99 against the 13 s rule, so the test fails | `sp-029.md` |
| Large payloads | 64 KiB jobs 15.7/s; 1 MiB jobs 0.97/s | `sp-029.md` |
| Cost per job | about 13 KB written to the log; 120 KB to 300 KiB of memory churn (goal 40 KiB) | `sp-029.md`, `pf-final.md` |
| Queues | not measured | |

### 3.2 Per store, not per customer

- **One store has one ceiling.** All shared-tier customers together share about 50 jobs/s.
- **A customer on a dedicated store has their own ceiling** of about the same size.
- **Nothing yet stops one customer from using the whole shared 50 jobs/s.** The protections are planned and not built: a concurrency-slot limit per workspace and per-tenant call limits.
- **Slots set a customer's own ceiling.** A job holds one slot while it runs, so a customer's rate is at most slots ÷ job seconds. For example, 10 slots with 1-second jobs is at most 10 jobs/s.
- **Slots are also a billed meter** (section 5), so heavy customers pay for the capacity they take.

### 3.3 Runs per month

| Store ceiling | Real runs a month | When this happens |
|---|---|---|
| 25 jobs/s | about 6.5 million | The real setup is half as fast as the plan |
| **50 jobs/s (the plan)** | **about 13 million** | The measured sustained rate |
| 100 jobs/s (the P-01 goal) | about 26 million | The goal; not reached yet |

- The arithmetic: 50 jobs/s × 2.59 million seconds is 130 million runs if traffic were perfectly flat. I plan at 10× peak over average, so 130 ÷ 10 is 13 million.
- Peaks from different customers rarely line up, so the real figure may be higher. That is inferred, not tested.
- Log volume at 50 jobs/s is about 650 KB/s, or about 56 GB a day. HA replicas and backups carry it.

### 3.4 Queues and schedules

- **Queues:** no limit in the spec and no test. A pool serving several queues claims one job at a time instead of in batches, so many small queues run slower than a few busy ones. The dashboard lists stop at 5,000, and telemetry bounds 1,000 series per instrument.
- **Schedules:** don't promise more than 100 per customer until the scheduler test passes (a placeholder to settle after Phase 0).

### 3.5 Where it breaks first

1. Many callers adding one job at a time.
2. Claim and finish round trips on the store.
3. About 1,000 schedules due in the same second.
4. Dashboard polling near the limit.
5. The outbox relay (24–26 messages/s per relay, customer side).

## 4. What it costs a month

All prices are USD, East US, on demand, compute only.

| Item | Per month | Source |
|---|---|---|
| AKS: 3 nodes of 4 vCPU plus control plane | ~$500 | my estimate |
| Gateway, load balancer, egress | ~$150–250 | my estimate |
| Postgres, PlanetScale 16 GiB, 3-node HA (lead) | ~$350 | vendor price (AWS; Azure price not verified) |
| Or Postgres, Azure 4 vCPU / 16 GB, zone-redundant HA, plus storage (fallback) | ~$580 | vendor price ($260 × 2 + ~$60) |
| ClickHouse Cloud Scale on Azure | ~$450–500 | vendor price ($0.33 per compute unit-hour, about 2 units always on; storage $25.30/TB) |
| Telemetry (SigNoz Cloud, Azure Monitor), Key Vault, registry, backups | ~$250–500 | my estimate (SigNoz Cloud lists $0.30 per GB of logs and traces) |
| **Total** | **~$1.7k–2.3k** | the low end uses PlanetScale |

- **Planning figure: $2,000 a month.**
- **Staging** adds about $800–1,000.
- **Not in the total:** payment fees, support, on-call, compliance, and your time.
- **Savings lever:** Azure reserved prices are about 40% lower for 1 year and 60% lower for 3 years on Postgres compute.
- **Dedicated store:** about $260–350 a month each for compute, plus storage, backups and egress.

## 5. How customers are charged (pay as you go)

Decision recorded 2026-11-11: everyone pays as they go. There are no plans and no minimum fee. Every workspace has a spending limit.

| Meter | Billed as |
|---|---|
| Runs | Per million. A run is the first committed Begin of a generation. Retries inside a generation are free, and so are the first 5 manual retries of a job. |
| Concurrency | Per slot-hour for the slot count set on an environment |
| Dedicated store | Per store-hour at cost plus margin. At 50% margin: about $0.53–0.72 an hour, which is $390–525 a month. |
| Retention | Not priced yet: bytes per job are not measured. Everyone gets the same short retention at launch. |

**Spending limits**
- A monthly limit per workspace, and optionally per environment. Alerts at 50%, 80% and 100%.
- At 100%, new dispatches and scheduled runs are refused with the typed `QuotaExceeded` code. Running jobs finish, retries inside a generation stay free, and nothing is lost.
- Queued jobs that have not begun are held, because beginning them would count as a run. Raising the limit releases them.
- New accounts need a card on file and start with a low limit until the first payment succeeds.
- If a payment fails, dispatch stops but export and the move to self-host stay allowed.
- The limit is not exact to the cent. The overshoot is bounded by peak rate × 5 s: at most 500 runs at 100 jobs/s, about $0.10 at $200 per million.
- Receipts are the billing source of truth.

## 6. What customers pay and what you earn

### 6.1 One customer's bill

A bill is runs × price per million, plus slot-hours and any dedicated store.

| Customer's runs a month | At $100 per million | At $200 | At $400 |
|---|---|---|---|
| 10,000 (tiny) | $1 | $2 | $4 |
| 100,000 (small) | $10 | $20 | $40 |
| 1,000,000 (medium) | $100 | $200 | $400 |
| 10,000,000 (large) | $1,000 | $2,000 | $4,000 |

### 6.2 Profit on the shared store, by price and how full it is

Fixed cost $2,000 a month. For 26 million runs I assume a bigger database and use $2,500. Payment fees and support are not included.

| Real runs a month | At $100 per million | At $200 | At $400 |
|---|---|---|---|
| 3.25 million (25% of plan) | −$1,675 | −$1,350 | −$700 |
| 6.5 million (50%) | −$1,350 | −$700 | +$600 |
| 13 million (100%) | −$700 | +$600 | +$3,200 |
| 26 million (100/s goal) | +$100 | +$2,700 | +$7,900 |

### 6.3 Break-even

| Price per million runs | Real runs a month to cover $2,000 | Fits in the planned 13 million? |
|---|---|---|
| $50 | 40 million | No |
| $100 | 20 million | No, needs about the 100 jobs/s result |
| $200 | 10 million | Yes, at 77% fill |
| $400 | 5 million | Yes, at 38% fill |

- Anything below about $150 per million only works if the real-setup test beats the 50 jobs/s plan.
- **I did not check competitors' prices.** Check them before fixing a price. The cost floor may sit above the market.
- A formula to pick the price: (monthly cost ÷ real runs at your target fill) × (1 + margin). The margin is your decision.

### 6.4 Enterprise customer on a dedicated store

Example: 10 million runs a month. That fits, because a dedicated store carries about 13 million.

| | At $100 per million | At $200 | At $400 |
|---|---|---|---|
| Runs | $1,000 | $2,000 | $4,000 |
| Store fee (at 50% margin) | ~$450 | ~$450 | ~$450 |
| Your store cost | −$260 to −$350 | −$260 to −$350 | −$260 to −$350 |
| Left per customer, before shared servers and support | ~$1,100–1,200 | ~$2,100–2,200 | ~$4,100–4,200 |

Their runs do not use the shared store's capacity.

## 7. How many customers fit

Customers pay for what they run, so the count depends on their size. On the shared store at 13 million real runs a month:

| Average customer | Runs a month | Customers that fit |
|---|---|---|
| Tiny | 10,000 | 1,300 |
| Small | 100,000 | 130 |
| Medium | 1,000,000 | 13 |
| Large | 10,000,000 | 1 |

**A mix that fits:** 400 tiny, 60 small and 3 medium customers. That is 463 customers and 13 million runs. It bills about **$1,300 a month at $100 per million, $2,600 at $200, and $5,200 at $400**.

Free usage is optional: a small free monthly allowance would use capacity and bring nothing in. I would keep one small allowance, with a card on file.

## 8. Choosing the store

Three levels, and a host counts as supported only if it passes the driver contract suite and the G-2 benchmark.

| Level | Who runs the store | Where the server runs |
|---|---|---|
| Shared | Us, pooled Postgres | Our Azure region |
| Dedicated | Us, one Postgres per customer | Our Azure region, in the region the customer picks |
| Bring your own | The customer's Postgres 16 to 18 | Next to their store, in their network |

- Put the server next to the store. Reserve holds row locks, so a 20–60 ms round trip between them makes contention worse.
- ClickHouse and SingleStore are not store options. They can only be analytics sinks.

### Moving to and from self-host (SP-035, proposed)

- Nothing like this exists today. The CLI `migrate` and `doctor` commands handle schema, not data.
- The hosted store uses the same `runwick` schema as self-host, so Postgres to Postgres is feasible. Pause queues, let leases expire, take a snapshot or use logical replication, restore, run `doctor`, switch the connection string, and keep the source read-only for a retention window.
- Open question: gate rows bind commands to a store identity, so a moved store likely needs a new ID or an explicit rebind. That needs a spike first.
- Moves between Postgres and SQLite or Redis need a logical export format. That is a second, larger stage.

## 9. Dashboards and telemetry

**Dashboards are three separate things**
- **Free self-host dashboard:** unchanged. One mount, one application, one store, no tenant isolation by design.
- **Hosted customer dashboard:** one app for many customers. It needs a session factory so each request resolves customer, environment and store, which is a change to spec 21. It also needs a cross-customer isolation fuzz, a shared rate limit at the gateway, and its Overview figures served from ClickHouse, which breaks the current "exact counts" rule. Pages customers will expect: usage and spend, spending limits, API keys, members, environments, audit export and store settings.
- **Internal dashboards:** service health in SigNoz, a small admin app for tenants (create, suspend, caps, keys, migrations, noisy-neighbour view, read-only audited "view as customer"), and a metering check against receipts.

**Telemetry**
- Our own metrics forbid tenant IDs as attributes and cap each instrument at 1,000 series per process, so per-customer numbers must come from ClickHouse rollups, not the `Runwick` meter.
- Everything goes through an OpenTelemetry Collector. Azure platform logs stay in Azure Monitor. Application telemetry goes to SigNoz. Use SigNoz Cloud for the beta, then self-host on AKS with an external ClickHouse if a customer needs the data in Azure. I found no SigNoz Cloud region on Azure.
- Add an Azure Monitor alert that fires if SigNoz stops sending data.

## 10. Build order

| Phase | What | Gate |
|---|---|---|
| 0. Facts (about a week) | Verify the unverified items. Run the contract suite and G-2, plus P-01, P-02, P-05, P-06 and P-07 with HA on and a new many-queues test, on PlanetScale first, then Azure. Run P-02 on every size to build a size ladder (jobs/s, log GB a day, bytes per job). | A written report per host with numbers |
| 1. Protocol | SP-031 | Contract suite passes |
| 2. Wire and driver | SP-033, SP-034 | Driver report with topology "hosted" |
| 2b. Dashboard seam | Spec 21 session factory and the ClickHouse Overview decision | Isolation fuzz passes |
| 3. Private beta | One region, shared store, collector and SigNoz, hosted dashboard, minimal admin app | Soak test, status page, on-call rota |
| 4. Dedicated and bring-your-own | Dedicated stores, then bring-your-own | Isolation fuzz across customers |
| 5. Migration | SP-035 stage 1, then stage 2 | Round-trip move test |
| 6. Billing | Metering from receipts, pay-as-you-go invoices, spending limits | Reconciliation matches receipts; overshoot within the bound |

The strategy document estimates 9 to 15 months after v1 for a credible hosted service.

## 11. Risks

- **The speed numbers may not hold on the real setup** (HA commits, the gRPC hop, a different CPU). Every figure in sections 3 and 6 moves with it.
- **The service is the customer's critical path.** Needs zone-redundant HA, a status page and an SLA.
- **Durability depends on the store host.** Confirm synchronous replication on each candidate before promising anything.
- **One customer can use the whole shared 50 jobs/s** until slot and call limits are built.
- **Tenant leakage through the dashboard.** The free dashboard has no tenant isolation. Do the fuzz before the second customer.
- **Cost surprises:** egress, log volume, and Azure's HA doubling.
- **Monitoring that fails with the service.** Keep SigNoz off the cluster it watches.

## 12. Not verified

1. PlanetScale prices on Azure, whether its HA replicates synchronously, and Private Link support.
2. Whether ClickHouse's managed Postgres will come to Azure.
3. gRPC support and idle-stream timeouts of the edge options.
4. Whether a transaction-mode pooler is safe for Reserve, Begin and Renew.
5. How store identity and receipts behave when a store moves.
6. Whether the dashboard mount code avoids assuming one store per process.
7. SigNoz Cloud on Azure, the exact SigNoz licence terms, and Azure Monitor's price and OTLP status.
8. Whether the shipped Grafana and Prometheus rules import into SigNoz.
9. Competitors' prices (not looked up).
10. Which payment and metering provider fits pay as you go with spending limits.
11. Throughput on the real setup and how many queues one application can hold.

## 13. Decisions for you

1. The price per million runs, and your margin.
2. Whether any usage is free each month.
3. The default spending limit for a new account.
4. Whether queued jobs are held at the limit (I recommend yes).
5. Whether PlanetScale or Azure Flexible Server wins, after Phase 0.
6. Whether to approve SP-035 and the dashboard seam change as backlog items.

## 14. Two questions that come up

### 14.1 Why not Redis for the shared store?

**Short answer:** Redis can be faster, but Runwick's Redis store is not faster today, and as specified it cannot run as a highly available cloud service.

- **Measured, same busy test machine:** adding from 16 callers 332/s on Redis against 306/s on PostgreSQL; draining with 64 workers 241/s on Redis against 381/s on PostgreSQL. BullMQ reached 925/s and 1,754/s on the same tests at the same safety level (`perf-plan-2.md`, from the queue shoot-out on an unmerged branch). So Redis the engine can be several times faster. Runwick's Redis store has not been tuned, and its performance qualification (SP-029) has not run on Redis. Spec 22 says Redis results say nothing about parity with PostgreSQL.
- **One node, no HA.** Spec 22 allows one standalone primary with `appendfsync always`. It refuses Cluster, Sentinel, replica promotion, and managed services that hide `CONFIG`. A failover profile is explicitly "not in SP-025". A node failure means downtime until it restarts from disk, and a lost disk loses the data, including the receipts you bill from. PostgreSQL with synchronous replicas has no equivalent gap.
- **Everything lives in RAM** (`maxmemory-policy noeviction`), so memory sets how much history you can keep. Bytes per job are not measured. As an example only, at 5 KB a job, 13 million runs a month with 7-day retention is about 3 million jobs, or about 15 GB of RAM. Redis prices on Azure are not researched.
- **One application is one hash slot and one script at a time.** Every customer in the same Redis shares one execution thread.
- **Not finished:** one conformance case is owed on Redis (G-4), and REDIS-01 and REDIS-07 are still being completed.

**Recommendation:** keep PostgreSQL for the shared and dedicated tiers. Offer Redis only as a bring-your-own store on the single-primary profile, if a customer asks. Redis speed in the cloud would be a project: tune the store and qualify it, then add a replica profile (replica plus `WAITAOF`). That changes the guarantees, so it needs a spec first.

### 14.2 Can a $5 PlanetScale database serve 100 jobs/s for a year?

**No**, for these reasons:

1. **Volume.** 100 jobs/s for a year is about 3.15 billion runs, or 259 million a month.
2. **Runwick does not hold 100 jobs/s today on any size.** The best sustained rate measured is 50 jobs/s (P-02). At 100/s arriving, the 8-CPU test machine finished 87.5/s and the backlog kept growing (P-01). `perf-plan-2.md` aims at this and has not reached it.
3. **$5 is the smallest size.** I take it to be PS-5. I did not see its row, but PS-10 has 1 GiB, so PS-5 has no more. A single node has no HA, so one failure is an outage.
4. **Writes.** About 13 KB of log per job at 100/s is 1.3 MB/s, about 112 GB a day, about 41 TB a year. Disk kept depends on retention, and bytes per job are not measured. With 7-day retention, about 60 million jobs are on disk at any time.
5. **Nobody has tested a year.** The longest run is a 10-minute smoke. The 24-hour soak is designed and not run.
6. **The money.** A customer at this volume pays about $25.9k a month at $100 per million runs, $51.8k at $200, or $103.6k at $400. They need a dedicated, HA store, and its cost is small next to that bill.

A $5 database fits development, trials and hobby use. I have not measured what it can do. **Phase 0 should include a size ladder:** run P-02 on each PlanetScale and Azure size, and record the sustainable jobs/s, log GB a day and bytes per job. That gives you a table of what each size can handle, and a price list that follows from it.

## 15. Pricing for bring your own cloud (BYOC)

*I read BYOC as: the customer runs the data plane (the Runwick server and the store) in their own cloud account or network, and your control plane bills and manages it. If you meant ClickHouse's BYOC, that is a custom sales quote with no published rates.*

### 15.1 What changes against hosted

| | Hosted shared | Hosted dedicated | BYOC, customer operates | BYOC, you operate |
|---|---|---|---|---|
| Who pays for the store and servers | You | You, billed back per store-hour | The customer, directly to their cloud | The customer, directly to their cloud |
| Your infrastructure cost | Shared (~$2k a month) | ~$260–350 a month per store | Control plane only | Control plane only, plus your operating time |
| What you bill | Runs, slot-hours | Runs, slot-hours, store-hours | Runs at a lower rate, plus an environment fee | The same, plus an operations fee |
| Where usage is counted | Receipts in your store | Receipts in your store | Signed usage reports from their data plane | The same |
| Availability promise | Yours | Yours | Control plane only | Shared; you promise only what you operate |

### 15.2 How to price it

- **The meters stay pay as you go.** Runs are billed per million, and each connected environment is billed per environment-hour. That second meter pays for the dashboard, sign-in, billing, alerts, upgrades and support, which you still provide when you don't run their servers.
- **Run price.** About 58% of the lean cost is the data plane: AKS, gateway and store, $1.0–1.3k of the $1.7–2.3k. BYOC moves that to the customer. At the same margin, the floor for the per-million price is about **42% of the hosted price**. Because BYOC needs more support and integration work, I would price it at **50–70% of hosted**. That band is my judgement, not a calculation.
- **Worked example:** a customer running 13 million runs a month. Hosted at $200 per million costs them $2,600. On BYOC at 60%, they pay you about $1,560 plus an environment fee, and pay their own cloud about $1,000 for the servers and store. Their total is close to hosted. They are paying for control and data residency, not for savings.
- **Operations fee (if you operate it for them):** hourly or flat, covering your on-call and upgrade time. Cost basis is your time, so set it from what an incident costs you.
- **Spending limits still apply**, but they are enforced by the customer's data plane from the last limit it received, and breaking one is possible only by tampering. That makes the limit contractual rather than technical.

### 15.3 The hard parts

1. **Counting runs you don't host.** The receipts live in the customer's store. Each data plane sends signed usage reports to your control plane, counted from receipts. The contract gives you the right to audit. If reports stop for a set number of days, you can suspend their control-plane access.
2. **Never stop their jobs over billing.** If the control plane is unreachable, the data plane keeps running work from its last known limit for a grace period. Workers already degrade honestly on disconnect, and this keeps that promise.
3. **Shipping your server.** Tenancy and metering live in the proprietary repository (strategy §5), so BYOC means shipping a closed server image to the customer. How you enforce the licence is open. Remember a customer can already self-host the free OSS library, so BYOC sells the managed layer, not the engine.
4. **Support boundary.** Your SLA covers the control plane. The data plane's availability depends on their cloud and store. Write that down per environment.
5. **Marketplace and managed deployment.** Azure Marketplace metered billing and a publisher-managed deployment into the customer's subscription are options for enterprise buyers. I did not research either.

### 15.4 Decisions for you

1. The BYOC run price as a share of the hosted price (50–70% is my suggestion).
2. The environment-hour fee, and whether you offer operations at all.
3. The usage-report terms: how often, audit right, and what happens when reports stop.
4. How the closed server image is licensed and protected.
5. The grace period when the control plane is unreachable.

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
