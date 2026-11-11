// pg-boss comparison benchmark (docs/plans/perf-plan-draft.md PF-01, PQ4): no-op jobs against the same PostgreSQL as Runwick.
//   node bench.mjs --mode drain  --n 20000 [--concurrency 16] [--batch 1] [--burst]
//   node bench.mjs --mode steady --rate 100 --seconds 120
// drain: bulk insert, then start workers and report completions/s until all are done.
// steady: one send() at a time at --rate/s; report completions/s and created-to-handler-end latency.
import { PgBoss } from 'pg-boss';
import pg from 'pg';
import { writeFileSync, readFileSync } from 'node:fs';

const arg = (name, dflt) => {
  const i = process.argv.indexOf('--' + name);
  return i < 0 ? dflt : (process.argv[i + 1] === undefined || process.argv[i + 1].startsWith('--') ? true : process.argv[i + 1]);
};
const admin = arg('conn', 'postgres://postgres:pg@127.0.0.1:55499/postgres');
const db = arg('db', 'bench_pgboss');
const mode = arg('mode', 'drain');
const n = Number(arg('n', 20000));
const rate = Number(arg('rate', 100));
const seconds = Number(arg('seconds', 120));
const concurrency = Number(arg('concurrency', 16));
const batch = Number(arg('batch', 1));
const burst = arg('burst', false) === true;
const payload = { pad: 'x'.repeat(Math.max(0, Number(arg('payload', 1024)) - 16)) };
const out = arg('out', '');
const maxSeconds = Number(arg('max-seconds', 1800));
const version = JSON.parse(readFileSync(new URL('./node_modules/pg-boss/package.json', import.meta.url))).version;

const c = new pg.Client({ connectionString: admin });
await c.connect();
await c.query(`DROP DATABASE IF EXISTS ${db} WITH (FORCE)`);
await c.query(`CREATE DATABASE ${db}`);
await c.end();

const url = new URL(admin);
url.pathname = '/' + db;
const boss = new PgBoss({ connectionString: url.toString(), max: concurrency + 4 });
boss.on('error', e => console.error('pg-boss error', e.message));
await boss.start();
const queue = 'noop';
await boss.createQueue(queue);
const result = { system: `pg-boss ${version}`, mode, concurrency, batchSize: batch, burstWhenBatchFull: burst, payloadBytes: JSON.stringify(payload).length };
const workOptions = { localConcurrency: concurrency, batchSize: batch, pollingIntervalSeconds: 0.5, includeMetadata: true, burstWhenBatchFull: burst };
const latencies = [];
let completed = 0;

if (mode === 'enqueue') {
  // --n single send() calls from --calls concurrent loops; no worker. Measures adding, not running.
  const calls = Number(arg('calls', 1));
  const lat = [];
  let next = 0;
  const t0 = performance.now();
  await Promise.all(Array.from({ length: calls }, async () => {
    while (next++ < n) {
      const t = performance.now();
      await boss.send(queue, payload);
      lat.push(performance.now() - t);
    }
  }));
  lat.sort((a, b) => a - b);
  const p = q => lat[Math.max(0, Math.min(lat.length - 1, Math.floor(q * lat.length) - 1))];
  Object.assign(result, { jobs: n, calls, insertsPerSec: n / ((performance.now() - t0) / 1000), insertLatencyMs: { p50: p(0.5), p99: p(0.99) } });
} else if (mode === 'bulk') {
  // --n jobs with boss.insert in chunks of 1000 from one caller; no worker.
  const t0 = performance.now();
  for (let i = 0; i < n; i += 1000) await boss.insert(queue, Array.from({ length: Math.min(1000, n - i) }, () => ({ data: payload })));
  Object.assign(result, { jobs: n, insertsPerSec: n / ((performance.now() - t0) / 1000) });
} else if (mode === 'drain') {
  const t0 = performance.now();
  for (let i = 0; i < n; i += 5000) {
    await boss.insert(queue, Array.from({ length: Math.min(5000, n - i) }, () => ({ data: payload })));
  }
  result.insertSeconds = (performance.now() - t0) / 1000;
  const start = performance.now();
  let resolveDone;
  const done = new Promise(r => { resolveDone = r; });
  await boss.work(queue, workOptions, async jobs => {
    completed += jobs.length;
    if (completed >= n) resolveDone();
  });
  await Promise.race([done, new Promise(r => setTimeout(r, maxSeconds * 1000))]);
  // The handler returns before pg-boss marks the batch complete; wait until the table agrees.
  const pc = new pg.Client({ connectionString: url.toString() });
  await pc.connect();
  for (;;) {
    const r = await pc.query(`SELECT count(*)::int AS open FROM pgboss.job WHERE name = $1 AND state <> 'completed'`, [queue]);
    if (r.rows[0].open === 0 || (performance.now() - start) / 1000 > maxSeconds) break;
    await new Promise(r2 => setTimeout(r2, 20));
  }
  await pc.end();
  const left = (await (async () => { const c2 = new pg.Client({ connectionString: url.toString() }); await c2.connect(); const x = await c2.query(`SELECT count(*)::int AS open FROM pgboss.job WHERE name = $1 AND state <> 'completed'`, [queue]); await c2.end(); return x.rows[0].open; })());
  result.jobs = n;
  result.finished = n - left;
  result.drainSeconds = (performance.now() - start) / 1000;
  result.completionsPerSec = result.finished / result.drainSeconds;
} else {
  await boss.work(queue, workOptions, async jobs => {
    const now = Date.now();
    for (const j of jobs) latencies.push(now - new Date(j.createdOn).getTime());
    completed += jobs.length;
  });
  const start = performance.now();
  const stopAt = Date.now() + seconds * 1000;
  let inserted = 0;
  let next = Date.now();
  const calls = Number(arg('calls', 16)); let inflight = 0; const pend = [];
  while (Date.now() < stopAt) {
    if (inflight < calls) { inflight++; pend.push(boss.send(queue, payload).then(() => { inserted++; }).finally(() => { inflight--; })); }
    next += 1000 / rate;
    const wait = next - Date.now();
    if (wait > 0) await new Promise(r => setTimeout(r, wait));
  }
  await Promise.all(pend);
  const deadline = Date.now() + 60_000;
  while (completed < inserted && Date.now() < deadline) await new Promise(r => setTimeout(r, 50));
  latencies.sort((a, b) => a - b);
  const pct = p => latencies.length ? latencies[Math.max(0, Math.min(latencies.length - 1, Math.floor(p * latencies.length) - 1))] : 0;
  result.offeredPerSec = rate;
  result.inserted = inserted;
  result.completed = completed;
  result.completionsPerSec = completed / ((performance.now() - start) / 1000);
  result.latencyMs = { p50: pct(0.5), p95: pct(0.95), p99: pct(0.99), max: pct(1) };
}

await boss.stop({ graceful: true, timeout: 10_000 });
const text = JSON.stringify(result, null, 2);
console.log(text);
if (out) writeFileSync(out, text);
process.exit(0);
