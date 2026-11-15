// BullMQ benchmark: no-op jobs, 1 KiB payload, against a dedicated throwaway Redis (it is FLUSHALLed).
//   node bench.mjs --redis 127.0.0.1:56391 --mode enqueue|bulk|drain|steady [--n 20000] [--calls 1] [--batch 1000] [--warmup-jobs 0] [--concurrency 16] [--rate 100] [--seconds 30] [--out f.json]
// Settings: removeOnComplete/removeOnFail with a count (BullMQ docs "Auto-removal of jobs"), worker maxRetriesPerRequest:null (docs: connections),
// everything else default. Completion is counted on the worker's 'completed' event, which fires after moveToFinished returned.
import { Queue, Worker } from 'bullmq';
import { Redis } from 'ioredis';
import { writeFileSync, readFileSync } from 'node:fs';
const arg = (k, d) => { const i = process.argv.indexOf('--' + k); return i < 0 ? d : process.argv[i + 1]; };
const [host, port] = arg('redis', '127.0.0.1:56391').split(':');
const mode = arg('mode', 'drain'), n = +arg('n', 20000), calls = +arg('calls', 1), conc = +arg('concurrency', 16);
const batch = +arg('batch', 1000), warmupJobs = +arg('warmup-jobs', 0);
const rate = +arg('rate', 100), seconds = +arg('seconds', 30), out = arg('out', ''), maxSeconds = +arg('max-seconds', 1800);
if (!Number.isInteger(batch) || batch < 1 || !Number.isInteger(warmupJobs) || warmupJobs < 0) {
  throw new Error('--batch must be positive and --warmup-jobs must be nonnegative');
}
const pad = 'x'.repeat(1024 - 32);
const payload = sequence => ({ id: String(sequence), pad });
const conn = { host, port: +port, maxRetriesPerRequest: null };
const admin = new Redis(conn); await admin.flushall();
const version = JSON.parse(readFileSync(new URL('./node_modules/bullmq/package.json', import.meta.url))).version;
const fsync = (await admin.config('GET', 'appendfsync'))[1];
const jobOpts = { removeOnComplete: { count: 1000 }, removeOnFail: { count: 1000 } };
const queue = new Queue('noop', { connection: new Redis(conn), defaultJobOptions: jobOpts });
const result = { system: `bullmq ${version}`, mode, concurrency: conc, redisAppendfsync: fsync };
const pct = (a, p) => a.length ? +a[Math.max(0, Math.min(a.length - 1, Math.floor(p * a.length) - 1))].toFixed(2) : 0;
const sleep = ms => new Promise(r => setTimeout(r, ms));
let completed = 0; const lat = [];
const startWorker = () => { const w = new Worker('noop', async job => { }, { connection: new Redis(conn), concurrency: conc });
  w.on('completed', job => { completed++; if (mode === 'steady') lat.push(Date.now() - job.data.t); }); return w; };

const enqueue = async (count, latencies, offset = 0) => {
  let next = 0;
  await Promise.all(Array.from({ length: calls }, async () => {
    while (next < count) {
      const data = payload(++next + offset);
      const t = performance.now();
      await queue.add('noop', data);
      if (latencies) latencies.push(performance.now() - t);
    }
  }));
};
const seedBulk = async () => {
  let next = 0;
  await Promise.all(Array.from({ length: calls }, async () => {
    while (next < n) {
      const start = next;
      next += batch;
      await queue.addBulk(Array.from({ length: Math.min(batch, n - start) }, (_, i) => ({ name: 'noop', data: payload(start + i + 1) })));
    }
  }));
};

if (mode === 'enqueue') {
  await enqueue(warmupJobs);
  const l = [], t0 = performance.now();
  await enqueue(n, l, warmupJobs);
  const elapsed = (performance.now() - t0) / 1000;
  l.sort((a, b) => a - b);
  const accepted = await queue.getWaitingCount() - warmupJobs;
  Object.assign(result, { jobs: n, calls, warmupJobs, insertsPerSec: +(n / elapsed).toFixed(1),
    accepted, insertLatencyMs: { p50: pct(l, .5), p95: pct(l, .95), p99: pct(l, .99) } });
} else if (mode === 'bulk') {
  const t0 = performance.now();
  await seedBulk();
  const elapsed = (performance.now() - t0) / 1000;
  const accepted = await queue.getWaitingCount();
  Object.assign(result, { jobs: n, batch, calls, accepted, insertsPerSec: +(n / elapsed).toFixed(1) });
} else if (mode === 'drain') {
  await seedBulk();
  const t0 = performance.now(); const w = startWorker();
  while (completed < n && (performance.now() - t0) / 1000 < maxSeconds) await sleep(5);
  const el = (performance.now() - t0) / 1000; await w.close();
  Object.assign(result, { jobs: n, finished: completed, drainSeconds: +el.toFixed(3), completionsPerSec: +(completed / el).toFixed(1) });
} else {
  const w = startWorker(); await sleep(500);
  const t0 = performance.now(); const stop = Date.now() + seconds * 1000; let inserted = 0, next = Date.now(), inflight = 0; const pend = [];
  while (Date.now() < stop) {
    if (inflight < 16) { inflight++; pend.push(queue.add('noop', { pad, t: Date.now() }).then(() => { inserted++; }).finally(() => inflight--)); }
    next += 1000 / rate; const wait = next - Date.now(); if (wait > 0) await sleep(wait);
  }
  await Promise.all(pend); const dl = Date.now() + 60000; while (completed < inserted && Date.now() < dl) await sleep(20);
  lat.sort((a, b) => a - b); await w.close();
  Object.assign(result, { offeredPerSec: rate, inserted, completed, completionsPerSec: +(completed / ((performance.now() - t0) / 1000)).toFixed(1),
    latencyMs: { p50: pct(lat, .5), p95: pct(lat, .95), p99: pct(lat, .99), max: pct(lat, 1) } });
}
const text = JSON.stringify(result, null, 2); console.log(text); if (out) writeFileSync(out, text); process.exit(0);
