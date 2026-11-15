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
const acceptedIds = new Set(), invokedIds = new Map(), settledIds = new Map(), settledAt = [];
const startWorker = () => { const w = new Worker('noop', async job => { const id = job.data.id; invokedIds.set(id, (invokedIds.get(id) ?? 0) + 1); lat.push(Date.now() - job.data.due); }, { connection: new Redis(conn), concurrency: conc });
  w.on('completed', job => { completed++; const id = job.data.id; settledIds.set(id, (settledIds.get(id) ?? 0) + 1); settledAt.push(performance.now()); }); return w; };

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
  const w = startWorker();
  await w.waitUntilReady();
  await sleep(12000);
  const t0 = performance.now();
  let offered = 0, inserted = 0, refused = 0, failed = 0, next = t0; const enqueueLat = [];
  const pending = new Set();
  while (offered < Math.round(rate * seconds)) {
    offered++;
    if (pending.size >= calls) {
      refused++;
    } else {
      const id = String(offered), due = Date.now() + 5000, started = performance.now();
      const add = queue.add('noop', { ...payload(offered), due }, { delay: 5000, timestamp: due - 5000 })
        .then(() => { inserted++; acceptedIds.add(id); enqueueLat.push(performance.now() - started); }, () => { failed++; })
        .finally(() => pending.delete(add));
      pending.add(add);
    }
    next += 1000 / rate;
    const wait = next - performance.now();
    if (wait > 0) await sleep(wait);
  }
  const actualOfferSeconds = (performance.now() - t0) / 1000;
  await Promise.all(pending);
  const drain = performance.now() + 60000;
  while (completed < inserted && performance.now() < drain) await sleep(20);
  lat.sort((a, b) => a - b); enqueueLat.sort((a, b) => a - b);
  await w.close();
  Object.assign(result, { seconds, offeredPerSec: rate, offered, inserted, refused, failed,
    completed, actualOfferSeconds, completedInDueWindow: settledAt.filter(t => t >= t0 + 5000 && t < t0 + 5000 + seconds * 1000).length, latencySamples: lat.length, identityMismatches: inserted - acceptedIds.size + [...acceptedIds].filter(id => invokedIds.get(id) !== 1 || settledIds.get(id) !== 1).length + [...invokedIds.keys()].filter(id => !acceptedIds.has(id)).length + [...settledIds.keys()].filter(id => !acceptedIds.has(id)).length, enqueueLatencyMs: { p50: pct(enqueueLat, .5), p95: pct(enqueueLat, .95), p99: pct(enqueueLat, .99) },
    latencyMs: { p50: pct(lat, .5), p95: pct(lat, .95), p99: pct(lat, .99), max: pct(lat, 1) } });
}
const text = JSON.stringify(result, null, 2); console.log(text); if (out) writeFileSync(out, text); process.exit(0);
