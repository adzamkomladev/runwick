// Shared CPU-heavy job (ci/README.md): BENCH_CPU_ITERS rounds of SHA-256 over a 1 KiB buffer; unset or 0 = no-op.
import { createHash } from 'node:crypto';
const iters = Number(process.env.BENCH_CPU_ITERS || 0);
export function cpuWork() {
  if (iters <= 0) return;
  const b = Buffer.alloc(1024, 'x');
  for (let i = 0; i < iters; i++) createHash('sha256').update(b).digest().copy(b, 0);
}
