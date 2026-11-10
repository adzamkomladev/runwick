import { Bar } from './charts'
import { bytes } from './usage'
import { cn } from './ui/utils'

// What a worker publishes about itself with each heartbeat (16 WorkerSummary.detail): its host and process, its pools with their
// settings, and its running and recent attempts. Older workers publish none; every reader here copes with a missing piece.

export const hostOf = (w) => w?.detail?.host ?? null

/** Share of memory in use on the machine (0–1), or null when unknown. */
export const memoryUsed = (h) => (h?.memoryTotalBytes ? 1 - (h.memoryAvailableBytes ?? 0) / h.memoryTotalBytes : null)

/** A thin meter: a track with a bar scaled on x (transform only). */
export function Meter({ value, className, label }) {
  if (value == null) return null
  const tone = value > 0.9 ? 'bg-chocolate-500' : value > 0.7 ? 'bg-chocolate-300' : 'bg-pine-blue-500 dark:bg-pine-blue-400'
  return (
    <span role="meter" aria-label={label} aria-valuemin={0} aria-valuemax={100} aria-valuenow={Math.round(value * 100)}
      className={cn('relative block h-1.5 overflow-hidden rounded-full bg-muted', className)}>
      <Bar fraction={Math.min(Math.max(value, 0), 1)} className={cn('rounded-full', tone)} />
    </span>
  )
}

/** "3.2 %" of the whole machine (process CPU over all cores). */
export const percent = (v) => (v == null ? '—' : `${v < 10 ? v.toFixed(1) : Math.round(v)} %`)

/** The CPU column of a worker row: cores and the process's share, with a meter. */
export function CpuCell({ host }) {
  if (!host) return <span className="text-muted-foreground">—</span>
  return (
    <span className="grid min-w-24 gap-1">
      <span className="text-xs tabular-nums"><span className="font-medium">{percent(host.processCpuPercent)}</span> <span className="text-muted-foreground">of {host.cpus} CPU{host.cpus === 1 ? '' : 's'}</span></span>
      <Meter value={host.processCpuPercent == null ? null : host.processCpuPercent / 100} label="Process CPU" />
    </span>
  )
}

/** The memory column: the process's working set, and the machine's memory in use with a meter. */
export function MemoryCell({ host }) {
  if (!host) return <span className="text-muted-foreground">—</span>
  return (
    <span className="grid min-w-32 gap-1">
      <span className="text-xs tabular-nums"><span className="font-medium">{bytes(host.workingSetBytes)}</span> <span className="text-muted-foreground">· {bytes(host.memoryAvailableBytes)} of {bytes(host.memoryTotalBytes)} free</span></span>
      <Meter value={memoryUsed(host)} label="Machine memory in use" />
    </span>
  )
}

/**
 * Pools grouped by name across the live workers: settings and queues from a worker that publishes detail, targets and in-flight
 * counts summed. A worker without detail contributes only its target (from the presence pool targets).
 */
export function poolsFrom(workers) {
  const pools = new Map()
  for (const w of workers) {
    const published = new Map((w.detail?.pools ?? []).map((p) => [p.name, p]))
    for (const [name, target] of Object.entries(w.poolTargets ?? {})) {
      const p = published.get(name)
      const pool = pools.get(name) ?? { name, workers: [], target: 0, inFlight: 0, known: false, queues: [], mode: null, min: null, max: null, laneSelection: null, laneWeights: null, settings: null }
      pool.workers.push({ worker: w, target, inFlight: p?.inFlight ?? null })
      pool.target += target
      pool.inFlight += p?.inFlight ?? 0
      if (p && !pool.known) Object.assign(pool, { known: true, queues: p.queues ?? [], mode: p.mode, min: p.min, max: p.max, laneSelection: p.laneSelection, laneWeights: p.laneWeights, settings: w.detail?.settings ?? null })
      pools.set(name, pool)
    }
  }
  return [...pools.values()].sort((a, b) => a.name.localeCompare(b.name))
}

/** "fixed 4" or "auto 1–8". */
export const modeText = (p) => (p.mode === 'auto' ? `auto ${p.min ?? '?'}–${p.max ?? '?'}` : p.mode === 'fixed' ? `fixed ${p.max ?? p.target}` : '—')
