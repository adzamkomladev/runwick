import { AnimatedNumber } from '../lib'
import { Spark } from './charts'
import { cn } from './ui/utils'

// The stats header of a list page (21 §4.12): a compact row of cards with exact figures over the page's range, each a tweening
// number, a line under it and, where a series exists, a sparkline. Figures come from exact store reads (16 GetJobStats and
// friends); nothing here counts a sample.

export function KpiStrip({ label, children, className }) {
  return <section aria-label={label} data-testid="kpis" className={cn('grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4', className)}>{children}</section>
}

export function Kpi({ label, value, more, suffix, foot, spark, color = 'text-chart-1', icon: Icon, testId }) {
  return (
    <div data-slot="card" className="grid min-w-0 content-start gap-1 overflow-hidden rounded-xl border bg-card px-4 pt-3 pb-2 text-card-foreground">
      <span className="flex items-center gap-1.5 truncate text-xs text-muted-foreground">{Icon && <Icon className="size-3.5 shrink-0" aria-hidden />}{label}</span>
      <span data-testid={testId} className="flex min-w-0 items-baseline gap-1 text-xl font-semibold tracking-tight tabular-nums">
        {typeof value === 'number' ? <AnimatedNumber value={value} more={more} /> : <span className="truncate">{value ?? '—'}</span>}
        {suffix && <span className="text-sm font-normal text-muted-foreground">{suffix}</span>}
      </span>
      <span className="truncate text-xs text-muted-foreground">{foot ?? ' '}</span>
      <div className="-mx-4 h-8">{spark && spark.length > 1 && <Spark small data={spark} color={color} label={`${label} over the range`} />}</div>
    </div>
  )
}

/** Sum of a state-count map, optionally of some states only. */
export const sum = (states, only) => Object.entries(states ?? {}).reduce((t, [k, v]) => (!only || only.includes(k) ? t + v : t), 0)

/** Success rate in percent of the jobs that ended (succeeded or failed), or null. */
export function successRate(states) {
  const ok = states?.succeeded ?? 0, bad = states?.failed ?? 0
  return ok + bad ? Math.round((ok / (ok + bad)) * 1000) / 10 : null
}

/** Jobs created per minute over the stats range. */
export function perMinute(stats) {
  const minutes = (Date.parse(stats.to) - Date.parse(stats.from)) / 60_000
  return minutes > 0 ? Math.round((sum(stats.states) / minutes) * 100) / 100 : 0
}
