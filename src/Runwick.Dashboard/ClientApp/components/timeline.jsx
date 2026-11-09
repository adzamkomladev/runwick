import {
  IconAlertTriangle, IconBan, IconCheck, IconCircleDashed, IconClockPlay, IconHandStop, IconPlayerPlay, IconRefresh, IconRotateClockwise2,
  IconUserBolt,
} from '@tabler/icons-react'
import { useRef } from 'react'
import { StatusBadge, toneOf } from './status'
import { Tooltip, TooltipContent, TooltipTrigger } from './ui/tooltip'
import { cn } from './ui/utils'

// The job's history as a vertical timeline, built from what GetJob returns (generations, attempts, terminal state) and, once
// loaded, the job's audit rows. Nothing is invented: a step whose time the store does not keep is not drawn.

const FINISH = {
  succeeded: ['Succeeded', IconCheck, 'succeeded'],
  permanent_failure: ['Failed', IconAlertTriangle, 'failed'],
  retryable_failure: ['Failed, will retry', IconRotateClockwise2, 'retrying'],
  user_cancelled: ['Cancelled by the handler', IconBan, 'cancelled'],
  interrupted: ['Interrupted', IconHandStop, 'retrying'],
  timed_out: ['Timed out', IconAlertTriangle, 'failed'],
}

const REASONS = { initial: 'Created', manual_retry: 'Manual retry', ManualRetry: 'Manual retry', rerun: 'Rerun', manual_rerun: 'Rerun', ManualRerun: 'Rerun' }

export function buildSteps(detail, audit) {
  const { job, generations, attempts } = detail
  const steps = [{ id: 'created', at: job.createdAt, title: 'Created', icon: IconCircleDashed, state: 'pending', note: <>Queue <b className="font-medium text-foreground">{job.queue}</b></> }]
  for (const g of generations) {
    if (g.generation === 1) continue
    steps.push({ id: `gen-${g.generation}`, at: g.createdAt, title: REASONS[g.createdReason] ?? g.createdReason, icon: IconRefresh, state: 'pending',
      actor: g.createdBy, note: `Generation ${g.generation}, up to ${g.maxExecutions} executions` })
  }
  const ordered = [...attempts].sort((a, b) => Date.parse(a.reservedAt) - Date.parse(b.reservedAt))
  ordered.forEach((a, i) => {
    const n = a.executionNumber ?? i + 1
    steps.push({ id: `${a.attemptId}-r`, at: a.reservedAt, title: `Attempt ${n} reserved`, icon: IconClockPlay, state: 'reserved', note: <>Worker <code>{a.workerInstanceId.slice(0, 8)}</code></> })
    if (a.authorizedAt) steps.push({ id: `${a.attemptId}-s`, at: a.authorizedAt, title: `Attempt ${n} started`, icon: IconPlayerPlay, state: 'running' })
    if (a.finishedAt) {
      const [title, icon, state] = FINISH[a.outcome] ?? [a.outcome ?? 'Finished', IconCheck, a.outcome]
      steps.push({ id: `${a.attemptId}-f`, at: a.finishedAt, title: `Attempt ${n}: ${title.toLowerCase()}`, icon, state, error: a.errorCode,
        note: a.disposition && a.disposition !== state && `Job moved to ${a.disposition}` })
    }
  })
  for (const e of audit?.items ?? []) {
    if (!/cancel/i.test(e.operation)) continue // retries and reruns already show as a generation
    steps.push({ id: e.auditId, at: e.occurredAt, title: job.state === 'cancelled' ? 'Cancelled' : 'Cancel requested', icon: IconBan, state: 'cancelled',
      actor: e.actorDisplay ?? e.actorSubject, note: e.reason })
  }
  steps.sort((a, b) => Date.parse(a.at) - Date.parse(b.at))
  const last = steps.at(-1)
  if (job.terminalAt) {
    if (!(last && Math.abs(Date.parse(last.at) - Date.parse(job.terminalAt)) < 1000 && last.state === job.state)) {
      steps.push({ id: 'terminal', at: job.terminalAt, title: `Ended: ${job.state}`, icon: job.state === 'succeeded' ? IconCheck : job.state === 'failed' ? IconAlertTriangle : IconBan,
        state: job.state, note: job.terminalReason })
    }
  } else {
    steps.push({ id: 'now', at: null, title: `Now ${job.state}`, state: job.state, live: true,
      note: job.cancelRequested ? 'Cancel requested; the handler decides when to stop.' : `Eligible from ${new Date(job.eligibleAt).toLocaleTimeString()}` })
  }
  return steps
}

const rtf = new Intl.RelativeTimeFormat(undefined, { numeric: 'auto' })
export function relative(at, now = Date.now()) {
  const s = (Date.parse(at) - now) / 1000
  for (const [unit, size] of [['day', 86400], ['hour', 3600], ['minute', 60]]) if (Math.abs(s) >= size) return rtf.format(Math.round(s / size), unit)
  return rtf.format(Math.round(s), 'second')
}

export function duration(ms) {
  if (ms < 1000) return `${Math.max(0, Math.round(ms))} ms`
  const s = ms / 1000
  if (s < 60) return `${s < 10 ? s.toFixed(1) : Math.round(s)} s`
  const m = Math.floor(s / 60)
  if (m < 60) return `${m} min ${Math.round(s % 60)} s`
  const h = Math.floor(m / 60)
  return h < 48 ? `${h} h ${m % 60} min` : `${Math.floor(h / 24)} d ${h % 24} h`
}

const exact = (at) => new Date(at).toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'medium' })

export function Timeline({ steps }) {
  const seen = useRef(null)
  if (seen.current === null) seen.current = new Set(steps.map((s) => s.id)) // steps present on first render do not animate
  return (
    <ol data-testid="timeline" className="relative grid">
      {steps.map((s, i) => {
        const prev = steps[i - 1]
        const spent = prev && (s.at ? Date.parse(s.at) : Date.now()) - Date.parse(prev.at)
        const Icon = s.icon
        const fresh = !seen.current.has(s.id)
        return (
          <li key={s.id} className={cn('group/step relative grid grid-cols-[2rem_1fr] gap-x-3 pb-6 last:pb-0', fresh && 'animate-node-in')}>
            {i < steps.length - 1 && <span aria-hidden className="absolute top-8 bottom-0 left-4 w-px -translate-x-1/2 bg-border" />}
            <span className={cn('relative z-10 grid size-8 place-items-center rounded-full ring-1 ring-inset transition-transform duration-200 group-hover/step:scale-110', toneOf(s.state).badge)}>
              {s.live
                ? <span className="relative flex size-2.5"><span className={cn('absolute inline-flex size-full rounded-full opacity-70 motion-safe:animate-ping', toneOf(s.state).dot)} /><span className={cn('relative inline-flex size-2.5 rounded-full', toneOf(s.state).dot)} /></span>
                : <Icon className="size-4" aria-hidden />}
            </span>
            <div className="grid min-w-0 gap-1 pt-1">
              <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
                <span className="text-sm font-medium">{s.title}</span>
                {s.live && <StatusBadge state={s.state} />}
                {spent > 0 && <span className="rounded bg-muted px-1.5 py-0.5 text-[0.7rem] text-muted-foreground tabular-nums" title="Time since the previous step">+{duration(spent)}</span>}
              </div>
              <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
                {s.at
                  ? <Tooltip><TooltipTrigger asChild><time dateTime={s.at} tabIndex={0} className="rounded-sm tabular-nums underline decoration-dotted underline-offset-2 outline-none focus-visible:ring-2 focus-visible:ring-ring/50">{exact(s.at)}</time></TooltipTrigger>
                    <TooltipContent>{relative(s.at)}</TooltipContent></Tooltip>
                  : <span>in this state for {duration(spent ?? 0)}</span>}
                {s.actor && <span className="inline-flex items-center gap-1"><IconUserBolt className="size-3.5" aria-hidden />{s.actor}</span>}
              </div>
              {s.note && <p className="text-xs text-muted-foreground">{s.note}</p>}
              {s.error && <p className="w-fit max-w-full rounded-md bg-rose-50 px-2 py-1 font-mono text-xs break-all text-rose-700 ring-1 ring-rose-600/15 ring-inset dark:bg-rose-400/10 dark:text-rose-300">{s.error}</p>}
            </div>
          </li>
        )
      })}
    </ol>
  )
}
