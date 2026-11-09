import { Link, usePage } from '@inertiajs/react'
import { DateRangePicker, presetMs } from '../components/date-range'
import { Skeleton } from '../components/ui/skeleton'
import {
  IconAlertTriangle, IconArrowDownRight, IconArrowRight, IconArrowUpRight, IconCircleCheck, IconListDetails, IconLoader, IconMinus,
} from '@tabler/icons-react'
import { useEffect, useRef, useState } from 'react'
import { BarList, Chart, Legend, Spark } from '../components/charts'
import { StatusBadge, StatusDot } from '../components/status'
import { relative } from '../components/timeline'
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from '../components/ui/card'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../components/ui/table'
import { Tooltip, TooltipContent, TooltipTrigger } from '../components/ui/tooltip'
import { cn } from '../components/ui/utils'
import { AnimatedNumber, Layout, time, usePoll } from '../lib'

// Every figure is computed from one page (up to 200 rows) of a read that already exists; there is no count read (16). The
// cards and charts cover the range in the picker (default the last 24 hours). When the read hit the 200-row limit, the captions
// say so and how far back the sample reaches; a delta (second half of the range vs the first) is shown only when the sample
// covers the whole range. "In flight now" is a live snapshot and says so.

const HOUR = 3_600_000
const BUCKETS = 24

/**
 * What a read covers within the chosen range: the whole range, or (when the 200-row page was full) only back to its oldest row,
 * since older rows exist but were not read. `sampleFrom` is null when the sample is complete.
 */
function coverage(page, range) {
  if (!page.next || page.items.length === 0) return { ...range, sampleFrom: null }
  return { ...range, sampleFrom: Math.min(...page.items.map((j) => Date.parse(j.createdAt))) }
}

/** The chosen range from the URL: a preset, a from/to pair, or the default of the last 24 hours (what the picker shows). */
function useRange(now) {
  const q = Object.fromEntries(new URL(usePage().url, window.location.href).searchParams)
  const custom = !q.range && (q.from || q.to)
  const from = q.range ? now - presetMs(q.range) : custom && q.from ? Date.parse(q.from) : now - 24 * HOUR
  const to = custom && q.to ? Date.parse(q.to) : now
  const fmt = (t) => new Date(t).toLocaleString([], { dateStyle: 'medium', timeStyle: 'short' })
  return { query: q, from, to, label: custom ? `${fmt(from)} to ${q.to ? fmt(to) : 'now'}` : `the last ${fmtSpan(to - from)}` }
}

/** The page's outline while its deferred props load (three groups: charts, in-flight counts, side panels). */
function OverviewSkeleton() {
  return (
    <div className="grid gap-6" aria-busy="true" aria-label="Loading">
      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">{[0, 1, 2, 3].map((i) => <Skeleton key={i} className="h-40 rounded-xl" />)}</div>
      <div className="grid gap-6 xl:grid-cols-3"><Skeleton className="h-80 rounded-xl xl:col-span-2" /><Skeleton className="h-80 rounded-xl" /></div>
    </div>
  )
}

function bucketize(items, field, { from, to }, filter = () => true) {
  const width = (to - from) / BUCKETS, out = Array(BUCKETS).fill(0)
  for (const i of items) {
    const at = i[field] && Date.parse(i[field])
    if (!at || at < from || at > to || !filter(i)) continue
    out[Math.min(BUCKETS - 1, Math.floor((at - from) / width))]++
  }
  return out
}

const fmtSpan = (ms) => (ms >= 48 * HOUR ? `${Math.round(ms / (24 * HOUR))} d` : ms >= HOUR ? `${+(ms / HOUR).toFixed(ms < 3 * HOUR ? 1 : 0)} h` : ms >= 60_000 ? `${Math.round(ms / 60_000)} min` : `${Math.max(1, Math.round(ms / 1000))} s`)
const sum = (a) => a.reduce((t, v) => t + v, 0)

/** The last 30 polled values of a number, for figures with no time field. Lives as long as the page. */
function useHistory(value) {
  const [history, setHistory] = useState(() => (value == null ? [] : [value]))
  const last = useRef(value)
  useEffect(() => {
    if (value == null || value === last.current) return
    last.current = value
    setHistory((h) => [...h.slice(-29), value])
  }, [value])
  return history
}

function Delta({ now, before, invert, unit = '%' }) {
  if (before == null) return <span className="rounded-md bg-muted px-1.5 py-0.5 text-xs text-muted-foreground">—</span>
  const diff = unit === 'pt' ? now - before : before === 0 ? (now === 0 ? 0 : null) : ((now - before) / before) * 100
  if (diff == null) return <span className="rounded-md bg-muted px-1.5 py-0.5 text-xs font-medium text-muted-foreground">new</span>
  const up = diff > 0.5, down = diff < -0.5
  const good = invert ? down : up
  const Icon = up ? IconArrowUpRight : down ? IconArrowDownRight : IconMinus
  return (
    <span className={cn('inline-flex items-center gap-0.5 rounded-md px-1.5 py-0.5 text-xs font-medium tabular-nums ring-1 ring-inset',
      !up && !down ? 'bg-muted text-muted-foreground ring-border'
        : good ? 'bg-lime-yellow-50 text-lime-yellow-800 ring-lime-yellow-600/20 dark:bg-lime-yellow-400/10 dark:text-lime-yellow-400 dark:ring-lime-yellow-400/25'
          : 'bg-chocolate-50 text-chocolate-700 ring-chocolate-600/20 dark:bg-chocolate-400/10 dark:text-chocolate-300 dark:ring-chocolate-400/25')}>
      <Icon className="size-3.5" aria-hidden />{Math.abs(diff).toFixed(unit === 'pt' ? 1 : 0)}{unit === 'pt' ? ' pt' : '%'}
    </span>
  )
}

function Kpi({ title, value, suffix, delta, foot, spark, color, href, testId, icon: Icon }) {
  return (
    <Link href={href} data-testid={testId} className="group rounded-xl outline-none focus-visible:ring-[3px] focus-visible:ring-ring/50">
      <Card className="relative h-full gap-2 overflow-hidden pb-0 transition-[box-shadow,transform,border-color] duration-200 group-hover:-translate-y-0.5 group-hover:border-primary/30 group-hover:shadow-lg group-active:translate-y-0 group-active:scale-[0.99]">
        <CardHeader>
          <CardDescription className="flex items-center gap-1.5"><Icon className="size-4" aria-hidden />{title}</CardDescription>
          <CardAction><IconArrowRight className="size-4 -translate-x-1 text-muted-foreground opacity-0 transition-all duration-200 group-hover:translate-x-0 group-hover:opacity-100" aria-hidden /></CardAction>
          <CardTitle className="flex items-baseline gap-1 text-3xl font-semibold tracking-tight"><AnimatedNumber value={value} />{suffix && <span className="text-lg text-muted-foreground">{suffix}</span>}</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-1 px-5 text-xs text-muted-foreground">
          <div className="flex flex-wrap items-center gap-2">{delta}</div>
          <span>{foot}</span>
        </CardContent>
        <div className="mt-auto -mb-px">{spark}</div>
        <span aria-hidden className={cn('absolute inset-x-0 top-0 h-0.5 origin-left scale-x-0 bg-current transition-transform duration-300 group-hover:scale-x-100', color)} />
      </Card>
    </Link>
  )
}

const Panel = ({ title, description, action, className, children, testId }) => (
  <Card className={cn('gap-4', className)} data-testid={testId}>
    <CardHeader><CardTitle className="text-base">{title}</CardTitle>{description && <CardDescription>{description}</CardDescription>}{action && <CardAction>{action}</CardAction>}</CardHeader>
    <CardContent className="min-w-0">{children}</CardContent>
  </Card>
)

const More = ({ href, children }) => (
  <Link href={href} className="group inline-flex items-center gap-1 rounded-sm text-sm text-primary outline-none focus-visible:ring-2 focus-visible:ring-ring/50">
    {children}<IconArrowRight className="size-3.5 transition-transform group-hover:translate-x-0.5" aria-hidden />
  </Link>
)

const top = (items, key, n = 6) => Object.entries(items.reduce((m, i) => ({ ...m, [key(i)]: (m[key(i)] ?? 0) + 1 }), {}))
  .sort((a, b) => b[1] - a[1]).slice(0, n).map(([label, value]) => ({ label, value }))

export default function Overview(props) {
  const { can, recent, failed, pending, running } = props
  const live = usePoll(['recent', 'failed', 'pending', 'running', ...(can.queues ? ['queues'] : []), ...(can.workers ? ['workers', 'events'] : [])], 10)
  const now = Date.now()
  const range = useRange(now)
  const ready = [recent, failed, pending, running].every((p) => p !== undefined)
  return (
    <Layout heading="Overview" live={ready ? live : 0} actions={<DateRangePicker query={range.query} fallback="1d" />}>
      {ready ? <OverviewBody {...props} now={now} range={range} /> : <OverviewSkeleton />}
    </Layout>
  )
}

function OverviewBody({ base, can, recent, failed, pending, running, queues, workers, events, now, range }) {
  const span = coverage(recent, range)
  const labelAt = (i) => new Date(span.from + ((i + 0.5) * (span.to - span.from)) / BUCKETS)
  const created = bucketize(recent.items, 'createdAt', span)
  const finished = bucketize(recent.items, 'terminalAt', span)
  const ok = bucketize(recent.items, 'terminalAt', span, (j) => j.state === 'succeeded')
  const bad = bucketize(recent.items, 'terminalAt', span, (j) => j.state === 'failed')
  const series = created.map((c, i) => {
    const at = labelAt(i), width = span.to - span.from, short = width < 30 * 60 * 1000 // under 30 min, minutes alone repeat across labels
    // Past a day the clock repeats, so labels carry the date (and drop the time from 3 days up).
    const label = width >= 72 * HOUR ? at.toLocaleDateString([], { month: 'short', day: 'numeric' })
      : width > 24 * HOUR ? at.toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
        : at.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', ...(short && { second: '2-digit' }) })
    return { label,
      title: at.toLocaleString([], { dateStyle: 'medium', timeStyle: short ? 'medium' : 'short' }),
      created: c, finished: finished[i], succeeded: ok[i], failed: bad[i] }
  })
  const firstHalf = (a) => sum(a.slice(0, BUCKETS / 2)), secondHalf = (a) => sum(a.slice(BUCKETS / 2))
  const rate = (s, f) => (s + f ? (s / (s + f)) * 100 : null)
  const rateNow = rate(secondHalf(ok), secondHalf(bad)), rateBefore = rate(firstHalf(ok), firstHalf(bad))
  const failedSpan = coverage(failed, range)
  const failedBuckets = bucketize(failed.items, 'createdAt', failedSpan)
  const inFlight = pending.items.length + running.items.length
  const flightHistory = useHistory(inFlight)
  const states = top(recent.items, (j) => j.state, 8)
  const pendingByQueue = top(pending.items, (j) => j.queue)
  const failuresByJob = top(failed.items, (j) => j.jobName)
  // The halves compare only when the sample covers the whole range; a truncated sample would make the first half look empty.
  const whole = span.sampleFrom == null, failedWhole = failedSpan.sampleFrom == null
  const since = (t) => new Date(t).toLocaleString([], { dateStyle: 'short', timeStyle: 'short' })
  const sampleNote = whole ? `${recent.items.length} jobs in ${range.label}` : `newest 200 jobs in ${range.label} (back to ${since(span.sampleFrom)})`
  const ended = sum(ok) + sum(bad), rateAll = rate(sum(ok), sum(bad))
  const noDelta = <span className="text-muted-foreground">sample too short to compare halves</span>
  const pool = events?.items[0]?.pool
  const poolEvents = events ? events.items.filter((e) => e.pool === pool).slice(0, 24).reverse() : []
  return (
    <>
      <section aria-label="Key figures" className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <Kpi testId="kpi-created" icon={IconListDetails} title="Jobs created" href={`${base}/jobs`} color="text-chart-1"
          value={whole ? sum(created) : `${sum(created)}+`}
          delta={whole ? <><Delta now={secondHalf(created)} before={firstHalf(created)} /><span>second half vs first</span></> : noDelta}
          foot={sampleNote} spark={<Spark data={created} color="text-chart-1" label={`Jobs created in ${range.label}`} />} />
        <Kpi testId="kpi-success" icon={IconCircleCheck} title="Success rate" href={`${base}/jobs?state=succeeded`} color="text-chart-2"
          value={rateAll == null ? '—' : Math.round(rateAll)} suffix={rateAll == null ? null : '%'}
          delta={whole ? <><Delta now={rateNow ?? 0} before={rateNow == null ? null : rateBefore} unit="pt" /><span>second half vs first</span></> : noDelta}
          foot={`Of ${ended} ${whole ? '' : 'sampled '}jobs that ended in ${range.label}`} spark={<Spark data={ok.map((s, i) => (s + bad[i] ? (s / (s + bad[i])) * 100 : 100))} color="text-chart-2" label={`Success rate in ${range.label}`} />} />
        <Kpi testId="kpi-failed" icon={IconAlertTriangle} title="Failed" href={`${base}/jobs?state=failed`} color="text-chart-3"
          value={failed.next ? `${failed.items.length}+` : failed.items.length}
          delta={failedWhole ? <><Delta now={secondHalf(failedBuckets)} before={firstHalf(failedBuckets)} invert /><span>second half vs first</span></> : noDelta}
          foot={`Failed jobs created in ${range.label}${failedWhole ? '' : ` (newest 200, back to ${since(failedSpan.sampleFrom)})`}`} spark={<Spark data={failedBuckets} color="text-chart-3" label={`Failed jobs created in ${range.label}`} />} />
        <Kpi testId="kpi-inflight" icon={IconLoader} title="In flight now" href={`${base}/jobs?state=running`} color="text-chart-4"
          value={pending.next || running.next ? `${inFlight}+` : inFlight}
          delta={<><StatusBadge state="pending">{pending.next ? '200+' : pending.items.length} pending</StatusBadge><StatusBadge state="running">{running.next ? '200+' : running.items.length} running</StatusBadge></>}
          foot="Live snapshot, not limited to the range" spark={<Spark data={flightHistory} color="text-chart-4" label="Jobs in flight at each poll" />} />
      </section>

      <div className="grid gap-6 xl:grid-cols-3">
        <Panel className="xl:col-span-2" title="Throughput" testId="chart-throughput"
          description={`Per ${fmtSpan((span.to - span.from) / BUCKETS)} · ${sampleNote}`}
          action={<Legend series={[{ key: 'c', label: 'Created', color: 'text-chart-1' }, { key: 'f', label: 'Finished', color: 'text-chart-2' }]} />}>
          <Chart data={series} label="Jobs created and finished over time" height={260}
            series={[{ key: 'created', label: 'Created', color: 'text-chart-1' }, { key: 'finished', label: 'Finished', color: 'text-chart-2' }]} />
        </Panel>
        <Panel title="State of sampled jobs" description={whole ? `Created in ${range.label}` : `Newest 200 created in ${range.label}`}>
          <div className="grid gap-4">
            <div className="flex h-3 w-full overflow-hidden rounded-full bg-muted" role="img" aria-label="Share of sampled jobs per state">
              {states.map((s) => <Tooltip key={s.label}><TooltipTrigger asChild>
                <span className={cn('h-full transition-[width] duration-700 ease-out first:rounded-l-full last:rounded-r-full hover:brightness-110', STATE_BG[s.label] ?? 'bg-midnight-violet-400')}
                  ref={(el) => { if (el) el.style.width = `${(s.value / Math.max(recent.items.length, 1)) * 100}%` }} />
              </TooltipTrigger><TooltipContent>{s.label}: {s.value}</TooltipContent></Tooltip>)}
            </div>
            <ul className="grid gap-2">{states.map((s) => (
              <li key={s.label} className="flex items-center justify-between gap-3 rounded-md px-1 py-1 text-sm transition-colors hover:bg-muted/60">
                <span className="inline-flex items-center gap-2"><StatusDot state={s.label} />{s.label}</span>
                <span className="font-medium tabular-nums">{s.value} <span className="text-xs font-normal text-muted-foreground">({Math.round((s.value / Math.max(recent.items.length, 1)) * 100)}%)</span></span>
              </li>
            ))}</ul>
            {states.length === 0 && <p className="text-sm text-muted-foreground">No jobs created in {range.label}.</p>}
            {queues && <div className="grid gap-2 border-t pt-4">
              <div className="flex items-center justify-between text-sm"><span className="font-medium">Queues</span><More href={`${base}/queues`}>Manage</More></div>
              <ul className="flex flex-wrap gap-1.5">{queues.items.slice(0, 12).map((q) => <li key={q.name}><StatusBadge state={q.paused ? 'paused' : 'active'}>{q.name}</StatusBadge></li>)}</ul>
            </div>}
          </div>
        </Panel>
      </div>

      <div className="grid gap-6 lg:grid-cols-2 xl:grid-cols-3">
        <Panel title="Succeeded and failed" testId="chart-outcomes" description={`By end time · ${sampleNote}`}
          action={<Legend series={[{ key: 's', label: 'Succeeded', color: 'text-chart-2' }, { key: 'f', label: 'Failed', color: 'text-chart-3' }]} />}>
          <Chart type="bar" stacked data={series} height={200} label="Succeeded and failed jobs over time"
            series={[{ key: 'succeeded', label: 'Succeeded', color: 'text-chart-2' }, { key: 'failed', label: 'Failed', color: 'text-chart-3' }]} />
        </Panel>
        <Panel title="Pending by queue" description={pending.next ? 'Live: the 200 oldest pending jobs' : 'Live: every pending job'} action={can.queues && <More href={`${base}/queues`}>Queues</More>}>
          <BarList items={pendingByQueue} color="text-chart-4" empty="Nothing pending." />
        </Panel>
        <Panel title="Failures by job" description={`Failed jobs created in ${range.label}${failedWhole ? '' : ' (newest 200)'}`} action={<More href={`${base}/jobs?state=failed`}>All failed</More>}>
          <BarList items={failuresByJob} color="text-chart-3" empty="No failures." />
        </Panel>
      </div>

      <div className="grid gap-6 xl:grid-cols-3">
        <Card className="gap-0 pb-0 xl:col-span-2">
          <CardHeader className="pb-4"><CardTitle className="text-base">Recent failures</CardTitle>
            <CardAction><More href={`${base}/jobs?state=failed`}>View all</More></CardAction></CardHeader>
          <Table data-testid="overview-failed">
            <TableHeader><TableRow className="hover:bg-transparent"><TableHead>Job</TableHead><TableHead>Queue</TableHead><TableHead>State</TableHead><TableHead>Ended</TableHead></TableRow></TableHeader>
            <TableBody>{failed.items.slice(0, 6).map((j) => (
              <TableRow key={j.jobId} className="group/row">
                <TableCell className="max-w-72 truncate font-medium"><Link href={`${base}/jobs/${j.jobId}`} className="underline-offset-4 group-hover/row:text-primary hover:underline">{j.jobName}</Link></TableCell>
                <TableCell className="text-muted-foreground">{j.queue}</TableCell><TableCell><StatusBadge state={j.state} /></TableCell>
                <TableCell className="whitespace-nowrap text-muted-foreground">{j.terminalAt ? relative(j.terminalAt) : '—'}</TableCell>
              </TableRow>
            ))}</TableBody>
          </Table>
          {failed.items.length === 0 && <p className="px-5 py-10 text-center text-sm text-muted-foreground">No failures in this range.</p>}
        </Card>
        <div className="grid content-start gap-6">
          {workers === undefined ? <Skeleton className="h-56 rounded-xl" /> : workers ? <Panel title="Workers" testId="overview-workers" description={`${workers.next ? '200+' : workers.items.length} live`} action={<More href={`${base}/workers`}>Details</More>}>
            <ul className="grid gap-2">{workers.items.slice(0, 5).map((w) => {
              const age = now - Date.parse(w.heartbeatAt), ttl = Math.max(Date.parse(w.expiresAt) - Date.parse(w.heartbeatAt), 1)
              return (
                <li key={w.instanceId} className="grid gap-1.5 rounded-lg border p-2.5 text-sm transition-colors hover:bg-muted/50">
                  <span className="flex items-center justify-between gap-2"><span className="truncate font-medium">{w.hostName} <span className="font-normal text-muted-foreground">pid {w.processId}</span></span><StatusBadge state={w.status} /></span>
                  <span className="flex items-center gap-2 text-xs text-muted-foreground">
                    <span className="h-1 flex-1 overflow-hidden rounded-full bg-muted"><span className="block h-full rounded-full bg-lime-yellow-600 transition-[width] duration-700" ref={(el) => { if (el) el.style.width = `${Math.max(0, 100 - (age / ttl) * 100)}%` }} /></span>
                    heartbeat {relative(w.heartbeatAt, now)}
                  </span>
                </li>
              )
            })}</ul>
            {workers.items.length === 0 && <p className="text-sm text-muted-foreground">No live workers.</p>}
          </Panel> : null}
          {events === undefined ? <Skeleton className="h-48 rounded-xl" /> : events ? <Panel title={pool ? `Scaling target, ${pool}` : 'Scaling target'} description={events.items[0] ? `Last change ${relative(events.items[0].createdAt, now)}` : 'No scaling events in this range'}
            action={<More href={`${base}/scaling`}>History</More>}>
            {poolEvents.length > 0 ? <Chart type="bar" height={140} label={`Worker target of pool ${pool} at each change`}
              data={poolEvents.map((e) => ({ label: new Date(e.createdAt).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }), title: `${e.reason} (${time(e.createdAt)})`, target: e.newTarget }))}
              series={[{ key: 'target', label: 'Target', color: 'text-chart-5' }]} />
              : <p className="text-sm text-muted-foreground">The autoscaler has not changed a target recently.</p>}
          </Panel> : null}
        </div>
      </div>
    </>
  )
}

const STATE_BG = { pending: 'bg-midnight-violet-400', reserved: 'bg-pine-blue-400', running: 'bg-pine-blue-600', retrying: 'bg-chocolate-300', succeeded: 'bg-lime-yellow-600', failed: 'bg-chocolate-600', cancelled: 'bg-midnight-violet-300 dark:bg-midnight-violet-600' }
