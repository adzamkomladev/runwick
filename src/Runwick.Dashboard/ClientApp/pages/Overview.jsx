import { Link, usePage } from '@inertiajs/react'
import { DateRangePicker, presetMs } from '../components/date-range'
import {
  IconAlertTriangle, IconArrowDownRight, IconArrowRight, IconArrowUpRight, IconCircleCheck, IconListDetails, IconLoader, IconMinus,
} from '@tabler/icons-react'
import { useEffect, useRef, useState } from 'react'
import { Bar, BarList, Chart, Legend, Spark } from '../components/charts'
import { StatusBadge, StatusDot } from '../components/status'
import { relative } from '../components/timeline'
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from '../components/ui/card'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../components/ui/table'
import { Tooltip, TooltipContent, TooltipTrigger } from '../components/ui/tooltip'
import { cn } from '../components/ui/utils'
import { AnimatedNumber, Layout, NameLink, OpenButton, SkeletonScope, standIn, time, usePoll } from '../lib'

// Every figure is exact (16 GetJobStats over the range in the picker, default the last 24 hours; by queue and by job name). The
// delta compares the range's second half with its first. "In flight now" and "Pending by queue" are live and say so.

const HOUR = 3_600_000
const BUCKETS = 24

/** The chosen range from the URL: a preset, a from/to pair, or the default of the last 24 hours (what the picker shows). */
function useRange(now) {
  const q = Object.fromEntries(new URL(usePage().url, window.location.href).searchParams)
  const custom = !q.range && (q.from || q.to)
  const from = q.range ? now - presetMs(q.range) : custom && q.from ? Date.parse(q.from) : now - 24 * HOUR
  const to = custom && q.to ? Date.parse(q.to) : now
  const fmt = (t) => new Date(t).toLocaleString([], { dateStyle: 'medium', timeStyle: 'short' })
  return { query: q, from, to, label: custom ? `${fmt(from)} to ${q.to ? fmt(to) : 'now'}` : `the last ${fmtSpan(to - from)}` }
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

function Kpi({ title, value, more, suffix, delta, foot, spark, color, href, testId, icon: Icon }) {
  return (
    <Link href={href} data-testid={testId} className="group lift rounded-xl outline-none focus-visible:ring-[3px] focus-visible:ring-ring/50">
      <Card className="relative h-full gap-2 overflow-hidden pb-0">
        <CardHeader>
          <CardDescription className="flex items-center gap-1.5"><Icon className="size-4" aria-hidden />{title}</CardDescription>
          <CardAction><IconArrowRight className="size-4 -translate-x-1 text-muted-foreground opacity-0 transition-[translate,opacity] duration-200 ease-glide group-hover:translate-x-0 group-hover:opacity-100" aria-hidden /></CardAction>
          <CardTitle className="flex items-baseline gap-1 text-3xl font-semibold tracking-tight"><AnimatedNumber value={value} more={more} />{suffix && <span className="text-lg text-muted-foreground">{suffix}</span>}</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-1 px-5 text-xs text-muted-foreground">
          <div className="flex flex-wrap items-center gap-2">{delta}</div>
          <span>{foot}</span>
        </CardContent>
        <div className="mt-auto -mb-px">{spark}</div>
        <span aria-hidden className={cn('absolute inset-x-0 top-0 h-0.5 origin-left scale-x-0 bg-current transition-transform duration-700 ease-settle group-hover:scale-x-100', color)} />
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

/** The largest values of a map or of groups, as BarList items. */
const top = (pairs, n = 6) => pairs.filter(([, v]) => v > 0).sort((a, b) => b[1] - a[1]).slice(0, n).map(([label, value]) => ({ label, value }))

export default function Overview(props) {
  const { can, stats, failed } = props
  const live = usePoll(['stats', 'failed', ...(can.queues ? ['queues'] : []), ...(can.workers ? ['workers', 'events'] : [])], 10)
  const now = Date.now()
  const range = useRange(now)
  const ready = [stats, failed].every((p) => p !== undefined)
  // Until the deferred props arrive, the same body is drawn as a skeleton from stand-in props (21 §4.3).
  const stand = useStandIn(props)
  return (
    <Layout heading="Overview" live={ready ? live : null} actions={<DateRangePicker query={range.query} fallback="1d" />}>
      {ready ? <div className="grid min-w-0 animate-page-in gap-6"><OverviewBody {...props} now={now} range={range} /></div>
        : <SkeletonScope className="grid min-w-0 gap-6"><OverviewBody {...stand} now={now} range={range} /></SkeletonScope>}
    </Layout>
  )
}

/** The page's props with each missing (still deferred) one filled from its stand-in. */
function useStandIn(props) {
  const stand = standIn('Overview', {})
  return Object.fromEntries(Object.entries({ ...stand, ...props }).map(([k, v]) => [k, v === undefined ? stand[k] : v]))
}

function OverviewBody({ base, can, stats, failed, queues, workers, events, now, range }) {
  const span = { from: Date.parse(stats.from), to: Date.parse(stats.to) }
  const n = stats.buckets.length || BUCKETS
  const labelAt = (i) => new Date(span.from + ((i + 0.5) * (span.to - span.from)) / n)
  const created = stats.buckets.map((b) => b.created)
  const ok = stats.buckets.map((b) => b.succeeded), bad = stats.buckets.map((b) => b.failed)
  const finished = stats.buckets.map((b) => b.succeeded + b.failed + (b.cancelled ?? 0))
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
  const firstHalf = (a) => sum(a.slice(0, n / 2)), secondHalf = (a) => sum(a.slice(n / 2))
  const rate = (s, f) => (s + f ? (s / (s + f)) * 100 : null)
  const rateNow = rate(secondHalf(ok), secondHalf(bad)), rateBefore = rate(firstHalf(ok), firstHalf(bad))
  const pendingNow = stats.now?.pending ?? 0, runningNow = (stats.now?.running ?? 0) + (stats.now?.reserved ?? 0)
  const inFlight = pendingNow + runningNow
  const flightHistory = useHistory(inFlight)
  const states = top(Object.entries(stats.states), 8)
  const total = Object.values(stats.states).reduce((t, v) => t + v, 0)
  const pendingByQueue = top((stats.queues ?? []).map((g) => [g.key, g.now?.pending ?? 0]))
  const failuresByJob = top((stats.jobNames ?? []).map((g) => [g.key, g.states?.failed ?? 0]))
  const failedTotal = stats.states.failed ?? 0
  const note = `${total.toLocaleString()} jobs created in ${range.label}`
  const ended = sum(ok) + sum(bad), rateAll = rate(sum(ok), sum(bad))
  return (
    <>
      <section aria-label="Key figures" className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <Kpi testId="kpi-created" icon={IconListDetails} title="Jobs created" href={`${base}/runs`} color="text-chart-1"
          value={total}
          delta={<><Delta now={secondHalf(created)} before={firstHalf(created)} /><span>second half vs first</span></>}
          foot={`Created in ${range.label}`} spark={<Spark data={created} color="text-chart-1" label={`Jobs created in ${range.label}`} />} />
        <Kpi testId="kpi-success" icon={IconCircleCheck} title="Success rate" href={`${base}/runs?state=succeeded`} color="text-chart-2"
          value={rateAll == null ? '—' : Math.round(rateAll)} suffix={rateAll == null ? null : '%'}
          delta={<><Delta now={rateNow ?? 0} before={rateNow == null ? null : rateBefore} unit="pt" /><span>second half vs first</span></>}
          foot={`Of ${ended.toLocaleString()} jobs that ended in ${range.label}`} spark={<Spark data={ok.map((s, i) => (s + bad[i] ? (s / (s + bad[i])) * 100 : 100))} color="text-chart-2" label={`Success rate in ${range.label}`} />} />
        <Kpi testId="kpi-failed" icon={IconAlertTriangle} title="Failed" href={`${base}/runs?state=failed`} color="text-chart-3"
          value={failedTotal}
          delta={<><Delta now={secondHalf(bad)} before={firstHalf(bad)} invert /><span>second half vs first</span></>}
          foot={`Created in ${range.label}, failed now`} spark={<Spark data={bad} color="text-chart-3" label={`Failed jobs created in ${range.label}`} />} />
        <Kpi testId="kpi-inflight" icon={IconLoader} title="In flight now" href={`${base}/runs?state=running`} color="text-chart-4"
          value={inFlight}
          delta={<><StatusBadge state="pending"><AnimatedNumber value={pendingNow} /> pending</StatusBadge><StatusBadge state="running"><AnimatedNumber value={runningNow} /> running</StatusBadge></>}
          foot="Live snapshot, not limited to the range" spark={<Spark data={flightHistory} color="text-chart-4" label="Jobs in flight at each poll" />} />
      </section>

      <div className="grid grid-cols-1 gap-6 xl:grid-cols-3">
        <Panel className="xl:col-span-2" title="Throughput" testId="chart-throughput"
          description={`Per ${fmtSpan((span.to - span.from) / n)} · ${note}`}
          action={<Legend series={[{ key: 'c', label: 'Created', color: 'text-chart-1' }, { key: 'f', label: 'Finished', color: 'text-chart-2' }]} />}>
          <Chart data={series} label="Jobs created and finished over time" height={260}
            series={[{ key: 'created', label: 'Created', color: 'text-chart-1' }, { key: 'finished', label: 'Finished', color: 'text-chart-2' }]} />
        </Panel>
        <Panel title="State of jobs" description={`Created in ${range.label}`}>
          <div className="grid gap-4">
            <div className="relative h-3 w-full overflow-hidden rounded-full bg-muted" role="img" aria-label="Share of jobs per state">
              {/* Each segment is a full-width bar moved and scaled on x (transform only), so a poll glides it to its new share. */}
              {states.map((s, i) => {
                const all = Math.max(total, 1), from = states.slice(0, i).reduce((t, x) => t + x.value, 0) / all
                return <Tooltip key={s.label}><TooltipTrigger asChild>
                  <span className={cn('absolute inset-y-0 left-0 w-full origin-left animate-grow-x glide-slow hover:opacity-85', STATE_BG[s.label] ?? 'bg-midnight-violet-400')}
                    ref={(el) => { if (el) el.style.transform = `translateX(${from * 100}%) scaleX(${s.value / all})` }} />
                </TooltipTrigger><TooltipContent>{s.label}: {s.value}</TooltipContent></Tooltip>
              })}
            </div>
            <ul className="grid gap-2">{states.map((s) => (
              <li key={s.label} className="tint flex items-center justify-between gap-3 rounded-md px-1 py-1 text-sm">
                <span className="inline-flex items-center gap-2"><StatusDot state={s.label} />{s.label}</span>
                <span className="font-medium tabular-nums"><AnimatedNumber value={s.value} /> <span className="text-xs font-normal text-muted-foreground">({Math.round((s.value / Math.max(total, 1)) * 100)}%)</span></span>
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

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-2 xl:grid-cols-3">
        <Panel title="Succeeded and failed" testId="chart-outcomes" description={`By end time · ${(stats?.buckets ?? []).reduce((n, b) => n + b.succeeded + b.failed, 0).toLocaleString()} jobs ended in ${range.label}`}
          action={<Legend series={[{ key: 's', label: 'Succeeded', color: 'text-chart-2' }, { key: 'f', label: 'Failed', color: 'text-chart-3' }]} />}>
          <Chart type="bar" stacked data={series} height={200} label="Succeeded and failed jobs over time"
            series={[{ key: 'succeeded', label: 'Succeeded', color: 'text-chart-2' }, { key: 'failed', label: 'Failed', color: 'text-chart-3' }]} />
        </Panel>
        <Panel title="Pending by queue" description="Live: every pending job" action={can.queues && <More href={`${base}/queues`}>Queues</More>}>
          <BarList items={pendingByQueue} color="text-chart-4" empty="Nothing pending." />
        </Panel>
        <Panel title="Failures by job" description={`Failed jobs created in ${range.label}`} action={<More href={`${base}/runs?state=failed`}>All failed</More>}>
          <BarList items={failuresByJob} color="text-chart-3" empty="No failures." />
        </Panel>
      </div>

      <div className="grid grid-cols-1 gap-6 xl:grid-cols-3">
        <Card className="gap-0 pb-0 xl:col-span-2">
          <CardHeader className="pb-4"><CardTitle className="text-base">Recent failures</CardTitle>
            <CardAction><More href={`${base}/runs?state=failed`}>View all</More></CardAction></CardHeader>
          <Table data-testid="overview-failed">
            <TableHeader><TableRow className="hover:bg-transparent"><TableHead>Job</TableHead><TableHead>Queue</TableHead><TableHead>State</TableHead><TableHead>Ended</TableHead><TableHead className="w-10" /></TableRow></TableHeader>
            <TableBody>{failed.items.slice(0, 6).map((j) => (
              <TableRow key={j.jobId}>
                <TableCell className="max-w-72 truncate"><NameLink href={`${base}/runs/${j.jobId}`}>{j.jobName}</NameLink></TableCell>
                <TableCell className="text-muted-foreground">{j.queue}</TableCell><TableCell><StatusBadge state={j.state} /></TableCell>
                <TableCell className="whitespace-nowrap text-muted-foreground">{j.terminalAt ? relative(j.terminalAt) : '—'}</TableCell>
                <TableCell className="text-right"><OpenButton href={`${base}/runs/${j.jobId}`} label={`Open run of ${j.jobName}`} /></TableCell>
              </TableRow>
            ))}</TableBody>
          </Table>
          {failed.items.length === 0 && <p className="px-5 py-10 text-center text-sm text-muted-foreground">No failures in this range.</p>}
        </Card>
        <div className="grid content-start gap-6">
          {workers === undefined ? <SkeletonScope><WorkersPanel base={base} now={now} workers={standIn('Overview', {}).workers} /></SkeletonScope>
            : workers && <div className="animate-page-in"><WorkersPanel base={base} now={now} workers={workers} /></div>}
          {events === undefined ? <SkeletonScope><ScalingPanel base={base} now={now} events={standIn('Overview', {}).events} /></SkeletonScope>
            : events && <div className="animate-page-in"><ScalingPanel base={base} now={now} events={events} /></div>}
        </div>
      </div>
    </>
  )
}

function WorkersPanel({ base, now, workers }) {
  return (
          <Panel title="Workers" testId="overview-workers" description={`${workers.length} live`} action={<More href={`${base}/workers`}>Details</More>}>
            <ul className="grid gap-2">{workers.slice(0, 5).map((w) => {
              const age = now - Date.parse(w.heartbeatAt), ttl = Math.max(Date.parse(w.expiresAt) - Date.parse(w.heartbeatAt), 1)
              return (
                <li key={w.instanceId} className="tint grid gap-1.5 rounded-lg border p-2.5 text-sm">
                  <span className="flex items-center justify-between gap-2"><span className="truncate font-medium">{w.hostName} <span className="font-normal text-muted-foreground">pid {w.processId}</span></span><StatusBadge state={w.status} /></span>
                  <span className="flex items-center gap-2 text-xs text-muted-foreground">
                    <span className="relative h-1 flex-1 overflow-hidden rounded-full bg-muted"><Bar fraction={Math.max(0, 1 - age / ttl)} className="rounded-full bg-lime-yellow-600" /></span>
                    heartbeat {relative(w.heartbeatAt, now)}
                  </span>
                </li>
              )
            })}</ul>
            {workers.length === 0 && <p className="text-sm text-muted-foreground">No live workers.</p>}
          </Panel>
  )
}

function ScalingPanel({ base, now, events }) {
  const pool = events.items[0]?.pool
  const poolEvents = events.items.filter((e) => e.pool === pool).slice(0, 24).reverse()
  return (
          <Panel title={pool ? `Scaling target, ${pool}` : 'Scaling target'} description={events.items[0] ? `Last change ${relative(events.items[0].createdAt, now)}` : 'No scaling events in this range'}
            action={<More href={`${base}/scaling`}>History</More>}>
            {poolEvents.length > 0 ? <Chart type="bar" height={140} label={`Worker target of pool ${pool} at each change`}
              data={poolEvents.map((e) => ({ label: new Date(e.createdAt).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }), title: `${e.reason} (${time(e.createdAt)})`, target: e.newTarget }))}
              series={[{ key: 'target', label: 'Target', color: 'text-chart-5' }]} />
              : <p className="text-sm text-muted-foreground">The autoscaler has not changed a target recently.</p>}
          </Panel>
  )
}

const STATE_BG = { pending: 'bg-midnight-violet-400', reserved: 'bg-pine-blue-400', running: 'bg-pine-blue-600', retrying: 'bg-chocolate-300', succeeded: 'bg-lime-yellow-600', failed: 'bg-chocolate-600', cancelled: 'bg-midnight-violet-300 dark:bg-midnight-violet-600' }
