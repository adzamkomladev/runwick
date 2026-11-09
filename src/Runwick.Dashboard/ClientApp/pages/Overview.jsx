import { Link } from '@inertiajs/react'
import { IconAlertTriangle, IconArrowRight, IconClockHour4, IconGauge, IconListDetails, IconServer2 } from '@tabler/icons-react'
import { useEffect, useRef, useState } from 'react'
import { Badge } from '../components/ui/badge'
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from '../components/ui/card'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../components/ui/table'
import { cn } from '../components/ui/utils'
import { AnimatedNumber, Layout, Sparkline, StateBadge, time, usePoll } from '../lib'

// Every figure here is a tally of one page of an existing read (at most 50 rows): "50+" when the read has a next page.
// The dashboard has no count read (16), so nothing here is a total, a depth or a rate.
const tally = (page) => (page.next ? '50+' : page.items.length)

/** Rows per time bucket, oldest first: `buckets` buckets of `minutes` each, ending now. */
function buckets(items, field, count, minutes) {
  const now = Date.now(), width = minutes * 60_000, out = Array(count).fill(0)
  for (const i of items) {
    const b = count - 1 - Math.floor((now - Date.parse(i[field])) / width)
    if (b >= 0 && b < count) out[b]++
  }
  return out
}

/** The last 30 polled values of a number, for figures that have no time field to bucket. Lives as long as the page. */
function useHistory(value) {
  const [history, setHistory] = useState(() => (typeof value === 'number' ? [value] : []))
  const last = useRef(value)
  useEffect(() => {
    if (typeof value !== 'number' || value === last.current) return
    last.current = value
    setHistory((h) => [...h.slice(-29), value])
  }, [value])
  return history
}

function Kpi({ title, value, hint, data, chart, icon: Icon, href, testId, label }) {
  return (
    <Link href={href} data-testid={testId} className="group rounded-xl outline-none focus-visible:ring-[3px] focus-visible:ring-ring/50">
      <Card className="h-full gap-3 pb-0 transition-[box-shadow,transform,border-color] duration-200 group-hover:-translate-y-0.5 group-hover:border-primary/30 group-hover:shadow-md group-active:translate-y-0">
        <CardHeader>
          <CardDescription className="flex items-center gap-1.5"><Icon className="size-4" />{title}</CardDescription>
          <CardTitle className="text-3xl font-semibold tracking-tight"><AnimatedNumber value={value} /></CardTitle>
          <CardAction><IconArrowRight className="size-4 text-muted-foreground opacity-0 transition-all duration-200 group-hover:translate-x-0.5 group-hover:opacity-100" /></CardAction>
        </CardHeader>
        <CardContent className="grid gap-2 px-0">
          <p className="px-5 text-xs text-muted-foreground">{hint}</p>
          <Sparkline data={data} className={chart} label={label} />
        </CardContent>
      </Card>
    </Link>
  )
}

const Unavailable = ({ title }) => (
  <Card className="gap-2"><CardHeader><CardDescription>{title}</CardDescription><CardTitle className="text-base text-muted-foreground">Not in your permissions</CardTitle></CardHeader></Card>
)

export default function Overview({ base, can, recent, failed, pending, queues, workers, events }) {
  const live = usePoll(['recent', 'failed', 'pending', ...(can.queues ? ['queues'] : []), ...(can.workers ? ['workers', 'events'] : [])], 10)
  const pendingHistory = useHistory(tally(pending))
  const workerHistory = useHistory(workers ? tally(workers) : null)
  const pool = events?.items[0]?.pool
  const poolEvents = events ? events.items.filter((e) => e.pool === pool).reverse() : []
  const latest = events?.items[0]
  const paused = queues?.items.filter((q) => q.paused).length ?? 0
  return (
    <Layout heading="Overview" live={live}
      description="Figures count the rows of one read, up to 50 (shown as 50+). They are not totals; queue depth is in the telemetry dashboard.">
      <section aria-label="Key figures" className="grid gap-4 sm:grid-cols-2 xl:grid-cols-5">
        <Kpi title="Created, last hour" value={tally(recent)} icon={IconListDetails} href={`${base}/jobs`} testId="kpi-created" chart="text-chart-1"
          hint="Jobs per 5 minutes" data={buckets(recent.items, 'createdAt', 12, 5)} label="Jobs created per 5 minutes over the last hour" />
        <Kpi title="Failed, last 24 h" value={tally(failed)} icon={IconAlertTriangle} href={`${base}/jobs?state=failed`} testId="kpi-failed" chart="text-chart-3"
          hint="By creation hour" data={buckets(failed.items, 'createdAt', 24, 60)} label="Failed jobs by creation hour over the last 24 hours" />
        <Kpi title="Pending" value={tally(pending)} icon={IconClockHour4} href={`${base}/jobs?state=pending`} testId="kpi-pending" chart="text-chart-4"
          hint="Since this page opened" data={pendingHistory} label="Pending jobs at each poll since this page opened" />
        {workers ? <Kpi title="Live workers" value={tally(workers)} icon={IconServer2} href={`${base}/workers`} testId="kpi-workers" chart="text-chart-2"
          hint="Since this page opened" data={workerHistory} label="Live workers at each poll since this page opened" /> : <Unavailable title="Live workers" />}
        {events ? <Kpi title={pool ? `Target, ${pool}` : 'Scaling target'} value={latest ? latest.newTarget : '—'} icon={IconGauge} href={`${base}/scaling`} testId="kpi-scaling"
          chart="text-chart-5" hint={latest ? `Last change ${time(latest.createdAt)}` : 'No scaling events in 7 days'}
          data={poolEvents.map((e) => e.newTarget)} label={`Worker target of pool ${pool ?? ''} at each scaling event`} /> : <Unavailable title="Scaling target" />}
      </section>
      <div className="grid gap-6 lg:grid-cols-3">
        <Card className="gap-0 pb-0 lg:col-span-2">
          <CardHeader className="pb-4"><CardTitle>Recent failures</CardTitle><CardDescription>Failed jobs created in the last 24 hours, newest first</CardDescription>
            <CardAction><Link href={`${base}/jobs?state=failed`} className="text-sm text-primary underline-offset-4 hover:underline">View all</Link></CardAction></CardHeader>
          <Table data-testid="overview-failed">
            <TableHeader><TableRow className="hover:bg-transparent"><TableHead>Job</TableHead><TableHead>Queue</TableHead><TableHead>Created</TableHead></TableRow></TableHeader>
            <TableBody>{failed.items.slice(0, 6).map((j) => (
              <TableRow key={j.jobId}>
                <TableCell className="max-w-72 truncate font-medium"><Link href={`${base}/jobs/${j.jobId}`} className="hover:text-primary hover:underline underline-offset-4">{j.jobName}</Link></TableCell>
                <TableCell className="text-muted-foreground">{j.queue}</TableCell><TableCell className="whitespace-nowrap text-muted-foreground">{time(j.createdAt)}</TableCell>
              </TableRow>
            ))}</TableBody>
          </Table>
          {failed.items.length === 0 && <p className="px-5 py-8 text-center text-sm text-muted-foreground">No failures in the last 24 hours.</p>}
        </Card>
        {queues ? <Card className="gap-3">
          <CardHeader><CardTitle>Queues</CardTitle><CardDescription>{paused ? `${paused} paused` : 'None paused'}{queues.next && ' (first 50 shown)'}</CardDescription>
            <CardAction><Link href={`${base}/queues`} className="text-sm text-primary underline-offset-4 hover:underline">Manage</Link></CardAction></CardHeader>
          <CardContent>
            <ul className="grid gap-1">{queues.items.slice(0, 8).map((q) => (
              <li key={q.name} className="flex items-center justify-between gap-3 rounded-md px-2 py-1.5 text-sm transition-colors hover:bg-muted/60">
                <span className="truncate">{q.name}</span><StateBadge state={q.paused ? 'paused' : 'running'} />
              </li>
            ))}</ul>
          </CardContent>
        </Card> : <Unavailable title="Queues" />}
      </div>
      {events && events.items.length > 0 && <Card className="gap-3">
        <CardHeader><CardTitle>Latest scaling events</CardTitle><CardAction><Link href={`${base}/scaling`} className="text-sm text-primary underline-offset-4 hover:underline">History</Link></CardAction></CardHeader>
        <CardContent>
          <ol className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">{events.items.slice(0, 4).map((e) => (
            <li key={e.eventId} className="grid gap-1 rounded-lg border p-3 text-sm">
              <span className="flex items-center justify-between gap-2"><span className="font-medium">{e.pool}</span>
                <Badge variant={e.newTarget > e.oldTarget ? 'success' : e.newTarget < e.oldTarget ? 'warning' : 'outline'} className="tabular-nums">{e.oldTarget} → {e.newTarget}</Badge></span>
              <span className={cn('line-clamp-2 text-muted-foreground')}>{e.reason}</span>
              <span className="text-xs text-muted-foreground">{time(e.createdAt)}</span>
            </li>
          ))}</ol>
        </CardContent>
      </Card>}
    </Layout>
  )
}
