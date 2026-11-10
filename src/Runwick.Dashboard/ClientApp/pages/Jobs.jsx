import { IconAlertTriangle, IconCircleCheck, IconClockHour4, IconListDetails, IconPercentage, IconPlayerPlay, IconPlugConnectedX } from '@tabler/icons-react'
import { Spark } from '../components/charts'
import { DateRangePicker } from '../components/date-range'
import { Kpi, KpiStrip, successRate, sum } from '../components/kpis'
import { ms } from '../components/stat'
import { relative } from '../components/timeline'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../components/ui/table'
import { Layout, NameLink, OpenButton, StateBadge, TableCard, usePoll, useSort } from '../lib'

/** Whether a live worker has a handler for the job: true, false, or null when the caller cannot read workers. */
export const handled = (workers, name) => (workers ? workers.some((w) => w.detail?.handlers?.includes(name)) : null)

export function HandlerCell({ workers, name }) {
  const h = handled(workers, name)
  if (h == null) return <span className="text-muted-foreground">—</span>
  return h
    ? <span className="flex items-center gap-1 text-xs text-muted-foreground"><IconCircleCheck className="size-3.5 text-success" aria-hidden />live</span>
    : <span data-testid="no-handler" className="flex items-center gap-1 text-xs text-warning"><IconPlugConnectedX className="size-3.5" aria-hidden />no live handler</span>
}

export default function Jobs({ base, can, types, stats, workers, query }) {
  const live = usePoll(['stats', ...(can.workers ? ['workers'] : [])], 15)
  const groups = Object.fromEntries((stats.jobNames ?? []).map((g) => [g.key, g]))
  const rows0 = types.map((t) => ({ ...t, g: groups[t.name] }))
  const [rows, head] = useSort(rows0)
  const missing = workers ? types.filter((t) => t.registered !== false && handled(workers, t.name) === false).length : null
  const rate = successRate(stats.states)
  return (
    <Layout heading="Jobs" live={live} description="The job types this application knows: registered here, handled by a worker, seen in runs or used by a schedule."
      actions={<DateRangePicker query={query} fallback="1d" />}>
      <KpiStrip label="Job stats">
        <Kpi testId="kpi-types" icon={IconListDetails} label="Job types" value={types.length} foot={`${(stats.jobNames ?? []).filter((g) => sum(g.states) > 0).length} ran in the range`} />
        <Kpi testId="kpi-runs" icon={IconPlayerPlay} label="Runs" value={sum(stats.states)} foot="Created in the range" spark={stats.buckets.map((b) => b.created)} />
        <Kpi testId="kpi-success" icon={IconPercentage} label="Success rate" value={rate ?? '—'} suffix={rate == null ? null : '%'} foot="Of runs that ended"
          spark={stats.buckets.map((b) => b.succeeded)} color="text-chart-2" />
        <Kpi testId="kpi-failed" icon={IconAlertTriangle} label="Failed" value={stats.states.failed ?? 0} foot={`${stats.retries} retries`} spark={stats.buckets.map((b) => b.failed)} color="text-chart-3" />
        <Kpi testId="kpi-p95" icon={IconClockHour4} label="p95 duration" value={ms(stats.duration.p95Ms)} foot={`median ${ms(stats.duration.medianMs)}`} spark={stats.buckets.map((b) => b.p95Ms ?? 0)} color="text-chart-4" />
        <Kpi testId="kpi-unhandled" icon={IconPlugConnectedX} label="No live handler" value={missing ?? '—'} foot={missing ? 'Runs of these wait for a worker' : 'Every type has a worker'} />
      </KpiStrip>
      <TableCard empty={types.length === 0 && 'No job types are known yet.'}>
        <Table data-testid="job-types">
          <TableHeader><TableRow className="hover:bg-transparent">
            {head('name', 'Job', (t) => t.name)}{head('queue', 'Queue', (t) => t.queue ?? '', 'hidden lg:table-cell')}
            {head('runs', 'Runs', (t) => sum(t.g?.states))}{head('success', 'Success', (t) => successRate(t.g?.states) ?? -1, 'hidden sm:table-cell')}
            {head('failed', 'Failed', (t) => t.g?.states?.failed ?? 0)}{head('p95', 'p95', (t) => t.g?.duration?.p95Ms ?? -1, 'hidden md:table-cell')}
            {head('last', 'Last run', (t) => (t.g?.lastAt ? Date.parse(t.g.lastAt) : 0), 'hidden md:table-cell')}
            <TableHead className="hidden w-28 xl:table-cell">Trend</TableHead><TableHead className="hidden lg:table-cell">Handler</TableHead>
            <TableHead className="hidden xl:table-cell">Schedules</TableHead><TableHead className="w-10" />
          </TableRow></TableHeader>
          <TableBody>{rows.map((t) => {
            const href = `${base}/jobs/${encodeURIComponent(t.name)}`, g = t.g, r = successRate(g?.states)
            return (
              <TableRow key={t.name} data-testid="job-row">
                <TableCell className="max-w-72 truncate"><NameLink href={href} testId="job-link">{t.name}</NameLink>{!t.registered && <span className="ml-1.5 text-xs text-muted-foreground">not registered here</span>}</TableCell>
                <TableCell className="hidden text-muted-foreground lg:table-cell">{t.queue ? <NameLink href={`${base}/queues/${encodeURIComponent(t.queue)}`} className="font-normal">{t.queue}</NameLink> : '—'}</TableCell>
                <TableCell className="tabular-nums">{sum(g?.states).toLocaleString()}</TableCell>
                <TableCell className="hidden tabular-nums sm:table-cell">{r == null ? '—' : `${r} %`}</TableCell>
                <TableCell className={g?.states?.failed ? 'tabular-nums text-destructive' : 'tabular-nums text-muted-foreground'}>{(g?.states?.failed ?? 0).toLocaleString()}</TableCell>
                <TableCell className="hidden tabular-nums md:table-cell">{ms(g?.duration?.p95Ms)}</TableCell>
                <TableCell className="hidden whitespace-nowrap md:table-cell">{g?.lastAt
                  ? <span className="inline-flex items-center gap-1.5">{g.lastJobId ? <NameLink href={`${base}/runs/${g.lastJobId}`} className="font-normal text-muted-foreground">{relative(g.lastAt)}</NameLink> : relative(g.lastAt)}{g.lastState && <StateBadge state={g.lastState} />}</span>
                  : <span className="text-muted-foreground">none in range</span>}</TableCell>
                <TableCell className="hidden w-28 xl:table-cell">{g?.buckets?.length > 1 && <Spark small data={g.buckets.map((b) => b.created)} label={`Runs of ${t.name}`} color="text-chart-1" />}</TableCell>
                <TableCell className="hidden lg:table-cell"><HandlerCell workers={workers} name={t.name} /></TableCell>
                <TableCell className="hidden max-w-40 truncate xl:table-cell">{t.schedules.length === 0 ? <span className="text-muted-foreground">—</span>
                  : t.schedules.map((k) => <NameLink key={k} href={`${base}/schedules/${encodeURIComponent(k)}`} className="mr-2 font-normal">{k}</NameLink>)}</TableCell>
                <TableCell className="text-right"><OpenButton href={href} label={`Open job ${t.name}`} testId="job-open" /></TableCell>
              </TableRow>
            )
          })}</TableBody>
        </Table>
      </TableCard>
    </Layout>
  )
}
