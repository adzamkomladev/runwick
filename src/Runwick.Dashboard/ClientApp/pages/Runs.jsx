import { Link, router, usePage } from '@inertiajs/react'
import { IconAlertTriangle, IconBan, IconChevronDown, IconClockHour4, IconGauge, IconListDetails, IconLoader2, IconPercentage, IconRepeat } from '@tabler/icons-react'
import { useEffect, useState } from 'react'
import { DateRangePicker } from '../components/date-range'
import { Kpi, KpiStrip, perMinute, successRate, sum } from '../components/kpis'
import { ms } from '../components/stat'
import { Button } from '../components/ui/button'
import { StatusDot } from '../components/status'
import { relative } from '../components/timeline'
import { Checkbox } from '../components/ui/checkbox'
import { Tooltip, TooltipContent, TooltipTrigger } from '../components/ui/tooltip'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../components/ui/table'
import { cn } from '../components/ui/utils'
import { CommandBar, CommandButton, Copyable, FilterSheet, flash, Layout, NameLink, OpenButton, StateBadge, TableCard, time, useChanged, useCommand, usePoll, useSort } from '../lib'

const STATES = ['pending', 'reserved', 'running', 'retrying', 'succeeded', 'failed', 'cancelled']

/** Quick state filters: links that keep the other filters. */
/** The Runs header: exact figures over the range and filters in the URL (16 GetJobStats). */
export function RunStats({ stats }) {
  const rate = successRate(stats.states), spark = (k) => stats.buckets.map((b) => b[k] ?? 0)
  return (
    <KpiStrip label="Run stats">
      <Kpi testId="kpi-total" icon={IconListDetails} label="Runs" value={sum(stats.states)} foot="Created in the range" spark={spark('created')} />
      <Kpi testId="kpi-success" icon={IconPercentage} label="Success rate" value={rate ?? '—'} suffix={rate == null ? null : '%'} foot="Of runs that ended" spark={spark('succeeded')} color="text-chart-2" />
      <Kpi testId="kpi-failed" icon={IconAlertTriangle} label="Failed" value={stats.states.failed ?? 0} foot={`${(stats.states.cancelled ?? 0).toLocaleString()} cancelled`} spark={spark('failed')} color="text-chart-3" />
      <Kpi testId="kpi-retries" icon={IconRepeat} label="Retries" value={stats.retries} foot="Extra executions" />
      <Kpi testId="kpi-duration" icon={IconClockHour4} label="Duration p95" value={ms(stats.duration.p95Ms)} foot={`median ${ms(stats.duration.medianMs)}`} spark={spark('p95Ms')} color="text-chart-4" />
      <Kpi testId="kpi-throughput" icon={IconGauge} label="Throughput" value={perMinute(stats)} suffix="/min" foot={`${(stats.now?.pending ?? 0).toLocaleString()} pending now`} />
    </KpiStrip>
  )
}

export function StateChips({ query }) {
  const { url } = usePage()
  const href = (state) => {
    const u = new URL(url, window.location.href)
    u.searchParams.delete('cursor')
    if (state) u.searchParams.set('state', state); else u.searchParams.delete('state')
    return u.pathname + u.search
  }
  return (
    <nav aria-label="Filter by state" className="flex flex-wrap gap-1.5">
      {[null, ...STATES].map((st) => {
        const on = (query.state ?? null) === st
        return (
          <Link key={st ?? 'all'} href={href(st)} aria-current={on ? 'true' : undefined}
            className={cn('inline-flex h-7 items-center gap-1.5 rounded-full border px-3 text-xs font-medium press outline-none focus-visible:ring-[3px] focus-visible:ring-ring/50',
              on ? 'border-primary bg-primary text-primary-foreground shadow-sm' : 'bg-card text-muted-foreground hover:border-foreground/20 hover:text-foreground')}>
            {st && <StatusDot state={st} />}{st ?? 'All'}
          </Link>
        )
      })}
    </nav>
  )
}

/**
 * "Load more": a partial reload of the merge prop `jobs` with the next cursor; Inertia appends the rows (matched on the job ID)
 * and the URL stays the first page. Polling pauses while more than the first page is shown, as it did for a cursor in the URL.
 */
function useLoadMore(jobs) {
  const { url } = usePage()
  const [state, setState] = useState({ url, more: false, loading: false })
  useEffect(() => setState({ url, more: false, loading: false }), [url])
  const load = () => {
    setState((x) => ({ ...x, loading: true }))
    router.reload({ only: ['jobs'], data: { cursor: jobs.next }, preserveUrl: true, preserveScroll: true,
      onFinish: () => setState((x) => ({ ...x, more: true, loading: false })) })
  }
  return { more: state.url === url && state.more, loading: state.loading, load }
}

export default function Runs({ base, can, jobs, stats, query, queueNames }) {
  const more = useLoadMore(jobs)
  const live = usePoll(['jobs', 'stats'], more.more ? 0 : 5)
  const command = useCommand()
  const [selected, setSelected] = useState({})
  const [rows, head] = useSort(jobs.items)
  const changed = useChanged(jobs.items, (j) => j.jobId, (j) => j.state + j.cancelRequested)
  const chosen = jobs.items.filter((j) => selected[j.jobId])
  const toggle = (id) => setSelected((s) => ({ ...s, [id]: !s[id] }))
  const all = jobs.items.length > 0 && chosen.length === jobs.items.length
  const toggleAll = () => setSelected(all ? {} : Object.fromEntries(jobs.items.map((j) => [j.jobId, true])))
  const bulkUrl = `${base}/runs:cancel`
  const cancelSelected = () => command.bulk(`Cancel ${chosen.length} job(s)`, bulkUrl,
    chosen.map((j) => ({ jobId: j.jobId, expectedGeneration: j.generation, expectedRevision: j.revision })))
  return (
    <Layout heading="Runs" live={live}
      actions={can.cancel && <CommandButton command={command} url={bulkUrl} data-testid="bulk-cancel" icon={IconBan}
        disabled={chosen.length === 0 || chosen.length > 100} onClick={cancelSelected}>
        Cancel selected ({chosen.length})
      </CommandButton>}>
      <CommandBar command={command} />
      <RunStats stats={stats} />
      <div className="flex flex-wrap items-center justify-between gap-3">
        <StateChips query={query} />
        <div className="flex items-center gap-2">
          <DateRangePicker query={query} />
          <FilterSheet values={query} fields={[{ name: 'queue', label: 'Queue', options: queueNames ?? [], free: true }, { name: 'jobName', label: 'Job name' }]} />
        </div>
      </div>
      <TableCard pager={jobs.next && <div className="flex justify-center border-t pt-3">
        <Button variant="outline" size="sm" data-testid="load-more" onClick={more.load} disabled={more.loading}>
          {more.loading ? <IconLoader2 className="animate-spin" /> : <IconChevronDown />}Load more
        </Button>
      </div>}
        empty={jobs.items.length === 0 && 'No jobs in this range.'}>
        <Table data-testid="jobs">
          <TableHeader>
            <TableRow className="hover:bg-transparent">
              {can.cancel && <TableHead className="w-10"><Checkbox aria-label="select all" checked={all} onCheckedChange={toggleAll} /></TableHead>}
              {head('job', 'Job', (j) => j.jobName)}
              {head('queue', 'Queue', (j) => j.queue, 'hidden sm:table-cell')}
              {head('state', 'State', (j) => j.state)}
              {head('created', 'Created', (j) => Date.parse(j.createdAt), 'hidden md:table-cell')}
              {head('revision', 'Revision', (j) => j.revision, 'hidden text-right md:table-cell')}
              <TableHead className="w-10" />
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((j) => (
              <TableRow key={j.jobId} data-testid="run-row" data-state={selected[j.jobId] ? 'selected' : undefined}
                className={flash(changed[j.jobId])}>
                {can.cancel && <TableCell><Checkbox aria-label="select" checked={!!selected[j.jobId]} onCheckedChange={() => toggle(j.jobId)} /></TableCell>}
                <TableCell className="max-w-80 font-medium">
                  <Copyable value={j.jobName} label="Copy job name">
                    <NameLink href={`${base}/runs/${j.jobId}`} testId="run-link">{j.jobName}</NameLink>
                  </Copyable>
                </TableCell>
                <TableCell data-testid="run-queue" className="hidden text-muted-foreground sm:table-cell"><Copyable value={j.queue} label="Copy queue name" /></TableCell>
                <TableCell data-testid="run-state"><StateBadge state={j.state} />{j.cancelRequested && <span className="ml-1.5 text-xs text-warning">cancel requested</span>}</TableCell>
                <TableCell className="hidden whitespace-nowrap text-muted-foreground md:table-cell">
                  <Tooltip><TooltipTrigger asChild><time dateTime={j.createdAt} className="cursor-default">{relative(j.createdAt)}</time></TooltipTrigger><TooltipContent>{time(j.createdAt)}</TooltipContent></Tooltip>
                </TableCell>
                <TableCell className="hidden text-right tabular-nums md:table-cell">{j.revision}</TableCell>
                <TableCell className="text-right"><OpenButton href={`${base}/runs/${j.jobId}`} label={`Open run of ${j.jobName}`} testId="run-open" /></TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableCard>
    </Layout>
  )
}
