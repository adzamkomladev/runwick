import { IconAlertTriangle, IconCircleCheck, IconClockHour4, IconHourglass, IconLoader, IconPercentage, IconPlayerPause, IconPlayerPlay } from '@tabler/icons-react'
import { DateRangePicker } from '../components/date-range'
import { modeText, poolsFrom } from '../components/host'
import { Chart, Legend } from '../components/charts'
import { duration, relative } from '../components/timeline'
import { StatCard } from '../components/stat'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../components/ui/card'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../components/ui/table'
import { CommandBar, CommandButton, Copyable, flash, Layout, NameLink, OpenButton, StateBadge, TableCard, useChanged, useCommand, usePoll } from '../lib'

/** A bucket's axis label: the time of day, with the date when the range spans more than a day. */
export const bucketLabel = (at, span) => (span > 26 * 3600_000
  ? at.toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
  : at.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }))

export default function Queue({ base, can, queue: info, stats, jobs, workers, query }) {
  const live = usePoll(['queue', ...(can.jobs ? ['stats', 'jobs'] : []), ...(can.workers ? ['workers'] : [])], 5)
  const command = useCommand()
  const name = query.name
  const items = jobs?.items ?? []
  const changed = useChanged(items, (j) => j.jobId, (j) => j.state)
  const s = stats?.states ?? {}
  const now = stats?.now ?? {}
  const pending = now.pending ?? 0, running = (now.running ?? 0) + (now.reserved ?? 0), ok = s.succeeded ?? 0, bad = s.failed ?? 0
  const rate = ok + bad ? (bad / (ok + bad)) * 100 : null
  const span = stats ? Date.parse(stats.to) - Date.parse(stats.from) : 0
  const scope = 'Created in the range'
  const pools = poolsFrom(workers ?? []).filter((p) => !p.known || p.queues.some((q) => q.queue === name))
  const at = `${base}/queues/${encodeURIComponent(name)}/`, op = info.paused ? 'resume' : 'pause'
  return (
    <Layout live={live}
      heading={<span className="flex min-w-0 items-center gap-3"><Copyable value={name} label="Copy queue name"><span data-testid="queue-name" className="truncate">{name}</span></Copyable>
        <StateBadge state={info.paused ? 'paused' : 'running'} /></span>}
      description="Exact counts of jobs created in the range; pending, running, the oldest pending and the workers are live."
      actions={<>
        <DateRangePicker query={query} fallback="1d" />
        {can.queueControl && <CommandButton command={command} url={at + op} pair={at + (info.paused ? 'pause' : 'resume')} data-testid={`queue-${op}`}
          icon={info.paused ? IconPlayerPlay : IconPlayerPause}
          onClick={() => command.ask(`${info.paused ? 'Resume' : 'Pause'} ${name}`, at + op, { expectedRevision: info.revision, view: 'detail' })}>
          {info.paused ? 'Resume' : 'Pause'}
        </CommandButton>}
      </>}>
      <CommandBar command={command} />
      {stats && <>
        <section aria-label="Queue stats" className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-6">
          <StatCard testId="stat-pending" icon={IconHourglass} label="Pending now" value={pending} foot={`${(now.scheduled ?? 0).toLocaleString()} scheduled for later`} />
          <StatCard testId="stat-running" icon={IconLoader} label="Running now" value={running} foot="Reserved or running" />
          <StatCard testId="stat-succeeded" icon={IconCircleCheck} label="Succeeded" value={ok} foot={scope} />
          <StatCard testId="stat-failed" icon={IconAlertTriangle} label="Failed" value={bad} foot={scope} />
          <StatCard testId="stat-failure-rate" icon={IconPercentage} label="Failure rate" value={rate == null ? '—' : Math.round(rate * 10) / 10} suffix={rate == null ? null : '%'}
            foot="Failed of the jobs that ended" />
          <StatCard testId="stat-oldest" icon={IconClockHour4} label="Oldest pending" value={stats.oldestPendingEligibleAt ? duration(Date.now() - Date.parse(stats.oldestPendingEligibleAt)) : 'none'}
            foot={stats.oldestPendingEligibleAt ? `Eligible ${relative(stats.oldestPendingEligibleAt)} (live)` : 'Nothing waiting (live)'} />
        </section>
        <Card className="gap-4">
          <CardHeader><CardTitle className="text-base">Throughput</CardTitle>
            <CardDescription>Created, succeeded and failed per {duration(span / Math.max(stats.buckets.length, 1))}</CardDescription></CardHeader>
          <CardContent className="grid gap-3">
            <Legend series={[{ key: 'c', label: 'Created', color: 'text-chart-1' }, { key: 's', label: 'Succeeded', color: 'text-chart-2' }, { key: 'f', label: 'Failed', color: 'text-chart-3' }]} />
            <Chart label={`Throughput of ${name}`} height={240}
              data={stats.buckets.map((b) => ({ label: bucketLabel(new Date(b.from), span), title: new Date(b.from).toLocaleString(), created: b.created, succeeded: b.succeeded, failed: b.failed }))}
              series={[{ key: 'created', label: 'Created', color: 'text-chart-1' }, { key: 'succeeded', label: 'Succeeded', color: 'text-chart-2' }, { key: 'failed', label: 'Failed', color: 'text-chart-3' }]} />
          </CardContent>
        </Card>
      </>}
      <div className="grid items-start gap-6 xl:grid-cols-[minmax(0,1fr)_26rem]">
        {jobs && <TableCard title="Recent jobs" description="The newest jobs created in the range" empty={items.length === 0 && 'No jobs in this range.'}>
          <Table data-testid="queue-jobs">
            <TableHeader><TableRow className="hover:bg-transparent"><TableHead>Job</TableHead><TableHead>State</TableHead><TableHead className="hidden md:table-cell">Created</TableHead><TableHead className="w-10" /></TableRow></TableHeader>
            <TableBody>{items.map((j) => (
              <TableRow key={j.jobId} className={flash(changed[j.jobId])}>
                <TableCell className="max-w-72 truncate"><NameLink href={`${base}/runs/${j.jobId}`}>{j.jobName}</NameLink></TableCell>
                <TableCell><StateBadge state={j.state} /></TableCell>
                <TableCell className="hidden whitespace-nowrap text-muted-foreground md:table-cell">{relative(j.createdAt)}</TableCell>
                <TableCell className="text-right"><OpenButton href={`${base}/runs/${j.jobId}`} label={`Open run of ${j.jobName}`} /></TableCell>
              </TableRow>
            ))}</TableBody>
          </Table>
        </TableCard>}
        {workers && <TableCard title="Served by" description={`${workers.length} live worker${workers.length === 1 ? ' subscribes' : 's subscribe'} to this queue`}
          empty={pools.length === 0 && 'No live worker serves this queue.'}>
          <Table data-testid="queue-pools">
            <TableHeader><TableRow className="hover:bg-transparent"><TableHead>Pool</TableHead><TableHead>Weight</TableHead><TableHead>Workers</TableHead><TableHead>Concurrency</TableHead></TableRow></TableHeader>
            <TableBody>{pools.map((p) => (
              <TableRow key={p.name}>
                <TableCell><NameLink href={`${base}/pools/${encodeURIComponent(p.name)}`}>{p.name}</NameLink></TableCell>
                <TableCell className="tabular-nums">{p.queues.find((q) => q.queue === name)?.weight ?? '—'}</TableCell>
                <TableCell className="max-w-40"><span className="flex flex-wrap gap-x-2">{p.workers.map(({ worker: w }) => (
                  <NameLink key={w.instanceId} href={`${base}/workers/${w.instanceId}`} className="font-normal">{w.hostName}</NameLink>))}</span></TableCell>
                <TableCell className="whitespace-nowrap text-xs text-muted-foreground tabular-nums">{p.inFlight} / {p.target} · {modeText(p)}</TableCell>
              </TableRow>
            ))}</TableBody>
          </Table>
        </TableCard>}
      </div>
    </Layout>
  )
}
