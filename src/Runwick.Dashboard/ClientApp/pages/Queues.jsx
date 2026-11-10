import { IconAlertTriangle, IconClockHour4, IconHourglass, IconPlayerPause, IconPlayerPlay, IconStack2 } from '@tabler/icons-react'
import { DateRangePicker } from '../components/date-range'
import { Kpi, KpiStrip, sum } from '../components/kpis'
import { duration } from '../components/timeline'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../components/ui/table'
import { CommandBar, CommandButton, Copyable, flash, Layout, NameLink, OpenButton, Pager, StateBadge, TableCard, useChanged, useCommand, usePoll, useSort } from '../lib'

export default function Queues({ base, can, queues, counts, stats, query }) {
  const live = usePoll(['queues', 'counts', ...(can.jobs ? ['stats'] : [])], 10)
  const groups = Object.fromEntries((stats?.queues ?? []).map((g) => [g.key, g]))
  const command = useCommand()
  const [rows, head] = useSort(queues.items)
  const changed = useChanged(queues.items, (q) => q.name, (q) => q.paused)
  return (
    <Layout heading="Queues" live={live} actions={stats && <DateRangePicker query={query} fallback="1d" />}>
      <CommandBar command={command} />
      <KpiStrip label="Queue stats">
        <Kpi testId="kpi-queues" icon={IconStack2} label="Queues" value={counts.total} foot={`${counts.total - counts.paused} running · ${counts.paused} paused`} />
        {stats && <>
          <Kpi testId="kpi-pending" icon={IconHourglass} label="Pending now" value={stats.now?.pending ?? 0} foot={`${(stats.now?.scheduled ?? 0).toLocaleString()} scheduled for later`} />
          <Kpi testId="kpi-oldest" icon={IconClockHour4} label="Oldest pending" value={stats.oldestPendingEligibleAt ? duration(Date.now() - Date.parse(stats.oldestPendingEligibleAt)) : 'none'} foot="Waiting since eligible" />
          <Kpi testId="kpi-failed" icon={IconAlertTriangle} label="Failed" value={stats.states.failed ?? 0} foot="Runs created in the range" spark={stats.buckets.map((b) => b.failed)} color="text-chart-3" />
        </>}
      </KpiStrip>
      <TableCard pager={<Pager page={queues} />} empty={queues.items.length === 0 && 'No queues.'}>
        <Table data-testid="queues">
          <TableHeader><TableRow className="hover:bg-transparent">
            {head('name', 'Queue', (q) => q.name)}{head('state', 'State', (q) => (q.paused ? 1 : 0))}{stats && head('pending', 'Pending', (q) => groups[q.name]?.now?.pending ?? 0)}{stats && head('runs', 'Runs in range', (q) => sum(groups[q.name]?.states), 'hidden md:table-cell')}{head('revision', 'Revision', (q) => q.revision, 'hidden sm:table-cell')}
            {can.queueControl && <TableHead />}<TableHead className="w-10" />
          </TableRow></TableHeader>
          <TableBody>{rows.map((q) => {
            const op = q.paused ? 'resume' : 'pause'
            const at = `${base}/queues/${encodeURIComponent(q.name)}/`, url = at + op
            return (
              <TableRow key={q.name} data-testid="queue-row" className={flash(changed[q.name])}>
                <TableCell><Copyable value={q.name} label="Copy queue name"><NameLink href={`${base}/queues/${encodeURIComponent(q.name)}`} testId="queue-link">{q.name}</NameLink></Copyable></TableCell>
                <TableCell data-testid="queue-state"><StateBadge state={q.paused ? 'paused' : 'running'} /></TableCell>
                {stats && <TableCell className="tabular-nums">{(groups[q.name]?.now?.pending ?? 0).toLocaleString()}</TableCell>}{stats && <TableCell className="hidden tabular-nums md:table-cell">{sum(groups[q.name]?.states).toLocaleString()}</TableCell>}<TableCell className="hidden tabular-nums sm:table-cell">{q.revision}</TableCell>
                {can.queueControl && <TableCell className="text-right">
                  <CommandButton command={command} url={url} pair={at + (q.paused ? 'pause' : 'resume')} data-testid={`queue-${op}`} icon={q.paused ? IconPlayerPlay : IconPlayerPause}
                    onClick={() => command.ask(`${q.paused ? 'Resume' : 'Pause'} ${q.name}`, url, { expectedRevision: q.revision })}>
                    {q.paused ? 'Resume' : 'Pause'}
                  </CommandButton>
                </TableCell>}
                <TableCell className="text-right"><OpenButton href={`${base}/queues/${encodeURIComponent(q.name)}`} label={`Open queue ${q.name}`} testId="queue-open" /></TableCell>
              </TableRow>
            )
          })}</TableBody>
        </Table>
      </TableCard>
    </Layout>
  )
}
