import { IconArrowRight, IconTrendingDown, IconTrendingUp } from '@tabler/icons-react'
import { Table, TableBody, TableCell, TableHeader, TableRow } from '../components/ui/table'
import { DateRangePicker } from '../components/date-range'
import { Copyable, FilterSheet, Layout, NameLink, Pager, TableCard, time, usePoll, useSort } from '../lib'


export default function Scaling({ base, events, query }) {
  const live = usePoll(['events'], 15)
  const [rows, head] = useSort(events.items)
  return (
    <Layout heading="Scaling history" live={live}>
      <TableCard toolbar={<div className="ml-auto flex items-center gap-2"><DateRangePicker query={query} /><FilterSheet values={query} fields={[{ name: 'instance', label: 'Instance' }, { name: 'pool', label: 'Pool' }]} /></div>} pager={<Pager page={events} />}
        empty={events.items.length === 0 && 'No scaling events in this range.'}>
        <Table data-testid="events">
          <TableHeader><TableRow className="hover:bg-transparent">
            {head('when', 'When', (e) => Date.parse(e.createdAt))}{head('instance', 'Instance', (e) => e.instanceId)}{head('pool', 'Pool', (e) => e.pool)}
            {head('target', 'Target', (e) => e.newTarget)}{head('desired', 'Desired', (e) => e.desired)}{head('candidate', 'Candidate', (e) => e.candidate)}
            {head('reason', 'Reason', (e) => e.reason)}
          </TableRow></TableHeader>
          <TableBody>{rows.map((e) => {
            const Trend = e.newTarget > e.oldTarget ? IconTrendingUp : e.newTarget < e.oldTarget ? IconTrendingDown : IconArrowRight
            return (
              <TableRow key={e.eventId} data-testid="event-row">
                <TableCell className="whitespace-nowrap text-muted-foreground">{time(e.createdAt)}</TableCell><TableCell className="max-w-40"><Copyable value={e.instanceId} label="Copy instance ID"><NameLink href={`${base}/workers/${e.instanceId}`} className="font-mono text-xs font-normal">{e.instanceId}</NameLink></Copyable></TableCell>
                <TableCell data-testid="event-pool"><NameLink href={`${base}/pools/${encodeURIComponent(e.pool)}`}>{e.pool}</NameLink></TableCell>
                <TableCell className="whitespace-nowrap tabular-nums"><span className="inline-flex items-center gap-1.5">{e.oldTarget}
                  <Trend aria-hidden className={`size-4 ${e.newTarget > e.oldTarget ? 'text-success' : e.newTarget < e.oldTarget ? 'text-warning' : 'text-muted-foreground'}`} />
                  <span className="sr-only">→</span>{e.newTarget}</span></TableCell>
                <TableCell className="tabular-nums">{e.desired ?? '—'}</TableCell><TableCell className="tabular-nums">{e.candidate ?? '—'}</TableCell>
                <TableCell data-testid="event-reason" className="max-w-80 break-words">{e.reason}</TableCell>
              </TableRow>
            )
          })}</TableBody>
        </Table>
      </TableCard>
    </Layout>
  )
}
