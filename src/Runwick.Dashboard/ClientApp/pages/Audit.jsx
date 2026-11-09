import { Badge } from '../components/ui/badge'
import { Table, TableBody, TableCell, TableHeader, TableRow } from '../components/ui/table'
import { DateRangePicker } from '../components/date-range'
import { Copyable, FilterSheet, Layout, Pager, TableCard, time, usePoll, useSort } from '../lib'


export default function Audit({ audit, query }) {
  const live = usePoll(['audit'], 15)
  const [rows, head] = useSort(audit.items)
  return (
    <Layout heading="Audit" live={live}>
      <TableCard toolbar={<div className="ml-auto flex items-center gap-2"><DateRangePicker query={query} /><FilterSheet values={query} fields={[{ name: 'targetKind', label: 'Target kind', options: ['job', 'queue', 'schedule', 'scope', 'rate_scope', 'application'], free: true }, { name: 'targetId', label: 'Target ID' }]} /></div>} pager={<Pager page={audit} />}
        empty={audit.items.length === 0 && 'No audit rows in this range.'}>
        <Table data-testid="audit">
          <TableHeader><TableRow className="hover:bg-transparent">
            {head('when', 'When', (a) => Date.parse(a.occurredAt))}{head('op', 'Operation', (a) => a.operation)}{head('target', 'Target', (a) => `${a.targetKind} ${a.targetId}`)}
            {head('actor', 'Actor', (a) => a.actorDisplay ?? a.actorSubject)}{head('reason', 'Reason', (a) => a.reason)}{head('rev', 'Revision', (a) => a.resultingRevision)}
          </TableRow></TableHeader>
          <TableBody>{rows.map((a) => (
            <TableRow key={a.auditId} data-testid="audit-row">
              <TableCell className="whitespace-nowrap text-muted-foreground">{time(a.occurredAt)}</TableCell>
              <TableCell><Badge variant="outline">{a.operation}</Badge></TableCell>
              <TableCell className="max-w-72 break-words"><span className="text-muted-foreground">{a.targetKind}</span> <Copyable value={a.targetId} label="Copy target ID"><code className="text-xs">{a.targetId}</code></Copyable></TableCell>
              <TableCell className="max-w-60 break-words">{a.actorDisplay ?? a.actorSubject} <span className="text-xs text-muted-foreground">({a.actorScheme})</span></TableCell>
              <TableCell data-testid="audit-reason" className="max-w-72 break-words">{a.reason ?? '—'}</TableCell>
              <TableCell className="whitespace-nowrap tabular-nums">{a.expectedRevision ?? '—'} → {a.resultingRevision ?? '—'}</TableCell>
            </TableRow>
          ))}</TableBody>
        </Table>
      </TableCard>
    </Layout>
  )
}
