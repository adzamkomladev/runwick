import { IconPlayerPause, IconPlayerPlay } from '@tabler/icons-react'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../components/ui/table'
import { CommandBar, CommandButton, Copyable, flash, Layout, Pager, StateBadge, TableCard, useChanged, useCommand, usePoll, useSort } from '../lib'

export default function Queues({ base, can, queues }) {
  const live = usePoll(['queues'], 10)
  const command = useCommand()
  const [rows, head] = useSort(queues.items)
  const changed = useChanged(queues.items, (q) => q.name, (q) => q.paused)
  return (
    <Layout heading="Queues" live={live}>
      <CommandBar command={command} />
      <TableCard pager={<Pager page={queues} />} empty={queues.items.length === 0 && 'No queues.'}>
        <Table data-testid="queues">
          <TableHeader><TableRow className="hover:bg-transparent">
            {head('name', 'Queue', (q) => q.name)}{head('state', 'State', (q) => (q.paused ? 1 : 0))}{head('revision', 'Revision', (q) => q.revision)}
            {can.queueControl && <TableHead />}
          </TableRow></TableHeader>
          <TableBody>{rows.map((q) => {
            const op = q.paused ? 'resume' : 'pause'
            const at = `${base}/queues/${encodeURIComponent(q.name)}/`, url = at + op
            return (
              <TableRow key={q.name} data-testid="queue-row" className={flash(changed[q.name])}>
                <TableCell className="font-medium"><Copyable value={q.name} label="Copy queue name" /></TableCell>
                <TableCell data-testid="queue-state"><StateBadge state={q.paused ? 'paused' : 'running'} /></TableCell>
                <TableCell className="tabular-nums">{q.revision}</TableCell>
                {can.queueControl && <TableCell className="text-right">
                  <CommandButton command={command} url={url} pair={at + (q.paused ? 'pause' : 'resume')} data-testid={`queue-${op}`} icon={q.paused ? IconPlayerPlay : IconPlayerPause}
                    onClick={() => command.ask(`${q.paused ? 'Resume' : 'Pause'} ${q.name}`, url, { expectedRevision: q.revision })}>
                    {q.paused ? 'Resume' : 'Pause'}
                  </CommandButton>
                </TableCell>}
              </TableRow>
            )
          })}</TableBody>
        </Table>
      </TableCard>
    </Layout>
  )
}
