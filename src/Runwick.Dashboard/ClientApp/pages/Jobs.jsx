import { Link } from '@inertiajs/react'
import { IconBan } from '@tabler/icons-react'
import { useState } from 'react'
import { Checkbox } from '../components/ui/checkbox'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../components/ui/table'
import { cn } from '../components/ui/utils'
import { CommandBar, CommandButton, Filters, Layout, Pager, StateBadge, TableCard, time, useChanged, useCommand, usePoll, useSort } from '../lib'

const STATES = ['pending', 'reserved', 'running', 'retrying', 'succeeded', 'failed', 'cancelled']
const FIELDS = [['queue', 'Queue'], ['state', 'State', STATES], ['jobName', 'Job name'], ['from', 'Created from'], ['to', 'Created to']]

export default function Jobs({ base, can, jobs, query }) {
  const live = usePoll(['jobs'], 5)
  const command = useCommand()
  const [selected, setSelected] = useState({})
  const [rows, head] = useSort(jobs.items)
  const changed = useChanged(jobs.items, (j) => j.jobId, (j) => j.state + j.cancelRequested)
  const chosen = jobs.items.filter((j) => selected[j.jobId])
  const toggle = (id) => setSelected((s) => ({ ...s, [id]: !s[id] }))
  const all = jobs.items.length > 0 && chosen.length === jobs.items.length
  const toggleAll = () => setSelected(all ? {} : Object.fromEntries(jobs.items.map((j) => [j.jobId, true])))
  const bulkUrl = `${base}/jobs:cancel`
  const cancelSelected = () => command.bulk(`Cancel ${chosen.length} job(s)`, bulkUrl,
    chosen.map((j) => ({ jobId: j.jobId, expectedGeneration: j.generation, expectedRevision: j.revision })))
  return (
    <Layout heading="Jobs" description="Newest first. Lists show what one read returned; there are no totals." live={live}
      actions={can.cancel && <CommandButton command={command} url={bulkUrl} data-testid="bulk-cancel" icon={IconBan}
        disabled={chosen.length === 0 || chosen.length > 100} onClick={cancelSelected}>
        Cancel selected ({chosen.length})
      </CommandButton>}>
      <CommandBar command={command} />
      <TableCard toolbar={<Filters fields={FIELDS} values={query} />} pager={<Pager page={jobs} />}
        empty={jobs.items.length === 0 && 'No jobs in this range.'}>
        <Table data-testid="jobs">
          <TableHeader>
            <TableRow className="hover:bg-transparent">
              {can.cancel && <TableHead className="w-10"><Checkbox aria-label="select all" checked={all} onCheckedChange={toggleAll} /></TableHead>}
              {head('job', 'Job', (j) => j.jobName)}
              {head('queue', 'Queue', (j) => j.queue)}
              {head('state', 'State', (j) => j.state)}
              {head('created', 'Created', (j) => Date.parse(j.createdAt))}
              {head('revision', 'Revision', (j) => j.revision, 'text-right')}
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((j) => (
              <TableRow key={`${j.jobId}:${changed[j.jobId] ?? 0}`} data-testid="job-row" data-state={selected[j.jobId] ? 'selected' : undefined}
                className={cn(changed[j.jobId] && 'animate-flash')}>
                {can.cancel && <TableCell><Checkbox aria-label="select" checked={!!selected[j.jobId]} onCheckedChange={() => toggle(j.jobId)} /></TableCell>}
                <TableCell className="max-w-80 truncate font-medium">
                  <Link href={`${base}/jobs/${j.jobId}`} data-testid="job-link" className="rounded-sm underline-offset-4 outline-none hover:text-primary hover:underline focus-visible:ring-[3px] focus-visible:ring-ring/50">{j.jobName}</Link>
                </TableCell>
                <TableCell data-testid="job-queue" className="text-muted-foreground">{j.queue}</TableCell>
                <TableCell data-testid="job-state"><StateBadge state={j.state} />{j.cancelRequested && <span className="ml-1.5 text-xs text-warning"> (cancel requested)</span>}</TableCell>
                <TableCell className="whitespace-nowrap text-muted-foreground">{time(j.createdAt)}</TableCell>
                <TableCell className="text-right tabular-nums">{j.revision}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableCard>
    </Layout>
  )
}
