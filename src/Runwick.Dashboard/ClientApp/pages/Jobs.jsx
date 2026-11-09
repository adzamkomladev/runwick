import { Link, router, usePage } from '@inertiajs/react'
import { IconBan, IconChevronDown, IconLoader2 } from '@tabler/icons-react'
import { useEffect, useState } from 'react'
import { DateRangePicker } from '../components/date-range'
import { Button } from '../components/ui/button'
import { StatusDot } from '../components/status'
import { relative } from '../components/timeline'
import { Checkbox } from '../components/ui/checkbox'
import { Tooltip, TooltipContent, TooltipTrigger } from '../components/ui/tooltip'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../components/ui/table'
import { cn } from '../components/ui/utils'
import { CommandBar, CommandButton, Copyable, FilterSheet, Layout, StateBadge, TableCard, time, useChanged, useCommand, usePoll, useSort } from '../lib'

const STATES = ['pending', 'reserved', 'running', 'retrying', 'succeeded', 'failed', 'cancelled']

/** Quick state filters: links that keep the other filters. */
function StateChips({ query }) {
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
            className={cn('inline-flex h-7 items-center gap-1.5 rounded-full border px-3 text-xs font-medium transition-[color,background-color,border-color,transform] duration-150 outline-none active:scale-95 focus-visible:ring-[3px] focus-visible:ring-ring/50',
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

export default function Jobs({ base, can, jobs, query, queueNames }) {
  const more = useLoadMore(jobs)
  const live = usePoll(['jobs'], more.more ? 0 : 5)
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
    <Layout heading="Jobs" live={live}
      actions={can.cancel && <CommandButton command={command} url={bulkUrl} data-testid="bulk-cancel" icon={IconBan}
        disabled={chosen.length === 0 || chosen.length > 100} onClick={cancelSelected}>
        Cancel selected ({chosen.length})
      </CommandButton>}>
      <CommandBar command={command} />
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
              {head('queue', 'Queue', (j) => j.queue)}
              {head('state', 'State', (j) => j.state)}
              {head('created', 'Created', (j) => Date.parse(j.createdAt), 'hidden md:table-cell')}
              {head('revision', 'Revision', (j) => j.revision, 'hidden text-right md:table-cell')}
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((j) => (
              <TableRow key={`${j.jobId}:${changed[j.jobId] ?? 0}`} data-testid="job-row" data-state={selected[j.jobId] ? 'selected' : undefined}
                className={cn(changed[j.jobId] && 'animate-flash')}>
                {can.cancel && <TableCell><Checkbox aria-label="select" checked={!!selected[j.jobId]} onCheckedChange={() => toggle(j.jobId)} /></TableCell>}
                <TableCell className="max-w-80 font-medium">
                  <Copyable value={j.jobName} label="Copy job name">
                    <Link href={`${base}/jobs/${j.jobId}`} data-testid="job-link" className="rounded-sm underline-offset-4 outline-none hover:text-primary hover:underline focus-visible:ring-[3px] focus-visible:ring-ring/50">{j.jobName}</Link>
                  </Copyable>
                </TableCell>
                <TableCell data-testid="job-queue" className="text-muted-foreground"><Copyable value={j.queue} label="Copy queue name" /></TableCell>
                <TableCell data-testid="job-state"><StateBadge state={j.state} />{j.cancelRequested && <span className="ml-1.5 text-xs text-warning">cancel requested</span>}</TableCell>
                <TableCell className="hidden whitespace-nowrap text-muted-foreground md:table-cell">
                  <Tooltip><TooltipTrigger asChild><time dateTime={j.createdAt} className="cursor-default">{relative(j.createdAt)}</time></TooltipTrigger><TooltipContent>{time(j.createdAt)}</TooltipContent></Tooltip>
                </TableCell>
                <TableCell className="hidden text-right tabular-nums md:table-cell">{j.revision}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableCard>
    </Layout>
  )
}
