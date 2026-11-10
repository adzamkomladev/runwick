import { IconCpu, IconInfoCircle } from '@tabler/icons-react'
import { Chart } from './charts'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from './ui/card'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from './ui/table'
import { Tooltip, TooltipContent, TooltipTrigger } from './ui/tooltip'
import { cn } from './ui/utils'

// What the worker process used while each attempt ran (16 §3 AttemptResourceUsage). Every figure is process-wide, so the
// card says so and shows how many other attempts shared the window; attempts without figures read "not recorded".

export function bytes(n) {
  if (n == null) return '—'
  const units = ['B', 'KiB', 'MiB', 'GiB']
  let v = n, u = 0
  while (v >= 1024 && u < units.length - 1) { v /= 1024; u++ }
  return `${v < 10 && u ? v.toFixed(1) : Math.round(v)} ${units[u]}`
}

export function cpu(us) {
  if (us == null) return '—'
  if (us < 1000) return `${us} µs`
  const ms = us / 1000
  return ms < 1000 ? `${ms < 10 ? ms.toFixed(1) : Math.round(ms)} ms` : `${(ms / 1000).toFixed(ms < 10000 ? 2 : 1)} s`
}

/** CPU time over the time the attempt ran, as a share of one core (above 100% means several cores were busy). */
function share(a) {
  if (!a.usage || !a.authorizedAt || !a.finishedAt) return null
  const ms = Date.parse(a.finishedAt) - Date.parse(a.authorizedAt)
  return ms > 0 ? Math.round(a.usage.processCpuMicroseconds / 10 / ms) : null
}

function Shared({ n }) {
  return n === 0
    ? <span className="inline-flex items-center rounded-md bg-lime-yellow-50 px-1.5 py-0.5 text-xs text-lime-yellow-800 ring-1 ring-lime-yellow-600/20 ring-inset dark:bg-lime-yellow-400/10 dark:text-lime-yellow-400 dark:ring-lime-yellow-400/25">alone</span>
    : <span className="inline-flex items-center rounded-md bg-chocolate-50 px-1.5 py-0.5 text-xs text-chocolate-700 ring-1 ring-chocolate-600/20 ring-inset dark:bg-chocolate-400/10 dark:text-chocolate-300 dark:ring-chocolate-400/25">with {n} other{n === 1 ? '' : 's'}</span>
}

function Stat({ label, value, foot, testId }) {
  return (
    <div className="tint grid gap-1 rounded-lg border bg-muted/30 p-3">
      <span className="text-xs text-muted-foreground">{label}</span>
      <span data-testid={testId} className="text-lg font-semibold tabular-nums">{value}</span>
      {foot && <span className="text-xs text-muted-foreground">{foot}</span>}
    </div>
  )
}

export function UsageCard({ attempts }) {
  const measured = attempts.map((a, i) => ({ ...a, n: a.executionNumber ?? i + 1 })).filter((a) => a.usage)
  const last = measured.at(-1)
  return (
    <Card className="gap-3" data-testid="usage">
      <CardHeader>
        <CardTitle className="flex items-center gap-2"><IconCpu className="size-4 text-muted-foreground" />Resources used</CardTitle>
        <CardDescription className="flex items-start gap-1.5">
          <span>Whole worker process, per attempt</span>
          <Tooltip><TooltipTrigger asChild><button type="button" aria-label="How these are measured" className="mt-0.5 shrink-0 rounded-sm outline-none focus-visible:ring-2 focus-visible:ring-ring/50"><IconInfoCircle className="size-3.5" /></button></TooltipTrigger>
            <TooltipContent className="max-w-72">.NET cannot attribute CPU or memory to one async job, so these are the whole worker process&apos;s figures between the start and end of the attempt. CPU and allocations are exact; working set is sampled about every 250 ms. &quot;Alone&quot; means no other attempt ran in that process meanwhile.</TooltipContent></Tooltip>
        </CardDescription>
      </CardHeader>
      <CardContent className="grid gap-4">
        {!last
          ? (
            <div className="grid gap-3 sm:grid-cols-3">
              {['CPU time', 'Allocated', 'Working set'].map((m) => (
                <div key={m} className="grid gap-1 rounded-lg border border-dashed p-3">
                  <span className="text-xs text-muted-foreground">{m}</span><span className="text-sm font-medium text-muted-foreground">not recorded</span>
                </div>
              ))}
              <p className="text-xs text-muted-foreground sm:col-span-3">No attempt of this job has figures: it has not run yet, it ran on an older worker, or the worker has RecordResourceUsage off.</p>
            </div>
            )
          : (
            <>
              <div className="flex flex-wrap items-center gap-2 text-sm">
                <span className="font-medium">Attempt {last.n}</span><Shared n={last.usage.overlappingAttempts} />
              </div>
              <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
                <Stat testId="usage-cpu" label="Process CPU time" value={cpu(last.usage.processCpuMicroseconds)}
                  foot={share(last) != null ? `≈ ${share(last)}% of one core while it ran` : null} />
                <Stat testId="usage-alloc" label="Process allocations" value={bytes(last.usage.processAllocatedBytes)} foot="managed heap, during the attempt" />
                <Stat testId="usage-peak" label="Working set peak" value={bytes(last.usage.workingSetPeakBytes)} foot="highest sample" />
                <Stat label="Working set at end" value={bytes(last.usage.workingSetEndBytes)}
                  foot={(() => { const d = last.usage.workingSetEndBytes - last.usage.workingSetStartBytes; return `${d >= 0 ? '+' : '−'}${bytes(Math.abs(d))} from ${bytes(last.usage.workingSetStartBytes)} at start` })()} />
              </div>
              {measured.length > 1 && (
                <div className="grid gap-4 md:grid-cols-2">
                  <div className="grid gap-1"><span className="text-xs text-muted-foreground">CPU time per attempt (ms)</span>
                    <Chart type="bar" height={140} label="CPU time per attempt" series={[{ key: 'cpu', label: 'CPU ms', color: 'text-chart-1' }]}
                      data={measured.map((a) => ({ label: `#${a.n}`, title: `Attempt ${a.n}`, cpu: Math.round(a.usage.processCpuMicroseconds / 1000) }))} /></div>
                  <div className="grid gap-1"><span className="text-xs text-muted-foreground">Working set peak per attempt (MiB)</span>
                    <Chart type="bar" height={140} label="Working set peak per attempt" series={[{ key: 'ws', label: 'Peak MiB', color: 'text-chart-4' }]}
                      data={measured.map((a) => ({ label: `#${a.n}`, title: `Attempt ${a.n}`, ws: Math.round(a.usage.workingSetPeakBytes / 1048576) }))} /></div>
                </div>
              )}
              <Table data-testid="usage-rows">
                <TableHeader><TableRow className="hover:bg-transparent"><TableHead>#</TableHead><TableHead>CPU</TableHead><TableHead>Allocated</TableHead><TableHead>Working set (peak)</TableHead><TableHead>Shared</TableHead></TableRow></TableHeader>
                <TableBody>{attempts.map((a, i) => (
                  <TableRow key={a.attemptId} className={cn(!a.usage && 'text-muted-foreground')}>
                    <TableCell className="tabular-nums">{a.executionNumber ?? i + 1}</TableCell>
                    {a.usage
                      ? <>
                        <TableCell className="tabular-nums">{cpu(a.usage.processCpuMicroseconds)}</TableCell>
                        <TableCell className="tabular-nums">{bytes(a.usage.processAllocatedBytes)}</TableCell>
                        <TableCell className="tabular-nums">{bytes(a.usage.workingSetPeakBytes)}</TableCell>
                        <TableCell><Shared n={a.usage.overlappingAttempts} /></TableCell>
                      </>
                      : <TableCell colSpan={4} className="text-xs">not recorded</TableCell>}
                  </TableRow>
                ))}</TableBody>
              </Table>
            </>
            )}
      </CardContent>
    </Card>
  )
}
