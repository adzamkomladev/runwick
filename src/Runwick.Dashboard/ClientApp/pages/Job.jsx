import { IconAlertTriangle, IconBan, IconCircleCheck, IconClockHour4, IconCopy, IconGauge, IconPercentage, IconPlayerPlay, IconRefresh, IconRepeat, IconRocket, IconSend } from '@tabler/icons-react'
import { useState } from 'react'
import { BarList, Chart, Legend } from '../components/charts'
import { DateRangePicker } from '../components/date-range'
import { modeText, poolsFrom } from '../components/host'
import { Kpi, KpiStrip, perMinute, successRate, sum } from '../components/kpis'
import { Field, ms } from '../components/stat'
import { relative } from '../components/timeline'
import { Button } from '../components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../components/ui/card'
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from '../components/ui/dropdown-menu'
import { Input } from '../components/ui/input'
import { Label } from '../components/ui/label'
import { NumberField } from '../components/ui/number-field'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../components/ui/select'
import { Sheet, SheetContent, SheetDescription, SheetFooter, SheetHeader, SheetTitle } from '../components/ui/sheet'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../components/ui/table'
import { Textarea } from '../components/ui/textarea'
import { toast } from '../components/ui/toast'
import { CommandBar, CommandButton, Copyable, FilterSheet, flash, Layout, NameLink, OpenButton, Pager, StateBadge, TableCard, useChanged, useCommand, usePoll } from '../lib'
import { bucketLabel } from './Queue'
import { HandlerCell } from './Jobs'
import { StateChips } from './Runs'

/** Trigger a run: a JSON payload checked here for syntax and on the server against the job's contract, and dispatch options. */
function TriggerSheet({ open, onClose, type, url, command }) {
  const [payload, setPayload] = useState('{\n  \n}')
  const [queue, setQueue] = useState('')
  const [priority, setPriority] = useState('default')
  const [delay, setDelay] = useState('')
  const [reason, setReason] = useState('')
  let error = null
  try { JSON.parse(payload) } catch (e) { error = e.message }
  const submit = (e) => {
    e.preventDefault()
    if (error) return
    command.run(`Trigger ${type.name}`, url, {
      payload: JSON.parse(payload), queue: queue || null, priority: priority === 'default' ? null : priority,
      notBefore: delay ? new Date(Date.now() + Number(delay) * 1000).toISOString() : null, reason: reason || null,
    }, 'done', (ok) => { if (ok) setTimeout(onClose, 500) })
  }
  return (
    <Sheet open={open} onOpenChange={(o) => { if (!o) onClose() }}>
      <SheetContent data-testid="trigger-sheet" className="sm:max-w-lg">
        <SheetHeader><SheetTitle>Trigger {type.name}</SheetTitle>
          <SheetDescription>Creates one run now, checked against the job's payload contract{type.payloadVersion ? ` (version ${type.payloadVersion})` : ''} and your permissions, and audited.</SheetDescription></SheetHeader>
        <form onSubmit={submit} className="flex min-h-0 flex-1 flex-col">
          <div className="fade-y grid flex-1 content-start gap-4 overflow-y-auto px-5">
            <div className="grid gap-1.5"><Label htmlFor="trigger-payload">Payload (JSON)</Label>
              <Textarea id="trigger-payload" data-testid="trigger-payload" value={payload} spellCheck={false} rows={10} onChange={(e) => setPayload(e.target.value)}
                aria-invalid={!!error} className="font-mono text-xs" />
              <span data-testid="trigger-payload-error" className={error ? 'text-xs text-destructive' : 'text-xs text-muted-foreground'}>{error ?? 'Valid JSON'}</span></div>
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
              <div className="grid gap-1.5"><Label htmlFor="trigger-queue">Queue</Label>
                <Input id="trigger-queue" data-testid="trigger-queue" value={queue} placeholder={type.queue ?? 'default'} onChange={(e) => setQueue(e.target.value)} /></div>
              <div className="grid gap-1.5"><Label htmlFor="trigger-priority">Priority</Label>
                <Select value={priority} onValueChange={setPriority}>
                  <SelectTrigger id="trigger-priority" data-testid="trigger-priority" className="w-full"><SelectValue /></SelectTrigger>
                  <SelectContent>{['default', 'high', 'medium', 'low'].map((p) => <SelectItem key={p} value={p}>{p === 'default' ? `the job's (${type.priority ?? 'medium'})` : p}</SelectItem>)}</SelectContent>
                </Select></div>
            </div>
            <div className="grid gap-1.5"><Label htmlFor="trigger-delay">Delay <span className="font-normal text-muted-foreground">(seconds, optional)</span></Label>
              <NumberField id="trigger-delay" aria-label="delay" value={delay} onChange={setDelay} min={0} /></div>
            <div className="grid gap-1.5"><Label htmlFor="trigger-reason">Reason <span className="font-normal text-muted-foreground">(optional, kept in audit)</span></Label>
              <Textarea id="trigger-reason" value={reason} maxLength={500} onChange={(e) => setReason(e.target.value)} /></div>
            <p className="text-xs text-muted-foreground">The command ID is the idempotency key: sending the same trigger again (after a lost answer) returns the same run. A job with a uniqueness rule reuses the run that holds its key.</p>
          </div>
          <SheetFooter>
            <Button type="button" variant="ghost" onClick={onClose}>Cancel</Button>
            <CommandButton command={command} url={url} type="submit" variant="default" size="default" data-testid="trigger-send" icon={IconSend} disabled={!!error}>Trigger</CommandButton>
          </SheetFooter>
        </form>
      </SheetContent>
    </Sheet>
  )
}

/** Dispatch snippets: C# through IJobDispatcher with the payload type, and the CLI with a payload file. */
const snippets = (type) => ({
  'C#': `await dispatcher.DispatchAsync(new ${(type.definition?.payloadType ?? 'MyPayload').split(/[.+]/).pop()}(/* … */));`,
  CLI: `runwick dispatch ${type.name} --payload @payload.json${type.queue ? ` --queue ${type.queue}` : ''}`,
})

/** A definition value as text: scalars as they are, objects as compact JSON. */
const show = (v) => (v == null ? '—' : typeof v === 'object' ? JSON.stringify(v) : typeof v === 'boolean' ? (v ? 'yes' : 'no') : String(v))
const label = (k) => k.replace(/([A-Z])/g, ' $1').replace(/^./, (c) => c.toUpperCase())

export default function Job({ base, can, type, stats, runs, workers, query }) {
  const live = usePoll(['stats', 'runs', ...(can.workers ? ['workers'] : [])], 10)
  const command = useCommand()
  const [triggering, setTriggering] = useState(false)
  const at = `${base}/jobs/${encodeURIComponent(type.name)}/`
  const s = stats.states, rate = successRate(s)
  const span = Date.parse(stats.to) - Date.parse(stats.from)
  const changed = useChanged(runs.items, (j) => j.jobId, (j) => j.state)
  const pools = poolsFrom(workers ?? []).filter((p) => !p.known || p.queues.some((q) => q.queue === type.queue))
  const failed = s.failed ?? 0, pending = stats.now?.pending ?? 0
  const ended = stats.buckets.reduce((t, b) => t + b.succeeded + b.failed + (b.cancelled ?? 0), 0)
  const buckets = stats.buckets.map((b) => ({ ...b, label: bucketLabel(new Date(b.from), span), title: new Date(b.from).toLocaleString() }))
  const copy = (lang, text) => navigator.clipboard?.writeText(text).then(() => toast.success(`${lang} snippet copied`), () => {})
  return (
    <Layout live={live}
      heading={<span className="flex min-w-0 items-center gap-3"><Copyable value={type.name} label="Copy job name"><span data-testid="job-name" className="truncate">{type.name}</span></Copyable>
        <span className="text-sm font-normal"><HandlerCell workers={workers} name={type.name} /></span></span>}
      description={type.registered ? `Registered here · queue ${type.queue ?? '—'} · priority ${type.priority ?? '—'} · payload version ${type.payloadVersion ?? '—'}` : 'Not registered in this host: seen in runs or schedules only.'}
      actions={<>
        <DateRangePicker query={query} fallback="1d" />
        <DropdownMenu>
          <DropdownMenuTrigger asChild><Button variant="outline" size="sm" data-testid="snippet"><IconCopy />Snippet</Button></DropdownMenuTrigger>
          <DropdownMenuContent align="end">{Object.entries(snippets(type)).map(([lang, text]) => (
            <DropdownMenuItem key={lang} data-testid={`snippet-${lang === 'C#' ? 'cs' : 'cli'}`} onSelect={() => copy(lang, text)}>Copy {lang} dispatch</DropdownMenuItem>))}
          </DropdownMenuContent>
        </DropdownMenu>
        {can.retry && <CommandButton command={command} url={at + 'retry-failed'} data-testid="retry-failed" icon={IconRefresh} disabled={failed === 0}
          onClick={() => command.ask(`Retry ${failed.toLocaleString()} failed run${failed === 1 ? '' : 's'} of ${type.name} created in the range`, at + 'retry-failed',
            { from: stats.from, to: stats.to, expectedCount: failed })}>Retry failed</CommandButton>}
        {can.cancel && <CommandButton command={command} url={at + 'cancel-pending'} data-testid="cancel-pending" icon={IconBan} disabled={pending === 0}
          onClick={() => command.ask(`Cancel ${pending.toLocaleString()} pending run${pending === 1 ? '' : 's'} of ${type.name}`, at + 'cancel-pending', { expectedCount: pending })}>Cancel pending</CommandButton>}
        {can.dispatch && type.registered && <Button size="sm" data-testid="trigger" onClick={() => setTriggering(true)}><IconPlayerPlay />Trigger</Button>}
      </>}>
      <CommandBar command={command} />
      {can.dispatch && type.registered && <TriggerSheet key={triggering ? 'open' : 'closed'} open={triggering} onClose={() => setTriggering(false)} type={type} url={at + 'dispatch'} command={command} />}
      <KpiStrip label="Job stats" className="xl:grid-cols-4 2xl:grid-cols-8">
        <Kpi testId="kpi-runs" icon={IconRocket} label="Runs" value={sum(s)} foot="Created in the range" spark={stats.buckets.map((b) => b.created)} />
        <Kpi testId="kpi-succeeded" icon={IconCircleCheck} label="Succeeded" value={s.succeeded ?? 0} spark={stats.buckets.map((b) => b.succeeded)} color="text-chart-2" />
        <Kpi testId="kpi-failed" icon={IconAlertTriangle} label="Failed" value={failed} spark={stats.buckets.map((b) => b.failed)} color="text-chart-3" />
        <Kpi testId="kpi-retries" icon={IconRepeat} label="Retries" value={stats.retries} foot="Extra executions" />
        <Kpi testId="kpi-success" icon={IconPercentage} label="Success rate" value={rate ?? '—'} suffix={rate == null ? null : '%'} foot="Of runs that ended" />
        <Kpi testId="kpi-median" icon={IconClockHour4} label="Median duration" value={ms(stats.duration.medianMs)} spark={stats.buckets.map((b) => b.medianMs ?? 0)} color="text-chart-4" />
        <Kpi testId="kpi-p95" icon={IconClockHour4} label="p95 duration" value={ms(stats.duration.p95Ms)} foot={`max ${ms(stats.duration.maxMs)}`} spark={stats.buckets.map((b) => b.p95Ms ?? 0)} color="text-chart-4" />
        <Kpi testId="kpi-throughput" icon={IconGauge} label="Throughput" value={perMinute(stats)} suffix="/min" foot={`${pending.toLocaleString()} pending now`} />
      </KpiStrip>
      <div className="grid grid-cols-1 gap-6 xl:grid-cols-2">
        <Card className="gap-4">
          <CardHeader><CardTitle className="text-base">Runs by outcome</CardTitle><CardDescription>Ended per {ms(span / Math.max(buckets.length, 1))}</CardDescription></CardHeader>
          <CardContent className="grid gap-3">
            <Legend series={[{ key: 's', label: 'Succeeded', color: 'text-chart-2' }, { key: 'f', label: 'Failed', color: 'text-chart-3' }, { key: 'c', label: 'Cancelled', color: 'text-chart-5' }]} />
            {ended === 0 ? <p className="grid h-[200px] place-items-center text-sm text-muted-foreground">No run of this job ended in the range.</p>
              : <Chart type="bar" stacked height={200} label={`Runs of ${type.name} by outcome`} data={buckets}
              series={[{ key: 'succeeded', label: 'Succeeded', color: 'text-chart-2' }, { key: 'failed', label: 'Failed', color: 'text-chart-3' }, { key: 'cancelled', label: 'Cancelled', color: 'text-chart-5' }]} />}
          </CardContent>
        </Card>
        <Card className="gap-4">
          <CardHeader><CardTitle className="text-base">Duration</CardTitle><CardDescription>Median and p95 of runs that ended, in milliseconds</CardDescription></CardHeader>
          <CardContent className="grid gap-3">
            <Legend series={[{ key: 'm', label: 'Median', color: 'text-chart-1' }, { key: 'p', label: 'p95', color: 'text-chart-4' }]} />
            {!stats.duration.count ? <p className="grid h-[200px] place-items-center text-sm text-muted-foreground">No durations yet: nothing ended in the range.</p>
              : <Chart height={200} label={`Duration of ${type.name}`} data={buckets.map((b) => ({ ...b, medianMs: b.medianMs ?? 0, p95Ms: b.p95Ms ?? 0 }))}
              series={[{ key: 'medianMs', label: 'Median', color: 'text-chart-1' }, { key: 'p95Ms', label: 'p95', color: 'text-chart-4' }]} />}
          </CardContent>
        </Card>
      </div>
      <div className="grid items-start gap-6 xl:grid-cols-[minmax(0,1fr)_24rem]">
        <div className="grid min-w-0 gap-6">
          <TableCard title="Runs" pager={<Pager page={runs} />} empty={runs.items.length === 0 && 'No runs in this range.'}
            toolbar={<div className="flex flex-wrap items-center gap-2"><StateChips query={query} /><FilterSheet values={query} fields={[{ name: 'queue', label: 'Queue', free: true, options: [] }]} /></div>}>
            <Table data-testid="job-runs">
              <TableHeader><TableRow className="hover:bg-transparent"><TableHead>Created</TableHead><TableHead>State</TableHead><TableHead className="hidden sm:table-cell">Queue</TableHead>
                <TableHead className="hidden md:table-cell">Executions</TableHead><TableHead className="w-10" /></TableRow></TableHeader>
              <TableBody>{runs.items.map((j) => (
                <TableRow key={j.jobId} data-testid="run-row" className={flash(changed[j.jobId])}>
                  <TableCell className="whitespace-nowrap"><NameLink href={`${base}/runs/${j.jobId}`} testId="run-link">{relative(j.createdAt)}</NameLink></TableCell>
                  <TableCell><StateBadge state={j.state} /></TableCell>
                  <TableCell className="hidden text-muted-foreground sm:table-cell">{j.queue}</TableCell>
                  <TableCell className="hidden tabular-nums md:table-cell">{j.executionCount}</TableCell>
                  <TableCell className="text-right"><OpenButton href={`${base}/runs/${j.jobId}`} label={`Open run of ${j.jobName}`} testId="run-open" /></TableCell>
                </TableRow>
              ))}</TableBody>
            </Table>
          </TableCard>
          {workers && <TableCard title="Handled by" description={`${workers.length} live worker${workers.length === 1 ? '' : 's'} with a handler`} empty={workers.length === 0 && 'No live worker has a handler for this job.'}>
            <Table data-testid="job-workers">
              <TableHeader><TableRow className="hover:bg-transparent"><TableHead>Pool</TableHead><TableHead>Workers</TableHead><TableHead>Concurrency</TableHead><TableHead className="w-10" /></TableRow></TableHeader>
              <TableBody>{pools.map((p) => (
                <TableRow key={p.name}>
                  <TableCell><NameLink href={`${base}/pools/${encodeURIComponent(p.name)}`}>{p.name}</NameLink></TableCell>
                  <TableCell><span className="flex flex-wrap gap-x-2">{p.workers.map(({ worker: w }) => <NameLink key={w.instanceId} href={`${base}/workers/${w.instanceId}`} className="font-normal">{w.hostName}</NameLink>)}</span></TableCell>
                  <TableCell className="whitespace-nowrap text-xs text-muted-foreground tabular-nums">{p.inFlight} / {p.target} · {modeText(p)}</TableCell>
                  <TableCell className="text-right"><OpenButton href={`${base}/pools/${encodeURIComponent(p.name)}`} label={`Open pool ${p.name}`} /></TableCell>
                </TableRow>
              ))}</TableBody>
            </Table>
          </TableCard>}
        </div>
        <div className="grid min-w-0 gap-6">
          <Card>
            <CardHeader><CardTitle className="text-base">Definition</CardTitle>{!type.registered && <CardDescription>Not registered in this host, so its settings are not known here.</CardDescription>}</CardHeader>
            <CardContent>
              <dl data-testid="job-definition" className="grid gap-3">
                <Field label="Queue">{type.queue ? <NameLink href={`${base}/queues/${encodeURIComponent(type.queue)}`}>{type.queue}</NameLink> : '—'}</Field>
                <Field label="Priority">{type.priority}</Field>
                <Field label="Payload version">{type.payloadVersion}</Field>
                {Object.entries(type.definition ?? {}).map(([k, v]) => <Field key={k} label={label(k)}><span className="font-mono text-xs break-all">{show(v)}</span></Field>)}
              </dl>
            </CardContent>
          </Card>
          <Card>
            <CardHeader><CardTitle className="text-base">Schedules</CardTitle></CardHeader>
            <CardContent>{type.schedules.length === 0 ? <p className="text-sm text-muted-foreground">No schedule uses this job.</p>
              : <ul data-testid="job-schedules" className="grid gap-2">{type.schedules.map((k) => (
                <li key={k} className="flex items-center justify-between gap-2 text-sm"><NameLink href={`${base}/schedules/${encodeURIComponent(k)}`}>{k}</NameLink>
                  <OpenButton href={`${base}/schedules/${encodeURIComponent(k)}`} label={`Open schedule ${k}`} /></li>))}</ul>}
            </CardContent>
          </Card>
          <Card>
            <CardHeader><CardTitle className="text-base">Top error codes</CardTitle><CardDescription>Failed attempts in the range</CardDescription></CardHeader>
            <CardContent data-testid="job-errors"><BarList color="text-chart-3" empty="No failed attempts." items={(stats.errorCodes ?? []).map((e) => ({ label: e.code, value: e.count }))} /></CardContent>
          </Card>
        </div>
      </div>
    </Layout>
  )
}
