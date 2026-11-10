import { IconAlertTriangle, IconBolt, IconCalendarEvent, IconListCheck, IconHistory, IconPlayerPause, IconPlayerPlay, IconShieldCheck } from '@tabler/icons-react'
import { useState } from 'react'
import { Chart, Legend } from '../components/charts'
import { duration, relative } from '../components/timeline'
import { Field, ms, StatCard } from '../components/stat'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../components/ui/card'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../components/ui/select'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../components/ui/table'
import { CommandBar, CommandButton, Copyable, flash, Layout, NameLink, OpenButton, StateBadge, TableCard, time, useChanged, useCommand, usePoll } from '../lib'
import { bucketLabel } from './Queue'

const BUCKETS = 20

/** Occurrences over their time span in equal buckets, split by what became of each: succeeded, failed, other job states, skipped. */
function runs(occurrences) {
  const at = (o) => Date.parse(o.scheduledAt ?? o.materializedAt)
  if (occurrences.length === 0) return { data: [], span: 0 }
  const from = Math.min(...occurrences.map(at)), to = Math.max(...occurrences.map(at), from + 60_000)
  const size = (to - from) / BUCKETS
  const data = Array.from({ length: BUCKETS }, (_, i) => {
    const start = new Date(from + i * size)
    return { label: bucketLabel(start, to - from), title: start.toLocaleString(), succeeded: 0, failed: 0, other: 0, skipped: 0 }
  })
  for (const o of occurrences) {
    const b = data[Math.min(Math.floor((at(o) - from) / size), BUCKETS - 1)]
    if (o.disposition === 'skipped') b.skipped++
    else if (o.jobState === 'succeeded') b.succeeded++
    else if (o.jobState === 'failed') b.failed++
    else b.other++
  }
  return { data, span: to - from }
}

export default function Schedule({ base, can, schedule: detail }) {
  const live = usePoll(['schedule'], 10)
  const command = useCommand()
  const [policy, setPolicy] = useState('skipPaused')
  const { schedule: s, definition: d, occurrences } = detail
  const changed = useChanged(occurrences, (o) => `${o.kind}/${o.key}`, (o) => o.jobState)
  const url = (op) => `${base}/schedules/${encodeURIComponent(s.key)}/${op}`
  const chart = runs(occurrences)
  const count = (pred) => occurrences.filter(pred).length
  return (
    <Layout live={live}
      heading={<span className="flex min-w-0 items-center gap-3"><Copyable value={s.key} label="Copy schedule key"><span data-testid="schedule-key" className="truncate">{s.key}</span></Copyable>
        <StateBadge state={s.paused ? 'paused' : 'active'} />{s.needsReview && <span className="text-sm font-normal text-warning">needs review</span>}</span>}
      description={`Definition revision ${s.definitionRevision}, control revision ${s.controlRevision}`}
      actions={<>
        {can.scheduleControl && s.paused && <Select value={policy} onValueChange={setPolicy}>
          <SelectTrigger aria-label="Resume policy" data-testid="resume-policy" className="h-8 min-w-48"><SelectValue /></SelectTrigger>
          <SelectContent>
            <SelectItem value="skipPaused">skip paused occurrences</SelectItem>
            <SelectItem value="runOnce">run once</SelectItem>
            <SelectItem value="catchUp">catch up</SelectItem>
          </SelectContent>
        </Select>}
        {can.scheduleControl && (s.paused
          ? <CommandButton command={command} url={url('resume')} pair={url('pause')} data-testid="schedule-resume" icon={IconPlayerPlay}
            onClick={() => command.ask(`Resume ${s.key}`, url('resume'), { expectedRevision: s.controlRevision, policy, view: 'detail' })}>Resume</CommandButton>
          : <CommandButton command={command} url={url('pause')} pair={url('resume')} data-testid="schedule-pause" icon={IconPlayerPause}
            onClick={() => command.ask(`Pause ${s.key}`, url('pause'), { expectedRevision: s.controlRevision, view: 'detail' })}>Pause</CommandButton>)}
        {can.scheduleTrigger && <CommandButton command={command} url={url('trigger')} data-testid="schedule-trigger" icon={IconBolt}
          onClick={() => command.ask(`Trigger ${s.key}`, url('trigger'), { expectedRevision: s.definitionRevision, view: 'detail' })}>Trigger</CommandButton>}
        {can.scheduleEdit && s.needsReview && <CommandButton command={command} url={url('revalidate')} data-testid="schedule-revalidate" icon={IconShieldCheck}
          onClick={() => command.ask(`Revalidate ${s.key}`, url('revalidate'), { expectedRevision: s.definitionRevision, view: 'detail' })}>Revalidate</CommandButton>}
      </>}>
      <CommandBar command={command} />
      <section aria-label="Schedule stats" className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <StatCard testId="stat-next" icon={IconCalendarEvent} label="Next occurrence" value={s.nextDueAt ? relative(s.nextDueAt) : 'none'} foot={time(s.nextDueAt)} />
        <StatCard testId="stat-last" icon={IconHistory} label="Last occurrence" value={s.lastMaterializedAt ? relative(s.lastMaterializedAt) : 'never'} foot={time(s.lastMaterializedAt)} />
        <StatCard testId="stat-created" icon={IconListCheck} label="Jobs created" value={count((o) => o.disposition !== 'skipped')} foot={occurrences.length ? `Of the ${occurrences.length} newest occurrences` : 'No occurrences yet'} />
        <StatCard testId="stat-failed" icon={IconAlertTriangle} label="Failed" value={count((o) => o.jobState === 'failed')} foot={`${count((o) => o.disposition === 'skipped')} skipped`} />
      </section>
      <div className="grid items-start gap-6 xl:grid-cols-[minmax(0,1fr)_24rem]">
        <div className="grid min-w-0 gap-6">
          <Card className="gap-4">
            <CardHeader><CardTitle className="text-base">Runs</CardTitle>
              <CardDescription>{occurrences.length ? `The newest ${occurrences.length} occurrences, per ${duration(chart.span / BUCKETS)}, by what became of their job` : 'No occurrences yet'}</CardDescription></CardHeader>
            <CardContent className="grid gap-3">
              <Legend series={[{ key: 's', label: 'Succeeded', color: 'text-chart-2' }, { key: 'f', label: 'Failed', color: 'text-chart-3' }, { key: 'o', label: 'Other', color: 'text-chart-1' }, { key: 'k', label: 'Skipped', color: 'text-chart-5' }]} />
              <Chart type="bar" stacked height={200} label={`Runs of ${s.key}`} data={chart.data}
                series={[{ key: 'succeeded', label: 'Succeeded', color: 'text-chart-2' }, { key: 'failed', label: 'Failed', color: 'text-chart-3' }, { key: 'other', label: 'Other', color: 'text-chart-1' }, { key: 'skipped', label: 'Skipped', color: 'text-chart-5' }]} />
            </CardContent>
          </Card>
          <TableCard title="Occurrence history" description="Newest first" empty={occurrences.length === 0 && 'No occurrences yet.'}>
            <Table data-testid="occurrences">
              <TableHeader><TableRow className="hover:bg-transparent"><TableHead>Scheduled for</TableHead><TableHead>Outcome</TableHead><TableHead className="hidden md:table-cell">Materialized</TableHead><TableHead className="hidden lg:table-cell">Kind</TableHead><TableHead className="w-10" /></TableRow></TableHeader>
              <TableBody>{occurrences.map((o) => (
                <TableRow key={`${o.kind}/${o.key}`} data-testid="occurrence-row" className={flash(changed[`${o.kind}/${o.key}`])}>
                  <TableCell className="whitespace-nowrap">{o.jobId ? <NameLink href={`${base}/runs/${o.jobId}`}>{time(o.scheduledAt ?? o.materializedAt)}</NameLink> : time(o.scheduledAt ?? o.materializedAt)}</TableCell>
                  <TableCell>{o.disposition === 'skipped' ? <StateBadge state="skipped" /> : o.jobState ? <StateBadge state={o.jobState} /> : <StateBadge state={o.disposition} />}
                    {o.coalesced && <span className="ml-1.5 text-xs text-muted-foreground">coalesced</span>}</TableCell>
                  <TableCell className="hidden whitespace-nowrap text-muted-foreground md:table-cell">{relative(o.materializedAt)}</TableCell>
                  <TableCell className="hidden text-muted-foreground lg:table-cell">{o.kind}</TableCell>
                  <TableCell className="text-right">{o.jobId && <OpenButton href={`${base}/runs/${o.jobId}`} label="Open this occurrence's job" />}</TableCell>
                </TableRow>
              ))}</TableBody>
            </Table>
          </TableCard>
        </div>
        <Card>
          <CardHeader><CardTitle className="text-base">Definition</CardTitle></CardHeader>
          <CardContent>
            <dl data-testid="definition" className="grid gap-4">
              {d.kind === 'cron' || d.cronText
                ? <Field label="Expression"><Copyable value={d.cronText} label="Copy expression"><code className="font-mono">{d.cronText}</code></Copyable>
                  <span className="block text-xs text-muted-foreground">{[d.cronDialect, d.zoneId].filter(Boolean).join(' · ')}</span></Field>
                : <Field label="Runs once at">{time(d.dueAt)}</Field>}
              <Field label="Job"><code>{d.jobName}</code></Field>
              <Field label="Queue"><NameLink href={`${base}/queues/${encodeURIComponent(d.queue)}`}>{d.queue}</NameLink></Field>
              <Field label="Misfire">{d.misfirePolicy}, grace {ms(d.misfireGraceMs)}</Field>
              <Field label="Overlap">{d.overlapPolicy}</Field>
              <Field label="Time-zone gaps and folds">{d.gapPolicy} / {d.foldPolicy}</Field>
              <Field label="Catch-up">at most {d.maxBatch} per batch, {d.maxEpisodeJobs} per episode, back {ms(d.maxCatchUpAgeMs)}</Field>
            </dl>
          </CardContent>
        </Card>
      </div>
    </Layout>
  )
}
