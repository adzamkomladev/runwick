import { IconAlertTriangle, IconCheck, IconDatabase, IconDeviceFloppy, IconPencil, IconTrash } from '@tabler/icons-react'
import { Kpi, KpiStrip } from '../components/kpis'
import { useState } from 'react'
import { Badge } from '../components/ui/badge'
import { Button } from '../components/ui/button'
import { Card, CardAction, CardContent, CardHeader, CardTitle } from '../components/ui/card'
import { Label } from '../components/ui/label'
import { NumberField } from '../components/ui/number-field'
import { Sheet, SheetContent, SheetDescription, SheetFooter, SheetHeader, SheetTitle } from '../components/ui/sheet'
import { Textarea } from '../components/ui/textarea'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../components/ui/table'
import { AnimatedNumber, CommandButton, Copyable, Layout, TableCard, time, useCommand, usePoll } from '../lib'

const NAMES = [['completedDetailHours', 'Completed job detail'], ['failedDetailHours', 'Failed job detail'], ['jobHistoryHours', 'Job history'],
  ['auditHours', 'Audit'], ['scalingEventsHours', 'Scaling events'], ['scheduleOccurrencesHours', 'Schedule occurrences'],
  ['workerPresenceHours', 'Worker presence']]

const hours = (h) => (h == null ? 'forever' : h % 24 === 0 ? `${h / 24} d` : `${h} h`)

/** The policy edit in a side sheet: seven whole-hour values (empty keeps forever) and an optional reason. */
function PolicySheet({ open, onOpenChange, retention, url, command }) {
  // Remounted on every open (key below), so the fields start from the policy the page shows.
  const [values, setValues] = useState(() => Object.fromEntries(NAMES.map(([n]) => [n, retention.policy[n] == null ? '' : String(retention.policy[n])])))
  const [reason, setReason] = useState('')
  const submit = (e) => {
    e.preventDefault()
    const body = { expectedRevision: retention.revision, reason: reason || null }
    for (const [name] of NAMES) body[name] = values[name] === '' ? null : Number(values[name])
    command.run('Change the retention policy', url, body, 'done', (ok) => { if (ok) setTimeout(() => onOpenChange(false), 500) })
  }
  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent data-testid="policy-sheet">
        <SheetHeader><SheetTitle>Retention policy</SheetTitle><SheetDescription>Hours each record class is kept. Empty keeps it forever.</SheetDescription></SheetHeader>
        <form onSubmit={submit} className="flex min-h-0 flex-1 flex-col">
          <div className="fade-y grid flex-1 content-start gap-4 overflow-y-auto px-5">
            {NAMES.map(([name, label]) => (
              <div key={name} className="grid gap-1.5">
                <Label htmlFor={`policy-${name}`}>{label}</Label>
                <NumberField id={`policy-${name}`} aria-label={label} min={1} placeholder="forever" suffix="h" value={values[name] ?? ''} onChange={(v) => setValues((x) => ({ ...x, [name]: v }))} />
              </div>
            ))}
            <div className="grid gap-1.5"><Label htmlFor="policy-reason">Reason <span className="font-normal text-muted-foreground">(optional, kept in audit)</span></Label>
              <Textarea id="policy-reason" value={reason} maxLength={500} onChange={(e) => setReason(e.target.value)} /></div>
          </div>
          <SheetFooter>
            <Button type="button" variant="ghost" onClick={() => onOpenChange(false)}>Cancel</Button>
            <CommandButton command={command} url={url} type="submit" variant="default" size="default" data-testid="save-policy" icon={IconDeviceFloppy}>Save policy</CommandButton>
          </SheetFooter>
        </form>
      </SheetContent>
    </Sheet>
  )
}

export default function Retention({ base, can, retention }) {
  const live = usePoll(['retention'], 30)
  const command = useCommand()
  const [editing, setEditing] = useState(false)
  const url = `${base}/retention/policy`
  return (
    <Layout heading="Retention" live={live}
      actions={can.retentionManage && <CommandButton command={command} url={url} variant="default" data-testid="edit-policy" icon={IconPencil} onClick={() => setEditing(true)}>Edit policy</CommandButton>}>
      <PolicySheet key={editing ? `open-${retention.revision}` : 'closed'} open={editing} onOpenChange={setEditing} retention={retention} url={url} command={command} />
      <KpiStrip label="Retention stats" className="xl:grid-cols-5">
        <Kpi testId="kpi-kept" icon={IconDatabase} label="Rows kept" value={retention.classes.reduce((t, c) => t + (c.totalRows ?? 0), 0)} foot={`${retention.classes.length} record classes`} />
        <Kpi testId="kpi-removed" icon={IconTrash} label="Removed today" value={retention.classes.reduce((t, c) => t + (c.todayRows ?? 0), 0)} foot="Rows past their retention" />
        <Kpi testId="kpi-batches" label="Batches today" value={retention.classes.reduce((t, c) => t + (c.todayBatches ?? 0), 0)} />
        <Kpi testId="kpi-failures" icon={IconAlertTriangle} label="Failures today" value={retention.classes.reduce((t, c) => t + (c.todayFailures ?? 0), 0)} />
        <Kpi testId="kpi-behind" icon={IconCheck} label="Behind" value={retention.classes.filter((c) => c.behind).length} foot="Classes not caught up" />
      </KpiStrip>
      <Card className="gap-4">
        <CardHeader><CardTitle>Policy</CardTitle><CardAction><Badge variant="secondary">revision <span data-testid="retention-revision" className="ml-1">{retention.revision}</span></Badge></CardAction></CardHeader>
        <CardContent>
          <dl data-testid="policy" className="grid grid-cols-2 gap-3 sm:grid-cols-4 lg:grid-cols-7">
            {NAMES.map(([name, label]) => (
              <div key={name} className="grid gap-1 rounded-lg border bg-muted/30 p-3">
                <dt className="text-xs text-muted-foreground">{label}</dt>
                <dd className="text-lg font-semibold tabular-nums">{hours(retention.policy[name])}</dd>
              </div>
            ))}
          </dl>
        </CardContent>
      </Card>
      <TableCard title="Cleanup progress" pager={<p className="px-4 pt-3 text-xs text-muted-foreground border-t">as of {new Date(retention.observedAt).toLocaleTimeString()}</p>}>
        <Table data-testid="classes">
          <TableHeader><TableRow className="hover:bg-transparent"><TableHead>Record class</TableHead><TableHead>Last batch</TableHead><TableHead>Caught up</TableHead>
            <TableHead className="text-right">Rows removed</TableHead><TableHead>Today</TableHead><TableHead>Last error</TableHead></TableRow></TableHeader>
          <TableBody>{retention.classes.map((c) => (
            <TableRow key={c.recordClass}>
              <TableCell className="font-medium">{c.recordClass}{c.behind
                ? <Badge variant="warning" className="ml-2"><IconAlertTriangle />behind</Badge>
                : <Badge variant="success" className="ml-2"><IconCheck />on time</Badge>}</TableCell>
              <TableCell className="whitespace-nowrap text-muted-foreground">{time(c.lastBatchAt)}</TableCell><TableCell className="whitespace-nowrap text-muted-foreground">{time(c.caughtUpAt)}</TableCell>
              <TableCell className="text-right"><AnimatedNumber value={c.totalRows} /></TableCell>
              <TableCell className="text-xs whitespace-nowrap">{c.todayRows} rows, {c.todayBatches} batches, {c.todayFailures} failures</TableCell>
              <TableCell>{c.lastErrorCode ? <Copyable value={c.lastErrorCode} label="Copy error code"><code className="text-destructive">{c.lastErrorCode}</code></Copyable> : '—'}</TableCell>
            </TableRow>
          ))}</TableBody>
        </Table>
      </TableCard>
    </Layout>
  )
}
