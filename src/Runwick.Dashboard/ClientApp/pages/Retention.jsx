import { IconAlertTriangle, IconCheck, IconDeviceFloppy } from '@tabler/icons-react'
import { Badge } from '../components/ui/badge'
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from '../components/ui/card'
import { Input } from '../components/ui/input'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../components/ui/table'
import { AnimatedNumber, CommandBar, CommandButton, Layout, TableCard, time, useCommand, usePoll } from '../lib'

const NAMES = [['completedDetailHours', 'Completed job detail'], ['failedDetailHours', 'Failed job detail'], ['jobHistoryHours', 'Job history'],
  ['auditHours', 'Audit'], ['scalingEventsHours', 'Scaling events'], ['scheduleOccurrencesHours', 'Schedule occurrences'],
  ['workerPresenceHours', 'Worker presence']]

export default function Retention({ base, can, retention }) {
  const live = usePoll(['retention'], 30)
  const command = useCommand()
  const url = `${base}/retention/policy`
  const submit = (e) => {
    e.preventDefault()
    const form = new FormData(e.target)
    const body = { expectedRevision: retention.revision }
    for (const [name] of NAMES) body[name] = form.get(name) === '' ? null : Number(form.get(name))
    command.ask('Change the retention policy', url, body)
  }
  return (
    <Layout heading="Retention" live={live}
      description={<>Policy revision <span data-testid="retention-revision">{retention.revision}</span>. Values in hours; empty keeps forever.</>}>
      <CommandBar command={command} />
      <Card>
        <CardHeader><CardTitle>Policy</CardTitle><CardDescription>How long each kind of record is kept before cleanup removes it.</CardDescription></CardHeader>
        <form onSubmit={submit} data-testid="policy" key={retention.revision} className="grid gap-4">
          <CardContent className="grid gap-x-6 gap-y-3 sm:grid-cols-2 lg:grid-cols-3">
            {NAMES.map(([name, label]) => (
              <label key={name} className="grid gap-1.5 text-sm">
                <span className="text-muted-foreground">{label}</span>
                {can.retentionManage
                  ? <span className="relative"><Input name={name} type="number" min="1" defaultValue={retention.policy[name] ?? ''} aria-label={label} placeholder="forever" className="pr-14" />
                    <span className="pointer-events-none absolute top-1/2 right-3 -translate-y-1/2 text-xs text-muted-foreground">hours</span></span>
                  : <span className="font-medium">{retention.policy[name] ?? 'forever'}</span>}
              </label>
            ))}
          </CardContent>
          {can.retentionManage && <CardFooter className="justify-end">
            <CommandButton command={command} url={url} type="submit" variant="default" data-testid="save-policy" icon={IconDeviceFloppy}>Save policy</CommandButton>
          </CardFooter>}
        </form>
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
              <TableCell>{c.lastErrorCode ? <code className="text-destructive">{c.lastErrorCode}</code> : '—'}</TableCell>
            </TableRow>
          ))}</TableBody>
        </Table>
      </TableCard>
    </Layout>
  )
}
