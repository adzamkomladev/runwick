import { router } from '@inertiajs/react'
import { IconBan, IconCpu, IconEye, IconLoader2, IconPlayerPlay, IconRefresh } from '@tabler/icons-react'
import { Fragment } from 'react'
import { buildSteps, duration, Timeline } from '../components/timeline'
import { Button } from '../components/ui/button'
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from '../components/ui/card'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../components/ui/table'
import { CommandBar, CommandButton, CopyButton, Layout, Revealed, StateBadge, TableCard, time, useCommand, usePoll, useReveal } from '../lib'

const Field = ({ label, children }) => <div className="grid min-w-0 gap-1"><dt className="text-xs text-muted-foreground">{label}</dt><dd className="min-w-0 text-sm">{children}</dd></div>

const ran = (a) => a.authorizedAt && a.finishedAt ? duration(Date.parse(a.finishedAt) - Date.parse(a.authorizedAt)) : '—'

export default function Job({ base, can, job: detail, audit }) {
  const live = usePoll(['job'], 5)
  const command = useCommand()
  const { value, reveal, loading, clear } = useReveal()
  const job = detail.job
  const url = `${base}/jobs/${job.jobId}`
  const position = { expectedGeneration: job.generation, expectedRevision: job.revision }
  const control = (op, label) =>
    command.ask(label, `${url}/${op}`, position, op === 'cancel' && ['reserved', 'running'].includes(job.state) ? 'cancel requested' : 'done')
  const total = job.terminalAt ? duration(Date.parse(job.terminalAt) - Date.parse(job.createdAt)) : null
  return (
    <Layout live={live}
      heading={<span className="flex min-w-0 items-center gap-3"><span data-testid="job-name" className="truncate">{job.jobName}</span><StateBadge state={job.state} key={job.state} className="animate-page-in" /></span>}
      description={<span className="flex items-center gap-1"><code className="text-xs">{job.jobId}</code><CopyButton value={job.jobId} label="Copy job ID" /></span>}
      actions={<>
        {can.cancel && <CommandButton command={command} url={`${url}/cancel`} data-testid="cancel" icon={IconBan} onClick={() => control('cancel', 'Cancel')}>Cancel</CommandButton>}
        {can.retry && <CommandButton command={command} url={`${url}/retry`} data-testid="retry" icon={IconRefresh} onClick={() => control('retry', 'Retry')}>Retry</CommandButton>}
        {can.rerun && <CommandButton command={command} url={`${url}/rerun`} data-testid="rerun" icon={IconPlayerPlay} onClick={() => control('rerun', 'Rerun')}>Rerun</CommandButton>}
        {can.revealPayload && <Button size="sm" variant="secondary" data-testid="reveal" disabled={loading === 'payload'}
          onClick={() => reveal('payload', `${url}/payload:reveal`)}>{loading === 'payload' ? <IconLoader2 className="animate-spin" /> : <IconEye />}Reveal payload</Button>}
      </>}>
      <CommandBar command={command} />
      <div className="grid items-start gap-6 xl:grid-cols-[minmax(0,1fr)_24rem]">
        <div className="grid min-w-0 gap-6">
          <Card>
            <CardContent>
              <dl className="grid gap-5 sm:grid-cols-2 lg:grid-cols-4">
                <Field label="Queue"><span data-testid="job-queue" className="font-medium">{job.queue}</span></Field>
                <Field label="State"><span data-testid="job-state"><StateBadge state={job.state} />{job.cancelRequested && <span data-testid="cancel-requested" className="ml-1.5 text-xs text-amber-700 dark:text-amber-300"> (cancel requested)</span>}</span></Field>
                <Field label="Generation / revision"><span className="tabular-nums"><span data-testid="generation">{job.generation}</span> / <span data-testid="revision">{job.revision}</span></span></Field>
                <Field label="Executions">{job.executionCount}</Field>
                <Field label="Created">{time(job.createdAt)}</Field>
                <Field label="Ended">{job.terminalAt ? <>{time(job.terminalAt)} <span className="text-xs text-muted-foreground">({total} in all)</span></> : '—'}</Field>
                <Field label="Terminal reason"><span data-testid="terminal-reason">{job.terminalReason ?? '—'}</span></Field>
                <Field label="Priority">{job.priority}</Field>
                {job.detailExpiredAt && <Field label="Detail expired"><span data-testid="detail-expired">{time(job.detailExpiredAt)}</span></Field>}
              </dl>
            </CardContent>
          </Card>
          {value?.key === 'payload' && <Revealed value={value} testId="payload" title="Payload" onHide={clear} />}
          <TableCard title="Attempts" description="Errors show as a code until revealed." empty={detail.attempts.length === 0 && 'No attempts yet.'}>
            <Table data-testid="attempts">
              <TableHeader><TableRow className="hover:bg-transparent"><TableHead>#</TableHead><TableHead>Reserved</TableHead><TableHead>Ran for</TableHead><TableHead>Outcome</TableHead><TableHead>Error code</TableHead><TableHead /></TableRow></TableHeader>
              <TableBody>{detail.attempts.map((a, i) => (
                <Fragment key={a.attemptId}>
                  <TableRow>
                    <TableCell className="text-muted-foreground tabular-nums">{a.executionNumber ?? i + 1}</TableCell>
                    <TableCell className="whitespace-nowrap text-muted-foreground">{time(a.reservedAt)}</TableCell>
                    <TableCell className="whitespace-nowrap tabular-nums">{ran(a)}</TableCell>
                    <TableCell>{a.outcome ? <StateBadge state={a.outcome} /> : <StateBadge state="running" />}</TableCell>
                    <TableCell data-testid="error-code" className="max-w-72 font-mono text-xs break-all">{a.errorCode ?? '—'}</TableCell>
                    <TableCell className="text-right">
                      {a.errorCode && can.revealError &&
                        <Button size="sm" variant="ghost" data-testid="reveal-error" disabled={loading === a.attemptId}
                          onClick={() => reveal(a.attemptId, `${base}/attempts/${a.attemptId}/error:reveal`)}>
                          {loading === a.attemptId ? <IconLoader2 className="animate-spin" /> : <IconEye />}Reveal error
                        </Button>}
                    </TableCell>
                  </TableRow>
                  {value?.key === a.attemptId && <TableRow className="hover:bg-transparent"><TableCell colSpan={6}>
                    <Revealed value={value} testId="error" title="Error" onHide={clear} />
                  </TableCell></TableRow>}
                </Fragment>
              ))}</TableBody>
            </Table>
          </TableCard>
          <Card className="gap-3">
            <CardHeader>
              <CardTitle className="flex items-center gap-2"><IconCpu className="size-4 text-muted-foreground" />CPU and memory</CardTitle>
              <CardDescription>Not recorded. Runwick does not yet measure CPU time or memory per attempt, so there is nothing true to show here.</CardDescription>
            </CardHeader>
            <CardContent className="grid gap-3 sm:grid-cols-3">
              {['CPU time', 'Allocated', 'Working set'].map((m) => (
                <div key={m} className="grid gap-1 rounded-lg border border-dashed p-3">
                  <span className="text-xs text-muted-foreground">{m}</span><span className="text-sm font-medium text-muted-foreground">not recorded</span>
                </div>
              ))}
            </CardContent>
          </Card>
          <TableCard title="Generations">
            <Table>
              <TableHeader><TableRow className="hover:bg-transparent"><TableHead>#</TableHead><TableHead>Reason</TableHead><TableHead>By</TableHead><TableHead>Created</TableHead><TableHead>Started</TableHead></TableRow></TableHeader>
              <TableBody>{detail.generations.map((g) => (
                <TableRow key={g.generation}><TableCell className="tabular-nums">{g.generation}</TableCell><TableCell>{g.createdReason}</TableCell><TableCell>{g.createdBy ?? '—'}</TableCell>
                  <TableCell className="whitespace-nowrap text-muted-foreground">{time(g.createdAt)}</TableCell><TableCell className="tabular-nums">{g.startedCount} / {g.maxExecutions}</TableCell></TableRow>
              ))}</TableBody>
            </Table>
          </TableCard>
        </div>
        <div className="grid min-w-0 gap-6 xl:sticky xl:top-20">
          <Card>
            <CardHeader>
              <CardTitle>Status history</CardTitle>
              <CardDescription>Each step with its time and how long the previous one lasted.</CardDescription>
            </CardHeader>
            <CardContent><Timeline key={job.jobId} steps={buildSteps(detail, audit)} /></CardContent>
          </Card>
          {can.audit && <Card>
            <CardHeader><CardTitle>Audit</CardTitle><CardDescription>Who acted on this job, and why.</CardDescription>
              {!audit && <CardAction><Button size="sm" variant="outline" data-testid="load-audit" onClick={() => router.reload({ only: ['audit'] })}>Show audit</Button></CardAction>}
            </CardHeader>
            {audit && <CardContent>
              <ol data-testid="audit" className="grid animate-page-in gap-3">
                {audit.items.map((e) => (
                  <li key={e.auditId} className="grid gap-0.5 rounded-lg border p-3 text-sm">
                    <span><span className="font-medium">{e.operation}</span> by {e.actorDisplay ?? e.actorSubject}</span>
                    {e.reason && <span className="text-muted-foreground">{e.reason}</span>}
                    <span className="text-xs text-muted-foreground">{time(e.occurredAt)}</span>
                  </li>
                ))}
                {audit.items.length === 0 && <li className="text-sm text-muted-foreground">No audit rows for this job.</li>}
              </ol>
            </CardContent>}
          </Card>}
        </div>
      </div>
    </Layout>
  )
}
