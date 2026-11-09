import { router } from '@inertiajs/react'
import { IconBan, IconEye, IconLoader2, IconPlayerPlay, IconRefresh } from '@tabler/icons-react'
import { Fragment } from 'react'
import { Button } from '../components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../components/ui/table'
import { CommandBar, CommandButton, CopyButton, Layout, Revealed, StateBadge, TableCard, time, useCommand, usePoll, useReveal } from '../lib'

const Field = ({ label, children }) => <div className="grid gap-1"><dt className="text-xs text-muted-foreground">{label}</dt><dd className="min-w-0 text-sm">{children}</dd></div>

export default function Job({ base, can, job: detail, audit }) {
  const live = usePoll(['job'], 5)
  const command = useCommand()
  const { value, reveal, loading, clear } = useReveal()
  const job = detail.job
  const url = `${base}/jobs/${job.jobId}`
  const position = { expectedGeneration: job.generation, expectedRevision: job.revision }
  const control = (op, label) =>
    command.ask(label, `${url}/${op}`, position, op === 'cancel' && ['reserved', 'running'].includes(job.state) ? 'cancel requested' : 'done')
  return (
    <Layout heading={<span data-testid="job-name">{job.jobName}</span>} live={live}
      actions={<>
        {can.cancel && <CommandButton command={command} url={`${url}/cancel`} data-testid="cancel" icon={IconBan} onClick={() => control('cancel', 'Cancel')}>Cancel</CommandButton>}
        {can.retry && <CommandButton command={command} url={`${url}/retry`} data-testid="retry" icon={IconRefresh} onClick={() => control('retry', 'Retry')}>Retry</CommandButton>}
        {can.rerun && <CommandButton command={command} url={`${url}/rerun`} data-testid="rerun" icon={IconPlayerPlay} onClick={() => control('rerun', 'Rerun')}>Rerun</CommandButton>}
        {can.revealPayload && <Button size="sm" variant="secondary" data-testid="reveal" disabled={loading === 'payload'}
          onClick={() => reveal('payload', `${url}/payload:reveal`)}>{loading === 'payload' ? <IconLoader2 className="animate-spin" /> : <IconEye />}Reveal payload</Button>}
      </>}>
      <CommandBar command={command} />
      <Card>
        <CardContent>
          <dl className="grid gap-5 sm:grid-cols-2 lg:grid-cols-4">
            <Field label="ID"><span className="flex items-center gap-1"><code className="truncate">{job.jobId}</code><CopyButton value={job.jobId} label="Copy job ID" /></span></Field>
            <Field label="Queue"><span data-testid="job-queue">{job.queue}</span></Field>
            <Field label="State"><span data-testid="job-state"><StateBadge state={job.state} key={job.state} className="animate-page-in" />{job.cancelRequested && <span data-testid="cancel-requested" className="ml-1.5 text-xs text-warning"> (cancel requested)</span>}</span></Field>
            <Field label="Generation / revision"><span className="tabular-nums"><span data-testid="generation">{job.generation}</span> / <span data-testid="revision">{job.revision}</span></span></Field>
            <Field label="Terminal reason"><span data-testid="terminal-reason">{job.terminalReason ?? '—'}</span></Field>
            <Field label="Created">{time(job.createdAt)}</Field>
            {job.detailExpiredAt && <Field label="Detail expired"><span data-testid="detail-expired">{time(job.detailExpiredAt)}</span></Field>}
          </dl>
        </CardContent>
      </Card>
      {value?.key === 'payload' && <Revealed value={value} testId="payload" title="Payload" onHide={clear} />}
      <TableCard title="Generations">
        <Table>
          <TableHeader><TableRow className="hover:bg-transparent"><TableHead>#</TableHead><TableHead>Reason</TableHead><TableHead>By</TableHead><TableHead>Created</TableHead><TableHead>Started</TableHead></TableRow></TableHeader>
          <TableBody>{detail.generations.map((g) => (
            <TableRow key={g.generation}><TableCell className="tabular-nums">{g.generation}</TableCell><TableCell>{g.createdReason}</TableCell><TableCell>{g.createdBy ?? '—'}</TableCell>
              <TableCell className="whitespace-nowrap text-muted-foreground">{time(g.createdAt)}</TableCell><TableCell className="tabular-nums">{g.startedCount} / {g.maxExecutions}</TableCell></TableRow>
          ))}</TableBody>
        </Table>
      </TableCard>
      <TableCard title="Attempts" description="Errors show as a code until revealed." empty={detail.attempts.length === 0 && 'No attempts yet.'}>
        <Table data-testid="attempts">
          <TableHeader><TableRow className="hover:bg-transparent"><TableHead>Reserved</TableHead><TableHead>Finished</TableHead><TableHead>Outcome</TableHead><TableHead>Error code</TableHead><TableHead /></TableRow></TableHeader>
          <TableBody>{detail.attempts.map((a) => (
            <Fragment key={a.attemptId}>
              <TableRow>
                <TableCell className="whitespace-nowrap text-muted-foreground">{time(a.reservedAt)}</TableCell><TableCell className="whitespace-nowrap text-muted-foreground">{time(a.finishedAt)}</TableCell>
                <TableCell>{a.outcome ? <StateBadge state={a.outcome} /> : '—'}</TableCell>
                <TableCell data-testid="error-code" className="max-w-72 font-mono text-xs break-all">{a.errorCode ?? '—'}</TableCell>
                <TableCell className="text-right">
                  {a.errorCode && can.revealError &&
                    <Button size="sm" variant="ghost" data-testid="reveal-error" disabled={loading === a.attemptId}
                      onClick={() => reveal(a.attemptId, `${base}/attempts/${a.attemptId}/error:reveal`)}>
                      {loading === a.attemptId ? <IconLoader2 className="animate-spin" /> : <IconEye />}Reveal error
                    </Button>}
                </TableCell>
              </TableRow>
              {value?.key === a.attemptId && <TableRow className="hover:bg-transparent"><TableCell colSpan={5}>
                <Revealed value={value} testId="error" title="Error" onHide={clear} />
              </TableCell></TableRow>}
            </Fragment>
          ))}</TableBody>
        </Table>
      </TableCard>
      {can.audit && <Card>
        <CardHeader><CardTitle>Audit</CardTitle></CardHeader>
        <CardContent>
          {audit ? (
            <ol data-testid="audit" className="grid animate-page-in gap-0 border-l pl-4">
              {audit.items.map((e) => (
                <li key={e.auditId} className="relative pb-4 text-sm last:pb-0">
                  <span aria-hidden className="absolute top-1.5 -left-[21px] size-2.5 rounded-full border-2 border-card bg-primary" />
                  <span className="font-medium">{e.operation}</span> by {e.actorDisplay ?? e.actorSubject}
                  {e.reason && <> — <span>{e.reason}</span></>}
                  <span className="block text-xs text-muted-foreground">{time(e.occurredAt)}</span>
                </li>
              ))}
              {audit.items.length === 0 && <li className="text-sm text-muted-foreground">No audit rows for this job.</li>}
            </ol>
          ) : <Button size="sm" variant="outline" data-testid="load-audit" onClick={() => router.reload({ only: ['audit'] })}>Show audit</Button>}
        </CardContent>
      </Card>}
    </Layout>
  )
}
