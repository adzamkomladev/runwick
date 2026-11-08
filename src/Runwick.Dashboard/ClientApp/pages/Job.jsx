import { router } from '@inertiajs/react'
import { CommandBar, Layout, time, useCommand, usePoll, useReveal } from '../lib'

export default function Job({ base, can, job: detail, audit }) {
  usePoll(['job'], 5)
  const command = useCommand()
  const { value, reveal } = useReveal()
  const job = detail.job
  const url = `${base}/jobs/${job.jobId}`
  const position = { expectedGeneration: job.generation, expectedRevision: job.revision }
  const control = (op, label) =>
    command.ask(label, `${url}/${op}`, position, op === 'cancel' && ['reserved', 'running'].includes(job.state) ? 'cancel requested' : 'done')
  return (
    <Layout heading={<span data-testid="job-name">{job.jobName}</span>}>
      <dl>
        <dt>ID</dt><dd><code>{job.jobId}</code></dd>
        <dt>Queue</dt><dd data-testid="job-queue">{job.queue}</dd>
        <dt>State</dt><dd data-testid="job-state">{job.state}{job.cancelRequested && <span data-testid="cancel-requested"> (cancel requested)</span>}</dd>
        <dt>Generation / revision</dt><dd><span data-testid="generation">{job.generation}</span> / <span data-testid="revision">{job.revision}</span></dd>
        <dt>Terminal reason</dt><dd data-testid="terminal-reason">{job.terminalReason ?? '—'}</dd>
        <dt>Created</dt><dd>{time(job.createdAt)}</dd>
        {job.detailExpiredAt && <><dt>Detail expired</dt><dd data-testid="detail-expired">{time(job.detailExpiredAt)}</dd></>}
      </dl>
      <CommandBar command={command} />
      <p>
        {can.cancel && <button data-testid="cancel" onClick={() => control('cancel', 'Cancel')}>Cancel</button>}
        {can.retry && <button data-testid="retry" onClick={() => control('retry', 'Retry')}>Retry</button>}
        {can.rerun && <button data-testid="rerun" onClick={() => control('rerun', 'Rerun')}>Rerun</button>}
        {can.revealPayload && <button data-testid="reveal" onClick={() => reveal('payload', `${url}/payload:reveal`)}>Reveal payload</button>}
      </p>
      {value?.key === 'payload' && <pre data-testid="payload">{value.text}</pre>}
      <h2>Generations</h2>
      <table>
        <thead><tr><th>#</th><th>Reason</th><th>By</th><th>Created</th><th>Started</th></tr></thead>
        <tbody>{detail.generations.map((g) => (
          <tr key={g.generation}><td>{g.generation}</td><td>{g.createdReason}</td><td>{g.createdBy ?? '—'}</td><td>{time(g.createdAt)}</td>
            <td>{g.startedCount} / {g.maxExecutions}</td></tr>
        ))}</tbody>
      </table>
      <h2>Attempts</h2>
      <table data-testid="attempts">
        <thead><tr><th>Reserved</th><th>Finished</th><th>Outcome</th><th>Error code</th><th /></tr></thead>
        <tbody>{detail.attempts.map((a) => (
          <tr key={a.attemptId}>
            <td>{time(a.reservedAt)}</td><td>{time(a.finishedAt)}</td><td>{a.outcome ?? '—'}</td>
            <td data-testid="error-code">{a.errorCode ?? '—'}</td>
            <td>
              {a.errorCode && can.revealError &&
                <button data-testid="reveal-error" onClick={() => reveal(a.attemptId, `${base}/attempts/${a.attemptId}/error:reveal`)}>Reveal error</button>}
              {value?.key === a.attemptId && <pre data-testid="error">{value.text}</pre>}
            </td>
          </tr>
        ))}</tbody>
      </table>
      {can.audit && <>
        <h2>Audit</h2>
        {audit ? (
          <ul data-testid="audit">{audit.items.map((e) => (
            <li key={e.auditId}>{time(e.occurredAt)} {e.operation} by {e.actorDisplay ?? e.actorSubject}{e.reason && <> — <span>{e.reason}</span></>}</li>
          ))}</ul>
        ) : <button data-testid="load-audit" onClick={() => router.reload({ only: ['audit'] })}>Show audit</button>}
      </>}
    </Layout>
  )
}
