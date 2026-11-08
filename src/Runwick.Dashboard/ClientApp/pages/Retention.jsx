import { CommandBar, Layout, time, useCommand, usePoll } from '../lib'

const NAMES = [['completedDetailHours', 'Completed job detail'], ['failedDetailHours', 'Failed job detail'], ['jobHistoryHours', 'Job history'],
  ['auditHours', 'Audit'], ['scalingEventsHours', 'Scaling events'], ['scheduleOccurrencesHours', 'Schedule occurrences'],
  ['workerPresenceHours', 'Worker presence']]

export default function Retention({ base, can, retention }) {
  usePoll(['retention'], 30)
  const command = useCommand()
  const submit = (e) => {
    e.preventDefault()
    const form = new FormData(e.target)
    const body = { expectedRevision: retention.revision }
    for (const [name] of NAMES) body[name] = form.get(name) === '' ? null : Number(form.get(name))
    command.ask('Change the retention policy', `${base}/retention/policy`, body)
  }
  return (
    <Layout heading="Retention">
      <p>Policy revision <span data-testid="retention-revision">{retention.revision}</span> (values in hours; empty keeps forever).</p>
      <CommandBar command={command} />
      <form onSubmit={submit} data-testid="policy" key={retention.revision}>
        <table><tbody>{NAMES.map(([name, label]) => (
          <tr key={name}><th>{label}</th><td>
            {can.retentionManage
              ? <input name={name} type="number" min="1" defaultValue={retention.policy[name] ?? ''} aria-label={label} />
              : <span>{retention.policy[name] ?? 'forever'}</span>}
          </td></tr>
        ))}</tbody></table>
        {can.retentionManage && <button type="submit" data-testid="save-policy">Save policy</button>}
      </form>
      <h2>Cleanup progress</h2>
      <table data-testid="classes">
        <thead><tr><th>Record class</th><th>Last batch</th><th>Caught up</th><th>Rows removed</th><th>Today</th><th>Last error</th></tr></thead>
        <tbody>{retention.classes.map((c) => (
          <tr key={c.recordClass}><td>{c.recordClass}{c.behind && ' (behind)'}</td><td>{time(c.lastBatchAt)}</td><td>{time(c.caughtUpAt)}</td>
            <td>{c.totalRows}</td><td>{c.todayRows} rows, {c.todayBatches} batches, {c.todayFailures} failures</td><td>{c.lastErrorCode ?? '—'}</td></tr>
        ))}</tbody>
      </table>
      <p className="pager">as of {new Date(retention.observedAt).toLocaleTimeString()}</p>
    </Layout>
  )
}
