import { Link } from '@inertiajs/react'
import { useState } from 'react'
import { CommandBar, Filters, Layout, Pager, time, useCommand, usePoll } from '../lib'

const FIELDS = [['queue', 'Queue'], ['state', 'State'], ['jobName', 'Job name'], ['from', 'Created from'], ['to', 'Created to']]

export default function Jobs({ base, can, jobs, query }) {
  usePoll(['jobs'], 5)
  const command = useCommand()
  const [selected, setSelected] = useState({})
  const chosen = jobs.items.filter((j) => selected[j.jobId])
  const toggle = (id) => setSelected((s) => ({ ...s, [id]: !s[id] }))
  const cancelSelected = () => command.bulk(`Cancel ${chosen.length} job(s)`, `${base}/jobs:cancel`,
    chosen.map((j) => ({ jobId: j.jobId, expectedGeneration: j.generation, expectedRevision: j.revision })))
  return (
    <Layout heading="Jobs">
      <Filters fields={FIELDS} values={query} />
      <CommandBar command={command} />
      {can.cancel && <button data-testid="bulk-cancel" disabled={chosen.length === 0 || chosen.length > 100} onClick={cancelSelected}>
        Cancel selected ({chosen.length})
      </button>}
      <table data-testid="jobs">
        <thead><tr>{can.cancel && <th />}<th>Job</th><th>Queue</th><th>State</th><th>Created</th><th>Revision</th></tr></thead>
        <tbody>
          {jobs.items.map((j) => (
            <tr key={j.jobId} data-testid="job-row">
              {can.cancel && <td><input type="checkbox" aria-label="select" checked={!!selected[j.jobId]} onChange={() => toggle(j.jobId)} /></td>}
              <td><Link href={`${base}/jobs/${j.jobId}`} data-testid="job-link">{j.jobName}</Link></td>
              <td data-testid="job-queue">{j.queue}</td>
              <td data-testid="job-state">{j.state}{j.cancelRequested && ' (cancel requested)'}</td>
              <td>{time(j.createdAt)}</td>
              <td>{j.revision}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {jobs.items.length === 0 && <p className="muted">No jobs in this range.</p>}
      <Pager page={jobs} />
    </Layout>
  )
}
