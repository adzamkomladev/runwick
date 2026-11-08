import { Link } from '@inertiajs/react'
import { CommandBar, Layout, Pager, time, useCommand, usePoll } from '../lib'

export default function Workers({ base, can, workers, scopes, query }) {
  usePoll(['workers', 'scopes'], 10)
  const command = useCommand()
  const setCapacity = (s, e) => {
    e.preventDefault()
    const capacity = Number(new FormData(e.target).get('capacity'))
    command.ask(`Set ${s.kind}/${s.key} capacity to ${capacity}`,
      `${base}/scopes/${encodeURIComponent(s.kind)}/${encodeURIComponent(s.key)}/capacity`, { capacity, expectedPolicyRevision: s.policyRevision })
  }
  return (
    <Layout heading="Workers and capacity">
      <p>
        {query.expired ? <Link href={`${base}/workers`}>Hide expired</Link> : <Link href={`${base}/workers?expired=true`}>Show expired</Link>}
      </p>
      <table data-testid="workers">
        <thead><tr><th>Host</th><th>Process</th><th>Build</th><th>Status</th><th>Heartbeat</th><th>Expires</th></tr></thead>
        <tbody>{workers.items.map((w) => (
          <tr key={w.instanceId}><td>{w.hostName}</td><td>{w.processId}</td><td>{w.buildId}</td><td>{w.status}</td>
            <td>{time(w.heartbeatAt)}</td><td>{time(w.expiresAt)}</td></tr>
        ))}</tbody>
      </table>
      {workers.items.length === 0 && <p className="muted">No live workers.</p>}
      <Pager page={workers} />
      <h2>Concurrency scopes</h2>
      <CommandBar command={command} />
      <table data-testid="scopes">
        <thead><tr><th>Kind</th><th>Key</th><th>Capacity</th><th>Live permits</th><th>Revision</th>{can.capacityEdit && <th />}</tr></thead>
        <tbody>{scopes.items.map((s) => (
          <tr key={`${s.kind}/${s.key}`} data-testid="scope-row">
            <td>{s.kind}</td><td>{s.key}</td><td data-testid="scope-capacity">{s.capacity}</td><td>{s.livePermits}</td><td>{s.policyRevision}</td>
            {can.capacityEdit && <td>
              <form onSubmit={(e) => setCapacity(s, e)}>
                <input name="capacity" type="number" min="0" defaultValue={s.capacity} aria-label="capacity" />
                <button type="submit" data-testid="set-capacity">Set</button>
              </form>
            </td>}
          </tr>
        ))}</tbody>
      </table>
      <Pager page={scopes} cursorParam="scopeCursor" />
    </Layout>
  )
}
