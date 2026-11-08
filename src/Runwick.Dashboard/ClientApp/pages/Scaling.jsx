import { Filters, Layout, Pager, time, usePoll } from '../lib'

const FIELDS = [['instance', 'Instance'], ['pool', 'Pool'], ['from', 'From'], ['to', 'To']]

export default function Scaling({ events, query }) {
  usePoll(['events'], 15)
  return (
    <Layout heading="Scaling history">
      <Filters fields={FIELDS} values={query} />
      <table data-testid="events">
        <thead><tr><th>When</th><th>Instance</th><th>Pool</th><th>Target</th><th>Desired</th><th>Candidate</th><th>Reason</th></tr></thead>
        <tbody>{events.items.map((e) => (
          <tr key={e.eventId} data-testid="event-row">
            <td>{time(e.createdAt)}</td><td><code>{e.instanceId}</code></td><td data-testid="event-pool">{e.pool}</td>
            <td>{e.oldTarget} → {e.newTarget}</td><td>{e.desired ?? '—'}</td><td>{e.candidate ?? '—'}</td>
            <td data-testid="event-reason">{e.reason}</td>
          </tr>
        ))}</tbody>
      </table>
      {events.items.length === 0 && <p className="muted">No scaling events in this range.</p>}
      <Pager page={events} />
    </Layout>
  )
}
