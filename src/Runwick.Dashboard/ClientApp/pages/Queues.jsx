import { CommandBar, Layout, Pager, useCommand, usePoll } from '../lib'

export default function Queues({ base, can, queues }) {
  usePoll(['queues'], 10)
  const command = useCommand()
  return (
    <Layout heading="Queues">
      <p className="muted">Paused state and revision only. Queue depth and age are in the telemetry dashboard (runwick.queue.* metrics).</p>
      <CommandBar command={command} />
      <table data-testid="queues">
        <thead><tr><th>Queue</th><th>State</th><th>Revision</th>{can.queueControl && <th />}</tr></thead>
        <tbody>{queues.items.map((q) => (
          <tr key={q.name} data-testid="queue-row">
            <td>{q.name}</td>
            <td data-testid="queue-state">{q.paused ? 'paused' : 'running'}</td>
            <td>{q.revision}</td>
            {can.queueControl && <td>
              <button data-testid={`queue-${q.paused ? 'resume' : 'pause'}`}
                onClick={() => command.ask(`${q.paused ? 'Resume' : 'Pause'} ${q.name}`,
                  `${base}/queues/${encodeURIComponent(q.name)}/${q.paused ? 'resume' : 'pause'}`, { expectedRevision: q.revision })}>
                {q.paused ? 'Resume' : 'Pause'}
              </button>
            </td>}
          </tr>
        ))}</tbody>
      </table>
      <Pager page={queues} />
    </Layout>
  )
}
