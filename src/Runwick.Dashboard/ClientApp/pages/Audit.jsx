import { Filters, Layout, Pager, time, usePoll } from '../lib'

const FIELDS = [['targetKind', 'Target kind'], ['targetId', 'Target ID'], ['from', 'From'], ['to', 'To']]

export default function Audit({ audit, query }) {
  usePoll(['audit'], 15)
  return (
    <Layout heading="Audit">
      <Filters fields={FIELDS} values={query} />
      <table data-testid="audit">
        <thead><tr><th>When</th><th>Operation</th><th>Target</th><th>Actor</th><th>Reason</th><th>Revision</th></tr></thead>
        <tbody>{audit.items.map((a) => (
          <tr key={a.auditId} data-testid="audit-row">
            <td>{time(a.occurredAt)}</td><td>{a.operation}</td><td>{a.targetKind} <code>{a.targetId}</code></td>
            <td>{a.actorDisplay ?? a.actorSubject} ({a.actorScheme})</td><td data-testid="audit-reason">{a.reason ?? '—'}</td>
            <td>{a.expectedRevision ?? '—'} → {a.resultingRevision ?? '—'}</td>
          </tr>
        ))}</tbody>
      </table>
      <Pager page={audit} />
    </Layout>
  )
}
