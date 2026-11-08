import { useState } from 'react'
import { CommandBar, Layout, Pager, time, useCommand, usePoll } from '../lib'

export default function Schedules({ base, can, schedules }) {
  usePoll(['schedules'], 10)
  const command = useCommand()
  const [policy, setPolicy] = useState('skipPaused')
  const url = (s, op) => `${base}/schedules/${encodeURIComponent(s.key)}/${op}`
  return (
    <Layout heading="Schedules">
      <CommandBar command={command} />
      {can.scheduleControl && <label>Resume policy{' '}
        <select value={policy} onChange={(e) => setPolicy(e.target.value)} data-testid="resume-policy">
          <option value="skipPaused">skip paused occurrences</option>
          <option value="runOnce">run once</option>
          <option value="catchUp">catch up</option>
        </select>
      </label>}
      <table data-testid="schedules">
        <thead><tr><th>Key</th><th>State</th><th>Next due</th><th>Last run</th><th>Revisions</th><th /></tr></thead>
        <tbody>{schedules.items.map((s) => (
          <tr key={s.key} data-testid="schedule-row">
            <td>{s.key}</td>
            <td data-testid="schedule-state">{s.paused ? 'paused' : 'active'}{s.needsReview && ' (needs review)'}</td>
            <td>{time(s.nextDueAt)}</td><td>{time(s.lastMaterializedAt)}</td>
            <td>definition {s.definitionRevision}, control {s.controlRevision}</td>
            <td>
              {can.scheduleControl && (s.paused
                ? <button data-testid="schedule-resume" onClick={() => command.ask(`Resume ${s.key}`, url(s, 'resume'), { expectedRevision: s.controlRevision, policy })}>Resume</button>
                : <button data-testid="schedule-pause" onClick={() => command.ask(`Pause ${s.key}`, url(s, 'pause'), { expectedRevision: s.controlRevision })}>Pause</button>)}
              {can.scheduleTrigger && <button data-testid="schedule-trigger"
                onClick={() => command.ask(`Trigger ${s.key}`, url(s, 'trigger'), { expectedRevision: s.definitionRevision })}>Trigger</button>}
              {can.scheduleEdit && s.needsReview && <button data-testid="schedule-revalidate"
                onClick={() => command.ask(`Revalidate ${s.key}`, url(s, 'revalidate'), { expectedRevision: s.definitionRevision })}>Revalidate</button>}
            </td>
          </tr>
        ))}</tbody>
      </table>
      <Pager page={schedules} />
    </Layout>
  )
}
