import { IconBolt, IconPlayerPause, IconPlayerPlay, IconShieldCheck } from '@tabler/icons-react'
import { useState } from 'react'
import { Label } from '../components/ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../components/ui/select'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../components/ui/table'
import { cn } from '../components/ui/utils'
import { CommandBar, CommandButton, Layout, Pager, StateBadge, TableCard, time, useChanged, useCommand, usePoll, useSort } from '../lib'

export default function Schedules({ base, can, schedules }) {
  const live = usePoll(['schedules'], 10)
  const command = useCommand()
  const [policy, setPolicy] = useState('skipPaused')
  const [rows, head] = useSort(schedules.items)
  const changed = useChanged(schedules.items, (s) => s.key, (s) => `${s.paused}/${s.lastMaterializedAt}`)
  const url = (s, op) => `${base}/schedules/${encodeURIComponent(s.key)}/${op}`
  return (
    <Layout heading="Schedules" live={live}>
      <CommandBar command={command} />
      <TableCard pager={<Pager page={schedules} />} empty={schedules.items.length === 0 && 'No schedules.'}
        toolbar={can.scheduleControl && <div className="grid gap-1.5">
          <Label htmlFor="resume-policy" className="text-xs text-muted-foreground">Resume policy</Label>
          <Select value={policy} onValueChange={setPolicy}>
            <SelectTrigger id="resume-policy" data-testid="resume-policy" className="h-8 min-w-52"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="skipPaused">skip paused occurrences</SelectItem>
              <SelectItem value="runOnce">run once</SelectItem>
              <SelectItem value="catchUp">catch up</SelectItem>
            </SelectContent>
          </Select>
        </div>}>
        <Table data-testid="schedules">
          <TableHeader><TableRow className="hover:bg-transparent">
            {head('key', 'Key', (s) => s.key)}{head('state', 'State', (s) => (s.paused ? 1 : 0))}{head('due', 'Next due', (s) => Date.parse(s.nextDueAt))}
            {head('last', 'Last run', (s) => Date.parse(s.lastMaterializedAt))}<TableHead>Revisions</TableHead><TableHead />
          </TableRow></TableHeader>
          <TableBody>{rows.map((s) => (
            <TableRow key={`${s.key}:${changed[s.key] ?? 0}`} data-testid="schedule-row" className={cn(changed[s.key] && 'animate-flash')}>
              <TableCell className="font-medium">{s.key}</TableCell>
              <TableCell data-testid="schedule-state"><StateBadge state={s.paused ? 'paused' : 'active'} />{s.needsReview && <span className="ml-1.5 text-xs text-warning"> (needs review)</span>}</TableCell>
              <TableCell className="whitespace-nowrap">{time(s.nextDueAt)}</TableCell><TableCell className="whitespace-nowrap text-muted-foreground">{time(s.lastMaterializedAt)}</TableCell>
              <TableCell className="text-xs whitespace-nowrap text-muted-foreground">definition {s.definitionRevision}, control {s.controlRevision}</TableCell>
              <TableCell>
                <div className="flex justify-end gap-2">
                  {can.scheduleControl && (s.paused
                    ? <CommandButton command={command} url={url(s, 'resume')} data-testid="schedule-resume" icon={IconPlayerPlay}
                      onClick={() => command.ask(`Resume ${s.key}`, url(s, 'resume'), { expectedRevision: s.controlRevision, policy })}>Resume</CommandButton>
                    : <CommandButton command={command} url={url(s, 'pause')} data-testid="schedule-pause" icon={IconPlayerPause}
                      onClick={() => command.ask(`Pause ${s.key}`, url(s, 'pause'), { expectedRevision: s.controlRevision })}>Pause</CommandButton>)}
                  {can.scheduleTrigger && <CommandButton command={command} url={url(s, 'trigger')} data-testid="schedule-trigger" icon={IconBolt}
                    onClick={() => command.ask(`Trigger ${s.key}`, url(s, 'trigger'), { expectedRevision: s.definitionRevision })}>Trigger</CommandButton>}
                  {can.scheduleEdit && s.needsReview && <CommandButton command={command} url={url(s, 'revalidate')} data-testid="schedule-revalidate" icon={IconShieldCheck}
                    onClick={() => command.ask(`Revalidate ${s.key}`, url(s, 'revalidate'), { expectedRevision: s.definitionRevision })}>Revalidate</CommandButton>}
                </div>
              </TableCell>
            </TableRow>
          ))}</TableBody>
        </Table>
      </TableCard>
    </Layout>
  )
}
