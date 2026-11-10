import { IconAdjustments, IconRocket, IconServer, IconStack3 } from '@tabler/icons-react'
import { CpuCell, MemoryCell, Meter, hostOf, modeText, poolsFrom } from '../components/host'
import { Field, StatCard } from '../components/stat'
import { relative } from '../components/timeline'
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../components/ui/table'
import { Layout, NameLink, OpenButton, StateBadge, TableCard, usePoll } from '../lib'
import { settingName, settingValue } from './Worker'

export default function Pool({ base, workers, events, query }) {
  const live = usePoll(['workers', 'events'], 10)
  const name = query.name
  const p = poolsFrom(workers ?? []).find((x) => x.name === name)
  const items = events?.items ?? []
  return (
    <Layout live={live} heading={<span data-testid="pool-name" className="truncate">{name}</span>}
      description={p ? `${p.workers.length} live worker${p.workers.length === 1 ? '' : 's'} run this pool. Read-only: pools are set in worker options.` : 'No live worker runs this pool.'}>
      {p && <>
        <section aria-label="Pool stats" className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
          <StatCard testId="stat-in-flight" icon={IconRocket} label="In flight" value={p.inFlight} foot={`Of a concurrency of ${p.target} across workers`}>
            <Meter value={p.target ? p.inFlight / p.target : 0} label="In flight of the concurrency" />
          </StatCard>
          <StatCard testId="stat-concurrency" icon={IconStack3} label="Concurrency now" value={p.target} foot="Sum of the workers' current targets" />
          <StatCard testId="stat-mode" icon={IconAdjustments} label="Mode" value={modeText(p)} foot={p.mode === 'auto' ? 'Limits per worker; the scaler moves the target between them' : 'Per worker'} />
          <StatCard testId="stat-workers" icon={IconServer} label="Workers" value={p.workers.length} />
        </section>
        <div className="grid items-start gap-6 xl:grid-cols-[minmax(0,1fr)_24rem]">
          <div className="grid min-w-0 gap-6">
            <TableCard title="Workers">
              <Table data-testid="pool-workers">
                <TableHeader><TableRow className="hover:bg-transparent"><TableHead>Host</TableHead><TableHead>Status</TableHead><TableHead>In flight</TableHead><TableHead className="hidden md:table-cell">CPU</TableHead><TableHead className="hidden lg:table-cell">Memory</TableHead><TableHead className="w-10" /></TableRow></TableHeader>
                <TableBody>{p.workers.map(({ worker: w, target, inFlight }) => (
                  <TableRow key={w.instanceId}>
                    <TableCell><NameLink href={`${base}/workers/${w.instanceId}`}>{w.hostName}</NameLink> <span className="text-xs text-muted-foreground tabular-nums">pid {w.processId}</span></TableCell>
                    <TableCell><StateBadge state={w.status} /></TableCell>
                    <TableCell className="tabular-nums">{inFlight ?? '—'} / {target}</TableCell>
                    <TableCell className="hidden md:table-cell"><CpuCell host={hostOf(w)} /></TableCell>
                    <TableCell className="hidden lg:table-cell"><MemoryCell host={hostOf(w)} /></TableCell>
                    <TableCell className="text-right"><OpenButton href={`${base}/workers/${w.instanceId}`} label={`Open worker ${w.hostName}`} /></TableCell>
                  </TableRow>
                ))}</TableBody>
              </Table>
            </TableCard>
            {events && <TableCard title="Scaling, last 24 h" empty={items.length === 0 && 'No target changes.'}>
              <Table data-testid="pool-events">
                <TableHeader><TableRow className="hover:bg-transparent"><TableHead>When</TableHead><TableHead>Target</TableHead><TableHead>Reason</TableHead><TableHead className="hidden md:table-cell">Worker</TableHead></TableRow></TableHeader>
                <TableBody>{items.slice(0, 20).map((e) => (
                  <TableRow key={e.eventId}>
                    <TableCell className="whitespace-nowrap text-muted-foreground">{relative(e.createdAt)}</TableCell>
                    <TableCell className="tabular-nums">{e.oldTarget} → {e.newTarget}</TableCell>
                    <TableCell className="text-muted-foreground">{e.reason}</TableCell>
                    <TableCell className="hidden md:table-cell"><NameLink href={`${base}/workers/${e.instanceId}`} className="font-normal">{e.instanceId.slice(0, 8)}</NameLink></TableCell>
                  </TableRow>
                ))}</TableBody>
              </Table>
            </TableCard>}
          </div>
          <div className="grid min-w-0 gap-6">
            <Card>
              <CardHeader><CardTitle className="text-base">Queues and weights</CardTitle></CardHeader>
              <CardContent>
                {!p.known ? <p className="text-sm text-muted-foreground">These workers publish no pool detail.</p>
                  : <ul data-testid="pool-queues" className="grid gap-2">{p.queues.map((q) => {
                    const total = p.queues.reduce((t, x) => t + x.weight, 0) || 1
                    return (
                      <li key={q.queue} className="grid gap-1 text-sm">
                        <span className="flex justify-between gap-2"><NameLink href={`${base}/queues/${encodeURIComponent(q.queue)}`}>{q.queue}</NameLink><span className="tabular-nums text-muted-foreground">×{q.weight}</span></span>
                        <Meter value={q.weight / total} label={`Share of ${q.queue}`} />
                      </li>
                    )
                  })}</ul>}
              </CardContent>
            </Card>
            {p.known && <Card>
              <CardHeader><CardTitle className="text-base">Settings</CardTitle></CardHeader>
              <CardContent>
                <dl data-testid="pool-settings" className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                  <Field label="Mode">{p.mode}</Field>
                  <Field label="Limits">{p.min ?? '—'} – {p.max ?? '—'}</Field>
                  <Field label="Lane selection">{p.laneSelection ?? '—'}</Field>
                  <Field label="Lane weights">{p.laneWeights ? `high ${p.laneWeights.high}, medium ${p.laneWeights.medium}, low ${p.laneWeights.low}` : '—'}</Field>
                  {Object.entries(p.settings ?? {}).map(([k, v]) => <Field key={k} label={settingName(k)}><span className="tabular-nums">{settingValue(k, v)}</span></Field>)}
                </dl>
              </CardContent>
            </Card>}
          </div>
        </div>
      </>}
    </Layout>
  )
}
