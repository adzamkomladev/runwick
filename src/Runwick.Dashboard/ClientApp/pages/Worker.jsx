import { IconActivityHeartbeat, IconCpu, IconDeviceDesktop, IconMemory, IconRocket, IconTag } from '@tabler/icons-react'
import { bytes, cpu } from '../components/usage'
import { hostOf, Meter, memoryUsed, modeText, percent } from '../components/host'
import { duration, relative } from '../components/timeline'
import { Field, ms, StatCard } from '../components/stat'
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../components/ui/table'
import { Copyable, Layout, NameLink, OpenButton, StateBadge, TableCard, time, usePoll } from '../lib'

/** "LeaseDurationMs" → "Lease duration". */
export const settingName = (k) => k.replace(/Ms$/, '').replace(/([A-Z])/g, ' $1').replace(/^./, (c) => c.toUpperCase()).trim()
export const settingValue = (k, v) => (typeof v === 'boolean' ? (v ? 'on' : 'off') : k.endsWith('Ms') ? ms(v) : String(v))

export default function Worker({ base, can, worker: w, events }) {
  const live = usePoll(['worker', 'events'], 5)
  const host = hostOf(w)
  const d = w.detail
  const now = Date.now()
  const ttl = Math.max(Date.parse(w.expiresAt) - Date.parse(w.heartbeatAt), 1)
  const running = d?.running ?? []
  const recent = d?.recent ?? []
  return (
    <Layout live={live}
      heading={<span className="flex min-w-0 items-center gap-3"><span data-testid="worker-host" className="truncate">{w.hostName}</span><StateBadge state={w.status} /></span>}
      description={<Copyable value={w.instanceId} label="Copy instance ID"><code className="text-xs">{w.instanceId}</code></Copyable>}>
      {!d && <p data-testid="no-detail" className="rounded-lg border border-dashed px-4 py-3 text-sm text-muted-foreground">This worker publishes no host or pool detail (an older Runwick version); only its presence row is shown.</p>}
      <section aria-label="Worker stats" className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <StatCard testId="stat-cpu" icon={IconCpu} label="Process CPU" value={host ? percent(host.processCpuPercent) : '—'}
          foot={host ? `Of ${host.cpus} CPU${host.cpus === 1 ? '' : 's'} on this machine` : null}>
          {host && <Meter value={(host.processCpuPercent ?? 0) / 100} label="Process CPU" />}
        </StatCard>
        <StatCard testId="stat-memory" icon={IconMemory} label="Working set" value={host ? bytes(host.workingSetBytes) : '—'}
          foot={host ? `Machine: ${bytes(host.memoryAvailableBytes)} free of ${bytes(host.memoryTotalBytes)} · GC heap ${bytes(host.gcHeapBytes)}` : null}>
          {host && <Meter value={memoryUsed(host)} label="Machine memory in use" />}
        </StatCard>
        <StatCard testId="stat-heartbeat" icon={IconActivityHeartbeat} label="Heartbeat" value={relative(w.heartbeatAt, now)} foot={`Expires ${relative(w.expiresAt, now)}`}>
          <Meter value={Math.max(0, 1 - (now - Date.parse(w.heartbeatAt)) / ttl)} label="Time left before the presence expires" />
        </StatCard>
        <StatCard testId="stat-running" icon={IconRocket} label="Running now" value={d?.runningCount ?? running.length} foot={`Started ${relative(w.startedAt, now)} (${duration(now - Date.parse(w.startedAt))} up)`} />
      </section>
      <div className="grid items-start gap-6 xl:grid-cols-[minmax(0,1fr)_24rem]">
        <div className="grid min-w-0 gap-6">
          <TableCard title="Pools" empty={Object.keys(w.poolTargets ?? {}).length === 0 && 'No pools.'}>
            <Table data-testid="worker-pools">
              <TableHeader><TableRow className="hover:bg-transparent"><TableHead>Pool</TableHead><TableHead>Concurrency</TableHead><TableHead>In flight</TableHead><TableHead>Queues</TableHead><TableHead className="w-10" /></TableRow></TableHeader>
              <TableBody>{Object.entries(w.poolTargets ?? {}).map(([name, target]) => {
                const p = d?.pools?.find((x) => x.name === name)
                return (
                  <TableRow key={name}>
                    <TableCell><NameLink href={`${base}/pools/${encodeURIComponent(name)}`}>{name}</NameLink></TableCell>
                    <TableCell className="whitespace-nowrap tabular-nums">{target}{p && <span className="text-muted-foreground"> · {modeText({ ...p, target })}</span>}</TableCell>
                    <TableCell className="tabular-nums">{p?.inFlight ?? '—'}</TableCell>
                    <TableCell className="text-xs">{p ? p.queues.map((q) => <span key={q.queue} className="mr-2 inline-block"><NameLink href={`${base}/queues/${encodeURIComponent(q.queue)}`} className="font-normal">{q.queue}</NameLink> <span className="text-muted-foreground">×{q.weight}</span></span>) : '—'}</TableCell>
                    <TableCell className="text-right"><OpenButton href={`${base}/pools/${encodeURIComponent(name)}`} label={`Open pool ${name}`} /></TableCell>
                  </TableRow>
                )
              })}</TableBody>
            </Table>
          </TableCard>
          {d && <TableCard title="Running now" description={d.runningCount > running.length ? `${running.length} of ${d.runningCount} shown` : null} empty={running.length === 0 && 'Nothing running.'}>
            <Table data-testid="worker-running">
              <TableHeader><TableRow className="hover:bg-transparent"><TableHead>Job</TableHead><TableHead>Queue</TableHead><TableHead className="hidden md:table-cell">Pool</TableHead><TableHead>Running for</TableHead><TableHead className="w-10" /></TableRow></TableHeader>
              <TableBody>{running.map((a) => (
                <TableRow key={a.attemptId}>
                  <TableCell className="max-w-64 truncate"><NameLink href={`${base}/runs/${a.jobId}`}>{a.jobName}</NameLink></TableCell>
                  <TableCell className="text-muted-foreground">{a.queue}</TableCell><TableCell className="hidden text-muted-foreground md:table-cell">{a.pool}</TableCell>
                  <TableCell className="whitespace-nowrap tabular-nums">{duration(now - Date.parse(a.startedAt))}</TableCell>
                  <TableCell className="text-right"><OpenButton href={`${base}/runs/${a.jobId}`} label={`Open run of ${a.jobName}`} /></TableCell>
                </TableRow>
              ))}</TableBody>
            </Table>
          </TableCard>}
          {d && <TableCard title="Recent attempts" description="The last attempts this worker finished, newest first" empty={recent.length === 0 && 'No finished attempts since it started.'}>
            <Table data-testid="worker-recent">
              <TableHeader><TableRow className="hover:bg-transparent"><TableHead>Job</TableHead><TableHead>Outcome</TableHead><TableHead>Ran for</TableHead><TableHead className="hidden md:table-cell">CPU</TableHead><TableHead className="hidden md:table-cell">Finished</TableHead><TableHead className="w-10" /></TableRow></TableHeader>
              <TableBody>{recent.map((a) => (
                <TableRow key={a.attemptId}>
                  <TableCell className="max-w-64 truncate"><NameLink href={`${base}/runs/${a.jobId}`}>{a.jobName}</NameLink> <span className="text-xs text-muted-foreground">{a.queue}</span></TableCell>
                  <TableCell>{a.outcome ? <StateBadge state={a.outcome} /> : '—'}</TableCell>
                  <TableCell className="whitespace-nowrap tabular-nums">{a.startedAt && a.finishedAt ? duration(Date.parse(a.finishedAt) - Date.parse(a.startedAt)) : '—'}</TableCell>
                  <TableCell className="hidden tabular-nums md:table-cell">{cpu(a.cpuMicroseconds)}</TableCell>
                  <TableCell className="hidden whitespace-nowrap text-muted-foreground md:table-cell">{a.finishedAt ? relative(a.finishedAt, now) : '—'}</TableCell>
                  <TableCell className="text-right"><OpenButton href={`${base}/runs/${a.jobId}`} label={`Open run of ${a.jobName}`} /></TableCell>
                </TableRow>
              ))}</TableBody>
            </Table>
          </TableCard>}
        </div>
        <div className="grid min-w-0 gap-6">
          <Card>
            <CardHeader><CardTitle className="text-base">Process and host</CardTitle></CardHeader>
            <CardContent>
              <dl className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-1">
                <Field label="Host">{w.hostName}</Field>
                <Field label="Process ID"><span className="tabular-nums">{w.processId}</span></Field>
                <Field label="Version"><Copyable value={w.buildId} label="Copy version"><code>{w.buildId}</code></Copyable></Field>
                <Field label="Started">{time(w.startedAt)}</Field>
                <Field label="Operating system"><span className="inline-flex items-center gap-1.5"><IconDeviceDesktop className="size-4 text-muted-foreground" aria-hidden />{host?.os ?? '—'}</span></Field>
                <Field label="Runtime">{host ? `${host.runtime} (${host.arch})` : '—'}</Field>
                <Field label="CPUs"><span className="tabular-nums">{host?.cpus ?? '—'}</span></Field>
                <Field label="Machine memory">{host ? `${bytes(host.memoryTotalBytes)} total, ${bytes(host.memoryAvailableBytes)} available` : '—'}</Field>
              </dl>
            </CardContent>
          </Card>
          {d?.settings && <Card>
            <CardHeader><CardTitle className="flex items-center gap-1.5 text-base"><IconTag className="size-4 text-muted-foreground" aria-hidden />Worker settings</CardTitle></CardHeader>
            <CardContent>
              <dl data-testid="worker-settings" className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                {Object.entries(d.settings).map(([k, v]) => <Field key={k} label={settingName(k)}><span className="tabular-nums">{settingValue(k, v)}</span></Field>)}
              </dl>
            </CardContent>
          </Card>}
          {events && <Card className="gap-3">
            <CardHeader><CardTitle className="text-base">Scaling, last 24 h</CardTitle></CardHeader>
            <CardContent>
              {events.items.length === 0 ? <p className="text-sm text-muted-foreground">No target changes.</p>
                : <ol className="grid gap-2">{events.items.slice(0, 8).map((e) => (
                  <li key={e.eventId} className="tint grid gap-0.5 rounded-lg border p-2.5 text-sm">
                    <span className="flex justify-between gap-2"><NameLink href={`${base}/pools/${encodeURIComponent(e.pool)}`}>{e.pool}</NameLink><span className="tabular-nums">{e.oldTarget} → {e.newTarget}</span></span>
                    <span className="text-xs text-muted-foreground">{e.reason} · {relative(e.createdAt, now)}</span>
                  </li>))}</ol>}
            </CardContent>
          </Card>}
        </div>
      </div>
    </Layout>
  )
}
