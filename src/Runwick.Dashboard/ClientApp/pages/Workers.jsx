import { router } from '@inertiajs/react'
import { IconCpu, IconDeviceFloppy, IconMemory, IconPencil, IconRocket, IconServer2 } from '@tabler/icons-react'
import { Kpi, KpiStrip } from '../components/kpis'
import { percent } from '../components/host'
import { bytes } from '../components/usage'
import { useState } from 'react'
import { Badge } from '../components/ui/badge'
import { Button } from '../components/ui/button'
import { Label } from '../components/ui/label'
import { NumberField } from '../components/ui/number-field'
import { Sheet, SheetContent, SheetDescription, SheetFooter, SheetHeader, SheetTitle } from '../components/ui/sheet'
import { Switch } from '../components/ui/switch'
import { Textarea } from '../components/ui/textarea'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../components/ui/table'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../components/ui/tabs'
import { CpuCell, hostOf, MemoryCell } from '../components/host'
import { AnimatedNumber, CommandButton, Copyable, flash, Layout, NameLink, OpenButton, Pager, StateBadge, TableCard, time, useChanged, useCommand, usePoll, useSort } from '../lib'

/** Edit a scope's capacity in a side sheet: the new value and an optional reason, checked against the policy revision shown. */
function CapacitySheet({ scope, url, command, onClose }) {
  const [capacity, setCapacity] = useState(String(scope?.capacity ?? ''))
  const [reason, setReason] = useState('')
  const submit = (e) => {
    e.preventDefault()
    command.run(`Set ${scope.kind}/${scope.key} capacity to ${capacity}`, url, { capacity: Number(capacity), expectedPolicyRevision: scope.policyRevision, reason: reason || null }, 'done', (ok) => { if (ok) setTimeout(onClose, 500) })
  }
  return (
    <Sheet open={!!scope} onOpenChange={(o) => { if (!o) onClose() }}>
      <SheetContent data-testid="capacity-sheet">
        {scope && <>
          <SheetHeader><SheetTitle>Capacity of {scope.kind}/{scope.key}</SheetTitle>
            <SheetDescription>Policy revision {scope.policyRevision}; {scope.livePermits} permits in use now.</SheetDescription></SheetHeader>
          <form onSubmit={submit} className="flex min-h-0 flex-1 flex-col">
            <div className="grid content-start gap-4 px-5">
              <div className="grid gap-1.5"><Label htmlFor="capacity">Capacity</Label>
                <NumberField id="capacity" name="capacity" aria-label="capacity" value={capacity} onChange={setCapacity} min={0} /></div>
              <div className="grid gap-1.5"><Label htmlFor="capacity-reason">Reason <span className="font-normal text-muted-foreground">(optional, kept in audit)</span></Label>
                <Textarea id="capacity-reason" value={reason} maxLength={500} onChange={(e) => setReason(e.target.value)} /></div>
            </div>
            <SheetFooter>
              <Button type="button" variant="ghost" onClick={onClose}>Cancel</Button>
              <CommandButton command={command} url={url} type="submit" variant="default" size="default" data-testid="set-capacity" icon={IconDeviceFloppy} disabled={capacity === '' || Number(capacity) === scope.capacity}>Save</CommandButton>
            </SheetFooter>
          </form>
        </>}
      </SheetContent>
    </Sheet>
  )
}

export default function Workers({ base, can, workers, fleet, scopes, query }) {
  const live = usePoll(['workers', 'fleet', 'scopes'], 10)
  const command = useCommand()
  const [workerRows, workerHead] = useSort(workers.items)
  const [scopeRows, scopeHead] = useSort(scopes.items)
  const changed = useChanged(scopes.items, (s) => `${s.kind}/${s.key}`, (s) => `${s.capacity}/${s.livePermits}`)
  const capacityUrl = (s) => `${base}/scopes/${encodeURIComponent(s.kind)}/${encodeURIComponent(s.key)}/capacity`
  const [editing, setEditing] = useState(null)
  return (
    <Layout heading="Workers and capacity" live={live}>
      <CapacitySheet key={editing ? `${editing.kind}/${editing.key}` : 'none'} scope={editing} url={editing && capacityUrl(editing)} command={command} onClose={() => setEditing(null)} />
      <KpiStrip label="Fleet stats">
        <Kpi testId="kpi-live" icon={IconServer2} label="Live workers" value={fleet.live} foot={`on ${fleet.hosts} host${fleet.hosts === 1 ? '' : 's'}${fleet.busiest ? ` · busiest ${fleet.busiest.hostName}` : ''}`} />
        <Kpi testId="kpi-concurrency" icon={IconRocket} label="Concurrency in use" value={fleet.inFlight} suffix={`/ ${fleet.target}`} foot={fleet.target ? `${Math.round((fleet.inFlight / fleet.target) * 100)} % of capacity` : 'No capacity'} />
        <Kpi testId="kpi-cpu" icon={IconCpu} label="Fleet CPU" value={percent(fleet.cpuPercent)} foot="Mean process share of its machine" />
        <Kpi testId="kpi-memory" icon={IconMemory} label="Machine memory" value={fleet.memoryTotalBytes ? `${Math.round((1 - fleet.memoryAvailableBytes / fleet.memoryTotalBytes) * 100)} %` : '—'}
          foot={fleet.memoryTotalBytes ? `${bytes(fleet.memoryAvailableBytes)} free of ${bytes(fleet.memoryTotalBytes)}` : 'Not published'} />
      </KpiStrip>
      <Tabs defaultValue="workers">
        <TabsList>
          <TabsTrigger value="workers">Workers <Badge variant="secondary" className="px-1.5">{workers.next ? '50+' : workers.items.length}</Badge></TabsTrigger>
          <TabsTrigger value="scopes">Concurrency scopes</TabsTrigger>
        </TabsList>
        <TabsContent value="workers">
          <TableCard pager={<Pager page={workers} />} empty={workers.items.length === 0 && 'No live workers.'}
            toolbar={<label className="ml-auto flex items-center gap-2 text-sm">
              <Switch data-testid="show-expired" checked={!!query.expired} onCheckedChange={(on) => router.get(`${base}/workers${on ? '?expired=true' : ''}`, {}, { preserveScroll: true })} />Show expired
            </label>}>
            <Table data-testid="workers">
              <TableHeader><TableRow className="hover:bg-transparent">
                {workerHead('host', 'Host', (w) => w.hostName)}{workerHead('pid', 'Process', (w) => w.processId)}{workerHead('build', 'Build', (w) => w.buildId)}
                {workerHead('status', 'Status', (w) => w.status)}{workerHead('beat', 'Heartbeat', (w) => Date.parse(w.heartbeatAt))}{workerHead('expires', 'Expires', (w) => Date.parse(w.expiresAt))}
                {workerHead('cpu', 'CPU', (w) => hostOf(w)?.processCpuPercent ?? -1, 'hidden lg:table-cell')}{workerHead('memory', 'Memory', (w) => hostOf(w)?.workingSetBytes ?? -1, 'hidden xl:table-cell')}<TableHead className="w-10" />
              </TableRow></TableHeader>
              <TableBody>{workerRows.map((w) => (
                <TableRow key={w.instanceId}>
                  <TableCell><NameLink href={`${base}/workers/${w.instanceId}`} testId="worker-link">{w.hostName}</NameLink></TableCell><TableCell className="tabular-nums">{w.processId}</TableCell><TableCell><Copyable value={w.buildId} label="Copy build ID"><code>{w.buildId}</code></Copyable></TableCell>
                  <TableCell><StateBadge state={w.status} /></TableCell>
                  <TableCell className="whitespace-nowrap text-muted-foreground">{time(w.heartbeatAt)}</TableCell><TableCell className="whitespace-nowrap text-muted-foreground">{time(w.expiresAt)}</TableCell>
                  <TableCell className="hidden lg:table-cell"><CpuCell host={hostOf(w)} /></TableCell><TableCell className="hidden xl:table-cell"><MemoryCell host={hostOf(w)} /></TableCell>
                  <TableCell className="text-right"><OpenButton href={`${base}/workers/${w.instanceId}`} label={`Open worker ${w.hostName}`} testId="worker-open" /></TableCell>
                </TableRow>
              ))}</TableBody>
            </Table>
          </TableCard>
        </TabsContent>
        <TabsContent value="scopes">
          <TableCard pager={<Pager page={scopes} cursorParam="scopeCursor" />} empty={scopes.items.length === 0 && 'No concurrency scopes.'}>
            <Table data-testid="scopes">
              <TableHeader><TableRow className="hover:bg-transparent">
                {scopeHead('kind', 'Kind', (s) => s.kind)}{scopeHead('key', 'Key', (s) => s.key)}{scopeHead('capacity', 'Capacity', (s) => s.capacity)}
                {scopeHead('permits', 'Live permits', (s) => s.livePermits)}{scopeHead('revision', 'Revision', (s) => s.policyRevision)}
                {can.capacityEdit && <TableHead />}
              </TableRow></TableHeader>
              <TableBody>{scopeRows.map((s) => {
                const id = `${s.kind}/${s.key}`
                return (
                  <TableRow key={id} data-testid="scope-row" className={flash(changed[id])}>
                    <TableCell className="text-muted-foreground">{s.kind}</TableCell><TableCell className="font-medium">{s.key}</TableCell>
                    <TableCell data-testid="scope-capacity" className="tabular-nums">{s.capacity}</TableCell>
                    <TableCell><AnimatedNumber value={s.livePermits} /></TableCell><TableCell className="tabular-nums">{s.policyRevision}</TableCell>
                    {can.capacityEdit && <TableCell className="text-right">
                      <CommandButton command={command} url={capacityUrl(s)} data-testid="edit-capacity" icon={IconPencil} onClick={() => setEditing(s)}>Edit</CommandButton>
                    </TableCell>}
                  </TableRow>
                )
              })}</TableBody>
            </Table>
          </TableCard>
        </TabsContent>
      </Tabs>
    </Layout>
  )
}
