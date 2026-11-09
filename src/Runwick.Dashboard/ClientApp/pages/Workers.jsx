import { Link } from '@inertiajs/react'
import { IconCheck } from '@tabler/icons-react'
import { Badge } from '../components/ui/badge'
import { Button } from '../components/ui/button'
import { Input } from '../components/ui/input'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../components/ui/table'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../components/ui/tabs'
import { cn } from '../components/ui/utils'
import { AnimatedNumber, CommandBar, CommandButton, Layout, Pager, StateBadge, TableCard, time, useChanged, useCommand, usePoll, useSort } from '../lib'

export default function Workers({ base, can, workers, scopes, query }) {
  const live = usePoll(['workers', 'scopes'], 10)
  const command = useCommand()
  const [workerRows, workerHead] = useSort(workers.items)
  const [scopeRows, scopeHead] = useSort(scopes.items)
  const changed = useChanged(scopes.items, (s) => `${s.kind}/${s.key}`, (s) => `${s.capacity}/${s.livePermits}`)
  const capacityUrl = (s) => `${base}/scopes/${encodeURIComponent(s.kind)}/${encodeURIComponent(s.key)}/capacity`
  const setCapacity = (s, e) => {
    e.preventDefault()
    const capacity = Number(new FormData(e.target).get('capacity'))
    command.ask(`Set ${s.kind}/${s.key} capacity to ${capacity}`, capacityUrl(s), { capacity, expectedPolicyRevision: s.policyRevision })
  }
  return (
    <Layout heading="Workers and capacity" live={live}>
      <CommandBar command={command} />
      <Tabs defaultValue="workers">
        <TabsList>
          <TabsTrigger value="workers">Workers <Badge variant="secondary" className="px-1.5">{workers.next ? '50+' : workers.items.length}</Badge></TabsTrigger>
          <TabsTrigger value="scopes">Concurrency scopes</TabsTrigger>
        </TabsList>
        <TabsContent value="workers">
          <TableCard pager={<Pager page={workers} />} empty={workers.items.length === 0 && 'No live workers.'}
            toolbar={<Button asChild size="sm" variant="outline">
              {query.expired ? <Link href={`${base}/workers`}>Hide expired</Link> : <Link href={`${base}/workers?expired=true`}>Show expired</Link>}
            </Button>}>
            <Table data-testid="workers">
              <TableHeader><TableRow className="hover:bg-transparent">
                {workerHead('host', 'Host', (w) => w.hostName)}{workerHead('pid', 'Process', (w) => w.processId)}{workerHead('build', 'Build', (w) => w.buildId)}
                {workerHead('status', 'Status', (w) => w.status)}{workerHead('beat', 'Heartbeat', (w) => Date.parse(w.heartbeatAt))}{workerHead('expires', 'Expires', (w) => Date.parse(w.expiresAt))}
              </TableRow></TableHeader>
              <TableBody>{workerRows.map((w) => (
                <TableRow key={w.instanceId}>
                  <TableCell className="font-medium">{w.hostName}</TableCell><TableCell className="tabular-nums">{w.processId}</TableCell><TableCell><code>{w.buildId}</code></TableCell>
                  <TableCell><StateBadge state={w.status} /></TableCell>
                  <TableCell className="whitespace-nowrap text-muted-foreground">{time(w.heartbeatAt)}</TableCell><TableCell className="whitespace-nowrap text-muted-foreground">{time(w.expiresAt)}</TableCell>
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
                  <TableRow key={`${id}:${changed[id] ?? 0}`} data-testid="scope-row" className={cn(changed[id] && 'animate-flash')}>
                    <TableCell className="text-muted-foreground">{s.kind}</TableCell><TableCell className="font-medium">{s.key}</TableCell>
                    <TableCell data-testid="scope-capacity" className="tabular-nums">{s.capacity}</TableCell>
                    <TableCell><AnimatedNumber value={s.livePermits} /></TableCell><TableCell className="tabular-nums">{s.policyRevision}</TableCell>
                    {can.capacityEdit && <TableCell>
                      <form onSubmit={(e) => setCapacity(s, e)} className="flex items-center justify-end gap-2">
                        <Input name="capacity" type="number" min="0" defaultValue={s.capacity} aria-label="capacity" className="h-8 w-24" />
                        <CommandButton command={command} url={capacityUrl(s)} type="submit" data-testid="set-capacity" icon={IconCheck}>Set</CommandButton>
                      </form>
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
