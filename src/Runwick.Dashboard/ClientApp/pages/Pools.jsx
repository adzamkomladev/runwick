import { IconAdjustments, IconRocket, IconServer2, IconStack3 } from '@tabler/icons-react'
import { Kpi, KpiStrip } from '../components/kpis'
import { modeText, poolsFrom } from '../components/host'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../components/ui/table'
import { Layout, NameLink, OpenButton, TableCard, usePoll } from '../lib'

export default function Pools({ base, workers }) {
  const live = usePoll(['workers'], 10)
  const pools = poolsFrom(workers)
  return (
    <Layout heading="Pools" live={live} description="Pools across the live workers: their queues, concurrency and what is in flight now.">
      <KpiStrip label="Pool stats">
        <Kpi testId="kpi-pools" icon={IconStack3} label="Pools" value={pools.length} foot={`${pools.filter((p) => !p.known).length} without published detail`} />
        <Kpi testId="kpi-modes" icon={IconAdjustments} label="Auto / fixed" value={`${pools.filter((p) => p.mode === 'auto').length} / ${pools.filter((p) => p.mode === 'fixed').length}`} foot="Scaled by Runwick / set in options" />
        <Kpi testId="kpi-slots" icon={IconRocket} label="Slots in use" value={pools.reduce((t, p) => t + p.inFlight, 0)} suffix={`/ ${pools.reduce((t, p) => t + p.target, 0)}`} foot="In flight of the summed targets" />
        <Kpi testId="kpi-workers" icon={IconServer2} label="Workers" value={workers.length} foot="Live, running at least one pool" />
      </KpiStrip>
      <TableCard title="Pools" description={`From ${workers.length} live worker${workers.length === 1 ? "" : "s"}`}
        empty={pools.length === 0 && 'No live worker runs a pool.'}>
        <Table data-testid="pools">
          <TableHeader><TableRow className="hover:bg-transparent"><TableHead>Pool</TableHead><TableHead>Queues</TableHead><TableHead>Mode</TableHead><TableHead>In flight</TableHead><TableHead>Workers</TableHead><TableHead className="w-10" /></TableRow></TableHeader>
          <TableBody>{pools.map((p) => {
            const href = `${base}/pools/${encodeURIComponent(p.name)}`
            return (
              <TableRow key={p.name} data-testid="pool-row">
                <TableCell><NameLink href={href} testId="pool-link">{p.name}</NameLink></TableCell>
                <TableCell className="text-xs">{p.known ? p.queues.map((q) => <span key={q.queue} className="mr-2 inline-block"><NameLink href={`${base}/queues/${encodeURIComponent(q.queue)}`} className="font-normal">{q.queue}</NameLink> <span className="text-muted-foreground">×{q.weight}</span></span>) : '—'}</TableCell>
                <TableCell className="whitespace-nowrap text-muted-foreground">{modeText(p)}</TableCell>
                <TableCell className="tabular-nums">{p.known ? `${p.inFlight} / ${p.target}` : `— / ${p.target}`}</TableCell>
                <TableCell className="tabular-nums">{p.workers.length}</TableCell>
                <TableCell className="text-right"><OpenButton href={href} label={`Open pool ${p.name}`} testId="pool-open" /></TableCell>
              </TableRow>
            )
          })}</TableBody>
        </Table>
      </TableCard>
    </Layout>
  )
}
