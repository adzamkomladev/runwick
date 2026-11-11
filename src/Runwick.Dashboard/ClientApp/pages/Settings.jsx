import { Link } from '@inertiajs/react'
import { Button } from '../components/ui/button'
import { IconDatabase, IconGauge } from '@tabler/icons-react'
import { StatusBadge } from '../components/status'
import { Badge } from '../components/ui/badge'
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card'
import { Copyable, Layout } from '../lib'

const Row = ({ name, children, testId }) => (
  <div className="grid gap-1 border-b py-3 last:border-0 sm:grid-cols-[14rem_1fr] sm:items-center sm:gap-4">
    <dt className="text-sm text-muted-foreground">{name}</dt><dd className="min-w-0 text-sm break-words" data-testid={testId}>{children}</dd>
  </div>
)

/** "maxPageSize" → "Max page size". */
const words = (key) => key.replace(/([a-z\d])([A-Z])/g, '$1 $2').replace(/[_-]+/g, ' ').toLowerCase().replace(/^./, (c) => c.toUpperCase())

/** GetSettings (16), read only: the application, its store, this process's roles and the management limits in force. */
export default function Settings({ base, can, settings }) {
  return (
    <Layout heading="Settings" actions={can.store && <Button asChild variant="outline" size="sm"><Link href={`${base}/store`}>Store status</Link></Button>}>
      <div className="grid items-start gap-6 lg:grid-cols-2" data-testid="settings">
        <Card>
          <CardHeader><CardTitle className="flex items-center gap-2"><IconDatabase className="size-4 text-muted-foreground" />Application</CardTitle></CardHeader>
          <CardContent>
            <dl>
              <Row name="Application"><Copyable value={settings.application} label="Copy application name"><span className="font-medium">{settings.application}</span></Copyable></Row>
              <Row name="Store"><span className="inline-flex flex-wrap items-center gap-2"><Badge variant="secondary">{settings.storeKind}</Badge><span className="text-muted-foreground">schema revision <span className="font-medium text-foreground tabular-nums">{settings.schemaRevision}</span></span></span></Row>
              <Row name="Persistence">{settings.persistenceProfile}</Row>
              <Row name="Roles in this process" testId="roles"><span className="flex flex-wrap gap-1.5">{settings.roles.map((r) => <StatusBadge key={r} state="active">{r}</StatusBadge>)}</span></Row>
              <Row name="Retention policy revision"><span className="tabular-nums">{settings.retentionPolicyRevision}</span></Row>
            </dl>
          </CardContent>
        </Card>
        <Card>
          <CardHeader><CardTitle className="flex items-center gap-2"><IconGauge className="size-4 text-muted-foreground" />Limits</CardTitle></CardHeader>
          <CardContent>
            <dl>{Object.entries(settings.limits).map(([k, v]) => (
              <Row key={k} name={<span title={k}>{words(k)}</span>}><span className="font-mono text-sm font-medium tabular-nums">{Number(v).toLocaleString()}</span></Row>
            ))}</dl>
          </CardContent>
        </Card>
      </div>
    </Layout>
  )
}
