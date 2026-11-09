import { Badge } from '../components/ui/badge'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../components/ui/card'
import { Layout } from '../lib'

const Row = ({ name, children, testId }) => (
  <div className="grid gap-1 border-b py-3 last:border-0 sm:grid-cols-[16rem_1fr] sm:gap-4">
    <dt className="text-sm text-muted-foreground">{name}</dt><dd className="min-w-0 text-sm break-words" data-testid={testId}>{children}</dd>
  </div>
)

export default function Settings({ settings }) {
  return (
    <Layout heading="Settings" description="How this process is configured. Read only.">
      <div className="grid gap-6 lg:grid-cols-2" data-testid="settings">
        <Card>
          <CardHeader><CardTitle>Application</CardTitle><CardDescription>Store and roles</CardDescription></CardHeader>
          <CardContent>
            <dl>
              <Row name="Application">{settings.application}</Row>
              <Row name="Store">{settings.storeKind} (schema revision {settings.schemaRevision})</Row>
              <Row name="Persistence">{settings.persistenceProfile}</Row>
              <Row name="Roles in this process" testId="roles">{settings.roles.join(', ')}</Row>
              <Row name="Retention policy revision">{settings.retentionPolicyRevision}</Row>
            </dl>
          </CardContent>
        </Card>
        <Card>
          <CardHeader><CardTitle>Limits</CardTitle><CardDescription>Management limits in force</CardDescription></CardHeader>
          <CardContent>
            <dl>{Object.entries(settings.limits).map(([k, v]) => <Row key={k} name={k}><Badge variant="secondary" className="font-mono">{String(v)}</Badge></Row>)}</dl>
          </CardContent>
        </Card>
      </div>
    </Layout>
  )
}
