import { IconDatabase } from '@tabler/icons-react'
import { StatusBadge } from '../components/status'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../components/ui/card'
import { Copyable, Layout, time, usePoll } from '../lib'

const STATUS = {
  Available: ['ready', 'Available'],
  Stale: ['degraded', 'Stale'],
  Unavailable: ['neutral', 'Unavailable'],
  Unsupported: ['neutral', 'Unsupported'],
  PermissionDenied: ['neutral', 'Permission denied'],
  Reachable: ['ready', 'Reachable'],
  Unknown: ['neutral', 'Unknown'],
  Healthy: ['ready', 'Healthy'],
  Degraded: ['degraded', 'Degraded'],
  Unhealthy: ['failed', 'Unhealthy'],
}
const PROVIDERS = { postgres: 'PostgreSQL', sqlite: 'SQLite', redis: 'Redis', unknown: 'Unknown' }
const ROLES = { producer: 'Producer', worker: 'Worker', scheduler: 'Scheduler', management: 'Management' }
const REASONS = {
  ok: 'Responded to the last check.',
  stale: 'The last check is older than the freshness window.',
  not_observed: 'Waiting for the first check from this host.',
  store_unavailable: 'The store check did not complete.',
  incompatible: 'The store did not pass its compatibility checks.',
}
const CAPABILITIES = {
  BatchClaim: 'Batch claim', BatchFinish: 'Batch finish', BatchRenewal: 'Batch renewal', RateAdmission: 'Rate admission',
  Scheduling: 'Scheduling', Retention: 'Retention', WakeNotifications: 'Wake notifications', OutboxDestination: 'Outbox destination',
}

function State({ value, urgent = false }) {
  const [tone, label] = STATUS[value] ?? STATUS.Unknown
  return <StatusBadge state={urgent && value === 'Unavailable' ? 'failed' : tone}>{label}</StatusBadge>
}

function Row({ label, children }) {
  return (
    <div className="grid gap-1 border-b py-3 last:border-0 sm:grid-cols-[minmax(8rem,0.85fr)_minmax(0,1.15fr)] sm:items-baseline sm:gap-4">
      <dt className="text-sm text-muted-foreground">{label}</dt>
      <dd className="min-w-0 break-words text-sm">{children}</dd>
    </div>
  )
}

function Fact({ fact, children }) {
  return <>{fact.value && children}{fact.status !== 'Available' && fact.status !== 'Stale' && <State value={fact.status} />}</>
}

function ObservationTime({ value }) {
  return value ? <time className="tabular-nums" dateTime={value}>{time(value)}</time> : <State value="Unavailable" />
}

function ageLabel(seconds) {
  return seconds < 60 ? `${Math.floor(seconds)} s old` : `${Math.floor(seconds / 60)} min old`
}

function storageSize(bytes) {
  if (bytes < 1024) return `${bytes.toLocaleString()} B`
  const unit = bytes < 1048576 ? [1024, 'KiB'] : [1048576, 'MiB']
  return `${(bytes / unit[0]).toLocaleString(undefined, { maximumFractionDigits: 1 })} ${unit[1]}`
}

function SqliteDetails({ stats }) {
  const { databaseFile, walFile, writer } = stats
  const stale = [databaseFile, walFile, writer].some(fact => fact.status === 'Stale')
  return (
    <Card>
      <CardHeader><CardTitle className="flex flex-wrap items-center gap-2"><h2>SQLite diagnostics</h2>{stale && <State value="Stale" />}</CardTitle></CardHeader>
      <CardContent><dl>
        <Row label="Database file"><Fact fact={databaseFile}>{databaseFile.value && storageSize(databaseFile.value.bytes)}</Fact></Row>
        <Row label="Write-ahead log"><Fact fact={walFile}>{walFile.value && storageSize(walFile.value.bytes)}</Fact></Row>
        <Row label="Writer commits in this process"><Fact fact={writer}>{writer.value?.commits.toLocaleString()}</Fact></Row>
        <Row label="Busy lock conflicts in this process"><Fact fact={writer}>{writer.value?.busyConflicts.toLocaleString()}</Fact></Row>
      </dl></CardContent>
    </Card>
  )
}

export default function Store({ snapshot }) {
  const live = usePoll(['snapshot'], 10)
  const { connection, identity, admission, persistence, capabilities } = snapshot
  const stale = identity.status === 'Stale'
  return (
    <Layout heading="Store" live={live}>
      <div className="grid min-w-0 gap-6" data-testid="store-snapshot">
        <Card>
          <CardHeader>
            <CardTitle className="flex flex-wrap items-center gap-2"><IconDatabase className="size-4 text-muted-foreground" aria-hidden /><h2>{PROVIDERS[snapshot.provider] ?? 'Unknown provider'}</h2><State value={connection.status} urgent /></CardTitle>
            <CardDescription>Connection from this dashboard host. {REASONS[connection.reason] ?? REASONS.not_observed}</CardDescription>
          </CardHeader>
          <CardContent>
            {stale && <p className="mb-3 rounded-md border bg-muted px-3 py-2 text-sm" role="status">Showing the last known observation from this host.</p>}
            <dl>
              <Row label="Snapshot read"><ObservationTime value={snapshot.observedAt} /></Row>
              <Row label="Last successful check">
                <span className="flex flex-wrap items-center gap-x-2 gap-y-1"><ObservationTime value={snapshot.lastSuccessfulAt} />
                  {snapshot.lastSuccessfulAgeSeconds != null && <span className="tabular-nums text-muted-foreground">{ageLabel(snapshot.lastSuccessfulAgeSeconds)}</span>}
                </span>
              </Row>
            </dl>
          </CardContent>
        </Card>
        <div className="grid min-w-0 items-start gap-6 xl:grid-cols-[minmax(0,1.35fr)_minmax(0,1fr)]">
          <div className="grid min-w-0 gap-6">
            <Card>
              <CardHeader><CardTitle className="flex flex-wrap items-center gap-2"><h2>Logical store</h2>{identity.status === 'Stale' && <State value="Stale" />}</CardTitle><CardDescription>Store-wide metadata from the compatibility check.</CardDescription></CardHeader>
              <CardContent>
                <dl>
                  <Row label="Store ID"><Fact fact={identity}><div className="mb-1 min-w-0 break-all"><Copyable value={identity.value?.storeId} label="Copy store ID"><code>{identity.value?.storeId}</code></Copyable></div></Fact></Row>
                  <Row label="Schema or layout revision"><Fact fact={identity}>{identity.value && <span className="mr-2 tabular-nums">{identity.value.schemaRevision}</span>}</Fact></Row>
                  <Row label="Protocol epoch"><Fact fact={identity}>{identity.value && <span className="mr-2 tabular-nums">{identity.value.protocolEpoch}</span>}</Fact></Row>
                  <Row label="Migration mode"><Fact fact={identity}>{identity.value && <span className="mr-2 capitalize">{identity.value.migrationMode}</span>}</Fact></Row>
                  <Row label="Recovery"><State value={snapshot.recovery.status} /></Row>
                  <Row label="Client acknowledgement profile"><Fact fact={persistence}><code>{persistence.value?.profile}</code></Fact></Row>
                  <Row label="Profile verification">{persistence.value?.verification === 'Available' ? <StatusBadge state="ready">Verified by this host</StatusBadge> : <State value={persistence.value?.verification ?? persistence.status} />}</Row>
                </dl>
              </CardContent>
            </Card>
            <Card>
              <CardHeader><CardTitle><h2>Provider diagnostics</h2></CardTitle><CardDescription>Server and node observations.</CardDescription></CardHeader>
              <CardContent>
                <dl>
                  <Row label={snapshot.provider === 'sqlite' ? 'SQLite library version' : 'Server version'}>
                    <Fact fact={snapshot.serverVersion}><span className="mr-2 tabular-nums">{snapshot.serverVersion.value}</span></Fact>
                    {snapshot.serverVersion.status === 'Stale' && <State value="Stale" />}
                  </Row>
                  <Row label="Topology"><State value={snapshot.topology.status} /></Row>
                </dl>
              </CardContent>
            </Card>
            {snapshot.sqlite && <SqliteDetails stats={snapshot.sqlite} />}
          </div>
          <div className="order-first grid min-w-0 gap-6 xl:order-none">
            <Card>
              <CardHeader><CardTitle className="flex flex-wrap items-center gap-2"><h2>Application admission</h2>{admission.status === 'Stale' && <State value="Stale" />}</CardTitle><CardDescription>{snapshot.application}</CardDescription></CardHeader>
              <CardContent><dl>
                <Row label="Admission mode"><Fact fact={admission}>{admission.value && <span className="mr-2 capitalize">{admission.value.admissionMode}</span>}</Fact></Row>
                <Row label="Protocol requirements"><Fact fact={admission}>{admission.value && <span className="mr-2">Epoch {admission.value.requiredProtocolEpoch} · {admission.value.supportsRequirements ? 'Recognized by this client' : 'Requirements differ'}</span>}</Fact></Row>
              </dl></CardContent>
            </Card>
            <Card>
              <CardHeader><CardTitle><h2>Roles in this process</h2></CardTitle><CardDescription>Cached readiness from each enabled role.</CardDescription></CardHeader>
              <CardContent>
                {!snapshot.roles.length && <p className="text-sm text-muted-foreground">No local role observations.</p>}
                <dl>{snapshot.roles.map(({ readiness: r }) => (
                  <Row key={r.role} label={ROLES[r.role] ?? r.role}>
                    <span className="flex flex-wrap items-center gap-2"><State value={['Healthy', 'Degraded', 'Unhealthy'][r.status] ?? r.status} />
                      {!['ok', 'ready', 'degraded'].includes(r.reason) && <span className="text-muted-foreground">{r.reason.replaceAll('_', ' ')}</span>}
                    </span>
                  </Row>
                ))}</dl>
                <p className="mt-3 border-t pt-3 text-xs text-muted-foreground">Client capabilities</p>
                {capabilities.status !== 'Available' ? <div className="mt-2"><State value={capabilities.status} /></div>
                  : capabilities.value?.length ? <ul className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-sm">
                    {capabilities.value.map((item) => <li key={item}>{CAPABILITIES[item]}</li>)}
                  </ul> : <p className="mt-2 text-sm text-muted-foreground">No optional client capabilities.</p>}
              </CardContent>
            </Card>
          </div>
        </div>
      </div>
    </Layout>
  )
}
