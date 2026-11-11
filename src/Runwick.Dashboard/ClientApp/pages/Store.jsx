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
  Incompatible: ['failed', 'Incompatible'],
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

function PostgresDetails({ stats }) {
  const { sampledAt, ...facts } = stats
  const { sessions, lockWaits, databaseSize, jobsTable, wal, recovery, replication } = facts
  const stale = Object.values(facts).some(fact => fact.status === 'Stale')
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex flex-wrap items-center gap-2"><h2>PostgreSQL diagnostics</h2>{stale && <State value="Stale" />}</CardTitle>
        {sampledAt && <CardDescription>Sampled <ObservationTime value={sampledAt} /></CardDescription>}
      </CardHeader>
      <CardContent className="space-y-4">
        <div>
          <h3 className="text-sm font-medium">Current database</h3>
          <dl>
            <Row label="Connected sessions"><Fact fact={sessions}>{sessions.value?.connectedSessions.toLocaleString()}</Fact></Row>
            <Row label="Sessions waiting on locks"><Fact fact={lockWaits}>{lockWaits.value?.sessionsWaiting.toLocaleString()}</Fact></Row>
            <Row label="Database size"><Fact fact={databaseSize}>{databaseSize.value && storageSize(databaseSize.value.bytes)}</Fact></Row>
            <Row label="Runwick jobs table and indexes"><Fact fact={jobsTable}>{jobsTable.value && storageSize(jobsTable.value.totalBytes)}</Fact></Row>
            <Row label="Estimated dead tuples in jobs table"><Fact fact={jobsTable}>{jobsTable.value?.estimatedDeadTuples.toLocaleString()}</Fact></Row>
            <Row label="Autovacuum runs on jobs table"><Fact fact={jobsTable}>{jobsTable.value?.autovacuumCount.toLocaleString()}</Fact></Row>
          </dl>
        </div>
        <div>
          <h3 className="text-sm font-medium">Connected server</h3>
          <dl>
            <Row label="WAL generated since statistics reset"><Fact fact={wal}>{wal.value && storageSize(wal.value.bytesGenerated)}</Fact></Row>
            <Row label="WAL statistics reset"><Fact fact={wal}><ObservationTime value={wal.value?.resetAt} /></Fact></Row>
            <Row label="Server in recovery"><Fact fact={recovery}>{recovery.value?.inRecovery ? 'Yes' : 'No'}</Fact></Row>
            <Row label="Connected WAL senders"><Fact fact={replication}>{replication.value?.connectedSenders.toLocaleString()}</Fact></Row>
            <Row label="Largest replica replay backlog">
              <Fact fact={replication}>{replication.value?.largestReplayBacklogBytes == null
                ? <State value="Unavailable" /> : storageSize(replication.value.largestReplayBacklogBytes)}</Fact>
            </Row>
          </dl>
        </div>
      </CardContent>
    </Card>
  )
}

function RedisSection({ title, fact, children }) {
  return <div>
    <h3 className="text-sm font-medium">{title}</h3>
    {fact.value ? <dl>{children(fact.value)}</dl> : <div className="pt-2"><State value={fact.status} /></div>}
  </div>
}

function RedisMetric({ label, value, format = n => n.toLocaleString() }) {
  return <Row label={label}>{value == null ? <State value="Unavailable" /> : format(value)}</Row>
}

const yesNo = value => value ? 'Yes' : 'No'
const aofStatus = value => value === 'Ok' ? 'OK' : 'Error'

function RedisDetails({ stats }) {
  const { sampledAt, ...facts } = stats
  const { memory, activity, aof, replication, intentRecovery } = facts
  const stale = Object.values(facts).some(fact => fact.status === 'Stale')
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex flex-wrap items-center gap-2"><h2>Redis diagnostics</h2>{stale && <State value="Stale" />}</CardTitle>
        <CardDescription>Connected server and current application. {sampledAt && <>Sampled <ObservationTime value={sampledAt} /></>}</CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <RedisSection title="Application recovery" fact={intentRecovery}>{v => <>
          <RedisMetric label="Pending intent" value={v.pending} format={yesNo} />
          {v.pending && <RedisMetric label="Next after-image index" value={v.nextProjectionIndex} />}
        </>}</RedisSection>
        <RedisSection title="Memory" fact={memory}>{v => <>
          <RedisMetric label="Memory used" value={v.usedBytes} format={storageSize} />
          <RedisMetric label="Memory limit" value={v.limitBytes} format={n => n === 0 ? 'No configured limit' : storageSize(n)} />
          <RedisMetric label="Memory RSS to used ratio" value={v.fragmentationRatio} format={n => n.toLocaleString(undefined, { maximumFractionDigits: 2 })} />
        </>}</RedisSection>
        <RedisSection title="Server activity" fact={activity}>{v => <>
          <RedisMetric label="Connected clients" value={v.connectedClients} />
          <RedisMetric label="Redis commands per second" value={v.commandsPerSecond} />
          <RedisMetric label="Evicted keys" value={v.evictedKeys} />
        </>}</RedisSection>
        <RedisSection title="Append-only file (AOF)" fact={aof}>{v => <>
          <RedisMetric label="Enabled" value={v.enabled} format={yesNo} />
          {v.enabled !== false && <>
            <RedisMetric label="Current AOF size" value={v.currentBytes} format={storageSize} />
            <RedisMetric label="Last AOF write" value={v.lastWrite} format={aofStatus} />
            <RedisMetric label="Rewrite in progress" value={v.rewriteInProgress} format={yesNo} />
            <RedisMetric label="Rewrite scheduled" value={v.rewriteScheduled} format={yesNo} />
            <RedisMetric label="Last rewrite result" value={v.lastRewrite} format={aofStatus} />
          </>}
        </>}</RedisSection>
        <RedisSection title="Replication" fact={replication}>{v => <>
          <RedisMetric label="Node role" value={v.role} />
          <RedisMetric label="Connected replicas" value={v.connectedReplicas} />
          <RedisMetric label="Server replication offset (bytes)" value={v.replicationOffset} />
          {v.role === 'Replica' && <>
            <RedisMetric label="Replica applied offset (bytes)" value={v.replicaAppliedOffset} />
            <RedisMetric label="Link to primary" value={v.primaryLinkUp} format={up => up ? 'Up' : 'Down'} />
          </>}
        </>}</RedisSection>
      </CardContent>
    </Card>
  )
}

const CHECK_SOURCES = { HostCompatibilityCheck: 'Host compatibility check' }
const TREND_METRICS = {
  SqliteWalBytes: ['SQLite write-ahead log size', storageSize],
  PostgresLockWaits: ['PostgreSQL sessions waiting on locks', value => value.toLocaleString()],
  RedisMemoryUsedBytes: ['Connected Redis server memory used', storageSize],
}

function StoreHistory({ checks = [], trend, trendStatus }) {
  const metric = TREND_METRICS[trend?.metric]
  return (
    <>
      <Card>
        <CardHeader>
          <CardTitle><h2>Recent store observations</h2></CardTitle>
          <CardDescription>Changes observed by this dashboard host. History clears when this process restarts.</CardDescription>
        </CardHeader>
        <CardContent>
          {checks.length === 0 ? <p className="text-sm text-muted-foreground">Waiting for the first store check.</p> :
            <ol role="list" className="divide-y">
              {[...checks].reverse().map((check, index) => (
                <li key={check.observedAt + index} className="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1 py-2.5 first:pt-0 last:pb-0">
                  <div className="space-y-1">
                    <State value={check.status} urgent />
                    <p className="text-xs text-muted-foreground">{CHECK_SOURCES[check.source]}</p>
                  </div>
                  <span className="text-sm"><ObservationTime value={check.observedAt} /></span>
                </li>
              ))}
            </ol>}
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle><h2>Recent diagnostic samples</h2></CardTitle>
          {metric && <CardDescription>Up to 12 observations from this process, sampled about every 10 seconds while available.</CardDescription>}
        </CardHeader>
        <CardContent>
          {metric ? (
            <table className="w-full text-left text-sm">
              <caption className="sr-only">{metric[0]} history, most recent first</caption>
              <thead className="border-b text-muted-foreground">
                <tr><th scope="col" className="pb-2 pr-3 font-medium">Sampled at</th>
                  <th scope="col" className="pb-2 font-medium">{metric[0]}</th></tr>
              </thead>
              <tbody className="divide-y">
                {[...trend.samples].reverse().map((sample, index) => (
                  <tr key={sample.sampledAt + index}>
                    <td className="py-2 pr-3"><ObservationTime value={sample.sampledAt} /></td>
                    <td className="py-2 tabular-nums">{sample.value == null ? <State value="Unavailable" /> : metric[1](sample.value)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : trendStatus === 'PermissionDenied' ? <State value="PermissionDenied" />
            : <p className="text-sm text-muted-foreground">No diagnostic samples are available.</p>}
        </CardContent>
      </Card>
    </>
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
            {snapshot.postgres && <PostgresDetails stats={snapshot.postgres} />}
            {snapshot.redis && <RedisDetails stats={snapshot.redis} />}
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
            <StoreHistory checks={snapshot.recentChecks} trend={snapshot.trend} trendStatus={snapshot.trendStatus} />
          </div>
        </div>
      </div>
    </Layout>
  )
}
