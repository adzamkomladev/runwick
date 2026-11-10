// Stand-in props for a page's skeleton when this tab has not loaded that page yet (lib.jsx renders the real page with them in
// skeleton mode, where every text run is drawn as a bar). Values only need the right shape and roughly the right length; once a
// page has loaded, its own last props are used instead, so a revisit matches exactly.

const at = new Date().toISOString()
const page = (n, row) => ({ items: Array.from({ length: n }, (_, i) => row(i)), next: null })
const ROWS = 10

const job = (i) => ({ jobId: `00000000-0000-0000-0000-${String(i).padStart(12, '0')}`, jobName: 'sample.job.name', queue: 'default', state: 'succeeded',
  cancelRequested: false, createdAt: at, updatedAt: at, terminalAt: at, revision: 4, generation: 1 })
const worker = (i) => ({
  instanceId: `00000000-0000-0000-0000-00000000000${i}`, hostName: 'worker-host', processId: 1000, buildId: 'build-0000', status: 'ready', startedAt: at, heartbeatAt: at, expiresAt: at,
  poolTargets: { default: 4 }, subscriptions: ['default'],
  detail: {
    v: 1, host: { os: 'Linux 6.8', runtime: '.NET 10.0', arch: 'X64', cpus: 8, memoryTotalBytes: 1.6e10, memoryAvailableBytes: 8e9, processCpuPercent: 4, workingSetBytes: 1.5e8, gcHeapBytes: 5e7 },
    pools: [{ name: 'default', queues: [{ queue: 'default', weight: 1 }], mode: 'fixed', target: 4, min: 4, max: 4, inFlight: 1, laneSelection: 'weighted', laneWeights: { high: 4, medium: 2, low: 1 } }],
    settings: { leaseDurationMs: 60000, heartbeatIntervalMs: 5000 }, running: [], runningCount: 0, recent: [],
  },
})
/** An exact-stats answer (16 GetJobStats) with zero counts. */
const stats = (groups) => ({
  from: at, to: at, observedAt: at, states: { succeeded: 0, failed: 0 }, retries: 0, duration: { count: 0, medianMs: null, p95Ms: null, maxMs: null },
  buckets: Array.from({ length: 24 }, () => ({ from: at, created: 0, succeeded: 0, failed: 0, cancelled: 0, medianMs: null, p95Ms: null })),
  now: { pending: 0, scheduled: 0, running: 0 }, oldestPendingEligibleAt: null, errorCodes: [], ...groups,
})
const jobType = (i) => ({ name: `sample.job.${i}`, registered: true, queue: 'default', priority: 'medium', payloadVersion: 1, definition: {}, schedules: [], seenInStore: true })
const usage = { processCpuMicroseconds: 100000, processAllocatedBytes: 400000, workingSetStartBytes: 1.5e8, workingSetEndBytes: 1.5e8, workingSetPeakBytes: 1.5e8, overlappingAttempts: 0 }

export const PLACEHOLDERS = {
  Overview: () => ({
    stats: stats({ states: { succeeded: 30, failed: 6, pending: 4 }, queues: [{ key: 'default', states: {}, now: { pending: 4 } }], jobNames: [{ key: 'sample.job.name', states: { failed: 6 } }] }),
    failed: page(6, (i) => ({ ...job(i), state: 'failed' })),
    queues: page(5, (i) => ({ name: `queue${i}`, paused: false, revision: 1 })),
    workers: [{ instanceId: 'w0', hostName: 'worker-host', processId: 1000, status: 'ready', heartbeatAt: at, expiresAt: at }],
    events: page(0, () => null),
  }),
  Runs: () => ({ jobs: page(ROWS, job), stats: stats(), query: {}, queueNames: [] }),
  Jobs: () => ({ types: Array.from({ length: 6 }, (_, i) => jobType(i)), stats: stats({ jobNames: [] }), workers: [worker(0)], query: { range: '1d' } }),
  Job: () => ({ type: jobType(0), stats: stats(), runs: page(ROWS, job), workers: [worker(0)], query: { name: 'sample.job.name', range: '1d' } }),
  Run: () => ({
    job: {
      job: { ...job(0), executionCount: 1, terminalReason: 'succeeded', priority: 'medium' },
      attempts: [{ attemptId: 'a0', executionNumber: 1, reservedAt: at, authorizedAt: at, finishedAt: at, outcome: 'succeeded', errorCode: null, workerInstanceId: 'w0', usage }],
      generations: [{ generation: 1, createdReason: 'initial', createdBy: null, createdAt: at, startedCount: 1, maxExecutions: 3 }],
    },
    audit: undefined,
  }),
  Queues: () => ({ queues: page(5, (i) => ({ name: `queue${i}`, paused: false, revision: 1 })), counts: { total: 5, paused: 0 }, stats: stats({ queues: [] }), query: {} }),
  Workers: () => ({
    workers: page(2, (i) => ({ instanceId: `w${i}`, hostName: 'worker-host', processId: 1000, buildId: 'build-0000', status: 'ready', heartbeatAt: at, expiresAt: at })),
    scopes: page(0, () => null), query: {},
    fleet: { live: 2, hosts: 1, target: 8, inFlight: 1, workingSetBytes: 3e8, cpuPercent: 4, memoryTotalBytes: 1.6e10, memoryAvailableBytes: 8e9, busiest: null },
  }),
  Schedules: () => ({ schedules: page(3, (i) => ({ key: `schedule.${i}`, paused: false, nextDueAt: at, lastMaterializedAt: at, definitionRevision: 1, controlRevision: 1 })),
    stats: { total: 3, paused: 0, needsReview: 0, nextDueAt: at, occurrences: {}, misfires: 0 }, query: {} }),
  Scaling: () => ({ events: page(ROWS, (i) => ({ eventId: i, createdAt: at, instanceId: 'instance-0000', pool: 'default', oldTarget: 1, newTarget: 2, desired: 2, candidate: 2, reason: 'queue depth rose' })), query: {} }),
  Audit: () => ({ audit: page(ROWS, (i) => ({ auditId: i, occurredAt: at, operation: 'queue.pause', targetKind: 'queue', targetId: 'default', actorSubject: 'admin', actorScheme: 'Cookies', reason: null, expectedRevision: 1, resultingRevision: 2 })), query: {},
    stats: { total: 0, operations: [], actors: [] } }),
  Retention: () => ({
    retention: {
      revision: 1, observedAt: at,
      policy: { completedDetailHours: 168, failedDetailHours: 720, jobHistoryHours: 2160, auditHours: 2160, scalingEventsHours: 168, scheduleOccurrencesHours: 720, workerPresenceHours: 24 },
      classes: ['job_detail_completed', 'job_detail_failed', 'job_history', 'job_events', 'management_audit', 'schedule_audit', 'scaling_events', 'schedule_occurrences', 'worker_presence']
        .map((recordClass) => ({ recordClass, behind: false, lastBatchAt: at, caughtUpAt: at, totalRows: 0, todayRows: 0, todayBatches: 0, todayFailures: 0, lastErrorCode: null })),
    },
  }),
  Queue: () => ({
    queue: { name: 'default', paused: false, revision: 1 }, stats: stats(),
    jobs: page(ROWS, job), workers: [worker(0)], query: { name: 'default' },
  }),
  Worker: () => ({ worker: worker(0), events: page(0, () => null) }),
  Schedule: () => ({
    schedule: {
      schedule: { key: 'schedule.0', paused: false, nextDueAt: at, lastMaterializedAt: at, definitionRevision: 1, controlRevision: 1 },
      definition: { kind: 'cron', cronText: '*/5 * * * *', cronDialect: 'standard', zoneId: 'UTC', jobName: 'sample.job.name', queue: 'default',
        misfirePolicy: 'skip', misfireGraceMs: 60000, overlapPolicy: 'allow', gapPolicy: 'skip', foldPolicy: 'first', maxBatch: 10, maxEpisodeJobs: 100, maxCatchUpAgeMs: 3600000 },
      occurrences: Array.from({ length: ROWS }, (_, i) => ({ kind: 'cron', key: `o${i}`, scheduledAt: at, materializedAt: at, disposition: 'created', coalesced: false, jobId: null, jobState: 'succeeded' })),
    },
  }),
  Pools: () => ({ workers: [worker(0), worker(1)] }),
  Pool: () => ({ workers: [worker(0), worker(1)], events: page(0, () => null), query: { name: 'default' } }),
  Settings: () => ({
    settings: { application: 'application', storeKind: 'postgres', schemaRevision: 18, persistenceProfile: 'durable', roles: ['dispatcher', 'worker'], retentionPolicyRevision: 1,
      limits: Object.fromEntries(['maxPageSize', 'maxBulkItems', 'maxRangeDays', 'maxRevealBytes', 'maxReasonLength', 'maxFilterLength'].map((k) => [k, 100])) },
  }),
}

/** Placeholder notifications: a few rows of the sheet's list. */
export const NOTIFICATIONS = { failed: Array.from({ length: 3 }, (_, i) => ({ jobId: `n${i}`, jobName: 'sample.job.name', queue: 'default', at })), scaling: [], paused: [] }
