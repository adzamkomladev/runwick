// Stand-in props for a page's skeleton when this tab has not loaded that page yet (lib.jsx renders the real page with them in
// skeleton mode, where every text run is drawn as a bar). Values only need the right shape and roughly the right length; once a
// page has loaded, its own last props are used instead, so a revisit matches exactly.

const at = new Date().toISOString()
const page = (n, row) => ({ items: Array.from({ length: n }, (_, i) => row(i)), next: null })
const ROWS = 10

const job = (i) => ({ jobId: `00000000-0000-0000-0000-${String(i).padStart(12, '0')}`, jobName: 'sample.job.name', queue: 'default', state: 'succeeded',
  cancelRequested: false, createdAt: at, updatedAt: at, terminalAt: at, revision: 4, generation: 1 })
const usage = { processCpuMicroseconds: 100000, processAllocatedBytes: 400000, workingSetStartBytes: 1.5e8, workingSetEndBytes: 1.5e8, workingSetPeakBytes: 1.5e8, overlappingAttempts: 0 }

export const PLACEHOLDERS = {
  Overview: () => ({
    recent: page(40, job), failed: page(6, (i) => ({ ...job(i), state: 'failed' })), pending: page(4, (i) => ({ ...job(i), state: 'pending' })),
    running: page(1, (i) => ({ ...job(i), state: 'running' })),
    queues: page(5, (i) => ({ name: `queue${i}`, paused: false, revision: 1 })),
    workers: page(1, (i) => ({ instanceId: `w${i}`, hostName: 'worker-host', processId: 1000, status: 'ready', heartbeatAt: at, expiresAt: at })),
    events: page(0, () => null),
  }),
  Jobs: () => ({ jobs: page(ROWS, job), query: {}, queueNames: [] }),
  Job: () => ({
    job: {
      job: { ...job(0), executionCount: 1, terminalReason: 'succeeded', priority: 'medium' },
      attempts: [{ attemptId: 'a0', executionNumber: 1, reservedAt: at, authorizedAt: at, finishedAt: at, outcome: 'succeeded', errorCode: null, workerInstanceId: 'w0', usage }],
      generations: [{ generation: 1, createdReason: 'initial', createdBy: null, createdAt: at, startedCount: 1, maxExecutions: 3 }],
    },
    audit: undefined,
  }),
  Queues: () => ({ queues: page(5, (i) => ({ name: `queue${i}`, paused: false, revision: 1 })) }),
  Workers: () => ({
    workers: page(2, (i) => ({ instanceId: `w${i}`, hostName: 'worker-host', processId: 1000, buildId: 'build-0000', status: 'ready', heartbeatAt: at, expiresAt: at })),
    scopes: page(0, () => null), query: {},
  }),
  Schedules: () => ({ schedules: page(3, (i) => ({ key: `schedule.${i}`, paused: false, nextDueAt: at, lastMaterializedAt: at, definitionRevision: 1, controlRevision: 1 })) }),
  Scaling: () => ({ events: page(ROWS, (i) => ({ eventId: i, createdAt: at, instanceId: 'instance-0000', pool: 'default', oldTarget: 1, newTarget: 2, desired: 2, candidate: 2, reason: 'queue depth rose' })), query: {} }),
  Audit: () => ({ audit: page(ROWS, (i) => ({ auditId: i, occurredAt: at, operation: 'queue.pause', targetKind: 'queue', targetId: 'default', actorSubject: 'admin', actorScheme: 'Cookies', reason: null, expectedRevision: 1, resultingRevision: 2 })), query: {} }),
  Retention: () => ({
    retention: {
      revision: 1, observedAt: at,
      policy: { completedDetailHours: 168, failedDetailHours: 720, jobHistoryHours: 2160, auditHours: 2160, scalingEventsHours: 168, scheduleOccurrencesHours: 720, workerPresenceHours: 24 },
      classes: ['job_detail_completed', 'job_detail_failed', 'job_history', 'job_events', 'management_audit', 'schedule_audit', 'scaling_events', 'schedule_occurrences', 'worker_presence']
        .map((recordClass) => ({ recordClass, behind: false, lastBatchAt: at, caughtUpAt: at, totalRows: 0, todayRows: 0, todayBatches: 0, todayFailures: 0, lastErrorCode: null })),
    },
  }),
  Settings: () => ({
    settings: { application: 'application', storeKind: 'postgres', schemaRevision: 18, persistenceProfile: 'durable', roles: ['dispatcher', 'worker'], retentionPolicyRevision: 1,
      limits: Object.fromEntries(['maxPageSize', 'maxBulkItems', 'maxRangeDays', 'maxRevealBytes', 'maxReasonLength', 'maxFilterLength'].map((k) => [k, 100])) },
  }),
}

/** Placeholder notifications: a few rows of the sheet's list. */
export const NOTIFICATIONS = { failed: Array.from({ length: 3 }, (_, i) => ({ jobId: `n${i}`, jobName: 'sample.job.name', queue: 'default', at })), scaling: [], paused: [] }
