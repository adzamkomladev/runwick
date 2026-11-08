-- 0015_management (SP-020, 16 §6): keyset list indexes for the management reads, and the disclosure audit kinds.
-- Additive except the widened management_audit checks. The manifest gains RecordDisclosure/1: one transaction that
-- writes a RevealPayload or RevealError audit row and then reads the detail it discloses.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Unfiltered and state-filtered job lists (created_at DESC, job_id DESC). jobs_queue_list serves the queue filter.
CREATE INDEX jobs_created_list
    ON runwick.jobs (application, created_at DESC, job_id DESC);

CREATE INDEX jobs_state_list
    ON runwick.jobs (application, state, created_at DESC, job_id DESC);

ALTER TABLE runwick.management_audit DROP CONSTRAINT management_audit_operation_v4_ck;

ALTER TABLE runwick.management_audit
    ADD CONSTRAINT management_audit_operation_v5_ck CHECK (operation IN (
        'Cancel', 'ManualRetry', 'RerunSucceeded', 'SetQueueState', 'EditScopePolicy',
        'EditRatePolicy', 'ActivateCapabilities',
        'SetScheduleState', 'UpdateSchedule', 'TriggerSchedule', 'ResolveCatchUp', 'RevalidateSchedule',
        'RecordScheduleEvaluationFailure',
        'SetPoolOverride',
        'RevealPayload', 'RevealError'));

ALTER TABLE runwick.management_audit DROP CONSTRAINT management_audit_target_kind_v4_ck;

ALTER TABLE runwick.management_audit
    ADD CONSTRAINT management_audit_target_kind_v5_ck CHECK (target_kind IN (
        'job', 'queue', 'scope', 'rate_scope', 'application', 'schedule', 'pool', 'attempt'));
