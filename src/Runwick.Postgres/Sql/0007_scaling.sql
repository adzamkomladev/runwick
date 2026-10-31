-- 0007_scaling (SP-014, 11 §5–§6): revisioned per-incarnation pool target overrides and bounded scaling history.
-- Neither table is referenced by leases, attempts or permits; presence pruning never touches them.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE TABLE runwick.pool_controls (
    application runwick.rw_identifier NOT NULL,
    instance_id uuid                  NOT NULL,
    pool_name   text COLLATE "C"      NOT NULL,
    target      integer,
    revision    bigint                NOT NULL DEFAULT 1,
    expires_at  timestamptz,
    reason      text,
    updated_at  timestamptz           NOT NULL,
    updated_by  text,
    CONSTRAINT pool_controls_pk PRIMARY KEY (application, instance_id, pool_name),
    CONSTRAINT pool_controls_pool_ck CHECK (octet_length(pool_name) BETWEEN 1 AND 64),
    CONSTRAINT pool_controls_target_ck CHECK (target IS NULL OR target BETWEEN 0 AND 10000),
    CONSTRAINT pool_controls_revision_ck CHECK (revision >= 1),
    CONSTRAINT pool_controls_expiry_ck CHECK (target IS NOT NULL OR expires_at IS NULL),
    CONSTRAINT pool_controls_reason_ck CHECK (reason IS NULL OR octet_length(reason) <= 512),
    CONSTRAINT pool_controls_updated_by_ck CHECK (updated_by IS NULL OR octet_length(updated_by) BETWEEN 1 AND 256)
);

CREATE TABLE runwick.scaling_events (
    application        runwick.rw_identifier NOT NULL,
    event_id           uuid                  NOT NULL,
    instance_id        uuid                  NOT NULL,
    pool_name          text COLLATE "C"      NOT NULL,
    old_target         integer               NOT NULL,
    new_target         integer               NOT NULL,
    desired            integer,
    candidate          integer,
    reason             text COLLATE "C"      NOT NULL,
    snapshot_at        timestamptz,
    membership_digest  bigint,
    created_at         timestamptz           NOT NULL,
    CONSTRAINT scaling_events_pk PRIMARY KEY (application, event_id),
    CONSTRAINT scaling_events_pool_ck CHECK (octet_length(pool_name) BETWEEN 1 AND 64),
    CONSTRAINT scaling_events_targets_ck CHECK (old_target >= 0 AND new_target >= 0 AND old_target <> new_target),
    CONSTRAINT scaling_events_reason_ck CHECK (octet_length(reason) BETWEEN 1 AND 64)
);

CREATE INDEX scaling_events_instance
    ON runwick.scaling_events (application, instance_id, created_at);

-- Events and audit: widened on top of 0006_scheduler's lists (v4/v3). Re-check against 0006 at SP-016 integration.

ALTER TABLE runwick.queue_events DROP CONSTRAINT queue_events_kind_v4_ck;

ALTER TABLE runwick.queue_events
    ADD CONSTRAINT queue_events_kind_v5_ck CHECK (event_kind IN (
        'dispatched', 'policy_drift',
        'attempt_reserved', 'attempt_started', 'attempt_cancelled_before_start',
        'attempt_finished', 'attempt_recovered', 'attempt_released',
        'job_cancelled', 'job_cancel_requested', 'job_retried', 'job_rerun',
        'queue_paused', 'queue_resumed', 'scope_policy_edited',
        'attempt_admission_deferred', 'rate_policy_edited', 'capabilities_activated',
        'schedule_registered', 'schedule_updated', 'schedule_materialized', 'schedule_paused', 'schedule_resumed',
        'schedule_triggered', 'schedule_needs_review', 'schedule_revalidated', 'catch_up_resolved',
        'pool_override_set'));

ALTER TABLE runwick.management_audit DROP CONSTRAINT management_audit_operation_v3_ck;

ALTER TABLE runwick.management_audit
    ADD CONSTRAINT management_audit_operation_v4_ck CHECK (operation IN (
        'Cancel', 'ManualRetry', 'RerunSucceeded', 'SetQueueState', 'EditScopePolicy',
        'EditRatePolicy', 'ActivateCapabilities',
        'SetScheduleState', 'UpdateSchedule', 'TriggerSchedule', 'ResolveCatchUp', 'RevalidateSchedule',
        'RecordScheduleEvaluationFailure',
        'SetPoolOverride'));

ALTER TABLE runwick.management_audit DROP CONSTRAINT management_audit_target_kind_v3_ck;

ALTER TABLE runwick.management_audit
    ADD CONSTRAINT management_audit_target_kind_v4_ck CHECK (target_kind IN (
        'job', 'queue', 'scope', 'rate_scope', 'application', 'schedule', 'pool'));
