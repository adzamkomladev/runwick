-- 0004_rates — expand phase. One transaction. Rate admission records (10 §3). Additive except the widened
-- queue_events kind and management_audit checks. Rate policies take effect only after the application activates
-- capability rate_admission (10 §6); until then Dispatch refuses rate scopes.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- §3.1 Rate scopes ---------------------------------------------------------------------------

CREATE TABLE runwick.rate_scopes (
    application         runwick.rw_identifier NOT NULL,
    rate_scope_id       uuid        NOT NULL,
    scope_kind          text COLLATE "C" NOT NULL,
    scope_key           runwick.rw_key NOT NULL,
    limit_count         integer     NOT NULL,
    window_ms           bigint      NOT NULL,
    policy_revision     bigint      NOT NULL DEFAULT 1,
    last_decision_at    timestamptz,
    not_before          timestamptz,
    created_at          timestamptz NOT NULL,
    updated_at          timestamptz NOT NULL,
    updated_by          text,
    CONSTRAINT rate_scopes_pk PRIMARY KEY (application, rate_scope_id),
    CONSTRAINT rate_scopes_natural_uq UNIQUE (application, scope_kind, scope_key),
    CONSTRAINT rate_scopes_kind_ck CHECK (scope_kind IN ('queue', 'job-type', 'resource')),
    CONSTRAINT rate_scopes_limit_ck CHECK (limit_count BETWEEN 1 AND 100000),
    CONSTRAINT rate_scopes_window_ck CHECK (window_ms BETWEEN 1000 AND 86400000),
    CONSTRAINT rate_scopes_revision_ck CHECK (policy_revision >= 1),
    CONSTRAINT rate_scopes_updated_by_ck CHECK (updated_by IS NULL OR octet_length(updated_by) BETWEEN 1 AND 256)
);

-- §3.2 Requirements: immutable for the job's life (every generation keeps the dispatch-time set) ---------

CREATE TABLE runwick.job_rate_requirements (
    application     runwick.rw_identifier NOT NULL,
    job_id          uuid NOT NULL,
    rate_scope_id   uuid NOT NULL,
    CONSTRAINT job_rate_requirements_pk PRIMARY KEY (application, job_id, rate_scope_id),
    CONSTRAINT job_rate_requirements_job_fk FOREIGN KEY (application, job_id)
        REFERENCES runwick.jobs (application, job_id) ON DELETE CASCADE,
    CONSTRAINT job_rate_requirements_scope_fk FOREIGN KEY (application, rate_scope_id)
        REFERENCES runwick.rate_scopes (application, rate_scope_id)
);

CREATE INDEX job_rate_requirements_scope
    ON runwick.job_rate_requirements (application, rate_scope_id);

-- §3.3 Grants: one row per scope per authorized execution. No FK to jobs: grant history survives job retention.

CREATE TABLE runwick.rate_grants (
    application     runwick.rw_identifier NOT NULL,
    rate_scope_id   uuid        NOT NULL,
    attempt_id      uuid        NOT NULL,
    job_id          uuid        NOT NULL,
    authorized_at   timestamptz NOT NULL,
    policy_revision bigint      NOT NULL,
    CONSTRAINT rate_grants_pk PRIMARY KEY (application, rate_scope_id, attempt_id),
    CONSTRAINT rate_grants_scope_fk FOREIGN KEY (application, rate_scope_id)
        REFERENCES runwick.rate_scopes (application, rate_scope_id),
    CONSTRAINT rate_grants_revision_ck CHECK (policy_revision >= 1)
);

CREATE INDEX rate_grants_window
    ON runwick.rate_grants (application, rate_scope_id, authorized_at);

-- Events and audit ---------------------------------------------------------------------------

ALTER TABLE runwick.queue_events DROP CONSTRAINT queue_events_kind_v2_ck;

ALTER TABLE runwick.queue_events
    ADD CONSTRAINT queue_events_kind_v3_ck CHECK (event_kind IN (
        'dispatched', 'policy_drift',
        'attempt_reserved', 'attempt_started', 'attempt_cancelled_before_start',
        'attempt_finished', 'attempt_recovered', 'attempt_released',
        'job_cancelled', 'job_cancel_requested', 'job_retried', 'job_rerun',
        'queue_paused', 'queue_resumed', 'scope_policy_edited',
        'attempt_admission_deferred', 'rate_policy_edited', 'capabilities_activated'));

ALTER TABLE runwick.management_audit DROP CONSTRAINT management_audit_operation_ck;

ALTER TABLE runwick.management_audit
    ADD CONSTRAINT management_audit_operation_v2_ck CHECK (operation IN (
        'Cancel', 'ManualRetry', 'RerunSucceeded', 'SetQueueState', 'EditScopePolicy',
        'EditRatePolicy', 'ActivateCapabilities'));

ALTER TABLE runwick.management_audit DROP CONSTRAINT management_audit_target_kind_ck;

ALTER TABLE runwick.management_audit
    ADD CONSTRAINT management_audit_target_kind_v2_ck CHECK (target_kind IN ('job', 'queue', 'scope', 'rate_scope', 'application'));
