-- 0001_initial (SQLite schema revision 1, 20 §3): the tables of PostgreSQL schema revision 15 under the SQLite rules.
-- Tables are STRICT; UUIDs are 16-byte big-endian BLOBs; instants are UTC epoch milliseconds; JSON is TEXT.
-- Runs inside the migration's BEGIN IMMEDIATE transaction (20 §9.2). No functions, no triggers.

CREATE TABLE runwick_schema_migrations (
    migration_id           TEXT    NOT NULL CHECK (migration_id GLOB '[0-9][0-9][0-9][0-9]_*'),
    phase                  TEXT    NOT NULL CHECK (phase IN ('preflight', 'expand', 'backfill', 'validate', 'activate', 'contract')),
    checksum               BLOB    NOT NULL CHECK (length(checksum) = 32),
    target_schema_revision INTEGER NOT NULL CHECK (target_schema_revision >= 1),
    state                  TEXT    NOT NULL CHECK (state IN ('started', 'applied', 'failed')),
    runner_build           TEXT    NOT NULL CHECK (length(CAST(runner_build AS BLOB)) BETWEEN 1 AND 256),
    started_at             INTEGER NOT NULL,
    completed_at           INTEGER,
    CONSTRAINT schema_migrations_completed_ck CHECK ((state = 'started') = (completed_at IS NULL)),
    CONSTRAINT schema_migrations_pk PRIMARY KEY (migration_id, phase)
) STRICT, WITHOUT ROWID;

CREATE TABLE runwick_store_metadata (
    singleton_id                   INTEGER NOT NULL CHECK (singleton_id = 1),
    store_id                       BLOB    NOT NULL CHECK (length(store_id) = 16),
    driver                         TEXT    NOT NULL CHECK (driver = 'sqlite'),
    schema_revision                INTEGER NOT NULL CHECK (schema_revision >= 0),
    protocol_epoch                 INTEGER NOT NULL CHECK (protocol_epoch >= 1),
    migration_mode                 TEXT    NOT NULL CHECK (migration_mode IN ('normal', 'expand', 'maintenance')),
    compatibility_manifest_version INTEGER NOT NULL CHECK (compatibility_manifest_version >= 1),
    compatibility_manifest         TEXT    NOT NULL CHECK (json_type(compatibility_manifest) = 'object'),
    manifest_digest                BLOB    NOT NULL CHECK (length(manifest_digest) = 32),
    revision                       INTEGER NOT NULL CHECK (revision >= 1),
    updated_at                     INTEGER NOT NULL,
    fault_hooks                    INTEGER NOT NULL DEFAULT 0 CHECK (fault_hooks IN (0, 1)),
    CONSTRAINT store_metadata_unvalidated_ck CHECK (schema_revision >= 1 OR migration_mode = 'maintenance'),
    CONSTRAINT store_metadata_pk PRIMARY KEY (singleton_id),
    CONSTRAINT store_metadata_store_id_uq UNIQUE (store_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE runwick_application_controls (
    application             TEXT    NOT NULL CHECK (length(CAST(application AS BLOB)) BETWEEN 1 AND 128 AND application GLOB '[A-Za-z0-9]*' AND application NOT GLOB '*[^A-Za-z0-9._-]*'),
    revision                INTEGER NOT NULL DEFAULT 1 CHECK (revision >= 1),
    required_protocol_epoch INTEGER NOT NULL DEFAULT 1 CHECK (required_protocol_epoch >= 1),
    required_capabilities   TEXT    NOT NULL DEFAULT '{}' CHECK (json_type(required_capabilities) = 'object'),
    admission_mode          TEXT    NOT NULL DEFAULT 'open' CHECK (admission_mode IN ('open', 'drain', 'maintenance')),
    created_at              INTEGER NOT NULL,
    updated_at              INTEGER NOT NULL,
    actor_subject           TEXT    CHECK (actor_subject IS NULL OR length(CAST(actor_subject AS BLOB)) BETWEEN 1 AND 256),
    CONSTRAINT application_controls_pk PRIMARY KEY (application)
) STRICT, WITHOUT ROWID;

CREATE TABLE runwick_queue_controls (
    application TEXT    NOT NULL,
    queue       TEXT    NOT NULL CHECK (length(CAST(queue AS BLOB)) BETWEEN 1 AND 128 AND queue GLOB '[A-Za-z0-9]*' AND queue NOT GLOB '*[^A-Za-z0-9._-]*'),
    paused      INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0, 1)),
    revision    INTEGER NOT NULL DEFAULT 1 CHECK (revision >= 1),
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL,
    updated_by  TEXT    CHECK (updated_by IS NULL OR length(CAST(updated_by AS BLOB)) BETWEEN 1 AND 256),
    CONSTRAINT queue_controls_pk PRIMARY KEY (application, queue)
) STRICT, WITHOUT ROWID;

CREATE TABLE runwick_jobs (
    application                TEXT    NOT NULL,
    job_id                     BLOB    NOT NULL CHECK (length(job_id) = 16),
    job_name                   TEXT    NOT NULL CHECK (length(CAST(job_name AS BLOB)) BETWEEN 1 AND 200 AND job_name GLOB '[A-Za-z0-9]*' AND job_name NOT GLOB '*[^A-Za-z0-9._-]*'),
    payload_version            INTEGER NOT NULL CHECK (payload_version >= 1),
    serializer_profile_id      TEXT    NOT NULL,
    serializer_profile_version INTEGER NOT NULL CHECK (serializer_profile_version >= 1),
    queue                      TEXT    NOT NULL,
    priority                   TEXT    NOT NULL CHECK (priority IN ('low', 'medium', 'high')),
    state                      TEXT    NOT NULL CHECK (state IN ('pending', 'reserved', 'running', 'retrying', 'succeeded', 'failed', 'cancelled')),
    eligible_at                INTEGER NOT NULL,
    created_at                 INTEGER NOT NULL,
    updated_at                 INTEGER NOT NULL,
    generation                 INTEGER NOT NULL DEFAULT 1 CHECK (generation >= 1),
    revision                   INTEGER NOT NULL DEFAULT 1 CHECK (revision >= 1),
    reservation_count          INTEGER NOT NULL DEFAULT 0 CHECK (reservation_count >= 0),
    execution_count            INTEGER NOT NULL DEFAULT 0,
    current_attempt_id         BLOB    CHECK (current_attempt_id IS NULL OR length(current_attempt_id) = 16),
    worker_instance_id         BLOB    CHECK (worker_instance_id IS NULL OR length(worker_instance_id) = 16),
    ownership_token            BLOB    CHECK (ownership_token IS NULL OR length(ownership_token) = 16),
    lease_expires_at           INTEGER,
    cancel_requested_at        INTEGER,
    cancel_reason              TEXT    CHECK (cancel_reason IS NULL OR length(CAST(cancel_reason AS BLOB)) <= 512),
    cancel_requested_by        TEXT    CHECK (cancel_requested_by IS NULL OR length(CAST(cancel_requested_by AS BLOB)) BETWEEN 1 AND 256),
    terminal_at                INTEGER,
    terminal_reason            TEXT,
    active_unique_key          TEXT    CHECK (active_unique_key IS NULL OR length(CAST(active_unique_key AS BLOB)) BETWEEN 1 AND 512),
    payload_retained           INTEGER NOT NULL DEFAULT 1 CHECK (payload_retained IN (0, 1)),
    detail_expired_at          INTEGER,
    CONSTRAINT jobs_cancel_ck CHECK (cancel_requested_at IS NOT NULL OR (cancel_reason IS NULL AND cancel_requested_by IS NULL)),
    CONSTRAINT jobs_detail_expired_ck CHECK (detail_expired_at IS NULL OR state IN ('succeeded', 'failed', 'cancelled')),
    CONSTRAINT jobs_execution_count_ck CHECK (execution_count >= 0 AND execution_count <= reservation_count),
    CONSTRAINT jobs_ownership_ck CHECK (CASE WHEN state IN ('reserved', 'running')
        THEN current_attempt_id IS NOT NULL AND worker_instance_id IS NOT NULL AND ownership_token IS NOT NULL AND lease_expires_at IS NOT NULL
        ELSE current_attempt_id IS NULL AND worker_instance_id IS NULL AND ownership_token IS NULL AND lease_expires_at IS NULL END),
    CONSTRAINT jobs_payload_retained_ck CHECK (payload_retained = 1 OR state IN ('succeeded', 'failed', 'cancelled')),
    CONSTRAINT jobs_terminal_ck CHECK (CASE WHEN state IN ('succeeded', 'failed', 'cancelled')
        THEN terminal_at IS NOT NULL AND terminal_reason IS NOT NULL ELSE terminal_at IS NULL AND terminal_reason IS NULL END),
    CONSTRAINT jobs_terminal_reason_ck CHECK (terminal_reason IS NULL
        OR (state = 'succeeded' AND terminal_reason = 'succeeded')
        OR (state = 'failed' AND terminal_reason IN ('permanent_failure', 'execution_budget_exhausted', 'reservation_abandonment_exhausted'))
        OR (state = 'cancelled' AND terminal_reason IN ('user_cancelled', 'cancellation_requested', 'cancelled_before_start', 'cancelled_while_waiting'))),
    CONSTRAINT jobs_pk PRIMARY KEY (application, job_id),
    CONSTRAINT jobs_profile_uq UNIQUE (application, job_id, serializer_profile_id, serializer_profile_version),
    CONSTRAINT jobs_current_attempt_fk FOREIGN KEY (application, job_id, current_attempt_id)
        REFERENCES runwick_job_attempts (application, job_id, attempt_id) DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT jobs_current_generation_fk FOREIGN KEY (application, job_id, generation)
        REFERENCES runwick_job_generations (application, job_id, generation) DEFERRABLE INITIALLY DEFERRED
) STRICT, WITHOUT ROWID;

CREATE INDEX runwick_jobs_ready ON runwick_jobs (application, queue, priority, eligible_at, job_id)
    WHERE state IN ('pending', 'retrying');
CREATE INDEX runwick_jobs_expired_lease ON runwick_jobs (application, lease_expires_at, job_id)
    WHERE state IN ('reserved', 'running');
CREATE INDEX runwick_jobs_terminal_retention ON runwick_jobs (application, terminal_at, job_id)
    WHERE state IN ('succeeded', 'failed', 'cancelled');
CREATE INDEX runwick_jobs_created_list ON runwick_jobs (application, created_at DESC, job_id DESC);
CREATE INDEX runwick_jobs_queue_list ON runwick_jobs (application, queue, created_at, job_id);
CREATE INDEX runwick_jobs_state_list ON runwick_jobs (application, state, created_at DESC, job_id DESC);

CREATE TABLE runwick_job_payloads (
    application                TEXT    NOT NULL,
    job_id                     BLOB    NOT NULL CHECK (length(job_id) = 16),
    payload_utf8               BLOB    NOT NULL CHECK (length(payload_utf8) BETWEEN 2 AND 1048576),
    payload_encoding_version   INTEGER NOT NULL DEFAULT 1 CHECK (payload_encoding_version = 1),
    serializer_profile_id      TEXT    NOT NULL,
    serializer_profile_version INTEGER NOT NULL,
    headers                    TEXT    NOT NULL DEFAULT '{"business":{},"diagnostic":{}}'
        CHECK (json_type(headers) = 'object' AND json_type(headers, '$.business') = 'object' AND json_type(headers, '$.diagnostic') = 'object'),
    CONSTRAINT job_payloads_pk PRIMARY KEY (application, job_id),
    CONSTRAINT job_payloads_job_profile_fk FOREIGN KEY (application, job_id, serializer_profile_id, serializer_profile_version)
        REFERENCES runwick_jobs (application, job_id, serializer_profile_id, serializer_profile_version) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE TABLE runwick_job_generations (
    application                    TEXT    NOT NULL,
    job_id                         BLOB    NOT NULL CHECK (length(job_id) = 16),
    generation                     INTEGER NOT NULL CHECK (generation >= 1),
    created_at                     INTEGER NOT NULL,
    created_by                     TEXT    CHECK (created_by IS NULL OR length(CAST(created_by AS BLOB)) BETWEEN 1 AND 256),
    created_reason                 TEXT    NOT NULL CHECK (created_reason IN ('initial', 'scheduled', 'manual_retry', 'rerun_succeeded')),
    policy_snapshot_version        INTEGER NOT NULL DEFAULT 1 CHECK (policy_snapshot_version >= 1),
    max_executions                 INTEGER NOT NULL CHECK (max_executions BETWEEN 1 AND 100),
    max_unstarted_abandonments     INTEGER NOT NULL CHECK (max_unstarted_abandonments BETWEEN 1 AND 100),
    execution_timeout_ms           INTEGER CHECK (execution_timeout_ms IS NULL OR execution_timeout_ms BETWEEN 1 AND 2592000000),
    retry_policy                   TEXT    NOT NULL CHECK (json_type(retry_policy) = 'object'),
    retry_policy_version           INTEGER NOT NULL DEFAULT 1 CHECK (retry_policy_version >= 1),
    unstarted_retry_policy         TEXT    NOT NULL CHECK (json_type(unstarted_retry_policy) = 'object'),
    unstarted_retry_policy_version INTEGER NOT NULL DEFAULT 1 CHECK (unstarted_retry_policy_version >= 1),
    started_count                  INTEGER NOT NULL DEFAULT 0,
    consecutive_unstarted_count    INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT job_generations_started_ck CHECK (started_count >= 0 AND started_count <= max_executions),
    CONSTRAINT job_generations_unstarted_ck CHECK (consecutive_unstarted_count >= 0 AND consecutive_unstarted_count <= max_unstarted_abandonments),
    CONSTRAINT job_generations_pk PRIMARY KEY (application, job_id, generation),
    CONSTRAINT job_generations_job_fk FOREIGN KEY (application, job_id) REFERENCES runwick_jobs (application, job_id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE TABLE runwick_job_attempts (
    application          TEXT    NOT NULL,
    attempt_id           BLOB    NOT NULL CHECK (length(attempt_id) = 16),
    job_id               BLOB    NOT NULL CHECK (length(job_id) = 16),
    generation           INTEGER NOT NULL,
    reservation_number   INTEGER NOT NULL CHECK (reservation_number >= 1),
    execution_number     INTEGER CHECK (execution_number IS NULL OR execution_number >= 1),
    worker_instance_id   BLOB    NOT NULL CHECK (length(worker_instance_id) = 16),
    ownership_token      BLOB    NOT NULL CHECK (length(ownership_token) = 16),
    reserved_at          INTEGER NOT NULL,
    authorized_at        INTEGER,
    finished_at          INTEGER,
    execution_elapsed_ms INTEGER,
    outcome              TEXT    CHECK (outcome IS NULL OR outcome IN ('succeeded', 'retryable_failure', 'permanent_failure', 'user_cancelled',
                                     'interrupted', 'timed_out', 'abandoned_before_start', 'abandoned_running', 'released',
                                     'cancelled_before_start', 'admission_deferred')),
    disposition          TEXT    CHECK (disposition IS NULL OR disposition IN ('pending', 'retrying', 'succeeded', 'failed', 'cancelled')),
    applied_reason       TEXT,
    retry_delay_ms       INTEGER,
    next_eligible_at     INTEGER,
    error                TEXT    CHECK (error IS NULL OR (json_type(error) = 'object' AND length(CAST(error AS BLOB)) <= 20480)),
    closed_by_command_id BLOB    CHECK (closed_by_command_id IS NULL OR length(closed_by_command_id) = 16),
    CONSTRAINT job_attempts_authorized_ck CHECK ((execution_number IS NULL) = (authorized_at IS NULL)),
    CONSTRAINT job_attempts_closed_ck CHECK ((finished_at IS NULL) = (outcome IS NULL) AND (finished_at IS NULL) = (disposition IS NULL)
        AND (finished_at IS NULL) = (closed_by_command_id IS NULL)),
    CONSTRAINT job_attempts_elapsed_ck CHECK (execution_elapsed_ms IS NULL OR (execution_elapsed_ms >= 0 AND execution_number IS NOT NULL)),
    CONSTRAINT job_attempts_executed_outcome_ck CHECK (outcome IS NULL OR (outcome IN ('succeeded', 'retryable_failure', 'permanent_failure',
        'user_cancelled', 'interrupted', 'timed_out', 'abandoned_running')) = (execution_number IS NOT NULL)),
    CONSTRAINT job_attempts_reason_ck CHECK ((applied_reason IS NOT NULL) = coalesce(disposition IN ('succeeded', 'failed', 'cancelled'), 0)),
    CONSTRAINT job_attempts_retry_ck CHECK ((retry_delay_ms IS NULL) = (next_eligible_at IS NULL)
        AND (retry_delay_ms IS NULL OR (retry_delay_ms >= 1 AND disposition IN ('pending', 'retrying')))),
    CONSTRAINT job_attempts_pk PRIMARY KEY (application, attempt_id),
    CONSTRAINT job_attempts_execution_uq UNIQUE (application, job_id, generation, execution_number),
    CONSTRAINT job_attempts_job_attempt_uq UNIQUE (application, job_id, attempt_id),
    CONSTRAINT job_attempts_reservation_uq UNIQUE (application, job_id, reservation_number),
    CONSTRAINT job_attempts_generation_fk FOREIGN KEY (application, job_id, generation)
        REFERENCES runwick_job_generations (application, job_id, generation) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE UNIQUE INDEX runwick_job_attempts_one_open ON runwick_job_attempts (application, job_id) WHERE finished_at IS NULL;

CREATE TABLE runwick_concurrency_scopes (
    application     TEXT    NOT NULL,
    scope_id        BLOB    NOT NULL CHECK (length(scope_id) = 16),
    scope_kind      TEXT    NOT NULL CHECK (scope_kind IN ('queue', 'job-type', 'resource', 'schedule')),
    scope_key       TEXT    NOT NULL CHECK (length(CAST(scope_key AS BLOB)) BETWEEN 1 AND 512),
    capacity        INTEGER NOT NULL CHECK (capacity >= 0),
    policy_revision INTEGER NOT NULL DEFAULT 1 CHECK (policy_revision >= 1),
    fencing_enabled INTEGER NOT NULL CHECK (fencing_enabled IN (0, 1)),
    fencing_counter INTEGER NOT NULL DEFAULT 0,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL,
    CONSTRAINT concurrency_scopes_fencing_ck CHECK (fencing_counter >= 0 AND (fencing_enabled = 1 OR fencing_counter = 0)),
    CONSTRAINT concurrency_scopes_fencing_kind_ck CHECK (fencing_enabled = 0 OR scope_kind = 'resource'),
    CONSTRAINT concurrency_scopes_pk PRIMARY KEY (application, scope_id),
    CONSTRAINT concurrency_scopes_natural_uq UNIQUE (application, scope_kind, scope_key)
) STRICT, WITHOUT ROWID;

CREATE TABLE runwick_job_concurrency_requirements (
    application TEXT NOT NULL,
    job_id      BLOB NOT NULL CHECK (length(job_id) = 16),
    scope_id    BLOB NOT NULL CHECK (length(scope_id) = 16),
    CONSTRAINT job_concurrency_requirements_pk PRIMARY KEY (application, job_id, scope_id),
    CONSTRAINT job_concurrency_requirements_job_fk FOREIGN KEY (application, job_id) REFERENCES runwick_jobs (application, job_id) ON DELETE CASCADE,
    CONSTRAINT job_concurrency_requirements_scope_fk FOREIGN KEY (application, scope_id) REFERENCES runwick_concurrency_scopes (application, scope_id)
) STRICT, WITHOUT ROWID;

CREATE INDEX runwick_job_concurrency_requirements_scope ON runwick_job_concurrency_requirements (application, scope_id, job_id);

CREATE TABLE runwick_concurrency_permits (
    application      TEXT    NOT NULL,
    scope_id         BLOB    NOT NULL CHECK (length(scope_id) = 16),
    attempt_id       BLOB    NOT NULL CHECK (length(attempt_id) = 16),
    job_id           BLOB    NOT NULL CHECK (length(job_id) = 16),
    ownership_token  BLOB    NOT NULL CHECK (length(ownership_token) = 16),
    lease_expires_at INTEGER NOT NULL,
    fencing_value    INTEGER CHECK (fencing_value IS NULL OR fencing_value >= 1),
    CONSTRAINT concurrency_permits_pk PRIMARY KEY (application, scope_id, attempt_id),
    CONSTRAINT concurrency_permits_attempt_fk FOREIGN KEY (application, job_id, attempt_id) REFERENCES runwick_job_attempts (application, job_id, attempt_id),
    CONSTRAINT concurrency_permits_requirement_fk FOREIGN KEY (application, job_id, scope_id)
        REFERENCES runwick_job_concurrency_requirements (application, job_id, scope_id),
    CONSTRAINT concurrency_permits_scope_fk FOREIGN KEY (application, scope_id) REFERENCES runwick_concurrency_scopes (application, scope_id)
) STRICT, WITHOUT ROWID;

CREATE INDEX runwick_permits_attempt ON runwick_concurrency_permits (application, attempt_id, scope_id);
CREATE INDEX runwick_permits_job_attempt ON runwick_concurrency_permits (application, job_id, attempt_id);
CREATE INDEX runwick_permits_job_scope ON runwick_concurrency_permits (application, job_id, scope_id);
CREATE INDEX runwick_permits_live_count ON runwick_concurrency_permits (application, scope_id, lease_expires_at);

CREATE TABLE runwick_rate_scopes (
    application      TEXT    NOT NULL,
    rate_scope_id    BLOB    NOT NULL CHECK (length(rate_scope_id) = 16),
    scope_kind       TEXT    NOT NULL CHECK (scope_kind IN ('queue', 'job-type', 'resource')),
    scope_key        TEXT    NOT NULL CHECK (length(CAST(scope_key AS BLOB)) BETWEEN 1 AND 512),
    limit_count      INTEGER NOT NULL CHECK (limit_count BETWEEN 1 AND 100000),
    window_ms        INTEGER NOT NULL CHECK (window_ms BETWEEN 1000 AND 86400000),
    policy_revision  INTEGER NOT NULL DEFAULT 1 CHECK (policy_revision >= 1),
    last_decision_at INTEGER,
    not_before       INTEGER,
    created_at       INTEGER NOT NULL,
    updated_at       INTEGER NOT NULL,
    updated_by       TEXT    CHECK (updated_by IS NULL OR length(CAST(updated_by AS BLOB)) BETWEEN 1 AND 256),
    CONSTRAINT rate_scopes_pk PRIMARY KEY (application, rate_scope_id),
    CONSTRAINT rate_scopes_natural_uq UNIQUE (application, scope_kind, scope_key)
) STRICT, WITHOUT ROWID;

CREATE TABLE runwick_job_rate_requirements (
    application   TEXT NOT NULL,
    job_id        BLOB NOT NULL CHECK (length(job_id) = 16),
    rate_scope_id BLOB NOT NULL CHECK (length(rate_scope_id) = 16),
    CONSTRAINT job_rate_requirements_pk PRIMARY KEY (application, job_id, rate_scope_id),
    CONSTRAINT job_rate_requirements_job_fk FOREIGN KEY (application, job_id) REFERENCES runwick_jobs (application, job_id) ON DELETE CASCADE,
    CONSTRAINT job_rate_requirements_scope_fk FOREIGN KEY (application, rate_scope_id) REFERENCES runwick_rate_scopes (application, rate_scope_id)
) STRICT, WITHOUT ROWID;

CREATE INDEX runwick_job_rate_requirements_scope ON runwick_job_rate_requirements (application, rate_scope_id);

CREATE TABLE runwick_rate_grants (
    application     TEXT    NOT NULL,
    rate_scope_id   BLOB    NOT NULL CHECK (length(rate_scope_id) = 16),
    attempt_id      BLOB    NOT NULL CHECK (length(attempt_id) = 16),
    job_id          BLOB    NOT NULL CHECK (length(job_id) = 16),
    authorized_at   INTEGER NOT NULL,
    policy_revision INTEGER NOT NULL CHECK (policy_revision >= 1),
    CONSTRAINT rate_grants_pk PRIMARY KEY (application, rate_scope_id, attempt_id),
    CONSTRAINT rate_grants_scope_fk FOREIGN KEY (application, rate_scope_id) REFERENCES runwick_rate_scopes (application, rate_scope_id)
) STRICT, WITHOUT ROWID;

CREATE INDEX runwick_rate_grants_window ON runwick_rate_grants (application, rate_scope_id, authorized_at);

CREATE TABLE runwick_dispatch_keys (
    application          TEXT    NOT NULL,
    job_name             TEXT    NOT NULL,
    key_kind             TEXT    NOT NULL CHECK (key_kind IN ('active', 'window')),
    key                  TEXT    NOT NULL CHECK (length(CAST(key AS BLOB)) BETWEEN 1 AND 512),
    bound_job_id         BLOB    CHECK (bound_job_id IS NULL OR length(bound_job_id) = 16),
    business_fingerprint BLOB    CHECK (business_fingerprint IS NULL OR length(business_fingerprint) = 32),
    policy_fingerprint   BLOB    CHECK (policy_fingerprint IS NULL OR length(policy_fingerprint) = 32),
    fingerprint_version  INTEGER CHECK (fingerprint_version IS NULL OR fingerprint_version >= 1),
    expires_at           INTEGER,
    created_at           INTEGER NOT NULL,
    updated_at           INTEGER NOT NULL,
    CONSTRAINT dispatch_keys_binding_ck CHECK (CASE key_kind
        WHEN 'active' THEN business_fingerprint IS NULL AND policy_fingerprint IS NULL AND fingerprint_version IS NULL AND expires_at IS NULL
        ELSE (bound_job_id IS NULL AND business_fingerprint IS NULL AND policy_fingerprint IS NULL AND fingerprint_version IS NULL AND expires_at IS NULL)
          OR (bound_job_id IS NOT NULL AND business_fingerprint IS NOT NULL AND policy_fingerprint IS NOT NULL AND fingerprint_version IS NOT NULL
              AND expires_at IS NOT NULL) END),
    CONSTRAINT dispatch_keys_pk PRIMARY KEY (application, job_name, key_kind, key)
) STRICT, WITHOUT ROWID;

CREATE TABLE runwick_storage_commands (
    application           TEXT    NOT NULL,
    retention_class       TEXT    NOT NULL CHECK (retention_class IN ('ownership', 'durable')),
    command_id            BLOB    NOT NULL CHECK (length(command_id) = 16),
    operation             TEXT    NOT NULL CHECK (operation GLOB '[A-Z][A-Za-z]*' AND operation NOT GLOB '*[^A-Za-z]*' AND length(operation) BETWEEN 2 AND 64),
    operation_version     INTEGER NOT NULL,
    protocol_epoch        INTEGER NOT NULL,
    fingerprint           BLOB    NOT NULL CHECK (length(fingerprint) = 32),
    fingerprint_version   INTEGER NOT NULL,
    not_after             INTEGER NOT NULL,
    receipt_codec_version INTEGER NOT NULL,
    retain_until          INTEGER NOT NULL,
    status                TEXT,
    result                TEXT    CHECK (result IS NULL OR json_type(result) = 'object'),
    decided_at            INTEGER,
    CONSTRAINT storage_commands_decision_ck CHECK ((status IS NULL) = (result IS NULL) AND (result IS NULL) = (decided_at IS NULL)),
    CONSTRAINT storage_commands_no_renew_ck CHECK (operation <> 'Renew'),
    CONSTRAINT storage_commands_operation_class_v2_ck CHECK (
        (operation IN ('Reserve', 'Begin', 'Recover', 'ReleaseReservation', 'ReserveAndBegin')) = (retention_class = 'ownership')),
    CONSTRAINT storage_commands_retain_ck CHECK (retain_until >= not_after + CASE retention_class WHEN 'ownership' THEN 600000 ELSE 86400000 END),
    CONSTRAINT storage_commands_versions_ck CHECK (operation_version >= 1 AND protocol_epoch >= 1 AND fingerprint_version >= 1 AND receipt_codec_version >= 1),
    CONSTRAINT storage_commands_pk PRIMARY KEY (application, retention_class, command_id)
) STRICT, WITHOUT ROWID;

CREATE INDEX runwick_command_by_id ON runwick_storage_commands (application, command_id);
CREATE INDEX runwick_command_retention ON runwick_storage_commands (application, retain_until, command_id);
CREATE INDEX runwick_command_class_retention ON runwick_storage_commands (application, retention_class, not_after);

CREATE TABLE runwick_queue_events (
    event_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    application    TEXT    NOT NULL,
    job_id         BLOB    CHECK (job_id IS NULL OR length(job_id) = 16),
    attempt_id     BLOB    CHECK (attempt_id IS NULL OR length(attempt_id) = 16),
    command_id     BLOB    CHECK (command_id IS NULL OR length(command_id) = 16),
    event_kind     TEXT    NOT NULL CHECK (event_kind IN ('dispatched', 'policy_drift', 'attempt_reserved', 'attempt_started',
        'attempt_cancelled_before_start', 'attempt_finished', 'attempt_recovered', 'attempt_released', 'job_cancelled', 'job_cancel_requested',
        'job_retried', 'job_rerun', 'queue_paused', 'queue_resumed', 'scope_policy_edited', 'attempt_admission_deferred', 'rate_policy_edited',
        'capabilities_activated', 'schedule_registered', 'schedule_updated', 'schedule_materialized', 'schedule_paused', 'schedule_resumed',
        'schedule_triggered', 'schedule_needs_review', 'schedule_revalidated', 'catch_up_resolved', 'pool_override_set')),
    occurred_at    INTEGER NOT NULL,
    actor_id       TEXT    CHECK (actor_id IS NULL OR length(CAST(actor_id AS BLOB)) BETWEEN 1 AND 256),
    details        TEXT    NOT NULL DEFAULT '{}' CHECK (json_type(details) = 'object' AND length(CAST(details AS BLOB)) <= 4096),
    schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1)
) STRICT;

CREATE INDEX runwick_events_cursor ON runwick_queue_events (application, event_id);
CREATE INDEX runwick_events_job_history ON runwick_queue_events (application, job_id, event_id) WHERE job_id IS NOT NULL;
CREATE INDEX runwick_events_retention ON runwick_queue_events (application, occurred_at, event_id);

CREATE TABLE runwick_outbox_deliveries (
    application          TEXT    NOT NULL,
    source_store_id      BLOB    NOT NULL CHECK (length(source_store_id) = 16),
    message_id           BLOB    NOT NULL CHECK (length(message_id) = 16),
    delivery_fingerprint BLOB    NOT NULL,
    fingerprint_version  INTEGER NOT NULL DEFAULT 1,
    accept_until         INTEGER NOT NULL,
    retain_until         INTEGER NOT NULL,
    job_id               BLOB    NOT NULL CHECK (length(job_id) = 16),
    disposition          TEXT    NOT NULL CHECK (disposition IN ('Created', 'Reused')),
    duplicate_match      TEXT    NOT NULL CHECK (duplicate_match IN ('None', 'Active', 'Window', 'ActiveAndWindow')),
    policy_drift         INTEGER NOT NULL CHECK (policy_drift IN (0, 1)),
    confirmed_profile    TEXT    NOT NULL CHECK (length(CAST(confirmed_profile AS BLOB)) BETWEEN 1 AND 64),
    first_command_id     BLOB    NOT NULL CHECK (length(first_command_id) = 16),
    delivered_at         INTEGER NOT NULL,
    CONSTRAINT outbox_deliveries_fingerprint_ck CHECK (length(delivery_fingerprint) = 32 AND fingerprint_version = 1),
    CONSTRAINT outbox_deliveries_window_ck CHECK (delivered_at < accept_until AND retain_until >= accept_until + 86400000),
    CONSTRAINT outbox_deliveries_pk PRIMARY KEY (application, source_store_id, message_id)
) STRICT, WITHOUT ROWID;

CREATE INDEX runwick_outbox_deliveries_cleanup ON runwick_outbox_deliveries (retain_until);

CREATE TABLE runwick_worker_instances (
    application         TEXT    NOT NULL,
    instance_id         BLOB    NOT NULL CHECK (length(instance_id) = 16),
    process_id          INTEGER NOT NULL CHECK (process_id >= 0),
    host_name           TEXT    NOT NULL CHECK (length(CAST(host_name AS BLOB)) BETWEEN 1 AND 256),
    build_id            TEXT    NOT NULL CHECK (length(CAST(build_id AS BLOB)) BETWEEN 1 AND 256),
    status              TEXT    NOT NULL CHECK (status IN ('starting', 'ready', 'draining', 'degraded', 'stopping')),
    capability_revision INTEGER NOT NULL CHECK (capability_revision >= 1),
    presence_sequence   INTEGER NOT NULL CHECK (presence_sequence >= 1),
    subscriptions       TEXT    NOT NULL DEFAULT '[]' CHECK (json_type(subscriptions) = 'array' AND json_array_length(subscriptions) <= 128),
    pool_targets        TEXT    NOT NULL DEFAULT '{}' CHECK (json_type(pool_targets) = 'object'),
    started_at          INTEGER NOT NULL,
    heartbeat_at        INTEGER NOT NULL,
    expires_at          INTEGER NOT NULL,
    CONSTRAINT worker_instances_expiry_ck CHECK (expires_at > heartbeat_at AND heartbeat_at >= started_at),
    CONSTRAINT worker_instances_pk PRIMARY KEY (application, instance_id)
) STRICT, WITHOUT ROWID;

CREATE INDEX runwick_worker_instances_expiry ON runwick_worker_instances (application, expires_at, instance_id);

CREATE TABLE runwick_worker_capabilities (
    application                TEXT    NOT NULL,
    instance_id                BLOB    NOT NULL CHECK (length(instance_id) = 16),
    capability_revision        INTEGER NOT NULL CHECK (capability_revision >= 1),
    job_name                   TEXT    NOT NULL,
    payload_version            INTEGER NOT NULL CHECK (payload_version >= 1),
    serializer_profile_id      TEXT    NOT NULL,
    serializer_profile_version INTEGER NOT NULL CHECK (serializer_profile_version >= 1),
    CONSTRAINT worker_capabilities_pk PRIMARY KEY (application, instance_id, capability_revision, job_name, payload_version,
        serializer_profile_id, serializer_profile_version),
    CONSTRAINT worker_capabilities_instance_fk FOREIGN KEY (application, instance_id)
        REFERENCES runwick_worker_instances (application, instance_id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE INDEX runwick_worker_capabilities_tuple ON runwick_worker_capabilities (application, job_name, payload_version, serializer_profile_id,
    serializer_profile_version, instance_id);

CREATE TABLE runwick_worker_subscriptions (
    application TEXT NOT NULL,
    instance_id BLOB NOT NULL CHECK (length(instance_id) = 16),
    queue       TEXT NOT NULL,
    CONSTRAINT worker_subscriptions_pk PRIMARY KEY (application, instance_id, queue),
    CONSTRAINT worker_subscriptions_instance_fk FOREIGN KEY (application, instance_id)
        REFERENCES runwick_worker_instances (application, instance_id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE INDEX runwick_worker_subscriptions_queue ON runwick_worker_subscriptions (application, queue, instance_id);

CREATE TABLE runwick_pool_controls (
    application TEXT    NOT NULL,
    instance_id BLOB    NOT NULL CHECK (length(instance_id) = 16),
    pool_name   TEXT    NOT NULL CHECK (length(CAST(pool_name AS BLOB)) BETWEEN 1 AND 64),
    target      INTEGER CHECK (target IS NULL OR target BETWEEN 0 AND 10000),
    revision    INTEGER NOT NULL DEFAULT 1 CHECK (revision >= 1),
    expires_at  INTEGER,
    reason      TEXT    CHECK (reason IS NULL OR length(CAST(reason AS BLOB)) <= 512),
    updated_at  INTEGER NOT NULL,
    updated_by  TEXT    CHECK (updated_by IS NULL OR length(CAST(updated_by AS BLOB)) BETWEEN 1 AND 256),
    CONSTRAINT pool_controls_expiry_ck CHECK (target IS NOT NULL OR expires_at IS NULL),
    CONSTRAINT pool_controls_pk PRIMARY KEY (application, instance_id, pool_name)
) STRICT, WITHOUT ROWID;

CREATE TABLE runwick_scaling_events (
    application       TEXT    NOT NULL,
    event_id          BLOB    NOT NULL CHECK (length(event_id) = 16),
    instance_id       BLOB    NOT NULL CHECK (length(instance_id) = 16),
    pool_name         TEXT    NOT NULL CHECK (length(CAST(pool_name AS BLOB)) BETWEEN 1 AND 64),
    old_target        INTEGER NOT NULL,
    new_target        INTEGER NOT NULL,
    desired           INTEGER,
    candidate         INTEGER,
    reason            TEXT    NOT NULL CHECK (length(CAST(reason AS BLOB)) BETWEEN 1 AND 64),
    snapshot_at       INTEGER,
    membership_digest INTEGER,
    created_at        INTEGER NOT NULL,
    CONSTRAINT scaling_events_targets_ck CHECK (old_target >= 0 AND new_target >= 0 AND old_target <> new_target),
    CONSTRAINT scaling_events_pk PRIMARY KEY (application, event_id)
) STRICT, WITHOUT ROWID;

CREATE INDEX runwick_scaling_events_instance ON runwick_scaling_events (application, instance_id, created_at);

CREATE TABLE runwick_schedules (
    application         TEXT    NOT NULL,
    schedule_id         BLOB    NOT NULL CHECK (length(schedule_id) = 16),
    schedule_key        TEXT    NOT NULL CHECK (length(CAST(schedule_key AS BLOB)) BETWEEN 1 AND 512),
    definition_revision INTEGER NOT NULL DEFAULT 1,
    control_revision    INTEGER NOT NULL DEFAULT 1,
    cursor_revision     INTEGER NOT NULL DEFAULT 1,
    enabled             INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    paused              INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0, 1)),
    needs_review        INTEGER NOT NULL DEFAULT 0 CHECK (needs_review IN (0, 1)),
    review_reason       TEXT    CHECK (review_reason IS NULL OR length(CAST(review_reason AS BLOB)) BETWEEN 1 AND 64),
    next_due_at         INTEGER,
    scan_after_at       INTEGER NOT NULL,
    evaluated_through   INTEGER,
    episode_id          BLOB    CHECK (episode_id IS NULL OR length(episode_id) = 16),
    episode_cutoff      INTEGER,
    episode_emitted     INTEGER NOT NULL DEFAULT 0,
    episode_limit       INTEGER,
    resume_policy       TEXT,
    resume_cutoff       INTEGER,
    created_at          INTEGER NOT NULL,
    updated_at          INTEGER NOT NULL,
    updated_by          TEXT    CHECK (updated_by IS NULL OR length(CAST(updated_by AS BLOB)) BETWEEN 1 AND 256),
    CONSTRAINT schedules_episode_ck CHECK ((episode_id IS NULL AND episode_cutoff IS NULL AND episode_emitted = 0 AND episode_limit IS NULL)
        OR (episode_id IS NOT NULL AND episode_cutoff IS NOT NULL AND episode_emitted >= 0 AND episode_limit BETWEEN 1 AND 10000
            AND episode_emitted <= episode_limit)),
    CONSTRAINT schedules_resume_ck CHECK ((resume_policy IS NULL AND resume_cutoff IS NULL)
        OR (resume_policy IN ('skip_paused', 'run_once', 'catch_up', 'skipped_by_edit') AND resume_cutoff IS NOT NULL)),
    CONSTRAINT schedules_revisions_ck CHECK (definition_revision >= 1 AND control_revision >= 1 AND cursor_revision >= 1),
    CONSTRAINT schedules_pk PRIMARY KEY (application, schedule_id),
    CONSTRAINT schedules_key_uq UNIQUE (application, schedule_key)
) STRICT, WITHOUT ROWID;

CREATE INDEX runwick_schedules_due ON runwick_schedules (application, scan_after_at, schedule_id)
    WHERE enabled = 1 AND paused = 0 AND needs_review = 0 AND next_due_at IS NOT NULL;

CREATE TABLE runwick_schedule_definitions (
    application                TEXT    NOT NULL,
    schedule_id                BLOB    NOT NULL CHECK (length(schedule_id) = 16),
    revision                   INTEGER NOT NULL,
    kind                       TEXT    NOT NULL,
    due_at                     INTEGER,
    cron_text                  TEXT    CHECK (cron_text IS NULL OR length(CAST(cron_text AS BLOB)) BETWEEN 1 AND 256),
    cron_dialect               TEXT,
    zone_id                    TEXT    CHECK (zone_id IS NULL OR length(CAST(zone_id AS BLOB)) BETWEEN 1 AND 64),
    evaluator_profile          TEXT,
    tzdb_version               TEXT,
    tzdb_sha256                BLOB,
    gap_policy                 TEXT    NOT NULL CHECK (gap_policy IN ('skip', 'shift')),
    fold_policy                TEXT    NOT NULL CHECK (fold_policy IN ('earlier', 'later', 'both')),
    misfire_policy             TEXT    NOT NULL CHECK (misfire_policy IN ('skip', 'run_once', 'catch_up')),
    misfire_grace_ms           INTEGER NOT NULL CHECK (misfire_grace_ms BETWEEN 0 AND 86400000),
    max_batch                  INTEGER NOT NULL CHECK (max_batch BETWEEN 1 AND 32),
    max_episode_jobs           INTEGER NOT NULL CHECK (max_episode_jobs BETWEEN 1 AND 10000),
    max_catchup_age_ms         INTEGER NOT NULL CHECK (max_catchup_age_ms BETWEEN 60000 AND 2678400000),
    overlap_policy             TEXT    NOT NULL CHECK (overlap_policy IN ('allow', 'serialize')),
    allow_reuse                INTEGER NOT NULL CHECK (allow_reuse IN (0, 1)),
    job_name                   TEXT    NOT NULL,
    payload_version            INTEGER NOT NULL,
    serializer_profile_id      TEXT    NOT NULL,
    serializer_profile_version INTEGER NOT NULL,
    payload_template           BLOB    NOT NULL,
    template                   TEXT    NOT NULL CHECK (json_type(template) = 'object'),
    definition_fingerprint     BLOB    NOT NULL CHECK (length(definition_fingerprint) = 32),
    cutoff_at                  INTEGER NOT NULL,
    created_at                 INTEGER NOT NULL,
    created_by                 TEXT    CHECK (created_by IS NULL OR length(CAST(created_by AS BLOB)) BETWEEN 1 AND 256),
    CONSTRAINT schedule_definitions_kind_ck CHECK ((kind = 'cron' AND due_at IS NULL AND cron_text IS NOT NULL AND cron_dialect IS NOT NULL
        AND evaluator_profile IS NOT NULL AND tzdb_version IS NOT NULL AND length(tzdb_sha256) = 32)
        OR (kind = 'one_time' AND due_at IS NOT NULL AND cron_text IS NULL AND tzdb_sha256 IS NULL)),
    CONSTRAINT schedule_definitions_versions_ck CHECK (payload_version >= 1 AND serializer_profile_version >= 1),
    CONSTRAINT schedule_definitions_pk PRIMARY KEY (application, schedule_id, revision),
    CONSTRAINT schedule_definitions_schedule_fk FOREIGN KEY (application, schedule_id) REFERENCES runwick_schedules (application, schedule_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE runwick_schedule_occurrences (
    application         TEXT    NOT NULL,
    schedule_id         BLOB    NOT NULL CHECK (length(schedule_id) = 16),
    occurrence_kind     TEXT    NOT NULL,
    occurrence_key      TEXT    NOT NULL CHECK (length(CAST(occurrence_key AS BLOB)) BETWEEN 1 AND 64),
    scheduled_at        INTEGER,
    definition_revision INTEGER NOT NULL,
    job_id              BLOB    CHECK (job_id IS NULL OR length(job_id) = 16),
    disposition         TEXT    NOT NULL,
    coalesced           INTEGER NOT NULL DEFAULT 0 CHECK (coalesced IN (0, 1)),
    covered_from        INTEGER,
    covered_through     INTEGER,
    materialized_at     INTEGER NOT NULL,
    command_id          BLOB    NOT NULL CHECK (length(command_id) = 16),
    CONSTRAINT schedule_occurrences_coverage_ck CHECK ((covered_from IS NULL AND covered_through IS NULL)
        OR (covered_from IS NOT NULL AND covered_through IS NOT NULL AND covered_from <= covered_through)),
    CONSTRAINT schedule_occurrences_disposition_ck CHECK ((disposition IN ('created', 'linked_existing') AND job_id IS NOT NULL)
        OR (disposition = 'skipped' AND job_id IS NULL)),
    CONSTRAINT schedule_occurrences_kind_ck CHECK ((occurrence_kind = 'scheduled' AND scheduled_at IS NOT NULL) OR occurrence_kind = 'manual'),
    CONSTRAINT schedule_occurrences_pk PRIMARY KEY (application, schedule_id, occurrence_kind, occurrence_key),
    CONSTRAINT schedule_occurrences_schedule_fk FOREIGN KEY (application, schedule_id) REFERENCES runwick_schedules (application, schedule_id)
) STRICT, WITHOUT ROWID;

CREATE INDEX runwick_schedule_occurrences_history ON runwick_schedule_occurrences (application, schedule_id, scheduled_at, occurrence_kind,
    occurrence_key);

CREATE TABLE runwick_schedule_audit_ranges (
    audit_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    application         TEXT    NOT NULL,
    schedule_id         BLOB    NOT NULL CHECK (length(schedule_id) = 16),
    definition_revision INTEGER NOT NULL,
    command_id          BLOB    NOT NULL CHECK (length(command_id) = 16),
    from_exclusive      INTEGER,
    through_inclusive   INTEGER NOT NULL,
    reason              TEXT    NOT NULL CHECK (reason IN ('misfire_skip', 'run_once_covered', 'skipped_paused', 'skipped_by_edit', 'catch_up_skipped')),
    created_at          INTEGER NOT NULL,
    CONSTRAINT schedule_audit_ranges_order_ck CHECK (from_exclusive IS NULL OR from_exclusive < through_inclusive),
    CONSTRAINT schedule_audit_ranges_schedule_fk FOREIGN KEY (application, schedule_id) REFERENCES runwick_schedules (application, schedule_id)
) STRICT;

CREATE INDEX runwick_schedule_audit_ranges_schedule ON runwick_schedule_audit_ranges (application, schedule_id, through_inclusive);

CREATE TABLE runwick_management_audit (
    audit_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    application        TEXT    NOT NULL,
    command_id         BLOB    NOT NULL CHECK (length(command_id) = 16),
    operation          TEXT    NOT NULL CHECK (operation IN ('Cancel', 'ManualRetry', 'RerunSucceeded', 'SetQueueState', 'EditScopePolicy',
        'EditRatePolicy', 'ActivateCapabilities', 'SetScheduleState', 'UpdateSchedule', 'TriggerSchedule', 'ResolveCatchUp', 'RevalidateSchedule',
        'RecordScheduleEvaluationFailure', 'SetPoolOverride', 'RevealPayload', 'RevealError', 'Dispatch')),
    target_kind        TEXT    NOT NULL CHECK (target_kind IN ('job', 'queue', 'scope', 'rate_scope', 'application', 'schedule', 'pool', 'attempt')),
    target_id          TEXT    NOT NULL CHECK (length(CAST(target_id AS BLOB)) BETWEEN 1 AND 1024),
    actor_subject      TEXT    NOT NULL CHECK (length(CAST(actor_subject AS BLOB)) BETWEEN 1 AND 256),
    actor_display      TEXT    CHECK (actor_display IS NULL OR length(CAST(actor_display AS BLOB)) <= 256),
    actor_scheme       TEXT    NOT NULL CHECK (actor_scheme GLOB '[A-Za-z0-9]*' AND actor_scheme NOT GLOB '*[^A-Za-z0-9._:-]*' AND length(actor_scheme) <= 64),
    reason             TEXT    CHECK (reason IS NULL OR length(CAST(reason AS BLOB)) <= 512),
    expected_revision  INTEGER,
    resulting_revision INTEGER,
    details            TEXT    NOT NULL DEFAULT '{}' CHECK (json_type(details) = 'object' AND length(CAST(details AS BLOB)) <= 4096),
    occurred_at        INTEGER NOT NULL,
    CONSTRAINT management_audit_command_uq UNIQUE (application, command_id, target_kind, target_id)
) STRICT;

CREATE INDEX runwick_management_audit_target ON runwick_management_audit (application, target_kind, target_id, audit_id);
CREATE INDEX runwick_management_audit_list ON runwick_management_audit (application, audit_id);
