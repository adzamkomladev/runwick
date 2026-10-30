-- 0001_initial — expand phase. One transaction. Runwick schema revision 1 (installed unvalidated: schema_revision 0).
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE SCHEMA runwick;

-- §4.1 Domains and helper functions ------------------------------------------------------

CREATE DOMAIN runwick.rw_identifier AS text COLLATE "C"
    CONSTRAINT rw_identifier_grammar CHECK (VALUE ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$');

CREATE DOMAIN runwick.rw_job_name AS text COLLATE "C"
    CONSTRAINT rw_job_name_grammar CHECK (VALUE ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$');

CREATE DOMAIN runwick.rw_key AS text COLLATE "C"
    CONSTRAINT rw_key_bytes CHECK (octet_length(VALUE) BETWEEN 1 AND 512)
    CONSTRAINT rw_key_no_control CHECK (VALUE !~ '[\u0001-\u001F\u007F-\u009F]');

CREATE DOMAIN runwick.rw_digest AS bytea
    CONSTRAINT rw_digest_length CHECK (octet_length(VALUE) = 32);

CREATE FUNCTION runwick.ms_floor(ts timestamptz) RETURNS timestamptz
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    RETURN date_trunc('milliseconds', ts AT TIME ZONE 'UTC') AT TIME ZONE 'UTC';

CREATE FUNCTION runwick.ms_ceil(ts timestamptz) RETURNS timestamptz
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    RETURN CASE WHEN runwick.ms_floor(ts) = ts THEN ts
                ELSE runwick.ms_floor(ts) + interval '1 millisecond' END;

-- §4.2 Store and application gates --------------------------------------------------------

CREATE TABLE runwick.store_metadata (
    singleton_id                    smallint    NOT NULL,
    store_id                        uuid        NOT NULL,
    schema_revision                 bigint      NOT NULL,
    protocol_epoch                  integer     NOT NULL,
    migration_mode                  text COLLATE "C" NOT NULL,
    compatibility_manifest_version  integer     NOT NULL,
    compatibility_manifest          jsonb       NOT NULL,
    manifest_digest                 runwick.rw_digest NOT NULL,
    revision                        bigint      NOT NULL,
    updated_at                      timestamptz NOT NULL,
    CONSTRAINT store_metadata_pk PRIMARY KEY (singleton_id),
    CONSTRAINT store_metadata_store_id_uq UNIQUE (store_id),
    CONSTRAINT store_metadata_singleton_ck CHECK (singleton_id = 1),
    CONSTRAINT store_metadata_schema_revision_ck CHECK (schema_revision >= 0),
    CONSTRAINT store_metadata_protocol_epoch_ck CHECK (protocol_epoch >= 1),
    CONSTRAINT store_metadata_migration_mode_ck CHECK (migration_mode IN ('normal', 'expand', 'maintenance')),
    CONSTRAINT store_metadata_unvalidated_ck CHECK (schema_revision >= 1 OR migration_mode = 'maintenance'),
    CONSTRAINT store_metadata_manifest_version_ck CHECK (compatibility_manifest_version >= 1),
    CONSTRAINT store_metadata_manifest_ck CHECK (jsonb_typeof(compatibility_manifest) = 'object'),
    CONSTRAINT store_metadata_revision_ck CHECK (revision >= 1)
);

CREATE TABLE runwick.schema_migrations (
    migration_id            text COLLATE "C" NOT NULL,
    phase                   text COLLATE "C" NOT NULL,
    checksum                runwick.rw_digest NOT NULL,
    target_schema_revision  bigint      NOT NULL,
    state                   text COLLATE "C" NOT NULL,
    runner_build            text        NOT NULL,
    started_at              timestamptz NOT NULL,
    completed_at            timestamptz,
    checkpoint              jsonb,
    diagnostic_code         text COLLATE "C",
    CONSTRAINT schema_migrations_pk PRIMARY KEY (migration_id, phase),
    CONSTRAINT schema_migrations_id_ck CHECK (migration_id ~ '^[0-9]{4}_[a-z0-9_]{1,100}$'),
    CONSTRAINT schema_migrations_phase_ck CHECK (phase IN ('preflight', 'expand', 'backfill', 'validate', 'activate', 'contract')),
    CONSTRAINT schema_migrations_target_ck CHECK (target_schema_revision >= 1),
    CONSTRAINT schema_migrations_state_ck CHECK (state IN ('started', 'applied', 'failed')),
    CONSTRAINT schema_migrations_completed_ck CHECK ((state = 'started') = (completed_at IS NULL)),
    CONSTRAINT schema_migrations_diagnostic_ck CHECK (diagnostic_code IS NULL OR state = 'failed'),
    CONSTRAINT schema_migrations_runner_ck CHECK (octet_length(runner_build) BETWEEN 1 AND 256),
    CONSTRAINT schema_migrations_checkpoint_ck CHECK (checkpoint IS NULL OR jsonb_typeof(checkpoint) = 'object')
);

CREATE TABLE runwick.application_controls (
    application             runwick.rw_identifier NOT NULL,
    revision                bigint      NOT NULL DEFAULT 1,
    required_protocol_epoch integer     NOT NULL DEFAULT 1,
    required_capabilities   jsonb       NOT NULL DEFAULT '{}'::jsonb,
    admission_mode          text COLLATE "C" NOT NULL DEFAULT 'open',
    created_at              timestamptz NOT NULL,
    updated_at              timestamptz NOT NULL,
    actor_subject           text,
    CONSTRAINT application_controls_pk PRIMARY KEY (application),
    CONSTRAINT application_controls_revision_ck CHECK (revision >= 1),
    CONSTRAINT application_controls_epoch_ck CHECK (required_protocol_epoch >= 1),
    CONSTRAINT application_controls_capabilities_ck CHECK (jsonb_typeof(required_capabilities) = 'object'),
    CONSTRAINT application_controls_mode_ck CHECK (admission_mode IN ('open', 'drain', 'maintenance')),
    CONSTRAINT application_controls_actor_ck CHECK (actor_subject IS NULL OR octet_length(actor_subject) BETWEEN 1 AND 256)
);

-- §4.3 Queue controls ------------------------------------------------------------------------

CREATE TABLE runwick.queue_controls (
    application runwick.rw_identifier NOT NULL,
    queue       runwick.rw_identifier NOT NULL,
    paused      boolean     NOT NULL DEFAULT false,
    revision    bigint      NOT NULL DEFAULT 1,
    created_at  timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL,
    updated_by  text,
    CONSTRAINT queue_controls_pk PRIMARY KEY (application, queue),
    CONSTRAINT queue_controls_revision_ck CHECK (revision >= 1),
    CONSTRAINT queue_controls_updated_by_ck CHECK (updated_by IS NULL OR octet_length(updated_by) BETWEEN 1 AND 256)
);

-- §4.4 Jobs, payloads, generations, attempts ----------------------------------------------

CREATE TABLE runwick.jobs (
    application                 runwick.rw_identifier NOT NULL,
    job_id                      uuid        NOT NULL,
    job_name                    runwick.rw_job_name NOT NULL,
    payload_version             integer     NOT NULL,
    serializer_profile_id       runwick.rw_identifier NOT NULL,
    serializer_profile_version  integer     NOT NULL,
    queue                       runwick.rw_identifier NOT NULL,
    priority                    text COLLATE "C" NOT NULL,
    state                       text COLLATE "C" NOT NULL,
    eligible_at                 timestamptz NOT NULL,
    created_at                  timestamptz NOT NULL,
    updated_at                  timestamptz NOT NULL,
    generation                  integer     NOT NULL DEFAULT 1,
    revision                    bigint      NOT NULL DEFAULT 1,
    reservation_count           bigint      NOT NULL DEFAULT 0,
    execution_count             bigint      NOT NULL DEFAULT 0,
    current_attempt_id          uuid,
    worker_instance_id          uuid,
    ownership_token             uuid,
    lease_expires_at            timestamptz,
    cancel_requested_at         timestamptz,
    cancel_reason               text,
    cancel_requested_by         text,
    terminal_at                 timestamptz,
    terminal_reason             text COLLATE "C",
    active_unique_key           runwick.rw_key,
    payload_retained            boolean     NOT NULL DEFAULT true,
    detail_expired_at           timestamptz,
    CONSTRAINT jobs_pk PRIMARY KEY (application, job_id),
    CONSTRAINT jobs_profile_uq UNIQUE (application, job_id, serializer_profile_id, serializer_profile_version),
    CONSTRAINT jobs_payload_version_ck CHECK (payload_version >= 1),
    CONSTRAINT jobs_profile_version_ck CHECK (serializer_profile_version >= 1),
    CONSTRAINT jobs_priority_ck CHECK (priority IN ('low', 'medium', 'high')),
    CONSTRAINT jobs_state_ck CHECK (state IN ('pending', 'reserved', 'running', 'retrying', 'succeeded', 'failed', 'cancelled')),
    CONSTRAINT jobs_generation_ck CHECK (generation >= 1),
    CONSTRAINT jobs_revision_ck CHECK (revision >= 1),
    CONSTRAINT jobs_reservation_count_ck CHECK (reservation_count >= 0),
    CONSTRAINT jobs_execution_count_ck CHECK (execution_count >= 0 AND execution_count <= reservation_count),
    CONSTRAINT jobs_eligible_ms_ck CHECK (eligible_at = runwick.ms_floor(eligible_at)),
    CONSTRAINT jobs_lease_ms_ck CHECK (lease_expires_at IS NULL OR lease_expires_at = runwick.ms_floor(lease_expires_at)),
    CONSTRAINT jobs_ownership_ck CHECK (
        CASE WHEN state IN ('reserved', 'running')
             THEN current_attempt_id IS NOT NULL AND worker_instance_id IS NOT NULL
                  AND ownership_token IS NOT NULL AND lease_expires_at IS NOT NULL
             ELSE current_attempt_id IS NULL AND worker_instance_id IS NULL
                  AND ownership_token IS NULL AND lease_expires_at IS NULL
        END),
    CONSTRAINT jobs_terminal_ck CHECK (
        CASE WHEN state IN ('succeeded', 'failed', 'cancelled')
             THEN terminal_at IS NOT NULL AND terminal_reason IS NOT NULL
             ELSE terminal_at IS NULL AND terminal_reason IS NULL
        END),
    CONSTRAINT jobs_terminal_reason_ck CHECK (
        terminal_reason IS NULL
        OR (state = 'succeeded' AND terminal_reason = 'succeeded')
        OR (state = 'failed' AND terminal_reason IN ('permanent_failure', 'execution_budget_exhausted', 'reservation_abandonment_exhausted'))
        OR (state = 'cancelled' AND terminal_reason IN ('user_cancelled', 'cancellation_requested', 'cancelled_before_start', 'cancelled_while_waiting'))),
    CONSTRAINT jobs_cancel_ck CHECK (cancel_requested_at IS NOT NULL OR (cancel_reason IS NULL AND cancel_requested_by IS NULL)),
    CONSTRAINT jobs_cancel_reason_ck CHECK (cancel_reason IS NULL OR octet_length(cancel_reason) <= 512),
    CONSTRAINT jobs_cancel_by_ck CHECK (cancel_requested_by IS NULL OR octet_length(cancel_requested_by) BETWEEN 1 AND 256),
    CONSTRAINT jobs_payload_retained_ck CHECK (payload_retained OR state IN ('succeeded', 'failed', 'cancelled')),
    CONSTRAINT jobs_detail_expired_ck CHECK (detail_expired_at IS NULL OR state IN ('succeeded', 'failed', 'cancelled'))
);

CREATE TABLE runwick.job_payloads (
    application                 runwick.rw_identifier NOT NULL,
    job_id                      uuid        NOT NULL,
    payload_utf8                bytea       NOT NULL,
    payload_encoding_version    integer     NOT NULL DEFAULT 1,
    serializer_profile_id       runwick.rw_identifier NOT NULL,
    serializer_profile_version  integer     NOT NULL,
    headers                     jsonb       NOT NULL DEFAULT '{"business":{},"diagnostic":{}}'::jsonb,
    CONSTRAINT job_payloads_pk PRIMARY KEY (application, job_id),
    CONSTRAINT job_payloads_job_profile_fk FOREIGN KEY (application, job_id, serializer_profile_id, serializer_profile_version)
        REFERENCES runwick.jobs (application, job_id, serializer_profile_id, serializer_profile_version)
        ON DELETE CASCADE,
    CONSTRAINT job_payloads_size_ck CHECK (octet_length(payload_utf8) BETWEEN 2 AND 1048576),
    CONSTRAINT job_payloads_encoding_ck CHECK (payload_encoding_version = 1),
    CONSTRAINT job_payloads_headers_ck CHECK (
        jsonb_typeof(headers) = 'object'
        AND jsonb_typeof(headers -> 'business') = 'object'
        AND jsonb_typeof(headers -> 'diagnostic') = 'object')
);

CREATE TABLE runwick.job_generations (
    application                     runwick.rw_identifier NOT NULL,
    job_id                          uuid        NOT NULL,
    generation                      integer     NOT NULL,
    created_at                      timestamptz NOT NULL,
    created_by                      text,
    created_reason                  text COLLATE "C" NOT NULL,
    policy_snapshot_version         integer     NOT NULL DEFAULT 1,
    max_executions                  integer     NOT NULL,
    max_unstarted_abandonments      integer     NOT NULL,
    execution_timeout_ms            bigint,
    retry_policy                    jsonb       NOT NULL,
    retry_policy_version            integer     NOT NULL DEFAULT 1,
    unstarted_retry_policy          jsonb       NOT NULL,
    unstarted_retry_policy_version  integer     NOT NULL DEFAULT 1,
    started_count                   integer     NOT NULL DEFAULT 0,
    consecutive_unstarted_count     integer     NOT NULL DEFAULT 0,
    CONSTRAINT job_generations_pk PRIMARY KEY (application, job_id, generation),
    CONSTRAINT job_generations_job_fk FOREIGN KEY (application, job_id)
        REFERENCES runwick.jobs (application, job_id) ON DELETE CASCADE,
    CONSTRAINT job_generations_generation_ck CHECK (generation >= 1),
    CONSTRAINT job_generations_created_by_ck CHECK (created_by IS NULL OR octet_length(created_by) BETWEEN 1 AND 256),
    CONSTRAINT job_generations_reason_ck CHECK (created_reason IN ('initial', 'scheduled', 'manual_retry', 'rerun_succeeded')),
    CONSTRAINT job_generations_snapshot_version_ck CHECK (policy_snapshot_version >= 1),
    CONSTRAINT job_generations_max_executions_ck CHECK (max_executions BETWEEN 1 AND 100),
    CONSTRAINT job_generations_max_unstarted_ck CHECK (max_unstarted_abandonments BETWEEN 1 AND 100),
    CONSTRAINT job_generations_timeout_ck CHECK (execution_timeout_ms IS NULL OR execution_timeout_ms BETWEEN 1 AND 2592000000),
    CONSTRAINT job_generations_retry_ck CHECK (
        jsonb_typeof(retry_policy) = 'object'
        AND retry_policy ?& ARRAY['backoff', 'evaluatorVersion', 'initialDelay', 'jitterKind', 'maximumDelay']),
    CONSTRAINT job_generations_unstarted_retry_ck CHECK (
        jsonb_typeof(unstarted_retry_policy) = 'object'
        AND unstarted_retry_policy ?& ARRAY['backoff', 'evaluatorVersion', 'initialDelay', 'jitterKind', 'maximumDelay']),
    CONSTRAINT job_generations_retry_version_ck CHECK (retry_policy_version >= 1 AND unstarted_retry_policy_version >= 1),
    CONSTRAINT job_generations_started_ck CHECK (started_count BETWEEN 0 AND max_executions),
    CONSTRAINT job_generations_unstarted_ck CHECK (consecutive_unstarted_count BETWEEN 0 AND max_unstarted_abandonments)
);

CREATE TABLE runwick.job_attempts (
    application             runwick.rw_identifier NOT NULL,
    attempt_id              uuid        NOT NULL,
    job_id                  uuid        NOT NULL,
    generation              integer     NOT NULL,
    reservation_number      bigint      NOT NULL,
    execution_number        integer,
    worker_instance_id      uuid        NOT NULL,
    ownership_token         uuid        NOT NULL,
    reserved_at             timestamptz NOT NULL,
    authorized_at           timestamptz,
    finished_at             timestamptz,
    execution_elapsed_ms    bigint,
    outcome                 text COLLATE "C",
    disposition             text COLLATE "C",
    applied_reason          text COLLATE "C",
    retry_delay_ms          bigint,
    next_eligible_at        timestamptz,
    error                   jsonb,
    closed_by_command_id    uuid,
    CONSTRAINT job_attempts_pk PRIMARY KEY (application, attempt_id),
    CONSTRAINT job_attempts_job_attempt_uq UNIQUE (application, job_id, attempt_id),
    CONSTRAINT job_attempts_reservation_uq UNIQUE (application, job_id, reservation_number),
    CONSTRAINT job_attempts_execution_uq UNIQUE (application, job_id, generation, execution_number),
    CONSTRAINT job_attempts_generation_fk FOREIGN KEY (application, job_id, generation)
        REFERENCES runwick.job_generations (application, job_id, generation) ON DELETE CASCADE,
    CONSTRAINT job_attempts_reservation_number_ck CHECK (reservation_number >= 1),
    CONSTRAINT job_attempts_execution_number_ck CHECK (execution_number IS NULL OR execution_number >= 1),
    CONSTRAINT job_attempts_authorized_ck CHECK ((execution_number IS NULL) = (authorized_at IS NULL)),
    CONSTRAINT job_attempts_closed_ck CHECK (
        (finished_at IS NULL) = (outcome IS NULL)
        AND (finished_at IS NULL) = (disposition IS NULL)
        AND (finished_at IS NULL) = (closed_by_command_id IS NULL)),
    CONSTRAINT job_attempts_outcome_ck CHECK (outcome IS NULL OR outcome IN (
        'succeeded', 'retryable_failure', 'permanent_failure', 'user_cancelled', 'interrupted', 'timed_out',
        'abandoned_before_start', 'abandoned_running', 'released', 'cancelled_before_start', 'admission_deferred')),
    CONSTRAINT job_attempts_executed_outcome_ck CHECK (
        outcome IS NULL
        OR (outcome IN ('succeeded', 'retryable_failure', 'permanent_failure', 'user_cancelled', 'interrupted', 'timed_out', 'abandoned_running')) = (execution_number IS NOT NULL)),
    CONSTRAINT job_attempts_disposition_ck CHECK (disposition IS NULL OR disposition IN ('pending', 'retrying', 'succeeded', 'failed', 'cancelled')),
    CONSTRAINT job_attempts_reason_ck CHECK ((applied_reason IS NOT NULL) = (disposition IN ('succeeded', 'failed', 'cancelled'))),
    CONSTRAINT job_attempts_elapsed_ck CHECK (execution_elapsed_ms IS NULL OR (execution_elapsed_ms >= 0 AND execution_number IS NOT NULL)),
    CONSTRAINT job_attempts_retry_ck CHECK (
        (retry_delay_ms IS NULL) = (next_eligible_at IS NULL)
        AND (retry_delay_ms IS NULL OR (retry_delay_ms >= 1 AND disposition IN ('pending', 'retrying')))),
    CONSTRAINT job_attempts_error_ck CHECK (error IS NULL OR (jsonb_typeof(error) = 'object' AND octet_length(error::text) <= 20480))
);

-- Circular and current-pointer foreign keys, checked at commit.
ALTER TABLE runwick.jobs
    ADD CONSTRAINT jobs_current_generation_fk FOREIGN KEY (application, job_id, generation)
        REFERENCES runwick.job_generations (application, job_id, generation)
        DEFERRABLE INITIALLY DEFERRED,
    ADD CONSTRAINT jobs_current_attempt_fk FOREIGN KEY (application, job_id, current_attempt_id)
        REFERENCES runwick.job_attempts (application, job_id, attempt_id)
        DEFERRABLE INITIALLY DEFERRED;

-- At most one open attempt per job.
CREATE UNIQUE INDEX job_attempts_one_open
    ON runwick.job_attempts (application, job_id) WHERE finished_at IS NULL;

-- §4.5 Concurrency scopes, requirements, permits -----------------------------------------

CREATE TABLE runwick.concurrency_scopes (
    application     runwick.rw_identifier NOT NULL,
    scope_id        uuid        NOT NULL,
    scope_kind      text COLLATE "C" NOT NULL,
    scope_key       runwick.rw_key NOT NULL,
    capacity        integer     NOT NULL,
    policy_revision bigint      NOT NULL DEFAULT 1,
    fencing_enabled boolean     NOT NULL,
    fencing_counter bigint      NOT NULL DEFAULT 0,
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL,
    CONSTRAINT concurrency_scopes_pk PRIMARY KEY (application, scope_id),
    CONSTRAINT concurrency_scopes_natural_uq UNIQUE (application, scope_kind, scope_key),
    CONSTRAINT concurrency_scopes_kind_ck CHECK (scope_kind IN ('queue', 'job-type', 'resource')),
    CONSTRAINT concurrency_scopes_capacity_ck CHECK (capacity >= 0),
    CONSTRAINT concurrency_scopes_revision_ck CHECK (policy_revision >= 1),
    CONSTRAINT concurrency_scopes_fencing_ck CHECK (fencing_counter >= 0 AND (fencing_enabled OR fencing_counter = 0)),
    CONSTRAINT concurrency_scopes_fencing_kind_ck CHECK (NOT fencing_enabled OR scope_kind = 'resource')
);

CREATE TABLE runwick.job_concurrency_requirements (
    application runwick.rw_identifier NOT NULL,
    job_id      uuid NOT NULL,
    scope_id    uuid NOT NULL,
    CONSTRAINT job_concurrency_requirements_pk PRIMARY KEY (application, job_id, scope_id),
    CONSTRAINT job_concurrency_requirements_job_fk FOREIGN KEY (application, job_id)
        REFERENCES runwick.jobs (application, job_id) ON DELETE CASCADE,
    CONSTRAINT job_concurrency_requirements_scope_fk FOREIGN KEY (application, scope_id)
        REFERENCES runwick.concurrency_scopes (application, scope_id)
);

CREATE TABLE runwick.concurrency_permits (
    application         runwick.rw_identifier NOT NULL,
    scope_id            uuid        NOT NULL,
    attempt_id          uuid        NOT NULL,
    job_id              uuid        NOT NULL,
    ownership_token     uuid        NOT NULL,
    lease_expires_at    timestamptz NOT NULL,
    fencing_value       bigint,
    CONSTRAINT concurrency_permits_pk PRIMARY KEY (application, scope_id, attempt_id),
    CONSTRAINT concurrency_permits_scope_fk FOREIGN KEY (application, scope_id)
        REFERENCES runwick.concurrency_scopes (application, scope_id),
    CONSTRAINT concurrency_permits_attempt_fk FOREIGN KEY (application, job_id, attempt_id)
        REFERENCES runwick.job_attempts (application, job_id, attempt_id),
    CONSTRAINT concurrency_permits_requirement_fk FOREIGN KEY (application, job_id, scope_id)
        REFERENCES runwick.job_concurrency_requirements (application, job_id, scope_id),
    CONSTRAINT concurrency_permits_lease_ms_ck CHECK (lease_expires_at = runwick.ms_floor(lease_expires_at)),
    CONSTRAINT concurrency_permits_fence_ck CHECK (fencing_value IS NULL OR fencing_value >= 1)
);

-- §4.6 Dispatch keys -----------------------------------------------------------------------

CREATE TABLE runwick.dispatch_keys (
    application             runwick.rw_identifier NOT NULL,
    job_name                runwick.rw_job_name NOT NULL,
    key_kind                text COLLATE "C" NOT NULL,
    key                     runwick.rw_key NOT NULL,
    bound_job_id            uuid,
    business_fingerprint    runwick.rw_digest,
    policy_fingerprint      runwick.rw_digest,
    fingerprint_version     integer,
    expires_at              timestamptz,
    created_at              timestamptz NOT NULL,
    updated_at              timestamptz NOT NULL,
    CONSTRAINT dispatch_keys_pk PRIMARY KEY (application, job_name, key_kind, key),
    CONSTRAINT dispatch_keys_kind_ck CHECK (key_kind IN ('active', 'window')),
    CONSTRAINT dispatch_keys_binding_ck CHECK (
        CASE key_kind
            WHEN 'active' THEN business_fingerprint IS NULL AND policy_fingerprint IS NULL
                               AND fingerprint_version IS NULL AND expires_at IS NULL
            ELSE (bound_job_id IS NULL AND business_fingerprint IS NULL AND policy_fingerprint IS NULL
                  AND fingerprint_version IS NULL AND expires_at IS NULL)
              OR (bound_job_id IS NOT NULL AND business_fingerprint IS NOT NULL AND policy_fingerprint IS NOT NULL
                  AND fingerprint_version IS NOT NULL AND expires_at IS NOT NULL)
        END),
    CONSTRAINT dispatch_keys_fingerprint_version_ck CHECK (fingerprint_version IS NULL OR fingerprint_version >= 1),
    CONSTRAINT dispatch_keys_expires_ms_ck CHECK (expires_at IS NULL OR expires_at = runwick.ms_floor(expires_at))
);

-- §4.7 Command receipts (partitioned by retention class) ----------------------------------

CREATE TABLE runwick.storage_commands (
    application             runwick.rw_identifier NOT NULL,
    retention_class         text COLLATE "C" NOT NULL,
    command_id              uuid        NOT NULL,
    operation               text COLLATE "C" NOT NULL,
    operation_version       integer     NOT NULL,
    protocol_epoch          integer     NOT NULL,
    fingerprint             runwick.rw_digest NOT NULL,
    fingerprint_version     integer     NOT NULL,
    not_after               timestamptz NOT NULL,
    receipt_codec_version   integer     NOT NULL,
    retain_until            timestamptz NOT NULL,
    status                  text COLLATE "C",
    result                  jsonb,
    decided_at              timestamptz,
    CONSTRAINT storage_commands_pk PRIMARY KEY (application, retention_class, command_id),
    CONSTRAINT storage_commands_class_ck CHECK (retention_class IN ('ownership', 'durable')),
    CONSTRAINT storage_commands_operation_ck CHECK (operation ~ '^[A-Z][A-Za-z]{1,63}$'),
    CONSTRAINT storage_commands_operation_class_ck CHECK (
        (operation IN ('Reserve', 'Begin', 'Recover', 'ReleaseReservation')) = (retention_class = 'ownership')),
    CONSTRAINT storage_commands_no_renew_ck CHECK (operation <> 'Renew'),
    CONSTRAINT storage_commands_versions_ck CHECK (
        operation_version >= 1 AND protocol_epoch >= 1 AND fingerprint_version >= 1 AND receipt_codec_version >= 1),
    CONSTRAINT storage_commands_not_after_ms_ck CHECK (not_after = runwick.ms_floor(not_after)),
    CONSTRAINT storage_commands_retain_ck CHECK (
        retain_until >= not_after + CASE retention_class WHEN 'ownership' THEN interval '10 minutes'
                                                         ELSE interval '24 hours' END),
    CONSTRAINT storage_commands_decision_ck CHECK (
        (status IS NULL) = (result IS NULL) AND (result IS NULL) = (decided_at IS NULL)),
    CONSTRAINT storage_commands_result_ck CHECK (result IS NULL OR jsonb_typeof(result) = 'object')
) PARTITION BY LIST (retention_class);

CREATE TABLE runwick.storage_commands_ownership
    PARTITION OF runwick.storage_commands FOR VALUES IN ('ownership');

CREATE TABLE runwick.storage_commands_durable
    PARTITION OF runwick.storage_commands FOR VALUES IN ('durable');

-- Receipt-placeholder guard: a committed receipt row must carry its decision.
CREATE FUNCTION runwick.storage_commands_assert_decided() RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM runwick.storage_commands AS c
        WHERE c.application = NEW.application
          AND c.retention_class = NEW.retention_class
          AND c.command_id = NEW.command_id
          AND (c.status IS NULL OR c.result IS NULL OR c.decided_at IS NULL)) THEN
        RAISE EXCEPTION 'runwick receipt % committed without a decision', NEW.command_id
            USING ERRCODE = 'RWR01';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER storage_commands_ownership_decided
    AFTER INSERT ON runwick.storage_commands_ownership
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION runwick.storage_commands_assert_decided();

CREATE CONSTRAINT TRIGGER storage_commands_durable_decided
    AFTER INSERT ON runwick.storage_commands_durable
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION runwick.storage_commands_assert_decided();

-- §4.8 Execution history events ------------------------------------------------------------

CREATE TABLE runwick.queue_events (
    application runwick.rw_identifier NOT NULL,
    event_id    bigint GENERATED ALWAYS AS IDENTITY,
    job_id      uuid,
    attempt_id  uuid,
    command_id  uuid,
    event_kind  text COLLATE "C" NOT NULL,
    occurred_at timestamptz NOT NULL,
    actor_id    text,
    details     jsonb       NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT queue_events_pk PRIMARY KEY (application, event_id),
    CONSTRAINT queue_events_kind_ck CHECK (event_kind IN (
        'dispatched', 'policy_drift',
        'attempt_reserved', 'attempt_started', 'attempt_cancelled_before_start',
        'attempt_finished', 'attempt_recovered', 'attempt_released')),
    CONSTRAINT queue_events_actor_ck CHECK (actor_id IS NULL OR octet_length(actor_id) BETWEEN 1 AND 256),
    CONSTRAINT queue_events_details_ck CHECK (jsonb_typeof(details) = 'object' AND octet_length(details::text) <= 4096)
);

-- §4.9 Worker presence ---------------------------------------------------------------------

CREATE TABLE runwick.worker_instances (
    application         runwick.rw_identifier NOT NULL,
    instance_id         uuid        NOT NULL,
    process_id          integer     NOT NULL,
    host_name           text        NOT NULL,
    build_id            text        NOT NULL,
    status              text COLLATE "C" NOT NULL,
    capability_revision bigint      NOT NULL,
    presence_sequence   bigint      NOT NULL,
    subscriptions       jsonb       NOT NULL DEFAULT '[]'::jsonb,
    pool_targets        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    started_at          timestamptz NOT NULL,
    heartbeat_at        timestamptz NOT NULL,
    expires_at          timestamptz NOT NULL,
    CONSTRAINT worker_instances_pk PRIMARY KEY (application, instance_id),
    CONSTRAINT worker_instances_process_ck CHECK (process_id >= 0),
    CONSTRAINT worker_instances_host_ck CHECK (octet_length(host_name) BETWEEN 1 AND 256),
    CONSTRAINT worker_instances_build_ck CHECK (octet_length(build_id) BETWEEN 1 AND 256),
    CONSTRAINT worker_instances_status_ck CHECK (status IN ('starting', 'ready', 'draining', 'degraded', 'stopping')),
    CONSTRAINT worker_instances_revision_ck CHECK (capability_revision >= 1),
    CONSTRAINT worker_instances_sequence_ck CHECK (presence_sequence >= 1),
    CONSTRAINT worker_instances_subscriptions_ck CHECK (jsonb_typeof(subscriptions) = 'array' AND jsonb_array_length(subscriptions) <= 128),
    CONSTRAINT worker_instances_pool_targets_ck CHECK (jsonb_typeof(pool_targets) = 'object'),
    CONSTRAINT worker_instances_expiry_ck CHECK (expires_at > heartbeat_at AND heartbeat_at >= started_at)
);

CREATE TABLE runwick.worker_capabilities (
    application                 runwick.rw_identifier NOT NULL,
    instance_id                 uuid        NOT NULL,
    capability_revision         bigint      NOT NULL,
    job_name                    runwick.rw_job_name NOT NULL,
    payload_version             integer     NOT NULL,
    serializer_profile_id       runwick.rw_identifier NOT NULL,
    serializer_profile_version  integer     NOT NULL,
    CONSTRAINT worker_capabilities_pk PRIMARY KEY (
        application, instance_id, capability_revision,
        job_name, payload_version, serializer_profile_id, serializer_profile_version),
    CONSTRAINT worker_capabilities_instance_fk FOREIGN KEY (application, instance_id)
        REFERENCES runwick.worker_instances (application, instance_id) ON DELETE CASCADE,
    CONSTRAINT worker_capabilities_revision_ck CHECK (capability_revision >= 1),
    CONSTRAINT worker_capabilities_versions_ck CHECK (payload_version >= 1 AND serializer_profile_version >= 1)
);

-- §4.11 Indexes ------------------------------------------------------------------------------

CREATE INDEX jobs_ready
    ON runwick.jobs (application, queue, priority, eligible_at, job_id)
    WHERE state IN ('pending', 'retrying');

CREATE INDEX jobs_expired_lease
    ON runwick.jobs (application, lease_expires_at, job_id)
    WHERE state IN ('reserved', 'running');

CREATE INDEX jobs_terminal_retention
    ON runwick.jobs (application, terminal_at, job_id)
    WHERE state IN ('succeeded', 'failed', 'cancelled');

CREATE INDEX jobs_queue_list
    ON runwick.jobs (application, queue, created_at, job_id);

CREATE INDEX job_concurrency_requirements_scope
    ON runwick.job_concurrency_requirements (application, scope_id, job_id);

CREATE INDEX permits_live_count
    ON runwick.concurrency_permits (application, scope_id, lease_expires_at);

CREATE INDEX permits_attempt
    ON runwick.concurrency_permits (application, attempt_id, scope_id);

CREATE INDEX permits_job_attempt
    ON runwick.concurrency_permits (application, job_id, attempt_id);

CREATE INDEX permits_job_scope
    ON runwick.concurrency_permits (application, job_id, scope_id);

CREATE INDEX command_retention
    ON runwick.storage_commands (application, retain_until, command_id);

CREATE INDEX command_by_id
    ON runwick.storage_commands (application, command_id);

CREATE INDEX events_job_history
    ON runwick.queue_events (application, job_id, event_id)
    WHERE job_id IS NOT NULL;

CREATE INDEX events_retention
    ON runwick.queue_events (application, occurred_at, event_id);

CREATE INDEX worker_instances_expiry
    ON runwick.worker_instances (application, expires_at, instance_id);

CREATE INDEX worker_capabilities_tuple
    ON runwick.worker_capabilities (application, job_name, payload_version,
                                    serializer_profile_id, serializer_profile_version, instance_id);

-- §4.12 Bootstrap rows ---------------------------------------------------------------------

INSERT INTO runwick.store_metadata (
    singleton_id, store_id, schema_revision, protocol_epoch, migration_mode,
    compatibility_manifest_version, compatibility_manifest, manifest_digest, revision, updated_at)
VALUES (1, gen_random_uuid(), 0, 1, 'maintenance',
        1, '{}'::jsonb, sha256(convert_to('{}', 'UTF8')), 1, runwick.ms_floor(clock_timestamp()));
