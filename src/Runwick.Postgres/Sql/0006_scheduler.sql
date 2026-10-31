-- 0006_scheduler — expand phase. One transaction. Schedules, definitions, occurrences and audit ranges (13 §3).
-- Additive except the widened queue_events kind, management_audit and concurrency scope kind checks. Schedule
-- creation takes effect only after the application activates capability `scheduler` (13 §6.1).
-- Integration note: the constraint version suffixes below (_v4/_v3/_v2) assume 0005 widened none of these checks;
-- rename to the next free version at merge and union the kind lists.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- §3.1 Schedules -------------------------------------------------------------------------------

CREATE TABLE runwick.schedules (
    application         runwick.rw_identifier NOT NULL,
    schedule_id         uuid        NOT NULL,
    schedule_key        runwick.rw_key NOT NULL,
    definition_revision bigint      NOT NULL DEFAULT 1,
    control_revision    bigint      NOT NULL DEFAULT 1,
    cursor_revision     bigint      NOT NULL DEFAULT 1,
    enabled             boolean     NOT NULL DEFAULT true,
    paused              boolean     NOT NULL DEFAULT false,
    needs_review        boolean     NOT NULL DEFAULT false,
    review_reason       text COLLATE "C",
    next_due_at         timestamptz,
    scan_after_at       timestamptz NOT NULL,
    evaluated_through   timestamptz,
    episode_id          uuid,
    episode_cutoff      timestamptz,
    episode_emitted     integer     NOT NULL DEFAULT 0,
    episode_limit       integer,
    resume_policy       text COLLATE "C",
    resume_cutoff       timestamptz,
    created_at          timestamptz NOT NULL,
    updated_at          timestamptz NOT NULL,
    updated_by          text,
    CONSTRAINT schedules_pk PRIMARY KEY (application, schedule_id),
    CONSTRAINT schedules_key_uq UNIQUE (application, schedule_key),
    CONSTRAINT schedules_revisions_ck CHECK (definition_revision >= 1 AND control_revision >= 1 AND cursor_revision >= 1),
    CONSTRAINT schedules_episode_ck CHECK (
        (episode_id IS NULL AND episode_cutoff IS NULL AND episode_emitted = 0 AND episode_limit IS NULL)
        OR (episode_id IS NOT NULL AND episode_cutoff IS NOT NULL AND episode_emitted >= 0
            AND episode_limit BETWEEN 1 AND 10000 AND episode_emitted <= episode_limit)),
    CONSTRAINT schedules_resume_ck CHECK (
        (resume_policy IS NULL AND resume_cutoff IS NULL)
        OR (resume_policy IN ('skip_paused', 'run_once', 'catch_up', 'skipped_by_edit') AND resume_cutoff IS NOT NULL)),
    CONSTRAINT schedules_review_ck CHECK (review_reason IS NULL OR octet_length(review_reason) BETWEEN 1 AND 64),
    CONSTRAINT schedules_updated_by_ck CHECK (updated_by IS NULL OR octet_length(updated_by) BETWEEN 1 AND 256)
);

-- Due probe (13 §6.9): queries repeat this predicate literally.
CREATE INDEX schedules_due
    ON runwick.schedules (application, scan_after_at, schedule_id)
    WHERE enabled AND NOT paused AND NOT needs_review AND next_due_at IS NOT NULL;

-- Immutable definition revisions (13 §3.1–§3.3) ------------------------------------------------

CREATE TABLE runwick.schedule_definitions (
    application                 runwick.rw_identifier NOT NULL,
    schedule_id                 uuid        NOT NULL,
    revision                    bigint      NOT NULL,
    kind                        text COLLATE "C" NOT NULL,
    due_at                      timestamptz,
    cron_text                   text,
    cron_dialect                text COLLATE "C",
    zone_id                     text COLLATE "C",
    evaluator_profile           text COLLATE "C",
    tzdb_version                text COLLATE "C",
    tzdb_sha256                 bytea,
    gap_policy                  text COLLATE "C" NOT NULL,
    fold_policy                 text COLLATE "C" NOT NULL,
    misfire_policy              text COLLATE "C" NOT NULL,
    misfire_grace_ms            bigint      NOT NULL,
    max_batch                   integer     NOT NULL,
    max_episode_jobs            integer     NOT NULL,
    max_catchup_age_ms          bigint      NOT NULL,
    overlap_policy              text COLLATE "C" NOT NULL,
    allow_reuse                 boolean     NOT NULL,
    job_name                    runwick.rw_job_name NOT NULL,
    payload_version             integer     NOT NULL,
    serializer_profile_id       text COLLATE "C" NOT NULL,
    serializer_profile_version  integer     NOT NULL,
    payload_template            bytea       NOT NULL,
    template                    jsonb       NOT NULL,
    definition_fingerprint      runwick.rw_digest NOT NULL,
    cutoff_at                   timestamptz NOT NULL,
    created_at                  timestamptz NOT NULL,
    created_by                  text,
    CONSTRAINT schedule_definitions_pk PRIMARY KEY (application, schedule_id, revision),
    CONSTRAINT schedule_definitions_schedule_fk FOREIGN KEY (application, schedule_id)
        REFERENCES runwick.schedules (application, schedule_id),
    CONSTRAINT schedule_definitions_kind_ck CHECK (
        (kind = 'cron' AND due_at IS NULL AND cron_text IS NOT NULL AND cron_dialect IS NOT NULL
            AND evaluator_profile IS NOT NULL AND tzdb_version IS NOT NULL AND octet_length(tzdb_sha256) = 32)
        OR (kind = 'one_time' AND due_at IS NOT NULL AND cron_text IS NULL AND tzdb_sha256 IS NULL)),
    CONSTRAINT schedule_definitions_text_ck CHECK (cron_text IS NULL OR octet_length(cron_text) BETWEEN 1 AND 256),
    CONSTRAINT schedule_definitions_zone_ck CHECK (zone_id IS NULL OR octet_length(zone_id) BETWEEN 1 AND 64),
    CONSTRAINT schedule_definitions_gap_ck CHECK (gap_policy IN ('skip', 'shift')),
    CONSTRAINT schedule_definitions_fold_ck CHECK (fold_policy IN ('earlier', 'later', 'both')),
    CONSTRAINT schedule_definitions_misfire_ck CHECK (misfire_policy IN ('skip', 'run_once', 'catch_up')),
    CONSTRAINT schedule_definitions_grace_ck CHECK (misfire_grace_ms BETWEEN 0 AND 86400000),
    CONSTRAINT schedule_definitions_batch_ck CHECK (max_batch BETWEEN 1 AND 32),
    CONSTRAINT schedule_definitions_episode_ck CHECK (max_episode_jobs BETWEEN 1 AND 10000),
    CONSTRAINT schedule_definitions_age_ck CHECK (max_catchup_age_ms BETWEEN 60000 AND 2678400000),
    CONSTRAINT schedule_definitions_overlap_ck CHECK (overlap_policy IN ('allow', 'serialize')),
    CONSTRAINT schedule_definitions_versions_ck CHECK (payload_version >= 1 AND serializer_profile_version >= 1),
    CONSTRAINT schedule_definitions_template_ck CHECK (jsonb_typeof(template) = 'object'),
    CONSTRAINT schedule_definitions_created_by_ck CHECK (created_by IS NULL OR octet_length(created_by) BETWEEN 1 AND 256)
);

-- Occurrences: job_id is a logical reference (no FK) so job retention never erases the mapping. ------------------

CREATE TABLE runwick.schedule_occurrences (
    application         runwick.rw_identifier NOT NULL,
    schedule_id         uuid        NOT NULL,
    occurrence_kind     text COLLATE "C" NOT NULL,
    occurrence_key      text COLLATE "C" NOT NULL,
    scheduled_at        timestamptz,
    definition_revision bigint      NOT NULL,
    job_id              uuid,
    disposition         text COLLATE "C" NOT NULL,
    coalesced           boolean     NOT NULL DEFAULT false,
    covered_from        timestamptz,
    covered_through     timestamptz,
    materialized_at     timestamptz NOT NULL,
    command_id          uuid        NOT NULL,
    CONSTRAINT schedule_occurrences_pk PRIMARY KEY (application, schedule_id, occurrence_kind, occurrence_key),
    CONSTRAINT schedule_occurrences_schedule_fk FOREIGN KEY (application, schedule_id)
        REFERENCES runwick.schedules (application, schedule_id),
    CONSTRAINT schedule_occurrences_kind_ck CHECK (
        (occurrence_kind = 'scheduled' AND scheduled_at IS NOT NULL) OR occurrence_kind = 'manual'),
    CONSTRAINT schedule_occurrences_key_ck CHECK (octet_length(occurrence_key) BETWEEN 1 AND 64),
    CONSTRAINT schedule_occurrences_disposition_ck CHECK (
        (disposition IN ('created', 'linked_existing') AND job_id IS NOT NULL) OR (disposition = 'skipped' AND job_id IS NULL)),
    CONSTRAINT schedule_occurrences_coverage_ck CHECK (
        (covered_from IS NULL AND covered_through IS NULL)
        OR (covered_from IS NOT NULL AND covered_through IS NOT NULL AND covered_from <= covered_through))
);

CREATE INDEX schedule_occurrences_history
    ON runwick.schedule_occurrences (application, schedule_id, scheduled_at, occurrence_kind, occurrence_key);

CREATE TABLE runwick.schedule_audit_ranges (
    application         runwick.rw_identifier NOT NULL,
    audit_id            bigint GENERATED ALWAYS AS IDENTITY,
    schedule_id         uuid        NOT NULL,
    definition_revision bigint      NOT NULL,
    command_id          uuid        NOT NULL,
    from_exclusive      timestamptz,
    through_inclusive   timestamptz NOT NULL,
    reason              text COLLATE "C" NOT NULL,
    created_at          timestamptz NOT NULL,
    CONSTRAINT schedule_audit_ranges_pk PRIMARY KEY (application, audit_id),
    CONSTRAINT schedule_audit_ranges_schedule_fk FOREIGN KEY (application, schedule_id)
        REFERENCES runwick.schedules (application, schedule_id),
    CONSTRAINT schedule_audit_ranges_reason_ck CHECK (reason IN (
        'misfire_skip', 'run_once_covered', 'skipped_paused', 'skipped_by_edit', 'catch_up_skipped')),
    CONSTRAINT schedule_audit_ranges_order_ck CHECK (from_exclusive IS NULL OR from_exclusive < through_inclusive)
);

CREATE INDEX schedule_audit_ranges_schedule
    ON runwick.schedule_audit_ranges (application, schedule_id, through_inclusive);

-- Overlap scope kind (13 §7.1) ---------------------------------------------------------------------

ALTER TABLE runwick.concurrency_scopes DROP CONSTRAINT concurrency_scopes_kind_ck;

ALTER TABLE runwick.concurrency_scopes
    ADD CONSTRAINT concurrency_scopes_kind_v2_ck CHECK (scope_kind IN ('queue', 'job-type', 'resource', 'schedule'));

-- Events and audit ---------------------------------------------------------------------------

ALTER TABLE runwick.queue_events DROP CONSTRAINT queue_events_kind_v3_ck;

ALTER TABLE runwick.queue_events
    ADD CONSTRAINT queue_events_kind_v4_ck CHECK (event_kind IN (
        'dispatched', 'policy_drift',
        'attempt_reserved', 'attempt_started', 'attempt_cancelled_before_start',
        'attempt_finished', 'attempt_recovered', 'attempt_released',
        'job_cancelled', 'job_cancel_requested', 'job_retried', 'job_rerun',
        'queue_paused', 'queue_resumed', 'scope_policy_edited',
        'attempt_admission_deferred', 'rate_policy_edited', 'capabilities_activated',
        'schedule_registered', 'schedule_updated', 'schedule_materialized', 'schedule_paused', 'schedule_resumed',
        'schedule_triggered', 'schedule_needs_review', 'schedule_revalidated', 'catch_up_resolved'));

ALTER TABLE runwick.management_audit DROP CONSTRAINT management_audit_operation_v2_ck;

ALTER TABLE runwick.management_audit
    ADD CONSTRAINT management_audit_operation_v3_ck CHECK (operation IN (
        'Cancel', 'ManualRetry', 'RerunSucceeded', 'SetQueueState', 'EditScopePolicy',
        'EditRatePolicy', 'ActivateCapabilities',
        'SetScheduleState', 'UpdateSchedule', 'TriggerSchedule', 'ResolveCatchUp', 'RevalidateSchedule',
        'RecordScheduleEvaluationFailure'));

ALTER TABLE runwick.management_audit DROP CONSTRAINT management_audit_target_kind_v2_ck;

ALTER TABLE runwick.management_audit
    ADD CONSTRAINT management_audit_target_kind_v3_ck CHECK (target_kind IN ('job', 'queue', 'scope', 'rate_scope', 'application', 'schedule'));
