-- 0002_controls — expand phase. One transaction. Runwick schema revision 2 (08 §5, §7). Additive except the
-- widened queue_events kind check; schema_revision stays 1 until validate.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- §5 Management audit ------------------------------------------------------------------------

CREATE TABLE runwick.management_audit (
    application         runwick.rw_identifier NOT NULL,
    audit_id            bigint GENERATED ALWAYS AS IDENTITY,
    command_id          uuid        NOT NULL,
    operation           text COLLATE "C" NOT NULL,
    target_kind         text COLLATE "C" NOT NULL,
    target_id           text        NOT NULL,
    actor_subject       text        NOT NULL,
    actor_display       text,
    actor_scheme        text COLLATE "C" NOT NULL,
    reason              text,
    expected_revision   bigint,
    resulting_revision  bigint,
    details             jsonb       NOT NULL DEFAULT '{}'::jsonb,
    occurred_at         timestamptz NOT NULL,
    CONSTRAINT management_audit_pk PRIMARY KEY (application, audit_id),
    CONSTRAINT management_audit_command_uq UNIQUE (application, command_id, target_kind, target_id),
    CONSTRAINT management_audit_operation_ck CHECK (operation IN (
        'Cancel', 'ManualRetry', 'RerunSucceeded', 'SetQueueState', 'EditScopePolicy')),
    CONSTRAINT management_audit_target_kind_ck CHECK (target_kind IN ('job', 'queue', 'scope')),
    CONSTRAINT management_audit_target_ck CHECK (octet_length(target_id) BETWEEN 1 AND 1024),
    CONSTRAINT management_audit_subject_ck CHECK (octet_length(actor_subject) BETWEEN 1 AND 256),
    CONSTRAINT management_audit_display_ck CHECK (actor_display IS NULL OR octet_length(actor_display) <= 256),
    CONSTRAINT management_audit_scheme_ck CHECK (actor_scheme ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$'),
    CONSTRAINT management_audit_reason_ck CHECK (reason IS NULL OR octet_length(reason) <= 512),
    CONSTRAINT management_audit_details_ck CHECK (jsonb_typeof(details) = 'object' AND octet_length(details::text) <= 4096)
);

CREATE INDEX management_audit_target
    ON runwick.management_audit (application, target_kind, target_id, audit_id);

-- §7 Versioned events and the commit-safe cursor ----------------------------------------------

ALTER TABLE runwick.queue_events
    ADD COLUMN schema_version smallint NOT NULL DEFAULT 1,
    ADD COLUMN tx_id xid8 NOT NULL DEFAULT pg_current_xact_id();

ALTER TABLE runwick.queue_events
    ADD CONSTRAINT queue_events_schema_version_ck CHECK (schema_version = 1);

ALTER TABLE runwick.queue_events DROP CONSTRAINT queue_events_kind_ck;

ALTER TABLE runwick.queue_events
    ADD CONSTRAINT queue_events_kind_v2_ck CHECK (event_kind IN (
        'dispatched', 'policy_drift',
        'attempt_reserved', 'attempt_started', 'attempt_cancelled_before_start',
        'attempt_finished', 'attempt_recovered', 'attempt_released',
        'job_cancelled', 'job_cancel_requested', 'job_retried', 'job_rerun',
        'queue_paused', 'queue_resumed', 'scope_policy_edited'));

CREATE INDEX events_cursor
    ON runwick.queue_events (application, tx_id, event_id);
