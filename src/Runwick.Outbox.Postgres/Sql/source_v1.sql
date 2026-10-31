-- Runwick outbox source schema, version 1 (14 §3.2). Versioned separately from the queue store schema.
-- Installed explicitly by PostgresOutboxSchema.InstallAsync (admin role); idempotent. The source UUID is created once
-- and never regenerated: a restore keeps it, a new database gets a new one.
SET LOCAL lock_timeout = '5s';

CREATE SCHEMA IF NOT EXISTS runwick_outbox;

CREATE TABLE IF NOT EXISTS runwick_outbox.outbox_metadata (
    singleton_id     smallint    NOT NULL DEFAULT 1,
    source_store_id  uuid        NOT NULL,
    schema_version   integer     NOT NULL,
    created_at       timestamptz NOT NULL,
    CONSTRAINT outbox_metadata_pk PRIMARY KEY (singleton_id),
    CONSTRAINT outbox_metadata_singleton_ck CHECK (singleton_id = 1)
);

INSERT INTO runwick_outbox.outbox_metadata (singleton_id, source_store_id, schema_version, created_at)
VALUES (1, gen_random_uuid(), 1, clock_timestamp())
ON CONFLICT (singleton_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS runwick_outbox.outbox_messages (
    message_id              uuid        NOT NULL,
    destination_store_id    uuid        NOT NULL,
    destination_application text COLLATE "C" NOT NULL,
    intent                  bytea       NOT NULL,
    intent_codec            smallint    NOT NULL DEFAULT 1,
    business_digest         bytea       NOT NULL,
    policy_digest           bytea       NOT NULL,
    delivery_fingerprint    bytea       NOT NULL,
    proposed_job_id         uuid        NOT NULL,
    staged_at               timestamptz NOT NULL,
    accept_until            timestamptz NOT NULL,
    state                   text COLLATE "C" NOT NULL DEFAULT 'pending',
    next_attempt_at         timestamptz NOT NULL,
    lease_owner             uuid,
    lease_token             uuid,
    lease_expires_at        timestamptz,
    attempts                integer     NOT NULL DEFAULT 0,
    revision                bigint      NOT NULL DEFAULT 1,
    transport_command_id    uuid,
    transport_not_after     timestamptz,
    delivered_job_id        uuid,
    delivered_disposition   text COLLATE "C",
    confirmed_profile       text COLLATE "C",
    delivered_at            timestamptz,
    last_error              text COLLATE "C",
    blocked_reason          text COLLATE "C",
    CONSTRAINT outbox_messages_pk PRIMARY KEY (message_id),
    CONSTRAINT outbox_messages_state_ck CHECK (state IN ('pending', 'leased', 'delivered', 'blocked')),
    CONSTRAINT outbox_messages_lease_ck CHECK ((state = 'leased') = (lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)),
    CONSTRAINT outbox_messages_delivered_ck CHECK ((state = 'delivered') = (delivered_job_id IS NOT NULL)),
    CONSTRAINT outbox_messages_window_ck CHECK (accept_until > staged_at AND accept_until <= staged_at + interval '30 days'),
    CONSTRAINT outbox_messages_digest_ck CHECK (octet_length(business_digest) = 32 AND octet_length(policy_digest) = 32
        AND octet_length(delivery_fingerprint) = 32),
    CONSTRAINT outbox_messages_intent_ck CHECK (octet_length(intent) BETWEEN 1 AND 2097152 AND intent_codec = 1),
    CONSTRAINT outbox_messages_error_ck CHECK ((last_error IS NULL OR octet_length(last_error) <= 256)
        AND (blocked_reason IS NULL OR octet_length(blocked_reason) <= 256))
);

CREATE INDEX IF NOT EXISTS outbox_messages_due
    ON runwick_outbox.outbox_messages (next_attempt_at) WHERE state = 'pending';

CREATE INDEX IF NOT EXISTS outbox_messages_lease_expiry
    ON runwick_outbox.outbox_messages (lease_expires_at) WHERE state = 'leased';
