-- Runwick outbox source schema for SQLite, version 1 (14 §3.3). Times are UTC epoch milliseconds; UUIDs are
-- lowercase 'D' text. Installed explicitly by SqliteOutboxSchema.InstallAsync; idempotent.
CREATE TABLE IF NOT EXISTS runwick_outbox_metadata (
    singleton_id     INTEGER NOT NULL PRIMARY KEY CHECK (singleton_id = 1),
    source_store_id  TEXT    NOT NULL,
    schema_version   INTEGER NOT NULL,
    created_at       INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS runwick_outbox_messages (
    message_id              TEXT    NOT NULL PRIMARY KEY,
    destination_store_id    TEXT    NOT NULL,
    destination_application TEXT    NOT NULL,
    intent                  BLOB    NOT NULL,
    intent_codec            INTEGER NOT NULL DEFAULT 1 CHECK (intent_codec = 1),
    business_digest         BLOB    NOT NULL CHECK (length(business_digest) = 32),
    policy_digest           BLOB    NOT NULL CHECK (length(policy_digest) = 32),
    delivery_fingerprint    BLOB    NOT NULL CHECK (length(delivery_fingerprint) = 32),
    proposed_job_id         TEXT    NOT NULL,
    staged_at               INTEGER NOT NULL,
    accept_until            INTEGER NOT NULL,
    state                   TEXT    NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'leased', 'delivered', 'blocked')),
    next_attempt_at         INTEGER NOT NULL,
    lease_owner             TEXT,
    lease_token             TEXT,
    lease_expires_at        INTEGER,
    attempts                INTEGER NOT NULL DEFAULT 0,
    revision                INTEGER NOT NULL DEFAULT 1,
    transport_command_id    TEXT,
    transport_not_after     INTEGER,
    delivered_job_id        TEXT,
    delivered_disposition   TEXT,
    confirmed_profile       TEXT,
    delivered_at            INTEGER,
    last_error              TEXT CHECK (last_error IS NULL OR length(last_error) <= 256),
    blocked_reason          TEXT CHECK (blocked_reason IS NULL OR length(blocked_reason) <= 256),
    CHECK ((state = 'leased') = (lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)),
    CHECK ((state = 'delivered') = (delivered_job_id IS NOT NULL)),
    CHECK (accept_until > staged_at AND accept_until <= staged_at + 2592000000),
    CHECK (length(intent) BETWEEN 1 AND 2097152)
) STRICT;

CREATE INDEX IF NOT EXISTS runwick_outbox_messages_due
    ON runwick_outbox_messages (next_attempt_at) WHERE state = 'pending';

CREATE INDEX IF NOT EXISTS runwick_outbox_messages_lease_expiry
    ON runwick_outbox_messages (lease_expires_at) WHERE state = 'leased';
