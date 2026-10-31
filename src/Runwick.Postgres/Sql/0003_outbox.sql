-- 0003_outbox — expand phase. One transaction. Runwick schema revision 3 (14 §5.2). Additive only; the number is
-- provisional and is assigned in merge order (plan "Migration numbering").
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- §5.2 Destination delivery mappings ----------------------------------------------------------------
-- One row per (application, source store, message): the retained answer to every later transport command for that
-- delivery identity. job_id is a logical reference, never a foreign key: job history may be pruned first.

CREATE TABLE runwick.outbox_deliveries (
    application           runwick.rw_identifier NOT NULL,
    source_store_id       uuid        NOT NULL,
    message_id            uuid        NOT NULL,
    delivery_fingerprint  bytea       NOT NULL,
    fingerprint_version   integer     NOT NULL DEFAULT 1,
    accept_until          timestamptz NOT NULL,
    retain_until          timestamptz NOT NULL,
    job_id                uuid        NOT NULL,
    disposition           text COLLATE "C" NOT NULL,
    duplicate_match       text COLLATE "C" NOT NULL,
    policy_drift          boolean     NOT NULL,
    confirmed_profile     text COLLATE "C" NOT NULL,
    first_command_id      uuid        NOT NULL,
    delivered_at          timestamptz NOT NULL,
    CONSTRAINT outbox_deliveries_pk PRIMARY KEY (application, source_store_id, message_id),
    CONSTRAINT outbox_deliveries_fingerprint_ck CHECK (octet_length(delivery_fingerprint) = 32 AND fingerprint_version = 1),
    CONSTRAINT outbox_deliveries_window_ck CHECK (delivered_at < accept_until AND retain_until >= accept_until + interval '24 hours'),
    CONSTRAINT outbox_deliveries_disposition_ck CHECK (disposition IN ('Created', 'Reused')),
    CONSTRAINT outbox_deliveries_match_ck CHECK (duplicate_match IN ('None', 'Active', 'Window', 'ActiveAndWindow')),
    CONSTRAINT outbox_deliveries_profile_ck CHECK (octet_length(confirmed_profile) BETWEEN 1 AND 64)
);

CREATE INDEX outbox_deliveries_cleanup
    ON runwick.outbox_deliveries (retain_until);
