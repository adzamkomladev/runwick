-- Runwick outbox source schema for SQLite, version 2 (17 §6): the index the relay's delivered-row cleanup reads.
-- Additive; runs after source_v1.sql and the metadata seed in the same InstallAsync transaction.
CREATE INDEX IF NOT EXISTS runwick_outbox_messages_delivered
    ON runwick_outbox_messages (delivered_at) WHERE state = 'delivered';

UPDATE runwick_outbox_metadata SET schema_version = 2 WHERE singleton_id = 1 AND schema_version = 1;
