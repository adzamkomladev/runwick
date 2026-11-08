-- Runwick outbox source schema, version 2 (17 §6): the index the relay's delivered-row cleanup reads. Additive; runs
-- after source_v1.sql in the same InstallAsync transaction, so a version 1 source is upgraded by installing again.
CREATE INDEX IF NOT EXISTS outbox_messages_delivered
    ON runwick_outbox.outbox_messages (delivered_at) WHERE state = 'delivered';

UPDATE runwick_outbox.outbox_metadata SET schema_version = 2 WHERE singleton_id = 1 AND schema_version = 1;
