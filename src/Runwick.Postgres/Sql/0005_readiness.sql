-- 0005_readiness (SP-012, 09 §3): per-queue worker subscriptions for the "no recently advertised capable worker"
-- diagnostic. Presence remains observational: no lease, attempt or permit references it.

CREATE TABLE runwick.worker_subscriptions (
    application runwick.rw_identifier NOT NULL,
    instance_id uuid                  NOT NULL,
    queue       runwick.rw_identifier NOT NULL,
    CONSTRAINT worker_subscriptions_pk PRIMARY KEY (application, instance_id, queue),
    CONSTRAINT worker_subscriptions_instance_fk FOREIGN KEY (application, instance_id)
        REFERENCES runwick.worker_instances (application, instance_id) ON DELETE CASCADE
);

CREATE INDEX worker_subscriptions_queue
    ON runwick.worker_subscriptions (application, queue, instance_id);
