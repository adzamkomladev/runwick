-- 0029_jobs_index_diet (04 Changes 2026-11-23): fewer index entries per job row version. Schema revision 29.
--
-- No jobs update can be HOT (every state change touches state, lease_expires_at or eligible_at), so each new row version
-- gets an entry in every index whose predicate it satisfies. Two of those entries are removed:
--
-- (1) jobs_profile_uq, the unique (application, job_id, profile id, profile version) index that existed only as the target
--     of the payload profile foreign key. Every jobs version (creation, claim, renewal, finish, detail expiry) wrote an
--     entry to it. The payload now references the job's primary key, and the profile agreement the composite key enforced
--     is kept by two triggers, so the same changes are refused:
--       - a payload insert or update whose profile differs from its job's (one keyed join per statement, set inserts too);
--       - a job profile change that leaves a payload of that job on another profile. Like the old key (ON UPDATE NO ACTION,
--         not deferrable), this is checked at the end of the statement: a statement that changes the payload and its job
--         together is accepted. It is a constraint trigger that fires only when an UPDATE names a profile column and a value
--         really changes; no released statement names them.
--     Both raise foreign_key_violation (23503) under the old constraint's name, as the foreign key did.
-- (2) jobs_terminal_retention now covers only terminal jobs whose detail has expired: the rows the history purge reads.
--     Terminal jobs whose detail is kept are already in jobs_detail_retention, so a finish writes one retention entry
--     instead of two. The job stats read of ended jobs reads both indexes (detail kept, detail expired) as two branches.
SET LOCAL lock_timeout = '5s';

ALTER TABLE runwick.job_payloads
    ADD CONSTRAINT job_payloads_job_fk FOREIGN KEY (application, job_id)
        REFERENCES runwick.jobs (application, job_id) ON DELETE CASCADE;

ALTER TABLE runwick.job_payloads DROP CONSTRAINT job_payloads_job_profile_fk;

ALTER TABLE runwick.jobs DROP CONSTRAINT jobs_profile_uq;

CREATE FUNCTION runwick.rw_payload_profile_check()
RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
    IF EXISTS (
        SELECT 1 FROM rw_new_payloads AS p
        LEFT JOIN runwick.jobs AS j ON j.application = p.application AND j.job_id = p.job_id
        WHERE j.job_id IS NULL
           OR j.serializer_profile_id IS DISTINCT FROM p.serializer_profile_id
           OR j.serializer_profile_version IS DISTINCT FROM p.serializer_profile_version) THEN
        RAISE EXCEPTION 'payload serializer profile does not match its job'
            USING ERRCODE = 'foreign_key_violation', TABLE = 'job_payloads', CONSTRAINT = 'job_payloads_job_profile_fk';
    END IF;
    RETURN NULL;
END
$fn$;

CREATE TRIGGER job_payloads_profile_insert AFTER INSERT ON runwick.job_payloads
    REFERENCING NEW TABLE AS rw_new_payloads FOR EACH STATEMENT EXECUTE FUNCTION runwick.rw_payload_profile_check();

CREATE TRIGGER job_payloads_profile_update AFTER UPDATE ON runwick.job_payloads
    REFERENCING NEW TABLE AS rw_new_payloads FOR EACH STATEMENT EXECUTE FUNCTION runwick.rw_payload_profile_check();

CREATE FUNCTION runwick.rw_job_profile_fixed()
RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
    IF EXISTS (
        SELECT 1 FROM runwick.job_payloads AS p
        WHERE p.application = NEW.application AND p.job_id = NEW.job_id
          AND (p.serializer_profile_id IS DISTINCT FROM NEW.serializer_profile_id
               OR p.serializer_profile_version IS DISTINCT FROM NEW.serializer_profile_version)) THEN
        RAISE EXCEPTION 'serializer profile of a job with a payload cannot change'
            USING ERRCODE = 'foreign_key_violation', TABLE = 'job_payloads', CONSTRAINT = 'job_payloads_job_profile_fk';
    END IF;
    RETURN NULL;
END
$fn$;

CREATE CONSTRAINT TRIGGER jobs_profile_fixed AFTER UPDATE OF serializer_profile_id, serializer_profile_version ON runwick.jobs
    NOT DEFERRABLE FOR EACH ROW
    WHEN (OLD.serializer_profile_id IS DISTINCT FROM NEW.serializer_profile_id
          OR OLD.serializer_profile_version IS DISTINCT FROM NEW.serializer_profile_version)
    EXECUTE FUNCTION runwick.rw_job_profile_fixed();

DROP INDEX runwick.jobs_terminal_retention;

CREATE INDEX jobs_terminal_retention
    ON runwick.jobs (application, terminal_at, job_id)
    WHERE detail_expired_at IS NOT NULL AND state IN ('succeeded', 'failed', 'cancelled');
