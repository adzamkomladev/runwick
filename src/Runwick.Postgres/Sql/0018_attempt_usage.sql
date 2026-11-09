-- 0018_attempt_usage (16 Changes 2026-11-09): what the worker process used during each attempt (CPU time, allocations,
-- working set; 16 §3 AttemptResourceUsage), stored with the attempt when its Finish applies. Schema revision 18.
--
-- One nullable jsonb column; NULL = not recorded (an attempt finished by an older binary, by a worker with
-- RecordResourceUsage off, or closed without running). Adding a nullable column without a default rewrites nothing.
-- Released functions are never replaced in place (04 §7.17): finish_v3 and finish_many_v3 run finish_v2 and
-- finish_many_v2 unchanged, then store the usage of each item that applied. The attempt row is already locked by the
-- close, so no lock is added to the order of 04 §5. Usage is not part of any receipt: a replay returns the same reply,
-- and the update only fills a NULL, so a replay never changes stored figures.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE runwick.job_attempts ADD COLUMN usage jsonb;

CREATE FUNCTION runwick.finish_v3(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
    p_fingerprint bytea, p_class text, p_deadline_cap interval, p_store_id uuid, p_revisions bigint[], p_epoch integer,
    p_capabilities jsonb, p_is_creation boolean, p_allow_drain boolean, p_staged boolean,
    p_job_id uuid, p_attempt_id uuid, p_worker_id uuid, p_token uuid, p_generation integer,
    p_outcome text, p_elapsed_ms bigint, p_requested_delay_ms bigint, p_error jsonb, p_entropy bigint,
    p_usage jsonb, p_faults jsonb DEFAULT NULL)
RETURNS runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_reply runwick.rw_reply_v1;
BEGIN
    v_reply := runwick.finish_v2(p_application, p_command_id, p_operation, p_not_after, p_fingerprint, p_class, p_deadline_cap,
        p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain, p_staged,
        p_job_id, p_attempt_id, p_worker_id, p_token, p_generation, p_outcome, p_elapsed_ms, p_requested_delay_ms, p_error, p_entropy,
        p_faults);
    IF p_usage IS NOT NULL AND v_reply.effect = 'Applied' THEN
        UPDATE runwick.job_attempts SET usage = p_usage
        WHERE application = p_application AND attempt_id = p_attempt_id AND usage IS NULL;
    END IF;
    RETURN v_reply;
END
$fn$;

CREATE FUNCTION runwick.finish_many_v3(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
    p_fingerprint bytea, p_class text, p_deadline_cap interval, p_store_id uuid, p_revisions bigint[], p_epoch integer,
    p_capabilities jsonb, p_is_creation boolean, p_allow_drain boolean, p_staged boolean,
    p_command_ids uuid[], p_not_afters timestamptz[], p_fingerprints bytea[],
    p_job_ids uuid[], p_attempt_ids uuid[], p_worker_ids uuid[], p_tokens uuid[], p_generations integer[],
    p_outcomes text[], p_elapsed_ms bigint[], p_requested_delays_ms bigint[], p_errors jsonb[], p_entropies bigint[],
    p_usages jsonb[], p_faults jsonb DEFAULT NULL)
RETURNS runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_reply runwick.rw_reply_v1;
BEGIN
    v_reply := runwick.finish_many_v2(p_application, p_command_id, p_operation, p_not_after, p_fingerprint, p_class, p_deadline_cap,
        p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain, p_staged,
        p_command_ids, p_not_afters, p_fingerprints, p_job_ids, p_attempt_ids, p_worker_ids, p_tokens, p_generations,
        p_outcomes, p_elapsed_ms, p_requested_delays_ms, p_errors, p_entropies, p_faults);
    IF v_reply.effect = 'Applied' THEN
        UPDATE runwick.job_attempts AS a SET usage = u.usage
        FROM ROWS FROM (pg_catalog.unnest(p_attempt_ids), pg_catalog.unnest(p_usages)) WITH ORDINALITY AS u(attempt_id, usage, k)
        WHERE u.usage IS NOT NULL
          AND (v_reply.result -> 'items' -> (u.k::integer - 1) ->> 'effect') = 'Applied'
          AND a.application = p_application AND a.attempt_id = u.attempt_id AND a.usage IS NULL;
    END IF;
    RETURN v_reply;
END
$fn$;
