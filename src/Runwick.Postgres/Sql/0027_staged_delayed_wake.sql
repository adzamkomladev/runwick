-- 0027_staged_delayed_wake (05 Changes 2026-11-22): a staged dispatch also wakes for a delayed job that is its lane's earliest.
--
-- dispatch_v3 (and rw_dispatch_core_v3 under it) notifies only for jobs due now. A job created with a later NotBefore that is
-- now the earliest not-yet-due job of its lane (queue and priority) is one an idle worker's last probe could not name, so the
-- worker slept its whole back-off (up to 10 s) past the due time. Direct dispatch already wakes for it after commit from the
-- client (0021); a caller-owned transaction has no client-side commit hook, so version 5 sends that notification inside the
-- transaction, where PostgreSQL delivers it only if the caller commits. It runs version 3 unchanged and then checks each created
-- delayed job: one primary-key and one jobs_ready lookup, the same check as the client-side wake pump. Nothing is sent on replay.
-- Released functions stay; clients at earlier revisions keep calling version 3.
SET LOCAL lock_timeout = '5s';

CREATE FUNCTION runwick.dispatch_v5(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
    p_fingerprint bytea, p_class text, p_deadline_cap interval, p_store_id uuid, p_revisions bigint[], p_epoch integer,
    p_capabilities jsonb, p_is_creation boolean, p_allow_drain boolean, p_staged boolean,
    p_many boolean, p_continue boolean, p_invalid text,
    p_job_ids uuid[], p_job_names text[], p_payload_versions integer[], p_profile_ids text[], p_profile_versions integer[],
    p_queues text[], p_priorities text[], p_not_before_ms bigint[], p_active_keys text[], p_window_keys text[],
    p_window_ms bigint[], p_business bytea[], p_policy bytea[], p_payloads bytea[], p_headers jsonb[],
    p_max_executions integer[], p_max_unstarted integer[], p_timeouts bigint[], p_retry jsonb[], p_unstarted jsonb[],
    p_scopes jsonb[], p_rates jsonb[],
    p_faults jsonb DEFAULT NULL)
RETURNS runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_reply runwick.rw_reply_v1;
BEGIN
    v_reply := runwick.dispatch_v3(p_application, p_command_id, p_operation, p_not_after, p_fingerprint, p_class, p_deadline_cap,
        p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain, p_staged, p_many, p_continue, p_invalid,
        p_job_ids, p_job_names, p_payload_versions, p_profile_ids, p_profile_versions, p_queues, p_priorities, p_not_before_ms,
        p_active_keys, p_window_keys, p_window_ms, p_business, p_policy, p_payloads, p_headers, p_max_executions, p_max_unstarted,
        p_timeouts, p_retry, p_unstarted, p_scopes, p_rates, p_faults);
    IF v_reply.effect IS DISTINCT FROM 'Applied' OR v_reply.replayed THEN
        RETURN v_reply;
    END IF;

    -- A created job not yet due wakes its queue while nothing not yet due precedes it in claim order (eligible_at, job_id).
    -- PostgreSQL folds identical notifications of one transaction.
    PERFORM pg_catalog.pg_notify('runwick_wake', p_application || '/' || x.queue)
    FROM pg_catalog.jsonb_array_elements(CASE WHEN p_many THEN v_reply.result -> 'items'
                                              ELSE pg_catalog.jsonb_build_array(v_reply.result) END) AS i(item)
    JOIN runwick.jobs AS x
      ON x.application = p_application AND x.job_id = (i.item ->> 'jobId')::uuid AND x.state IN ('pending', 'retrying')
     AND x.eligible_at > v_reply.decided_at
    WHERE i.item ->> 'disposition' = 'Created'
      AND NOT EXISTS (
          SELECT 1 FROM runwick.jobs AS j
          WHERE j.application = x.application AND j.queue = x.queue AND j.priority = x.priority
            AND j.state IN ('pending', 'retrying')
            AND j.eligible_at > v_reply.decided_at
            AND (j.eligible_at, j.job_id) < (x.eligible_at, x.job_id));
    RETURN v_reply;
END
$fn$;
