-- 0031_finish_and_claim (04 Changes 2026-11-23, combined finish and claim, 05 §12.7): a driver-only entry point that
-- decides up to 32 Finish/1 commands and one ReserveAndBegin/1 command in one transaction and one round trip. It adds no
-- protocol operation, table, index, event kind or receipt codec, and it reuses finish_many_v3 and reserve_and_begin_v3
-- unchanged: the finishes are decided exactly as FinishMany/1 decides them (each with its own receipt), then the claim
-- exactly as ReserveAndBegin/1 decides it (its own receipt, which holds only the claim), so a permit a finish frees can
-- serve the claim. The frame arguments are the claim command's; the finishes use p_finish_class and p_finish_deadline_cap.
--
-- Rows: the first row is the call's reply, then the payload rows of the begun jobs (as reserve_and_begin_v3).
--   * The claim was already decided (a resend): reserve_and_begin_v3's replay alone; nothing else is decided, and each
--     finish resolves from its own receipt.
--   * The claim applied (now, or replayed because a concurrent resend decided it first): result = the claim's result plus
--     "finished" (FinishMany/1's items) and "claimReplayed". replayed = true only when nothing in the call committed.
--   * NoWork, QueuePaused or CapacityBlocked: the finishes still apply; the claim's placeholder is removed (no effect, no
--     receipt) and its reply travels in result "claim". replayed = true only when no finish applied.
--   * Any other reply of either half (gates, CommandExpired, CommandConflict, InvariantFault, ...) is the only row and
--     commits nothing; the client then resolves each command alone with its own identity.
-- Lock order: the finishes take J, K, S of their jobs, then the claim takes its candidates (SKIP LOCKED) and K, S, R of
-- those. A claim that needs a concurrency or rate scope can therefore deadlock with another transaction; the 40P01 rolls
-- back the whole call and the runner retries it with the same identities (P-13), as for any transient conflict.
SET LOCAL lock_timeout = '5s';

CREATE FUNCTION runwick.finish_and_claim_v1(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
    p_fingerprint bytea, p_class text, p_deadline_cap interval, p_store_id uuid, p_revisions bigint[], p_epoch integer,
    p_capabilities jsonb, p_is_creation boolean, p_allow_drain boolean, p_staged boolean,
    p_queue text, p_priority text, p_after_eligible_at timestamptz, p_after_job_id uuid,
    p_names text[], p_versions integer[], p_profile_ids text[], p_profile_versions integer[],
    p_lease interval, p_worker_id uuid, p_tokens uuid[], p_min_defer interval, p_defer_jitters_ms integer[],
    p_finish_class text, p_finish_deadline_cap interval,
    p_command_ids uuid[], p_not_afters timestamptz[], p_fingerprints bytea[],
    p_job_ids uuid[], p_attempt_ids uuid[], p_worker_ids uuid[], p_finish_tokens uuid[], p_generations integer[],
    p_outcomes text[], p_elapsed_ms bigint[], p_requested_delays_ms bigint[], p_errors jsonb[], p_entropies bigint[],
    p_usages jsonb[], p_faults jsonb DEFAULT NULL)
RETURNS SETOF runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_finished runwick.rw_reply_v1;
    v_claim runwick.rw_reply_v1;
    v_row runwick.rw_reply_v1;
    v_payloads runwick.rw_reply_v1[] := '{}';
    v_none_applied boolean;
    v_reply runwick.rw_reply_v1;
BEGIN
    IF p_staged OR p_command_id = ANY (p_command_ids) THEN
        RETURN NEXT runwick.rw_no_effect_v1('InvalidRequest', 'invalid_request', NULL);
        RETURN;
    END IF;

    -- A resend of a decided claim replays the claim alone (its receipt holds only the claim), as on Redis.
    IF EXISTS (SELECT 1 FROM runwick.storage_commands AS c
               WHERE c.application = p_application AND c.retention_class = p_class AND c.command_id = p_command_id
                 AND c.result IS NOT NULL) THEN
        RETURN QUERY SELECT * FROM runwick.reserve_and_begin_v3(p_application, p_command_id, p_operation, p_not_after, p_fingerprint,
            p_class, p_deadline_cap, p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain, p_staged,
            p_queue, p_priority, p_after_eligible_at, p_after_job_id, p_names, p_versions, p_profile_ids, p_profile_versions,
            p_lease, p_worker_id, p_tokens, p_min_defer, p_defer_jitters_ms, p_faults);
        RETURN;
    END IF;

    -- The finishes, as FinishMany/1 (no call receipt: fingerprint NULL). The claim's gates and deadline bound the call.
    v_finished := runwick.finish_many_v3(p_application, p_command_id, 'FinishMany', p_not_after, NULL, p_finish_class,
        p_finish_deadline_cap, p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain, p_staged,
        p_command_ids, p_not_afters, p_fingerprints, p_job_ids, p_attempt_ids, p_worker_ids, p_finish_tokens, p_generations,
        p_outcomes, p_elapsed_ms, p_requested_delays_ms, p_errors, p_entropies, p_usages, p_faults);
    IF v_finished.status IS DISTINCT FROM 'Applied' THEN
        RETURN NEXT v_finished;
        RETURN;
    END IF;

    v_none_applied := v_finished.replayed; -- finish_many_v2 sets it when no item applied now

    -- The claim, as ReserveAndBegin/1, after the finishes: its reply row, then its payload rows.
    FOR v_row IN SELECT * FROM runwick.reserve_and_begin_v3(p_application, p_command_id, p_operation, p_not_after, p_fingerprint,
            p_class, p_deadline_cap, p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain, p_staged,
            p_queue, p_priority, p_after_eligible_at, p_after_job_id, p_names, p_versions, p_profile_ids, p_profile_versions,
            p_lease, p_worker_id, p_tokens, p_min_defer, p_defer_jitters_ms, p_faults) LOOP
        IF v_claim IS NULL THEN
            v_claim := v_row;
        ELSE
            v_payloads := v_payloads || v_row;
        END IF;
    END LOOP;

    IF v_claim.effect = 'Applied' THEN
        v_reply := runwick.rw_applied_v1('Applied', NULL, v_claim.decided_at, v_claim.result || pg_catalog.jsonb_build_object(
            'finished', v_finished.result -> 'items', 'claimReplayed', v_claim.replayed));
        v_reply.replayed := v_none_applied AND v_claim.replayed;
        RETURN NEXT v_reply;
        RETURN QUERY SELECT (p).* FROM unnest(v_payloads) AS p;
        RETURN;
    END IF;

    IF v_claim.status IN ('NoWork', 'QueuePaused', 'CapacityBlocked') THEN
        PERFORM runwick.rw_discard_receipt_v1(p_application, p_class, p_command_id);
        v_reply := runwick.rw_applied_v1('Applied', NULL, v_finished.decided_at, pg_catalog.jsonb_build_object(
            'finished', v_finished.result -> 'items',
            'claim', pg_catalog.jsonb_build_object('status', v_claim.status, 'code', v_claim.code,
                'decidedAt', runwick.rw_ms_v1(v_claim.decided_at), 'blocked', v_claim.blocked_scope_id,
                'resumeAt', runwick.rw_ms_v1(v_claim.resume_eligible_at), 'resumeJob', v_claim.resume_job_id)));
        v_reply.replayed := v_none_applied;
        RETURN NEXT v_reply;
        RETURN;
    END IF;

    RETURN NEXT v_claim;
END
$fn$;
