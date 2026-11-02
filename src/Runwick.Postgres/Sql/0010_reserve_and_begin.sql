-- 0010_reserve_and_begin (PF-06 batched claim, 03/04 Changes 2026-11-02): ReserveAndBegin/1, which claims up to 32 jobs
-- for one worker and begins each one in the same transaction, with one outcome per job (04 §7.18).
-- Additive for the protocol: the manifest gains ReserveAndBegin/1; Reserve/1 and Begin/1 are unchanged on the wire.
--
-- To write the claim once, the per-job bodies of Reserve and Begin move into two shared functions that the single and
-- the batched operation both call: rw_reserve_job_v1 (capacity check, fences, attempt, job, permits, event; 04 §7.3) and
-- rw_begin_job_v1 (ownership check, cancel/exhausted close, the rate step and its committed deferral, authorize;
-- 04 §7.5, 10 §4). A released function is never replaced in place (04 §7.17), so reserve_v1 and begin_v1 give way to
-- reserve_v2 and begin_v2, which run the same statements in the same order through those functions, and are dropped:
-- stop-and-migrate, no binary of revision 9 runs against revision 10.
--
-- Rules and fault hooks: as 0009_functions (LANGUAGE plpgsql VOLATILE, no SET clauses, no EXCEPTION blocks; lines ending
-- in "-- rw:fault" exist only in the fault-hooks variant).
SET LOCAL lock_timeout = '5s';

DROP FUNCTION runwick.reserve_v1(text, uuid, text, timestamptz, bytea, text, interval, uuid, bigint[], integer, jsonb, boolean,
    boolean, boolean, text, text, timestamptz, uuid, text[], integer[], text[], integer[], interval, uuid, uuid, jsonb);
DROP FUNCTION runwick.begin_v1(text, uuid, text, timestamptz, bytea, text, interval, uuid, bigint[], integer, jsonb, boolean,
    boolean, boolean, uuid, uuid, uuid, uuid, integer, interval, integer, jsonb);

-- ReserveAndBegin's receipt is an ownership receipt (03 §8.5: it carries leases and ownership tokens, like Reserve and
-- Begin), so the operation joins the ownership class.
ALTER TABLE runwick.storage_commands DROP CONSTRAINT storage_commands_operation_class_ck;

ALTER TABLE runwick.storage_commands
    ADD CONSTRAINT storage_commands_operation_class_v2_ck CHECK (
        (operation IN ('Reserve', 'Begin', 'Recover', 'ReleaseReservation', 'ReserveAndBegin')) = (retention_class = 'ownership'));

-- ------------------------------------------------------------------ shared per-job bodies

-- One job's reservation (04 §7.3 from G on), after the caller locked the job (rank 5) and every scope it requires (rank 7)
-- and read the decision time p_now after its last lock. The scope rows are read again under FOR UPDATE, which only
-- re-confirms locks this transaction already holds, so a batch sees the fencing counters and permits of the jobs it
-- reserved before this one. Returns CapacityBlocked (no effect: nothing written), InvariantFault invalid_transition (the
-- caller rolls back), or Applied with the Reserve receipt result (codec v1). R4 is the caller's.
CREATE FUNCTION runwick.rw_reserve_job_v1(p_application text, p_command_id uuid, p_now timestamptz, p_lease interval,
    p_worker_id uuid, p_token uuid, p_queue text, p_priority text, p_job_id uuid, p_faults jsonb)
RETURNS runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_state text;
    v_eligible timestamptz;
    v_generation integer;
    v_revision bigint;
    v_reservations bigint;
    v_name text;
    v_payload_version integer;
    v_profile_id text;
    v_profile_version integer;
    v_max_executions integer;
    v_max_unstarted integer;
    v_timeout bigint;
    v_retry jsonb;
    v_unstarted jsonb;
    v_scope_ids uuid[];
    v_capacities integer[];
    v_fencing boolean[];
    v_counters bigint[];
    v_fence_ids uuid[];
    v_fence_values bigint[];
    v_blocked uuid;
    v_expiry timestamptz;
    v_attempt uuid;
    v_new_revision bigint;
    v_n integer;
BEGIN
    -- The claimed row (locked by the caller): the values step C returned.
    SELECT j.state, j.eligible_at, j.generation, j.revision, j.reservation_count,
           j.job_name, j.payload_version, j.serializer_profile_id, j.serializer_profile_version
    INTO v_state, v_eligible, v_generation, v_revision, v_reservations, v_name, v_payload_version, v_profile_id, v_profile_version
    FROM runwick.jobs AS j
    WHERE j.application = p_application AND j.job_id = p_job_id;
    IF NOT FOUND OR v_state NOT IN ('pending', 'retrying') THEN
        RETURN runwick.rw_no_effect_v1('InvariantFault', 'invalid_transition', p_now);
    END IF;

    -- G: the generation's policy (dependent of the locked job).
    SELECT g.max_executions, g.max_unstarted_abandonments, g.execution_timeout_ms, g.retry_policy, g.unstarted_retry_policy
    INTO v_max_executions, v_max_unstarted, v_timeout, v_retry, v_unstarted
    FROM runwick.job_generations AS g
    WHERE g.application = p_application AND g.job_id = p_job_id AND g.generation = v_generation;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'generation row missing' USING ERRCODE = 'RWI04';
    END IF;

    -- S (already locked by the caller, UUID order): capacity, fencing and the current counters.
    SELECT COALESCE(pg_catalog.array_agg(x.scope_id ORDER BY x.scope_id), '{}'),
           COALESCE(pg_catalog.array_agg(x.capacity ORDER BY x.scope_id), '{}'),
           COALESCE(pg_catalog.array_agg(x.fencing_enabled ORDER BY x.scope_id), '{}'),
           COALESCE(pg_catalog.array_agg(x.fencing_counter ORDER BY x.scope_id), '{}')
    INTO v_scope_ids, v_capacities, v_fencing, v_counters
    FROM (SELECT s.scope_id, s.capacity, s.fencing_enabled, s.fencing_counter
          FROM runwick.concurrency_scopes AS s
          WHERE s.application = p_application
            AND s.scope_id IN (SELECT r.scope_id FROM runwick.job_concurrency_requirements AS r
                               WHERE r.application = p_application AND r.job_id = p_job_id)
          ORDER BY s.scope_id
          FOR UPDATE OF s) AS x;

    -- P: live permits per scope at the decision time; the first full scope (UUID order) blocks.
    IF pg_catalog.cardinality(v_scope_ids) > 0 THEN
        SELECT r.scope_id INTO v_blocked
        FROM unnest(v_scope_ids, v_capacities) WITH ORDINALITY AS r(scope_id, capacity, ord)
        WHERE (SELECT count(*) FROM runwick.concurrency_permits AS p
               WHERE p.application = p_application AND p.scope_id = r.scope_id AND p.lease_expires_at > p_now) >= r.capacity
        ORDER BY r.ord
        LIMIT 1;
        IF v_blocked IS NOT NULL THEN
            RETURN runwick.rw_no_effect_v1('CapacityBlocked', 'capacity_blocked', p_now, NULL, NULL, v_blocked, v_eligible, p_job_id);
        END IF;
    END IF;

    v_expiry := runwick.ms_floor(p_now + p_lease);
    v_attempt := runwick.rw_uuid_v7(p_now);
    PERFORM runwick.rw_fault('BeforeFirstMutation', 0, p_faults); -- rw:fault

    -- Fences: the returned counters are the permits' fencing values (count checked).
    IF true = ANY (v_fencing) THEN
        WITH u AS (
            UPDATE runwick.concurrency_scopes
            SET fencing_counter = fencing_counter + 1
            WHERE application = p_application AND fencing_enabled
              AND scope_id = ANY (ARRAY(SELECT f.id FROM unnest(v_scope_ids, v_fencing) AS f(id, on_) WHERE f.on_))
            RETURNING scope_id, fencing_counter)
        SELECT COALESCE(pg_catalog.array_agg(u.scope_id ORDER BY u.scope_id), '{}'),
               COALESCE(pg_catalog.array_agg(u.fencing_counter ORDER BY u.scope_id), '{}')
        INTO v_fence_ids, v_fence_values FROM u;
        IF v_fence_ids <> ARRAY(SELECT f.id FROM unnest(v_scope_ids, v_fencing) AS f(id, on_) WHERE f.on_ ORDER BY f.id)
           OR v_fence_values <> ARRAY(SELECT f.c + 1 FROM unnest(v_scope_ids, v_fencing, v_counters) AS f(id, on_, c) WHERE f.on_ ORDER BY f.id) THEN
            RAISE EXCEPTION 'rw:invalid_transition' USING ERRCODE = 'RWI01';
        END IF;
    ELSE
        v_fence_ids := '{}';
        v_fence_values := '{}';
    END IF;

    INSERT INTO runwick.job_attempts (
        application, attempt_id, job_id, generation, reservation_number, worker_instance_id, ownership_token, reserved_at)
    VALUES (p_application, v_attempt, p_job_id, v_generation, v_reservations + 1, p_worker_id, p_token, p_now);

    UPDATE runwick.jobs
    SET state = 'reserved', reservation_count = reservation_count + 1, revision = revision + 1,
        current_attempt_id = v_attempt, worker_instance_id = p_worker_id, ownership_token = p_token,
        lease_expires_at = v_expiry, updated_at = p_now
    WHERE application = p_application AND job_id = p_job_id AND state IN ('pending', 'retrying')
    RETURNING revision INTO v_new_revision;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n <> 1 OR v_new_revision <> v_revision + 1 THEN
        RETURN runwick.rw_no_effect_v1('InvariantFault', 'invalid_transition', p_now);
    END IF;

    IF pg_catalog.cardinality(v_scope_ids) > 0 THEN
        INSERT INTO runwick.concurrency_permits (
            application, scope_id, attempt_id, job_id, ownership_token, lease_expires_at, fencing_value)
        SELECT p_application, s.scope_id, v_attempt, p_job_id, p_token, v_expiry,
               v_fence_values[pg_catalog.array_position(v_fence_ids, s.scope_id)]
        FROM unnest(v_scope_ids) AS s(scope_id)
        ORDER BY s.scope_id;
    END IF;

    INSERT INTO runwick.queue_events (application, job_id, attempt_id, command_id, event_kind, occurred_at, details)
    VALUES (p_application, p_job_id, v_attempt, p_command_id, 'attempt_reserved', p_now,
            pg_catalog.jsonb_build_object('generation', v_generation, 'reservationNumber', v_reservations + 1));

    -- The Reserve receipt result (codec v1, ReceiptCodec.Reserve).
    RETURN runwick.rw_applied_v1('Applied', NULL, p_now, pg_catalog.jsonb_build_object(
        'jobId', p_job_id, 'generation', v_generation, 'revision', v_new_revision, 'state', 'reserved',
        'attemptId', v_attempt, 'workerInstanceId', p_worker_id, 'ownershipToken', p_token,
        'leaseExpiresAt', runwick.rw_ms_v1(v_expiry), 'reservationNumber', v_reservations + 1,
        'contract', pg_catalog.jsonb_build_object('name', v_name, 'payloadVersion', v_payload_version,
            'serializerProfileId', v_profile_id, 'serializerProfileVersion', v_profile_version),
        'queue', p_queue, 'priority', p_priority,
        'cursor', pg_catalog.jsonb_build_object('eligibleAt', runwick.rw_ms_v1(v_eligible), 'jobId', p_job_id),
        'policy', pg_catalog.jsonb_build_object('executionTimeout', v_timeout, 'fingerprintVersion', 1,
            'maxConsecutiveUnstartedAbandonments', v_max_unstarted, 'maxExecutions', v_max_executions,
            'retry', v_retry, 'unstartedRetry', v_unstarted),
        'fences', COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('scopeId', f.id, 'value', f.v) ORDER BY f.ord)
                            FROM unnest(v_fence_ids, v_fence_values) WITH ORDINALITY AS f(id, v, ord)), '[]'::jsonb)));
END
$fn$;

-- One job's Begin decision (04 §7.5 from the ownership check on, 10 §4), after the caller locked the job, its active key
-- when the plan closes, its scopes when the plan closes or is rated, and its rate scopes when rated, in lock order, and
-- read the decision time p_now after its last lock. The rate scope rows are read again under FOR UPDATE, which only
-- re-confirms the caller's locks, so a batch sees the grants and logical time of the jobs it began before this one.
-- Returns the no-effect ownership refusal, InvariantFault invalid_transition, or an Applied row (Applied, InvalidState
-- cancelled_before_start, BudgetExhausted, RateLimited) with the Begin receipt result (codec v1). R4 is the caller's.
CREATE FUNCTION runwick.rw_begin_job_v1(p_application text, p_command_id uuid, p_now timestamptz, p_job_id uuid, p_attempt_id uuid,
    p_worker_id uuid, p_token uuid, p_generation integer, p_min_defer interval, p_defer_jitter_ms integer, p_faults jsonb)
RETURNS runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_reply runwick.rw_reply_v1;
    v_found boolean;
    v_state text;
    v_generation integer;
    v_revision bigint;
    v_attempt uuid;
    v_worker uuid;
    v_token uuid;
    v_lease timestamptz;
    v_cancel timestamptz;
    v_active_key text;
    v_job_name text;
    v_max_executions integer;
    v_started integer;
    v_executions bigint;
    v_rated boolean;
    v_plan text;
    v_rate_ids uuid[] := '{}';
    v_limits integer[];
    v_windows bigint[];
    v_rate_revisions bigint[];
    v_last_decisions timestamptz[];
    v_not_befores timestamptz[];
    v_counts integer[];
    v_blocked uuid[];
    v_t timestamptz;
    v_hint timestamptz;
    v_oldest timestamptz;
    v_logical timestamptz;
    v_eligible timestamptz;
    v_outcome text;
    v_reason text;
    v_new_state text;
    v_code text;
    v_status text;
    v_new_revision bigint;
    v_n integer;
    v_i integer;
    v_execution_number integer;
    v_total bigint;
    v_result jsonb;
BEGIN
    -- J (locked by the caller)
    SELECT j.state, j.generation, j.revision, j.current_attempt_id, j.worker_instance_id, j.ownership_token,
           j.lease_expires_at, j.cancel_requested_at, j.active_unique_key, j.job_name
    INTO v_state, v_generation, v_revision, v_attempt, v_worker, v_token, v_lease, v_cancel, v_active_key, v_job_name
    FROM runwick.jobs AS j
    WHERE j.application = p_application AND j.job_id = p_job_id;
    v_found := FOUND;

    -- G behind J, and the plan.
    IF v_found THEN
        SELECT g.max_executions, g.started_count, j.execution_count,
               EXISTS (SELECT 1 FROM runwick.job_rate_requirements AS rr WHERE rr.application = j.application AND rr.job_id = j.job_id)
        INTO v_max_executions, v_started, v_executions, v_rated
        FROM runwick.jobs AS j
        JOIN runwick.job_generations AS g ON g.application = j.application AND g.job_id = j.job_id AND g.generation = j.generation
        WHERE j.application = p_application AND j.job_id = p_job_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'generation row missing' USING ERRCODE = 'RWI04';
        END IF;

        v_plan := CASE WHEN v_cancel IS NOT NULL THEN 'cancel' WHEN v_started >= v_max_executions THEN 'exhausted' ELSE 'authorize' END;
        v_rated := v_plan = 'authorize' AND v_rated;
    END IF;

    v_reply := runwick.rw_ownership_v1(v_found, v_state, v_attempt, v_token, v_worker, v_generation, v_lease,
        p_attempt_id, p_token, p_worker_id, p_generation, p_now, ARRAY['reserved']);
    IF v_reply.status IS NOT NULL THEN
        RETURN v_reply;
    END IF;

    -- Cancellation and an exhausted budget close the attempt before any charge.
    IF v_plan IN ('cancel', 'exhausted') THEN
        v_outcome := CASE v_plan WHEN 'cancel' THEN 'cancelled_before_start' ELSE 'abandoned_before_start' END;
        v_reason := CASE v_plan WHEN 'cancel' THEN 'cancelled_before_start' ELSE 'execution_budget_exhausted' END;
        v_new_state := CASE v_plan WHEN 'cancel' THEN 'cancelled' ELSE 'failed' END;
        v_status := CASE v_plan WHEN 'cancel' THEN 'InvalidState' ELSE 'BudgetExhausted' END;
        v_code := v_reason;
        PERFORM runwick.rw_fault('BeforeFirstMutation', 0, p_faults); -- rw:fault
        v_new_revision := runwick.rw_close_v1(p_application, p_command_id, p_now, p_job_id, p_attempt_id, p_token, v_generation,
            v_revision, v_job_name, v_active_key, v_outcome, v_new_state, v_reason,
            CASE v_plan WHEN 'cancel' THEN 'attempt_cancelled_before_start' ELSE 'attempt_finished' END,
            NULL, NULL, NULL, NULL, NULL);
        PERFORM runwick.rw_fault('AfterMutation', 1, p_faults); -- rw:fault
        v_result := pg_catalog.jsonb_build_object('jobId', p_job_id, 'generation', v_generation, 'revision', v_new_revision,
            'state', v_new_state, 'attemptOutcome', v_outcome, 'terminalReason', v_reason, 'errorCode', v_code);
        RETURN runwick.rw_applied_v1(v_status, v_code, p_now, v_result);
    END IF;

    PERFORM runwick.rw_fault('BeforeFirstMutation', 0, p_faults); -- rw:fault

    -- R (locked by the caller, UUID order).
    IF v_rated THEN
        SELECT COALESCE(pg_catalog.array_agg(x.rate_scope_id ORDER BY x.rate_scope_id), '{}'),
               COALESCE(pg_catalog.array_agg(x.limit_count ORDER BY x.rate_scope_id), '{}'),
               COALESCE(pg_catalog.array_agg(x.window_ms ORDER BY x.rate_scope_id), '{}'),
               COALESCE(pg_catalog.array_agg(x.policy_revision ORDER BY x.rate_scope_id), '{}'),
               COALESCE(pg_catalog.array_agg(x.last_decision_at ORDER BY x.rate_scope_id), '{}'),
               COALESCE(pg_catalog.array_agg(x.not_before ORDER BY x.rate_scope_id), '{}')
        INTO v_rate_ids, v_limits, v_windows, v_rate_revisions, v_last_decisions, v_not_befores
        FROM (SELECT s.rate_scope_id, s.limit_count, s.window_ms, s.policy_revision, s.last_decision_at, s.not_before
              FROM runwick.rate_scopes AS s
              WHERE s.application = p_application
                AND s.rate_scope_id IN (SELECT r.rate_scope_id FROM runwick.job_rate_requirements AS r
                                        WHERE r.application = p_application AND r.job_id = p_job_id)
              ORDER BY s.rate_scope_id
              FOR UPDATE OF s) AS x;
    END IF;

    -- Rate step (10 §4): all-or-none at logical time t = max(now, the scopes' last decisions).
    IF pg_catalog.cardinality(v_rate_ids) > 0 THEN
        v_t := GREATEST(p_now, (SELECT max(d) FROM unnest(v_last_decisions) AS d));
        SELECT pg_catalog.array_agg((SELECT count(*)::integer FROM runwick.rate_grants AS g
                                     WHERE g.application = p_application AND g.rate_scope_id = r.id
                                       AND g.authorized_at > v_t - r.w * interval '1 millisecond') ORDER BY r.ord)
        INTO v_counts
        FROM unnest(v_rate_ids, v_windows) WITH ORDINALITY AS r(id, w, ord);

        SELECT COALESCE(pg_catalog.array_agg(r.id ORDER BY r.ord), '{}') INTO v_blocked
        FROM unnest(v_rate_ids, v_limits, v_not_befores, v_counts) WITH ORDINALITY AS r(id, lim, nb, c, ord)
        WHERE r.c >= r.lim OR (r.nb IS NOT NULL AND v_t < r.nb);

        -- The scopes' logical time never goes backward, whichever way the decision goes.
        UPDATE runwick.rate_scopes SET last_decision_at = v_t
        WHERE application = p_application AND rate_scope_id = ANY (v_rate_ids)
          AND (last_decision_at IS NULL OR last_decision_at < v_t);

        IF pg_catalog.cardinality(v_blocked) = 0 THEN
            INSERT INTO runwick.rate_grants (application, rate_scope_id, attempt_id, job_id, authorized_at, policy_revision)
            SELECT p_application, s.id, p_attempt_id, p_job_id, v_t, s.revision
            FROM unnest(v_rate_ids, v_rate_revisions) AS s(id, revision)
            ORDER BY s.id;
            GET DIAGNOSTICS v_n = ROW_COUNT;
            IF v_n <> pg_catalog.cardinality(v_rate_ids) THEN
                RAISE EXCEPTION 'rw:invalid_transition' USING ERRCODE = 'RWI01';
            END IF;
        ELSE
            PERFORM runwick.rw_fault('AfterMutation', 1, p_faults); -- rw:fault

            -- Earliest logical instant at which every blocked scope could admit (capacity-control §4); not a reservation.
            v_hint := v_t;
            FOR v_i IN 1 .. pg_catalog.cardinality(v_rate_ids) LOOP
                CONTINUE WHEN NOT (v_rate_ids[v_i] = ANY (v_blocked));
                IF v_counts[v_i] >= v_limits[v_i] THEN
                    SELECT g.authorized_at INTO v_oldest
                    FROM runwick.rate_grants AS g
                    WHERE g.application = p_application AND g.rate_scope_id = v_rate_ids[v_i]
                      AND g.authorized_at > v_t - v_windows[v_i] * interval '1 millisecond'
                    ORDER BY g.authorized_at
                    OFFSET v_counts[v_i] - v_limits[v_i] LIMIT 1;
                    IF FOUND AND v_oldest + v_windows[v_i] * interval '1 millisecond' > v_hint THEN
                        v_hint := v_oldest + v_windows[v_i] * interval '1 millisecond';
                    END IF;
                END IF;

                IF v_not_befores[v_i] IS NOT NULL AND v_not_befores[v_i] > v_hint THEN
                    v_hint := v_not_befores[v_i];
                END IF;
            END LOOP;

            -- Logical hint to storage wall time: the same offset, so a backward clock defers longer, never shorter.
            v_logical := GREATEST(v_hint, v_t + p_min_defer);
            v_eligible := p_now + (v_logical - v_t) + p_defer_jitter_ms * interval '1 millisecond';
            IF runwick.ms_floor(v_eligible) <> v_eligible THEN
                v_eligible := runwick.ms_floor(v_eligible) + interval '1 millisecond';
            END IF;

            v_new_state := CASE WHEN v_started = 0 THEN 'pending' ELSE 'retrying' END;
            v_new_revision := runwick.rw_close_v1(p_application, p_command_id, p_now, p_job_id, p_attempt_id, p_token, v_generation,
                v_revision, v_job_name, v_active_key, 'admission_deferred', v_new_state, NULL, 'attempt_admission_deferred',
                NULL, GREATEST(1, runwick.rw_ms_v1(v_eligible) - runwick.rw_ms_v1(p_now)), v_eligible, NULL,
                pg_catalog.jsonb_build_object('blockingScopes', pg_catalog.to_jsonb(v_blocked), 'rateTime', runwick.rw_ms_v1(v_t)));
            PERFORM runwick.rw_fault('AfterMutation', 2, p_faults); -- rw:fault
            v_result := pg_catalog.jsonb_build_object('jobId', p_job_id, 'generation', v_generation, 'revision', v_new_revision,
                'state', v_new_state, 'deferredUntil', runwick.rw_ms_v1(v_eligible), 'blockingScopes', pg_catalog.to_jsonb(v_blocked));
            RETURN runwick.rw_applied_v1('RateLimited', 'rate_limited', p_now, v_result, (runwick.rw_ms_v1(v_eligible))::text);
        END IF;
    END IF;

    -- The effects: the job is locked, so the execution number, total and new revision are known and checked.
    v_execution_number := v_started + 1;
    v_total := v_executions + 1;
    UPDATE runwick.job_generations
    SET started_count = started_count + 1, consecutive_unstarted_count = 0
    WHERE application = p_application AND job_id = p_job_id AND generation = p_generation AND started_count < max_executions
    RETURNING started_count INTO v_n;
    IF v_n IS DISTINCT FROM v_execution_number THEN
        RETURN runwick.rw_no_effect_v1('InvariantFault', 'invalid_transition', p_now);
    END IF;

    UPDATE runwick.jobs
    SET state = 'running', execution_count = execution_count + 1, revision = revision + 1, updated_at = p_now
    WHERE application = p_application AND job_id = p_job_id AND state = 'reserved'
      AND current_attempt_id = p_attempt_id AND ownership_token = p_token
    RETURNING execution_count, revision INTO v_executions, v_new_revision;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n <> 1 OR v_executions <> v_total OR v_new_revision <> v_revision + 1 THEN
        RETURN runwick.rw_no_effect_v1('InvariantFault', 'invalid_transition', p_now);
    END IF;

    UPDATE runwick.job_attempts
    SET execution_number = v_execution_number, authorized_at = p_now
    WHERE application = p_application AND attempt_id = p_attempt_id AND finished_at IS NULL AND execution_number IS NULL;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n <> 1 THEN
        RETURN runwick.rw_no_effect_v1('InvariantFault', 'invalid_transition', p_now);
    END IF;

    INSERT INTO runwick.queue_events (application, job_id, attempt_id, command_id, event_kind, occurred_at, details)
    VALUES (p_application, p_job_id, p_attempt_id, p_command_id, 'attempt_started', p_now,
            pg_catalog.jsonb_build_object('generation', p_generation, 'executionNumber', v_execution_number));

    RETURN runwick.rw_applied_v1('Applied', NULL, p_now, pg_catalog.jsonb_build_object('jobId', p_job_id, 'generation', p_generation,
        'revision', v_new_revision, 'state', 'running', 'attemptId', p_attempt_id, 'workerInstanceId', p_worker_id,
        'ownershipToken', p_token, 'leaseExpiresAt', runwick.rw_ms_v1(v_lease), 'executionNumber', v_execution_number,
        'totalExecutionNumber', v_total, 'authorizedAt', runwick.rw_ms_v1(p_now)));
END
$fn$;

-- ------------------------------------------------------------------ Reserve/1 (04 §7.3), through rw_reserve_job_v1

CREATE FUNCTION runwick.reserve_v2(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
    p_fingerprint bytea, p_class text, p_deadline_cap interval, p_store_id uuid, p_revisions bigint[], p_epoch integer,
    p_capabilities jsonb, p_is_creation boolean, p_allow_drain boolean, p_staged boolean,
    p_queue text, p_priority text, p_after_eligible_at timestamptz, p_after_job_id uuid,
    p_names text[], p_versions integer[], p_profile_ids text[], p_profile_versions integer[],
    p_lease interval, p_worker_id uuid, p_token uuid,
    p_faults jsonb DEFAULT NULL)
RETURNS runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_reply runwick.rw_reply_v1;
    v_paused boolean;
    v_job_id uuid;
    v_payload bytea;
    v_headers jsonb;
    v_created timestamptz;
    v_clock timestamptz;
    v_now timestamptz;
BEGIN
    v_reply := runwick.rw_frame_v1(p_application, p_command_id, p_operation, p_not_after, p_fingerprint, p_class, p_deadline_cap,
        p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain, p_staged);
    IF v_reply.replayed THEN
        -- The reservation's payload row is immutable; the receipt names the job.
        SELECT p.payload_utf8, p.headers, j.created_at INTO v_payload, v_headers, v_created
        FROM runwick.job_payloads AS p JOIN runwick.jobs AS j USING (application, job_id)
        WHERE p.application = p_application AND p.job_id = (v_reply.result ->> 'jobId')::uuid;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'payload row missing for a waiting job' USING ERRCODE = 'RWI04';
        END IF;

        v_reply.payload := v_payload;
        v_reply.headers := v_headers;
        v_reply.created_at := v_created;
        RETURN v_reply;
    END IF;

    IF v_reply.status IS NOT NULL THEN
        RETURN v_reply;
    END IF;

    -- Q (04 §7.3): a paused or unregistered queue claims nothing.
    SELECT q.paused INTO v_paused FROM runwick.queue_controls AS q
    WHERE q.application = p_application AND q.queue = p_queue
    FOR SHARE;
    IF NOT FOUND THEN
        RETURN runwick.rw_no_effect_v1('NoWork', 'queue_not_registered', NULL);
    END IF;

    IF v_paused THEN
        RETURN runwick.rw_no_effect_v1('QueuePaused', 'queue_paused', NULL);
    END IF;

    -- C: the candidate claim.
    SELECT j.job_id
    INTO v_job_id
    FROM runwick.jobs AS j
    WHERE j.application = p_application
      AND j.queue = p_queue
      AND j.priority = p_priority
      AND j.state IN ('pending', 'retrying')
      AND j.eligible_at <= pg_catalog.statement_timestamp()
      AND (p_after_job_id IS NULL OR (j.eligible_at, j.job_id) > (p_after_eligible_at, p_after_job_id))
      AND EXISTS (
          SELECT 1
          FROM unnest(p_names, p_versions, p_profile_ids, p_profile_versions) AS c(name, version, profile_id, profile_version)
          WHERE c.name = j.job_name AND c.version = j.payload_version
            AND c.profile_id = j.serializer_profile_id
            AND c.profile_version = j.serializer_profile_version)
    ORDER BY j.eligible_at, j.job_id
    LIMIT 1
    FOR NO KEY UPDATE OF j SKIP LOCKED;
    IF NOT FOUND THEN
        RETURN runwick.rw_no_effect_v1('NoWork', 'no_work', NULL);
    END IF;

    SELECT p.payload_utf8, p.headers, j.created_at INTO v_payload, v_headers, v_created
    FROM runwick.job_payloads AS p JOIN runwick.jobs AS j USING (application, job_id)
    WHERE p.application = p_application AND p.job_id = v_job_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'payload row missing for a waiting job' USING ERRCODE = 'RWI04';
    END IF;

    -- S: the job's scopes in UUID order, the last blocking lock.
    PERFORM 1
    FROM runwick.concurrency_scopes AS s
    WHERE s.application = p_application
      AND s.scope_id IN (SELECT r.scope_id FROM runwick.job_concurrency_requirements AS r
                         WHERE r.application = p_application AND r.job_id = v_job_id)
    ORDER BY s.scope_id
    FOR UPDATE OF s;

    PERFORM runwick.rw_fault('AfterLocksBeforeFreshTime', 0, p_faults); -- rw:fault
    v_clock := pg_catalog.clock_timestamp();
    v_now := runwick.ms_floor(v_clock);
    IF v_clock >= p_not_after THEN
        RETURN runwick.rw_no_effect_v1('CommandExpired', 'command_expired', v_now);
    END IF;

    v_reply := runwick.rw_reserve_job_v1(p_application, p_command_id, v_now, p_lease, p_worker_id, p_token, p_queue, p_priority,
        v_job_id, p_faults);
    IF v_reply.effect <> 'Applied' THEN
        RETURN v_reply;
    END IF;

    PERFORM runwick.rw_finalize_v1(p_application, p_class, p_command_id, 'Applied', v_reply.result, v_now);
    PERFORM runwick.rw_fault('AfterMutation', 1, p_faults); -- rw:fault
    v_reply.payload := v_payload;
    v_reply.headers := v_headers;
    v_reply.created_at := v_created;
    RETURN v_reply;
END
$fn$;

-- ------------------------------------------------------------------ Begin/1 (04 §7.5, 10 §4), through rw_begin_job_v1

CREATE FUNCTION runwick.begin_v2(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
    p_fingerprint bytea, p_class text, p_deadline_cap interval, p_store_id uuid, p_revisions bigint[], p_epoch integer,
    p_capabilities jsonb, p_is_creation boolean, p_allow_drain boolean, p_staged boolean,
    p_job_id uuid, p_attempt_id uuid, p_worker_id uuid, p_token uuid, p_generation integer,
    p_min_defer interval, p_defer_jitter_ms integer,
    p_faults jsonb DEFAULT NULL)
RETURNS runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_reply runwick.rw_reply_v1;
    v_found boolean;
    v_cancel timestamptz;
    v_active_key text;
    v_max_executions integer;
    v_started integer;
    v_rated boolean;
    v_plan text;
    v_clock timestamptz;
    v_now timestamptz;
BEGIN
    v_reply := runwick.rw_frame_v1(p_application, p_command_id, p_operation, p_not_after, p_fingerprint, p_class, p_deadline_cap,
        p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain, p_staged);
    IF v_reply.replayed THEN
        -- A closed or deferred Begin replays its code with its receipt.
        IF v_reply.result ? 'deferredUntil' THEN
            v_reply.code := 'rate_limited';
            v_reply.detail := v_reply.result ->> 'deferredUntil';
        ELSIF v_reply.result ? 'attemptOutcome' THEN
            v_reply.code := v_reply.result ->> 'errorCode';
        END IF;

        RETURN v_reply;
    END IF;

    IF v_reply.status IS NOT NULL THEN
        RETURN v_reply;
    END IF;

    -- J
    SELECT j.cancel_requested_at, j.active_unique_key
    INTO v_cancel, v_active_key
    FROM runwick.jobs AS j
    WHERE j.application = p_application AND j.job_id = p_job_id
    FOR NO KEY UPDATE;
    v_found := FOUND;

    -- G behind J: a separate statement, so its snapshot is taken after the job lock.
    IF v_found THEN
        SELECT g.max_executions, g.started_count,
               EXISTS (SELECT 1 FROM runwick.job_rate_requirements AS rr WHERE rr.application = j.application AND rr.job_id = j.job_id)
        INTO v_max_executions, v_started, v_rated
        FROM runwick.jobs AS j
        JOIN runwick.job_generations AS g ON g.application = j.application AND g.job_id = j.job_id AND g.generation = j.generation
        WHERE j.application = p_application AND j.job_id = p_job_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'generation row missing' USING ERRCODE = 'RWI04';
        END IF;

        v_plan := CASE WHEN v_cancel IS NOT NULL THEN 'cancel' WHEN v_started >= v_max_executions THEN 'exhausted' ELSE 'authorize' END;
        v_rated := v_plan = 'authorize' AND v_rated;
    END IF;

    -- K (closing plans only), S, then R (rated plans only), in lock order (04 §5, 10 §4).
    IF v_found AND (v_plan <> 'authorize' OR v_rated) THEN
        IF v_plan IN ('cancel', 'exhausted') AND v_active_key IS NOT NULL THEN
            PERFORM 1
            FROM runwick.dispatch_keys AS k
            JOIN runwick.jobs AS j ON j.application = k.application AND j.job_name = k.job_name AND j.active_unique_key = k.key
            WHERE j.application = p_application AND j.job_id = p_job_id AND k.key_kind = 'active'
            FOR UPDATE OF k;
        END IF;

        PERFORM 1
        FROM runwick.concurrency_scopes AS s
        WHERE s.application = p_application
          AND s.scope_id IN (SELECT r.scope_id FROM runwick.job_concurrency_requirements AS r
                             WHERE r.application = p_application AND r.job_id = p_job_id)
        ORDER BY s.scope_id
        FOR UPDATE OF s;

        IF v_rated THEN
            PERFORM 1
            FROM runwick.rate_scopes AS s
            WHERE s.application = p_application
              AND s.rate_scope_id IN (SELECT r.rate_scope_id FROM runwick.job_rate_requirements AS r
                                      WHERE r.application = p_application AND r.job_id = p_job_id)
            ORDER BY s.rate_scope_id
            FOR UPDATE OF s;
        END IF;
    END IF;

    PERFORM runwick.rw_fault('AfterLocksBeforeFreshTime', 0, p_faults); -- rw:fault
    v_clock := pg_catalog.clock_timestamp();
    v_now := runwick.ms_floor(v_clock);
    IF v_clock >= p_not_after THEN
        RETURN runwick.rw_no_effect_v1('CommandExpired', 'command_expired', v_now);
    END IF;

    v_reply := runwick.rw_begin_job_v1(p_application, p_command_id, v_now, p_job_id, p_attempt_id, p_worker_id, p_token, p_generation,
        p_min_defer, p_defer_jitter_ms, p_faults);
    IF v_reply.effect <> 'Applied' THEN
        RETURN v_reply;
    END IF;

    PERFORM runwick.rw_finalize_v1(p_application, p_class, p_command_id, v_reply.status, v_reply.result, v_now);
    IF v_reply.status = 'Applied' THEN
        PERFORM runwick.rw_fault('AfterMutation', 1, p_faults); -- rw:fault
    END IF;

    RETURN v_reply;
END
$fn$;

-- ------------------------------------------------------------------ ReserveAndBegin/1 (04 §7.18)

-- Claims up to cardinality(p_tokens) (1-32) jobs of one lane for one worker and begins each one, in one transaction:
-- Q; C (the Reserve candidate claim with LIMIT n, FOR NO KEY UPDATE SKIP LOCKED, so it never waits); then the blocking
-- locks across all claimed jobs in rank order: active keys of jobs whose Begin closes (rank 6, bytewise), every required
-- concurrency scope (rank 7, UUID order), the rate scopes of rated jobs (UUID order); one fresh clock; then per job, in
-- candidate order, rw_reserve_job_v1 and rw_begin_job_v1. A job that is capacity-blocked, rate-deferred, cancelled or
-- exhausted gets its own outcome and never affects the others. One receipt for the command holds every outcome
-- (items, codec v1). The reply is one row for the command, then one payload row (status 'Payload', job ID in
-- conflicting_job_id) per begun job.
CREATE FUNCTION runwick.reserve_and_begin_v1(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
    p_fingerprint bytea, p_class text, p_deadline_cap interval, p_store_id uuid, p_revisions bigint[], p_epoch integer,
    p_capabilities jsonb, p_is_creation boolean, p_allow_drain boolean, p_staged boolean,
    p_queue text, p_priority text, p_after_eligible_at timestamptz, p_after_job_id uuid,
    p_names text[], p_versions integer[], p_profile_ids text[], p_profile_versions integer[],
    p_lease interval, p_worker_id uuid, p_tokens uuid[], p_min_defer interval, p_defer_jitters_ms integer[],
    p_faults jsonb DEFAULT NULL)
RETURNS SETOF runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_reply runwick.rw_reply_v1;
    v_job runwick.rw_reply_v1;
    v_paused boolean;
    v_max integer := pg_catalog.cardinality(p_tokens);
    v_ids uuid[];
    v_eligibles timestamptz[];
    v_closing uuid[];
    v_rated uuid[];
    v_clock timestamptz;
    v_now timestamptz;
    v_i integer;
    v_items jsonb := '[]'::jsonb;
    v_begun uuid[] := '{}';
    v_first_blocked uuid;
    v_last_cursor jsonb;
    v_last_eligible timestamptz;
    v_last_job uuid;
    v_attempt uuid;
    v_generation integer;
    v_result jsonb;
BEGIN
    IF v_max IS NULL OR v_max < 1 OR v_max > 32 OR pg_catalog.cardinality(p_defer_jitters_ms) IS DISTINCT FROM v_max THEN
        RAISE EXCEPTION 'rw:reserve_and_begin bounds' USING ERRCODE = 'RWI04';
    END IF;

    v_reply := runwick.rw_frame_v1(p_application, p_command_id, p_operation, p_not_after, p_fingerprint, p_class, p_deadline_cap,
        p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain, p_staged);
    IF v_reply.replayed THEN
        RETURN NEXT v_reply;
        RETURN QUERY
            SELECT 'Payload'::text, 'Applied'::text, NULL::text, NULL::text, p.job_id, NULL::uuid, NULL::timestamptz, NULL::uuid,
                   NULL::timestamptz, true, NULL::jsonb, p.payload_utf8, p.headers, j.created_at
            FROM runwick.job_payloads AS p JOIN runwick.jobs AS j USING (application, job_id)
            WHERE p.application = p_application
              AND p.job_id IN (SELECT (i ->> 'jobId')::uuid FROM pg_catalog.jsonb_array_elements(v_reply.result -> 'items') AS i
                               WHERE i ->> 'status' = 'Applied');
        RETURN;
    END IF;

    IF v_reply.status IS NOT NULL THEN
        RETURN NEXT v_reply;
        RETURN;
    END IF;

    -- Q (04 §7.3): a paused or unregistered queue claims nothing.
    SELECT q.paused INTO v_paused FROM runwick.queue_controls AS q
    WHERE q.application = p_application AND q.queue = p_queue
    FOR SHARE;
    IF NOT FOUND THEN
        RETURN NEXT runwick.rw_no_effect_v1('NoWork', 'queue_not_registered', NULL);
        RETURN;
    END IF;

    IF v_paused THEN
        RETURN NEXT runwick.rw_no_effect_v1('QueuePaused', 'queue_paused', NULL);
        RETURN;
    END IF;

    -- C: the candidate claim (Reserve's statement, up to v_max rows; skipped rows are other claimants').
    SELECT COALESCE(pg_catalog.array_agg(c.job_id ORDER BY c.eligible_at, c.job_id), '{}'),
           COALESCE(pg_catalog.array_agg(c.eligible_at ORDER BY c.eligible_at, c.job_id), '{}')
    INTO v_ids, v_eligibles
    FROM (SELECT j.job_id, j.eligible_at
          FROM runwick.jobs AS j
          WHERE j.application = p_application
            AND j.queue = p_queue
            AND j.priority = p_priority
            AND j.state IN ('pending', 'retrying')
            AND j.eligible_at <= pg_catalog.statement_timestamp()
            AND (p_after_job_id IS NULL OR (j.eligible_at, j.job_id) > (p_after_eligible_at, p_after_job_id))
            AND EXISTS (
                SELECT 1
                FROM unnest(p_names, p_versions, p_profile_ids, p_profile_versions) AS c(name, version, profile_id, profile_version)
                WHERE c.name = j.job_name AND c.version = j.payload_version
                  AND c.profile_id = j.serializer_profile_id
                  AND c.profile_version = j.serializer_profile_version)
          ORDER BY j.eligible_at, j.job_id
          LIMIT v_max
          FOR NO KEY UPDATE OF j SKIP LOCKED) AS c;
    IF pg_catalog.cardinality(v_ids) = 0 THEN
        RETURN NEXT runwick.rw_no_effect_v1('NoWork', 'no_work', NULL);
        RETURN;
    END IF;

    -- Begin plans, from the locked rows (a reservation changes neither the cancel request nor the budget).
    SELECT COALESCE(pg_catalog.array_agg(j.job_id) FILTER (WHERE j.cancel_requested_at IS NOT NULL OR g.started_count >= g.max_executions), '{}'),
           COALESCE(pg_catalog.array_agg(j.job_id) FILTER (WHERE j.cancel_requested_at IS NULL AND g.started_count < g.max_executions
               AND EXISTS (SELECT 1 FROM runwick.job_rate_requirements AS rr WHERE rr.application = j.application AND rr.job_id = j.job_id)), '{}')
    INTO v_closing, v_rated
    FROM runwick.jobs AS j
    JOIN runwick.job_generations AS g ON g.application = j.application AND g.job_id = j.job_id AND g.generation = j.generation
    WHERE j.application = p_application AND j.job_id = ANY (v_ids);

    -- K: active keys of the jobs whose Begin closes (rank 6, bytewise).
    IF pg_catalog.cardinality(v_closing) > 0 THEN
        PERFORM 1
        FROM runwick.dispatch_keys AS k
        JOIN runwick.jobs AS j ON j.application = k.application AND j.job_name = k.job_name AND j.active_unique_key = k.key
        WHERE j.application = p_application AND j.job_id = ANY (v_closing) AND k.key_kind = 'active'
        ORDER BY k.job_name COLLATE "C", k.key_kind COLLATE "C", k.key COLLATE "C"
        FOR UPDATE OF k;
    END IF;

    -- S: every scope any claimed job requires (rank 7, UUID order).
    PERFORM 1
    FROM runwick.concurrency_scopes AS s
    WHERE s.application = p_application
      AND s.scope_id IN (SELECT r.scope_id FROM runwick.job_concurrency_requirements AS r
                         WHERE r.application = p_application AND r.job_id = ANY (v_ids))
    ORDER BY s.scope_id
    FOR UPDATE OF s;

    -- R: every rate scope of the rated jobs (after all concurrency scopes, UUID order).
    IF pg_catalog.cardinality(v_rated) > 0 THEN
        PERFORM 1
        FROM runwick.rate_scopes AS s
        WHERE s.application = p_application
          AND s.rate_scope_id IN (SELECT r.rate_scope_id FROM runwick.job_rate_requirements AS r
                                  WHERE r.application = p_application AND r.job_id = ANY (v_rated))
        ORDER BY s.rate_scope_id
        FOR UPDATE OF s;
    END IF;

    PERFORM runwick.rw_fault('AfterLocksBeforeFreshTime', 0, p_faults); -- rw:fault
    v_clock := pg_catalog.clock_timestamp();
    v_now := runwick.ms_floor(v_clock);
    IF v_clock >= p_not_after THEN
        RETURN NEXT runwick.rw_no_effect_v1('CommandExpired', 'command_expired', v_now);
        RETURN;
    END IF;

    PERFORM runwick.rw_fault('BeforeFirstMutation', 0, p_faults); -- rw:fault

    -- Per job, in candidate order: Reserve, then Begin, each decided at v_now against rows this transaction holds.
    FOR v_i IN 1 .. pg_catalog.cardinality(v_ids) LOOP
        v_job := runwick.rw_reserve_job_v1(p_application, p_command_id, v_now, p_lease, p_worker_id, p_tokens[v_i], p_queue, p_priority,
            v_ids[v_i], NULL);
        IF v_job.status = 'CapacityBlocked' THEN
            v_first_blocked := COALESCE(v_first_blocked, v_job.blocked_scope_id);
            v_last_eligible := v_eligibles[v_i];
            v_last_job := v_ids[v_i];
            v_last_cursor := pg_catalog.jsonb_build_object('eligibleAt', runwick.rw_ms_v1(v_last_eligible), 'jobId', v_last_job);
            v_items := v_items || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('ordinal', v_i - 1, 'jobId', v_ids[v_i],
                'status', 'CapacityBlocked', 'code', 'capacity_blocked', 'blockedScopeId', v_job.blocked_scope_id, 'cursor', v_last_cursor));
            CONTINUE;
        END IF;

        IF v_job.effect <> 'Applied' THEN
            -- A broken invariant rolls the whole command back.
            RETURN NEXT v_job;
            RETURN;
        END IF;

        v_result := v_job.result;
        v_attempt := (v_result ->> 'attemptId')::uuid;
        v_generation := (v_result ->> 'generation')::integer;
        v_job := runwick.rw_begin_job_v1(p_application, p_command_id, v_now, v_ids[v_i], v_attempt, p_worker_id, p_tokens[v_i],
            v_generation, p_min_defer, p_defer_jitters_ms[v_i], NULL);
        IF v_job.effect <> 'Applied' THEN
            RETURN NEXT v_job;
            RETURN;
        END IF;

        IF v_job.status = 'Applied' THEN
            v_begun := v_begun || v_ids[v_i];
        END IF;

        v_items := v_items || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('ordinal', v_i - 1, 'jobId', v_ids[v_i],
            'status', v_job.status, 'code', v_job.code, 'reserve', v_result, 'begin', v_job.result));
        PERFORM runwick.rw_fault('AfterMutation', v_i, p_faults); -- rw:fault
    END LOOP;

    -- Nothing reserved: no effect, as a blocked Reserve (the first blocked scope; resume after the last candidate).
    IF v_first_blocked IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM pg_catalog.jsonb_array_elements(v_items) AS i WHERE i ->> 'status' <> 'CapacityBlocked') THEN
        RETURN NEXT runwick.rw_no_effect_v1('CapacityBlocked', 'capacity_blocked', v_now, NULL, NULL, v_first_blocked,
            v_last_eligible, v_last_job);
        RETURN;
    END IF;

    v_result := pg_catalog.jsonb_build_object('items', v_items);
    PERFORM runwick.rw_finalize_v1(p_application, p_class, p_command_id, 'Applied', v_result, v_now);
    RETURN NEXT runwick.rw_applied_v1('Applied', NULL, v_now, v_result);
    RETURN QUERY
        SELECT 'Payload'::text, 'Applied'::text, NULL::text, NULL::text, p.job_id, NULL::uuid, NULL::timestamptz, NULL::uuid,
               NULL::timestamptz, false, NULL::jsonb, p.payload_utf8, p.headers, j.created_at
        FROM runwick.job_payloads AS p JOIN runwick.jobs AS j USING (application, job_id)
        WHERE p.application = p_application AND p.job_id = ANY (v_begun);
    RETURN;
END
$fn$;
