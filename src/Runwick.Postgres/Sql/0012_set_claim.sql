-- 0012_set_claim (set-based claim, 04 Changes 2026-11-07): ReserveAndBegin/1 claims its common jobs with a few
-- statements for many jobs at once instead of about thirty statements per job. Nothing changes on the wire, in the
-- receipt, in the rows written or in the lock order; only how many statements the database runs.
--
-- A plain job (no concurrency scope, no rate scope, no cancel request, budget left; so no fence, no permit, no rate step
-- and no close) needs nothing from the jobs before it, so a run of consecutive plain jobs is reserved and begun by
-- rw_claim_plain_v1: one INSERT of the attempts, one UPDATE of the jobs, one UPDATE of the generations and one INSERT
-- of both events per job, in candidate order. Every other job still goes through rw_reserve_job_v1 and
-- rw_begin_job_v1, between the runs, so the candidate order of every effect (event IDs included) is the one of
-- reserve_and_begin_v1. A released function is never replaced in place (04 §7.17), so reserve_and_begin_v1 gives way to
-- reserve_and_begin_v2 and is dropped: stop-and-migrate, no binary of revision 11 runs against revision 12.
--
-- Rules and fault hooks: as 0009_functions (LANGUAGE plpgsql VOLATILE, no SET clauses, no EXCEPTION blocks; lines ending
-- in "-- rw:fault" exist only in the fault-hooks variant).
SET LOCAL lock_timeout = '5s';

DROP FUNCTION runwick.reserve_and_begin_v1(text, uuid, text, timestamptz, bytea, text, interval, uuid, bigint[], integer, jsonb,
    boolean, boolean, boolean, text, text, timestamptz, uuid, text[], integer[], text[], integer[], interval, uuid, uuid[], interval,
    integer[], jsonb);

-- ------------------------------------------------------------------ plain jobs, set-based

-- Reserves and begins a run of plain jobs (locked by the caller, decision time p_now) exactly as rw_reserve_job_v1 then
-- rw_begin_job_v1 would one by one: the attempt row ends as Begin leaves it, the job and generation rows as Begin leaves
-- them (revision + 2), and each job gets its attempt_reserved then attempt_started event, in p_ordinals order. Returns the
-- receipt items (codec v1, as reserve_and_begin_v1 builds them), or NULL when a row count is off (the caller reports
-- InvariantFault invalid_transition and the command rolls back, as the per-job checks do). R4 is the caller's.
CREATE FUNCTION runwick.rw_claim_plain_v1(p_application text, p_command_id uuid, p_now timestamptz, p_lease interval,
    p_worker_id uuid, p_queue text, p_priority text, p_ids uuid[], p_tokens uuid[], p_ordinals integer[])
RETURNS jsonb LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_expiry timestamptz := runwick.ms_floor(p_now + p_lease);
    v_n integer := pg_catalog.cardinality(p_ids);
    v_counts bigint[];
    v_items jsonb;
BEGIN
    WITH s AS MATERIALIZED (
        SELECT x.ord, x.job_id, x.token, runwick.rw_uuid_v7(p_now) AS attempt_id, j.generation, j.revision,
               j.reservation_count + 1 AS reservation_number, g.started_count + 1 AS execution_number,
               j.execution_count + 1 AS total, j.eligible_at, j.job_name, j.payload_version, j.serializer_profile_id,
               j.serializer_profile_version, g.max_executions, g.max_unstarted_abandonments, g.execution_timeout_ms,
               g.retry_policy, g.unstarted_retry_policy
        FROM unnest(p_ids, p_tokens, p_ordinals) AS x(job_id, token, ord)
        JOIN runwick.jobs AS j ON j.application = p_application AND j.job_id = x.job_id
        JOIN runwick.job_generations AS g ON g.application = j.application AND g.job_id = j.job_id AND g.generation = j.generation
        WHERE j.state IN ('pending', 'retrying') AND j.cancel_requested_at IS NULL AND g.started_count < g.max_executions),
    ins_attempts AS (
        INSERT INTO runwick.job_attempts (application, attempt_id, job_id, generation, reservation_number, execution_number,
            worker_instance_id, ownership_token, reserved_at, authorized_at)
        SELECT p_application, s.attempt_id, s.job_id, s.generation, s.reservation_number, s.execution_number, p_worker_id,
               s.token, p_now, p_now
        FROM s
        ORDER BY s.ord
        RETURNING 1),
    upd_jobs AS (
        UPDATE runwick.jobs AS j
        SET state = 'running', reservation_count = j.reservation_count + 1, execution_count = j.execution_count + 1,
            revision = j.revision + 2, current_attempt_id = s.attempt_id, worker_instance_id = p_worker_id,
            ownership_token = s.token, lease_expires_at = v_expiry, updated_at = p_now
        FROM s
        WHERE j.application = p_application AND j.job_id = s.job_id AND j.state IN ('pending', 'retrying')
          AND j.revision = s.revision
        RETURNING 1),
    upd_generations AS (
        UPDATE runwick.job_generations AS g
        SET started_count = g.started_count + 1, consecutive_unstarted_count = 0
        FROM s
        WHERE g.application = p_application AND g.job_id = s.job_id AND g.generation = s.generation
          AND g.started_count = s.execution_number - 1 AND g.started_count < g.max_executions
        RETURNING 1),
    ins_events AS (
        INSERT INTO runwick.queue_events (application, job_id, attempt_id, command_id, event_kind, occurred_at, details)
        SELECT p_application, s.job_id, s.attempt_id, p_command_id, k.kind, p_now,
               CASE k.step WHEN 1 THEN pg_catalog.jsonb_build_object('generation', s.generation, 'reservationNumber', s.reservation_number)
                           ELSE pg_catalog.jsonb_build_object('generation', s.generation, 'executionNumber', s.execution_number) END
        FROM s CROSS JOIN (VALUES (1, 'attempt_reserved'), (2, 'attempt_started')) AS k(step, kind)
        ORDER BY s.ord, k.step
        RETURNING 1)
    SELECT ARRAY[(SELECT count(*) FROM s), (SELECT count(*) FROM ins_attempts), (SELECT count(*) FROM upd_jobs),
                 (SELECT count(*) FROM upd_generations), (SELECT count(*) FROM ins_events)],
           (SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('ordinal', s.ord - 1, 'jobId', s.job_id,
                'status', 'Applied', 'code', NULL::text,
                -- The Reserve receipt result (rw_reserve_job_v1, codec v1): no scopes, so no fences.
                'reserve', pg_catalog.jsonb_build_object(
                    'jobId', s.job_id, 'generation', s.generation, 'revision', s.revision + 1, 'state', 'reserved',
                    'attemptId', s.attempt_id, 'workerInstanceId', p_worker_id, 'ownershipToken', s.token,
                    'leaseExpiresAt', runwick.rw_ms_v1(v_expiry), 'reservationNumber', s.reservation_number,
                    'contract', pg_catalog.jsonb_build_object('name', s.job_name, 'payloadVersion', s.payload_version,
                        'serializerProfileId', s.serializer_profile_id, 'serializerProfileVersion', s.serializer_profile_version),
                    'queue', p_queue, 'priority', p_priority,
                    'cursor', pg_catalog.jsonb_build_object('eligibleAt', runwick.rw_ms_v1(s.eligible_at), 'jobId', s.job_id),
                    'policy', pg_catalog.jsonb_build_object('executionTimeout', s.execution_timeout_ms, 'fingerprintVersion', 1,
                        'maxConsecutiveUnstartedAbandonments', s.max_unstarted_abandonments, 'maxExecutions', s.max_executions,
                        'retry', s.retry_policy, 'unstartedRetry', s.unstarted_retry_policy),
                    'fences', '[]'::jsonb),
                -- The Begin receipt result (rw_begin_job_v1, authorize).
                'begin', pg_catalog.jsonb_build_object('jobId', s.job_id, 'generation', s.generation,
                    'revision', s.revision + 2, 'state', 'running', 'attemptId', s.attempt_id, 'workerInstanceId', p_worker_id,
                    'ownershipToken', s.token, 'leaseExpiresAt', runwick.rw_ms_v1(v_expiry), 'executionNumber', s.execution_number,
                    'totalExecutionNumber', s.total, 'authorizedAt', runwick.rw_ms_v1(p_now))) ORDER BY s.ord)
            FROM s)
    INTO v_counts, v_items;

    IF v_counts <> ARRAY[v_n, v_n, v_n, v_n, 2 * v_n]::bigint[] THEN
        RETURN NULL;
    END IF;

    RETURN v_items;
END
$fn$;

-- ------------------------------------------------------------------ ReserveAndBegin/1 (04 §7.18)

-- Claims up to cardinality(p_tokens) (1-32) jobs of one lane for one worker and begins each one, in one transaction:
-- Q; C (the Reserve candidate claim with LIMIT n, FOR NO KEY UPDATE SKIP LOCKED, so it never waits); then the blocking
-- locks across all claimed jobs in rank order: active keys of jobs whose Begin closes (rank 6, bytewise), every required
-- concurrency scope (rank 7, UUID order), the rate scopes of rated jobs (UUID order); one fresh clock; then, in candidate
-- order, each run of consecutive plain jobs through rw_claim_plain_v1 and every other job through rw_reserve_job_v1 and
-- rw_begin_job_v1. A job that is capacity-blocked, rate-deferred, cancelled or exhausted gets its own outcome and never
-- affects the others. One receipt for the command holds every outcome (items, codec v1). The reply is one row for the
-- command, then one payload row (status 'Payload', job ID in conflicting_job_id) per begun job.
CREATE FUNCTION runwick.reserve_and_begin_v2(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
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
    v_scoped uuid[];
    v_set boolean;
    v_run_ids uuid[] := '{}';
    v_run_tokens uuid[] := '{}';
    v_run_ords integer[] := '{}';
    v_run_items jsonb;
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

    -- Begin plans, from the locked rows (a reservation changes neither the cancel request nor the budget), and the jobs
    -- that require a concurrency scope (Reserve's capacity check and fences).
    SELECT COALESCE(pg_catalog.array_agg(j.job_id) FILTER (WHERE j.cancel_requested_at IS NOT NULL OR g.started_count >= g.max_executions), '{}'),
           COALESCE(pg_catalog.array_agg(j.job_id) FILTER (WHERE j.cancel_requested_at IS NULL AND g.started_count < g.max_executions
               AND EXISTS (SELECT 1 FROM runwick.job_rate_requirements AS rr WHERE rr.application = j.application AND rr.job_id = j.job_id)), '{}'),
           COALESCE(pg_catalog.array_agg(j.job_id) FILTER (WHERE EXISTS (
               SELECT 1 FROM runwick.job_concurrency_requirements AS cr WHERE cr.application = j.application AND cr.job_id = j.job_id)), '{}')
    INTO v_closing, v_rated, v_scoped
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

    -- The set path needs a lease that outlives the decision (rw_begin_job_v1 refuses an expired one); the request bounds
    -- make that always so. A test session can turn the set path off to compare it with the per-job path.
    v_set := runwick.ms_floor(v_now + p_lease) > v_now;
    v_set := v_set AND pg_catalog.current_setting('runwick.claim_per_job', true) IS DISTINCT FROM 'on'; -- rw:fault

    -- In candidate order: a plain job joins the current run; any other job (and the end) first claims the run in one
    -- go, then goes through Reserve and Begin on its own, each decided at v_now against rows this transaction holds.
    FOR v_i IN 1 .. pg_catalog.cardinality(v_ids) + 1 LOOP
        IF v_i <= pg_catalog.cardinality(v_ids) AND v_set
           AND NOT (v_ids[v_i] = ANY (v_closing) OR v_ids[v_i] = ANY (v_rated) OR v_ids[v_i] = ANY (v_scoped)) THEN
            v_run_ids := v_run_ids || v_ids[v_i];
            v_run_tokens := v_run_tokens || p_tokens[v_i];
            v_run_ords := v_run_ords || v_i;
            CONTINUE;
        END IF;

        IF pg_catalog.cardinality(v_run_ids) > 0 THEN
            v_run_items := runwick.rw_claim_plain_v1(p_application, p_command_id, v_now, p_lease, p_worker_id, p_queue, p_priority,
                v_run_ids, v_run_tokens, v_run_ords);
            IF v_run_items IS NULL THEN
                -- A broken invariant rolls the whole command back.
                RETURN NEXT runwick.rw_no_effect_v1('InvariantFault', 'invalid_transition', v_now);
                RETURN;
            END IF;

            v_items := v_items || v_run_items;
            v_begun := v_begun || v_run_ids;
            PERFORM runwick.rw_fault('AfterMutation', o.k, p_faults) FROM unnest(v_run_ords) AS o(k); -- rw:fault
            v_run_ids := '{}';
            v_run_tokens := '{}';
            v_run_ords := '{}';
        END IF;

        EXIT WHEN v_i > pg_catalog.cardinality(v_ids);

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
