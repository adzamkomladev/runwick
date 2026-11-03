-- 0011_finish_many (PF-06 batched finish, 03/04 Changes 2026-11-02): FinishMany/1, which settles up to 32 Finish/1
-- commands of one worker in one transaction, each with its own command ID, its own durable receipt and its own outcome
-- (04 §7.19). Additive for the protocol: the manifest gains FinishMany/1; Finish/1 is unchanged on the wire.
--
-- To write the Finish decision once, finish_v1's per-job body moves into shared functions that the single and the
-- batched operation both call: rw_finish_plan_v1 (the 03 §6.4 precedence table) and rw_finish_job_v1 (ownership check,
-- retry decision, A -> P -> J' -> K' -> E; 04 §7.7). A released function is never replaced in place (04 §7.17), so
-- finish_v1 gives way to finish_v2, which runs the same statements in the same order through those functions, and is
-- dropped: stop-and-migrate, no binary of revision 10 runs against revision 11.
--
-- Rules and fault hooks: as 0009_functions (LANGUAGE plpgsql VOLATILE, no SET clauses, no EXCEPTION blocks; lines ending
-- in "-- rw:fault" exist only in the fault-hooks variant).
SET LOCAL lock_timeout = '5s';

DROP FUNCTION runwick.finish_v1(text, uuid, text, timestamptz, bytea, text, interval, uuid, bigint[], integer, jsonb, boolean,
    boolean, boolean, uuid, uuid, uuid, uuid, integer, text, bigint, bigint, jsonb, bigint, jsonb);

-- ------------------------------------------------------------------ shared per-job bodies

-- The Finish precedence table (03 §6.4): the new state and terminal reason from the outcome, the cancel request and the
-- execution budget. p_found = false (no job row) plans 'failed', as finish_v1 did; the ownership check refuses it anyway.
CREATE FUNCTION runwick.rw_finish_plan_v1(p_found boolean, p_outcome text, p_cancel timestamptz, p_started integer, p_max integer,
    OUT o_state text, OUT o_reason text)
LANGUAGE sql IMMUTABLE AS $fn$
    SELECT CASE
        WHEN p_outcome = 'succeeded' THEN 'succeeded'
        WHEN p_outcome = 'user_cancelled' THEN 'cancelled'
        WHEN p_cancel IS NOT NULL THEN 'cancelled'
        WHEN p_outcome = 'permanent_failure' THEN 'failed'
        WHEN p_found AND p_started < p_max THEN 'retrying'
        ELSE 'failed' END,
        CASE
        WHEN p_outcome = 'succeeded' THEN 'succeeded'
        WHEN p_outcome = 'user_cancelled' THEN 'user_cancelled'
        WHEN p_cancel IS NOT NULL THEN 'cancellation_requested'
        WHEN p_outcome = 'permanent_failure' THEN 'permanent_failure'
        WHEN p_found AND p_started < p_max THEN NULL
        ELSE 'execution_budget_exhausted' END
$fn$;

-- One job's Finish decision (04 §7.7 from the ownership check on), after the caller locked the job (rank 5), its active
-- key when the plan is terminal (rank 6) and every scope it requires (rank 7), and read the decision time p_now after
-- its last lock. Returns the no-effect ownership refusal, or Applied with the Finish receipt result (codec v1); a broken
-- row count raises RWI01 (rw_close_v1). R4 is the caller's.
CREATE FUNCTION runwick.rw_finish_job_v1(p_application text, p_command_id uuid, p_now timestamptz, p_job_id uuid, p_attempt_id uuid,
    p_worker_id uuid, p_token uuid, p_generation integer, p_outcome text, p_elapsed_ms bigint, p_requested_delay_ms bigint,
    p_error jsonb, p_entropy bigint, p_faults jsonb)
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
    v_retry jsonb;
    v_new_state text;
    v_reason text;
    v_delay bigint;
    v_eligible timestamptz;
    v_new_revision bigint;
    v_result jsonb;
BEGIN
    -- J (locked by the caller), then G.
    SELECT j.state, j.generation, j.revision, j.current_attempt_id, j.worker_instance_id, j.ownership_token,
           j.lease_expires_at, j.cancel_requested_at, j.active_unique_key, j.job_name
    INTO v_state, v_generation, v_revision, v_attempt, v_worker, v_token, v_lease, v_cancel, v_active_key, v_job_name
    FROM runwick.jobs AS j
    WHERE j.application = p_application AND j.job_id = p_job_id;
    v_found := FOUND;

    IF v_found THEN
        SELECT g.max_executions, g.started_count, g.retry_policy INTO v_max_executions, v_started, v_retry
        FROM runwick.job_generations AS g
        WHERE g.application = p_application AND g.job_id = p_job_id AND g.generation = v_generation;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'generation row missing' USING ERRCODE = 'RWI04';
        END IF;
    END IF;

    SELECT p.o_state, p.o_reason INTO v_new_state, v_reason
    FROM runwick.rw_finish_plan_v1(v_found, p_outcome, v_cancel, v_started, v_max_executions) AS p;

    v_reply := runwick.rw_ownership_v1(v_found, v_state, v_attempt, v_token, v_worker, v_generation, v_lease,
        p_attempt_id, p_token, p_worker_id, p_generation, p_now, ARRAY['running']);
    IF v_reply.status IS NOT NULL THEN
        RETURN v_reply;
    END IF;

    IF v_new_state = 'retrying' THEN
        v_delay := CASE WHEN p_outcome = 'retryable_failure' AND p_requested_delay_ms IS NOT NULL
                        THEN LEAST(p_requested_delay_ms, (v_retry ->> 'maximumDelay')::bigint)
                        ELSE runwick.rw_retry_delay_v1(v_started, (v_retry ->> 'initialDelay')::bigint,
                                                       (v_retry ->> 'maximumDelay')::bigint, p_entropy) END;
        v_eligible := p_now + v_delay * interval '1 millisecond';
    END IF;

    PERFORM runwick.rw_fault('BeforeFirstMutation', 0, p_faults); -- rw:fault
    v_new_revision := runwick.rw_close_v1(p_application, p_command_id, p_now, p_job_id, p_attempt_id, p_token, v_generation,
        v_revision, v_job_name, v_active_key, p_outcome, v_new_state, v_reason, 'attempt_finished',
        p_elapsed_ms, v_delay, v_eligible, p_error, NULL);
    v_result := pg_catalog.jsonb_build_object('jobId', p_job_id, 'generation', v_generation, 'revision', v_new_revision,
        'state', v_new_state, 'attemptId', p_attempt_id, 'attemptOutcome', p_outcome, 'disposition', v_new_state,
        'terminalReason', v_reason, 'eligibleAt', runwick.rw_ms_v1(v_eligible), 'retryDelay', v_delay);
    RETURN runwick.rw_applied_v1('Applied', NULL, p_now, v_result);
END
$fn$;

-- Removes the receipt placeholder this transaction inserted for a command that ends without effect (FinishMany/1: an
-- item refused while the others commit). Equivalent to the rollback a single operation does (03 §8.1: no effect, no
-- receipt): the row was never visible to anyone else, and a concurrent R1 of the same ID that waited on it inserts its own
-- after this transaction ends. Only an undecided row inserted by this transaction is removed (xmin), never another's.
CREATE FUNCTION runwick.rw_discard_receipt_v1(p_application text, p_class text, p_command_id uuid)
RETURNS void LANGUAGE plpgsql VOLATILE AS $fn$
BEGIN
    DELETE FROM runwick.storage_commands AS c
    WHERE c.application = p_application AND c.retention_class = p_class AND c.command_id = p_command_id
      AND c.result IS NULL AND c.xmin = pg_catalog.xid(pg_catalog.pg_current_xact_id());
END
$fn$;

-- ------------------------------------------------------------------ Finish/1 (04 §7.7), through rw_finish_job_v1

CREATE FUNCTION runwick.finish_v2(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
    p_fingerprint bytea, p_class text, p_deadline_cap interval, p_store_id uuid, p_revisions bigint[], p_epoch integer,
    p_capabilities jsonb, p_is_creation boolean, p_allow_drain boolean, p_staged boolean,
    p_job_id uuid, p_attempt_id uuid, p_worker_id uuid, p_token uuid, p_generation integer,
    p_outcome text, p_elapsed_ms bigint, p_requested_delay_ms bigint, p_error jsonb, p_entropy bigint,
    p_faults jsonb DEFAULT NULL)
RETURNS runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_reply runwick.rw_reply_v1;
    v_found boolean;
    v_cancel timestamptz;
    v_active_key text;
    v_max_executions integer;
    v_started integer;
    v_new_state text;
    v_clock timestamptz;
    v_now timestamptz;
BEGIN
    v_reply := runwick.rw_frame_v1(p_application, p_command_id, p_operation, p_not_after, p_fingerprint, p_class, p_deadline_cap,
        p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain, p_staged);
    IF v_reply.status IS NOT NULL THEN
        RETURN v_reply;
    END IF;

    -- J, then G behind it (a separate statement: its snapshot follows the job lock).
    SELECT j.cancel_requested_at, j.active_unique_key
    INTO v_cancel, v_active_key
    FROM runwick.jobs AS j
    WHERE j.application = p_application AND j.job_id = p_job_id
    FOR NO KEY UPDATE;
    v_found := FOUND;

    IF v_found THEN
        SELECT g.max_executions, g.started_count INTO v_max_executions, v_started
        FROM runwick.jobs AS j
        JOIN runwick.job_generations AS g ON g.application = j.application AND g.job_id = j.job_id AND g.generation = j.generation
        WHERE j.application = p_application AND j.job_id = p_job_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'generation row missing' USING ERRCODE = 'RWI04';
        END IF;
    END IF;

    SELECT p.o_state INTO v_new_state
    FROM runwick.rw_finish_plan_v1(v_found, p_outcome, v_cancel, v_started, v_max_executions) AS p;

    -- K (a terminal close only, found through the locked row), then S (04 §5).
    IF v_found THEN
        IF v_new_state IN ('succeeded', 'failed', 'cancelled') AND v_active_key IS NOT NULL THEN
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
    END IF;

    PERFORM runwick.rw_fault('AfterLocksBeforeFreshTime', 0, p_faults); -- rw:fault
    v_clock := pg_catalog.clock_timestamp();
    v_now := runwick.ms_floor(v_clock);
    IF v_clock >= p_not_after THEN
        RETURN runwick.rw_no_effect_v1('CommandExpired', 'command_expired', v_now);
    END IF;

    v_reply := runwick.rw_finish_job_v1(p_application, p_command_id, v_now, p_job_id, p_attempt_id, p_worker_id, p_token, p_generation,
        p_outcome, p_elapsed_ms, p_requested_delay_ms, p_error, p_entropy, p_faults);
    IF v_reply.effect <> 'Applied' THEN
        RETURN v_reply;
    END IF;

    PERFORM runwick.rw_finalize_v1(p_application, p_class, p_command_id, 'Applied', v_reply.result, v_now);
    PERFORM runwick.rw_fault('AfterMutation', 1, p_faults); -- rw:fault
    RETURN v_reply;
END
$fn$;

-- ------------------------------------------------------------------ FinishMany/1 (04 §7.19)

-- Settles up to 32 Finish/1 commands (one array element per item, in request order) in one transaction. The call itself
-- has no receipt (p_fingerprint is NULL, like RenewMany); every item is exactly a Finish/1 with its own command ID,
-- NotAfter and fingerprint, and its own durable 'Finish' receipt, so a single Finish/1 resubmission of an item replays
-- what the batch committed for it, and the other way round. Lock order (04 §5): gates; each item's receipt (rank 3) in
-- command-ID order; every job still to decide (rank 5) in UUID order, one statement; the active keys of the jobs whose
-- plan is terminal (rank 6), bytewise; every scope those jobs require (rank 7) in UUID order; one fresh clock. Then per
-- item, in request order, rw_finish_job_v1 and R4. A replayed or refused item never affects another: a refused item's
-- placeholder is discarded (rw_discard_receipt_v1). A broken invariant raises and rolls the whole call back.
-- The reply is Applied with {"items": [...]} (one per item, request order); replayed = true when no item applied now,
-- so the client rolls back (nothing to commit).
CREATE FUNCTION runwick.finish_many_v1(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
    p_fingerprint bytea, p_class text, p_deadline_cap interval, p_store_id uuid, p_revisions bigint[], p_epoch integer,
    p_capabilities jsonb, p_is_creation boolean, p_allow_drain boolean, p_staged boolean,
    p_command_ids uuid[], p_not_afters timestamptz[], p_fingerprints bytea[],
    p_job_ids uuid[], p_attempt_ids uuid[], p_worker_ids uuid[], p_tokens uuid[], p_generations integer[],
    p_outcomes text[], p_elapsed_ms bigint[], p_requested_delays_ms bigint[], p_errors jsonb[], p_entropies bigint[],
    p_faults jsonb DEFAULT NULL)
RETURNS runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_n integer := pg_catalog.cardinality(p_command_ids);
    v_reply runwick.rw_reply_v1;
    v_item runwick.rw_reply_v1;
    v_done jsonb[];
    v_k integer;
    v_pending uuid[];
    v_closing uuid[];
    v_clock timestamptz;
    v_now timestamptz;
    v_applied integer := 0;
BEGIN
    IF v_n IS NULL OR v_n < 1 OR v_n > 32
       OR pg_catalog.cardinality(p_not_afters) IS DISTINCT FROM v_n OR pg_catalog.cardinality(p_fingerprints) IS DISTINCT FROM v_n
       OR pg_catalog.cardinality(p_job_ids) IS DISTINCT FROM v_n OR pg_catalog.cardinality(p_attempt_ids) IS DISTINCT FROM v_n
       OR pg_catalog.cardinality(p_worker_ids) IS DISTINCT FROM v_n OR pg_catalog.cardinality(p_tokens) IS DISTINCT FROM v_n
       OR pg_catalog.cardinality(p_generations) IS DISTINCT FROM v_n OR pg_catalog.cardinality(p_outcomes) IS DISTINCT FROM v_n
       OR pg_catalog.cardinality(p_elapsed_ms) IS DISTINCT FROM v_n OR pg_catalog.cardinality(p_requested_delays_ms) IS DISTINCT FROM v_n
       OR pg_catalog.cardinality(p_errors) IS DISTINCT FROM v_n OR pg_catalog.cardinality(p_entropies) IS DISTINCT FROM v_n
       OR (SELECT count(DISTINCT c) FROM unnest(p_command_ids) AS c) <> v_n
       OR (SELECT count(DISTINCT j) FROM unnest(p_job_ids) AS j) <> v_n THEN
        RAISE EXCEPTION 'rw:finish_many bounds' USING ERRCODE = 'RWI04';
    END IF;

    -- Gates (open/drain). The call has no receipt of its own: p_fingerprint is NULL, so the frame stops after them.
    v_reply := runwick.rw_frame_v1(p_application, p_command_id, p_operation, p_not_after, p_fingerprint, p_class, p_deadline_cap,
        p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain, p_staged);
    IF v_reply.status IS NOT NULL THEN
        RETURN v_reply;
    END IF;

    v_done := pg_catalog.array_fill(NULL::jsonb, ARRAY[v_n]);

    -- R1-R3 per item (rank 3), in command-ID order: a replay returns its recorded reply; a refusal (conflict, expired,
    -- deadline too far, incomplete receipt) is that item's outcome; NULL leaves the item to decide.
    FOR v_k IN SELECT o.k FROM unnest(p_command_ids) WITH ORDINALITY AS o(id, k) ORDER BY o.id LOOP
        v_item := runwick.rw_receipt_v1(p_application, p_class, p_command_ids[v_k], 'Finish', p_fingerprints[v_k],
            p_not_afters[v_k], p_deadline_cap);
        IF v_item.status IS NULL THEN
            CONTINUE;
        END IF;

        IF NOT v_item.replayed THEN
            PERFORM runwick.rw_discard_receipt_v1(p_application, p_class, p_command_ids[v_k]);
        END IF;

        v_done[v_k] := pg_catalog.jsonb_build_object('commandId', p_command_ids[v_k], 'status', v_item.status, 'effect', v_item.effect,
            'replayed', v_item.replayed, 'code', v_item.code, 'decidedAt', runwick.rw_ms_v1(v_item.decided_at), 'result', v_item.result);
    END LOOP;

    SELECT COALESCE(pg_catalog.array_agg(p_job_ids[i.k]), '{}') INTO v_pending
    FROM pg_catalog.generate_subscripts(p_job_ids, 1) AS i(k) WHERE v_done[i.k] IS NULL;

    IF pg_catalog.cardinality(v_pending) > 0 THEN
        -- J: every job still to decide, UUID order (rank 5).
        PERFORM 1 FROM runwick.jobs AS j
        WHERE j.application = p_application AND j.job_id = ANY (v_pending)
        ORDER BY j.job_id
        FOR NO KEY UPDATE;

        -- Plans from the locked rows (03 §6.4): the jobs whose close is terminal and that hold an active key.
        SELECT COALESCE(pg_catalog.array_agg(j.job_id), '{}') INTO v_closing
        FROM pg_catalog.generate_subscripts(p_job_ids, 1) AS i(k)
        JOIN runwick.jobs AS j ON j.application = p_application AND j.job_id = p_job_ids[i.k]
        JOIN runwick.job_generations AS g ON g.application = j.application AND g.job_id = j.job_id AND g.generation = j.generation
        CROSS JOIN LATERAL runwick.rw_finish_plan_v1(true, p_outcomes[i.k], j.cancel_requested_at, g.started_count, g.max_executions) AS p
        WHERE v_done[i.k] IS NULL AND j.active_unique_key IS NOT NULL AND p.o_state IN ('succeeded', 'failed', 'cancelled');

        -- K: their active keys (rank 6, bytewise).
        IF pg_catalog.cardinality(v_closing) > 0 THEN
            PERFORM 1
            FROM runwick.dispatch_keys AS k
            JOIN runwick.jobs AS j ON j.application = k.application AND j.job_name = k.job_name AND j.active_unique_key = k.key
            WHERE j.application = p_application AND j.job_id = ANY (v_closing) AND k.key_kind = 'active'
            ORDER BY k.job_name COLLATE "C", k.key_kind COLLATE "C", k.key COLLATE "C"
            FOR UPDATE OF k;
        END IF;

        -- S: every scope those jobs require (rank 7, UUID order).
        PERFORM 1
        FROM runwick.concurrency_scopes AS s
        WHERE s.application = p_application
          AND s.scope_id IN (SELECT r.scope_id FROM runwick.job_concurrency_requirements AS r
                             WHERE r.application = p_application AND r.job_id = ANY (v_pending))
        ORDER BY s.scope_id
        FOR UPDATE OF s;
    END IF;

    PERFORM runwick.rw_fault('AfterLocksBeforeFreshTime', 0, p_faults); -- rw:fault
    v_clock := pg_catalog.clock_timestamp();
    v_now := runwick.ms_floor(v_clock);
    IF v_clock >= p_not_after THEN
        RETURN runwick.rw_no_effect_v1('CommandExpired', 'command_expired', v_now);
    END IF;

    PERFORM runwick.rw_fault('BeforeFirstMutation', 0, p_faults); -- rw:fault

    -- Per item, in request order, each decided at v_now as its own Finish/1.
    FOR v_k IN 1 .. v_n LOOP
        IF v_done[v_k] IS NOT NULL THEN
            CONTINUE;
        END IF;

        IF v_clock >= p_not_afters[v_k] THEN
            v_item := runwick.rw_no_effect_v1('CommandExpired', 'command_expired', v_now);
        ELSE
            v_item := runwick.rw_finish_job_v1(p_application, p_command_ids[v_k], v_now, p_job_ids[v_k], p_attempt_ids[v_k],
                p_worker_ids[v_k], p_tokens[v_k], p_generations[v_k], p_outcomes[v_k], p_elapsed_ms[v_k], p_requested_delays_ms[v_k],
                p_errors[v_k], p_entropies[v_k], NULL);
        END IF;

        IF v_item.effect = 'Applied' THEN
            PERFORM runwick.rw_finalize_v1(p_application, p_class, p_command_ids[v_k], 'Applied', v_item.result, v_now);
            v_applied := v_applied + 1;
        ELSE
            PERFORM runwick.rw_discard_receipt_v1(p_application, p_class, p_command_ids[v_k]);
        END IF;

        v_done[v_k] := pg_catalog.jsonb_build_object('commandId', p_command_ids[v_k], 'status', v_item.status, 'effect', v_item.effect,
            'replayed', false, 'code', v_item.code, 'decidedAt', runwick.rw_ms_v1(v_item.decided_at), 'result', v_item.result);
        IF v_item.effect = 'Applied' THEN
            PERFORM runwick.rw_fault('AfterMutation', v_k, p_faults); -- rw:fault
        END IF;
    END LOOP;

    v_reply := runwick.rw_applied_v1('Applied', NULL, v_now, pg_catalog.jsonb_build_object('items', pg_catalog.to_jsonb(v_done)));
    v_reply.replayed := v_applied = 0;
    RETURN v_reply;
END
$fn$;
