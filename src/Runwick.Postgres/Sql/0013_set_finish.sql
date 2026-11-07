-- 0013_set_finish (set-based finish, 04 Changes 2026-11-07): FinishMany/1 settles its common items with a few statements
-- for many jobs at once instead of about ten statements per job. Nothing changes on the wire, in the receipts, in the rows
-- written or in the lock order; only how many statements the database runs.
--
-- A plain item (a new command, not expired, outcome succeeded, owned at the decision time, no cancel request, no
-- concurrency scope) needs nothing from the items before it, so a run of consecutive plain items is closed by
-- rw_finish_plain_v1: one UPDATE of the attempts, one DELETE of permits, one UPDATE of the jobs, one UPDATE of the active
-- keys, one INSERT of the attempt_finished events and one UPDATE of the receipts (R4), in request order. Every other item
-- still goes through rw_finish_job_v1 and its own R4 between the runs, so the request order of every effect (event IDs
-- included) is the one of finish_many_v1. A released function is never replaced in place (04 §7.17), so finish_many_v1
-- gives way to finish_many_v2 and is dropped: stop-and-migrate, no binary of revision 12 runs against revision 13.
--
-- Rules and fault hooks: as 0009_functions (LANGUAGE plpgsql VOLATILE, no SET clauses, no EXCEPTION blocks; lines ending
-- in "-- rw:fault" exist only in the fault-hooks variant).
SET LOCAL lock_timeout = '5s';

DROP FUNCTION runwick.finish_many_v1(text, uuid, text, timestamptz, bytea, text, interval, uuid, bigint[], integer, jsonb, boolean,
    boolean, boolean, uuid[], timestamptz[], bytea[], uuid[], uuid[], uuid[], uuid[], integer[], text[], bigint[], bigint[], jsonb[],
    bigint[], jsonb);

-- ------------------------------------------------------------------ plain items, set-based

-- Closes a run of plain succeeded items (jobs locked by the caller, owned at p_now, decision time p_now) exactly as
-- rw_finish_job_v1 (rw_close_v1) then rw_finalize_v1 would one by one: A, P, J' (revision + 1), K', E, then R4 on each
-- item's own receipt, each table in p_ordinals order. The item arrays are the whole request's; p_ordinals picks the run's
-- items (1-based). Returns the items (as finish_many_v1 builds them), in p_ordinals order. A row count that is off
-- raises RWI01, as rw_close_v1 does.
CREATE FUNCTION runwick.rw_finish_plain_v1(p_application text, p_class text, p_now timestamptz, p_command_ids uuid[],
    p_job_ids uuid[], p_attempt_ids uuid[], p_tokens uuid[], p_elapsed_ms bigint[], p_errors jsonb[], p_ordinals integer[])
RETURNS jsonb LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_n integer := pg_catalog.cardinality(p_ordinals);
    v_counts bigint[];
    v_items jsonb;
BEGIN
    WITH s AS MATERIALIZED (
        SELECT x.ord, x.command_id, x.job_id, x.attempt_id, x.token, x.elapsed_ms, x.error, j.generation, j.revision,
               j.job_name, j.active_unique_key,
               pg_catalog.jsonb_build_object('jobId', x.job_id, 'generation', j.generation, 'revision', j.revision + 1,
                   'state', 'succeeded', 'attemptId', x.attempt_id, 'attemptOutcome', 'succeeded', 'disposition', 'succeeded',
                   'terminalReason', 'succeeded', 'eligibleAt', NULL::bigint, 'retryDelay', NULL::bigint) AS result
        FROM (SELECT o.ord, p_command_ids[o.ord] AS command_id, p_job_ids[o.ord] AS job_id, p_attempt_ids[o.ord] AS attempt_id,
                     p_tokens[o.ord] AS token, p_elapsed_ms[o.ord] AS elapsed_ms, p_errors[o.ord] AS error
              FROM unnest(p_ordinals) AS o(ord)) AS x
        JOIN runwick.jobs AS j ON j.application = p_application AND j.job_id = x.job_id),
    upd_attempts AS (
        UPDATE runwick.job_attempts AS a
        SET finished_at = p_now, outcome = 'succeeded', disposition = 'succeeded', applied_reason = 'succeeded',
            execution_elapsed_ms = s.elapsed_ms, retry_delay_ms = NULL, next_eligible_at = NULL, error = s.error,
            closed_by_command_id = s.command_id
        FROM s
        WHERE a.application = p_application AND a.attempt_id = s.attempt_id AND a.job_id = s.job_id AND a.finished_at IS NULL
        RETURNING 1),
    del_permits AS (
        DELETE FROM runwick.concurrency_permits AS p
        USING s
        WHERE p.application = p_application AND p.job_id = s.job_id AND p.attempt_id = s.attempt_id AND p.ownership_token = s.token
        RETURNING 1),
    upd_jobs AS (
        UPDATE runwick.jobs AS j
        SET state = 'succeeded', current_attempt_id = NULL, worker_instance_id = NULL, ownership_token = NULL,
            lease_expires_at = NULL, terminal_at = p_now, terminal_reason = 'succeeded', revision = j.revision + 1,
            updated_at = p_now
        FROM s
        WHERE j.application = p_application AND j.job_id = s.job_id AND j.current_attempt_id = s.attempt_id
          AND j.ownership_token = s.token AND j.revision = s.revision
        RETURNING 1),
    upd_keys AS (
        UPDATE runwick.dispatch_keys AS k
        SET bound_job_id = NULL, updated_at = p_now
        FROM s
        WHERE s.active_unique_key IS NOT NULL AND k.application = p_application AND k.job_name = s.job_name
          AND k.key_kind = 'active' AND k.key = s.active_unique_key AND k.bound_job_id = s.job_id
        RETURNING 1),
    ins_events AS (
        INSERT INTO runwick.queue_events (application, job_id, attempt_id, command_id, event_kind, occurred_at, details)
        SELECT p_application, s.job_id, s.attempt_id, s.command_id, 'attempt_finished', p_now,
               pg_catalog.jsonb_build_object('generation', s.generation, 'outcome', 'succeeded', 'disposition', 'succeeded',
                   'terminalReason', 'succeeded')
        FROM s
        ORDER BY s.ord
        RETURNING 1),
    upd_receipts AS (
        UPDATE runwick.storage_commands AS c
        SET status = 'Applied', result = s.result, decided_at = p_now
        FROM s
        WHERE c.application = p_application AND c.retention_class = p_class AND c.command_id = s.command_id AND c.result IS NULL
        RETURNING 1)
    SELECT ARRAY[(SELECT count(*) FROM s), (SELECT count(*) FROM upd_attempts), (SELECT count(*) FROM upd_jobs),
                 (SELECT count(*) FROM ins_events), (SELECT count(*) FROM upd_receipts)],
           (SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('commandId', s.command_id, 'status', 'Applied',
                'effect', 'Applied', 'replayed', false, 'code', NULL::text, 'decidedAt', runwick.rw_ms_v1(p_now),
                'result', s.result) ORDER BY s.ord)
            FROM s)
    INTO v_counts, v_items;

    IF v_counts <> ARRAY[v_n, v_n, v_n, v_n, v_n]::bigint[] THEN
        RAISE EXCEPTION 'rw:invalid_transition' USING ERRCODE = 'RWI01';
    END IF;

    RETURN v_items;
END
$fn$;

-- ------------------------------------------------------------------ FinishMany/1 (04 §7.19)

-- As finish_many_v1 (0011_finish_many), up to the fresh clock and BeforeFirstMutation. Then, in request order: a plain
-- item joins the current run; any other item (and the end) first closes the run in one go (rw_finish_plain_v1, then
-- AfterMutation for each of its items), then is decided on its own through rw_finish_job_v1 and R4 as before.
CREATE FUNCTION runwick.finish_many_v2(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
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
    v_plain integer[] := '{}';
    v_set boolean;
    v_run integer[] := '{}';
    v_run_items jsonb;
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

    -- The plain items, from the locked rows at v_now: a new command not expired, outcome succeeded, owned (the ownership
    -- check passes), no cancel request (the plan is 'succeeded' either way; kept on the per-job path), no concurrency scope.
    -- A test session can turn the set path off to compare it with the per-job path.
    v_set := pg_catalog.cardinality(v_pending) > 0;
    v_set := v_set AND pg_catalog.current_setting('runwick.finish_per_job', true) IS DISTINCT FROM 'on'; -- rw:fault
    IF v_set THEN
        SELECT COALESCE(pg_catalog.array_agg(i.k), '{}') INTO v_plain
        FROM pg_catalog.generate_subscripts(p_job_ids, 1) AS i(k)
        JOIN runwick.jobs AS j ON j.application = p_application AND j.job_id = p_job_ids[i.k]
        JOIN runwick.job_generations AS g ON g.application = j.application AND g.job_id = j.job_id AND g.generation = j.generation
        WHERE v_done[i.k] IS NULL AND v_clock < p_not_afters[i.k] AND p_outcomes[i.k] = 'succeeded'
          AND j.cancel_requested_at IS NULL
          AND NOT EXISTS (SELECT 1 FROM runwick.job_concurrency_requirements AS r
                          WHERE r.application = p_application AND r.job_id = j.job_id)
          AND (runwick.rw_ownership_v1(true, j.state, j.current_attempt_id, j.ownership_token, j.worker_instance_id, j.generation,
                   j.lease_expires_at, p_attempt_ids[i.k], p_tokens[i.k], p_worker_ids[i.k], p_generations[i.k], v_now,
                   ARRAY['running'])).status IS NULL;
    END IF;

    -- Per item, in request order, each decided at v_now as its own Finish/1; runs of plain items in one go.
    FOR v_k IN 1 .. v_n + 1 LOOP
        IF v_k <= v_n AND v_k = ANY (v_plain) THEN
            v_run := v_run || v_k;
            CONTINUE;
        END IF;

        IF pg_catalog.cardinality(v_run) > 0 THEN
            v_run_items := runwick.rw_finish_plain_v1(p_application, p_class, v_now, p_command_ids, p_job_ids, p_attempt_ids,
                p_tokens, p_elapsed_ms, p_errors, v_run);
            FOR v_i IN 1 .. pg_catalog.cardinality(v_run) LOOP
                v_done[v_run[v_i]] := v_run_items -> (v_i - 1);
            END LOOP;
            v_applied := v_applied + pg_catalog.cardinality(v_run);
            PERFORM runwick.rw_fault('AfterMutation', o.k, p_faults) FROM unnest(v_run) AS o(k); -- rw:fault
            v_run := '{}';
        END IF;

        EXIT WHEN v_k > v_n;
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
