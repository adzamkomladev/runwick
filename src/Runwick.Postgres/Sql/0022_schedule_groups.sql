-- Keep one active implementation of a materialization effect for both entry points.
SET LOCAL lock_timeout = '5s';

CREATE FUNCTION runwick.rw_materialize_core_v1(p_application text, p_command_id uuid, p_not_after timestamptz, p_class text,
    p_schedule_id uuid, p_expected_definition bigint, p_expected_control bigint, p_expected_cursor bigint, p_expected_episode uuid,
    p_cutoff timestamptz, p_max_plan_bytes bigint,
    p_occ_scheduled timestamptz[], p_occ_keys text[], p_occ_from timestamptz[], p_occ_through timestamptz[], p_occ_coalesced boolean[],
    p_after_through timestamptz, p_after_next timestamptz, p_after_scan timestamptz, p_after_episode uuid,
    p_after_episode_cutoff timestamptz, p_after_episode_emitted integer, p_after_episode_limit integer, p_review_reason text,
    p_clear boolean, p_audit_from timestamptz[], p_audit_through timestamptz[], p_audit_reasons text[],
    p_invalid text,
    p_job_ids uuid[], p_job_names text[], p_payload_versions integer[], p_profile_ids text[], p_profile_versions integer[],
    p_queues text[], p_priorities text[], p_not_before_ms bigint[], p_active_keys text[], p_window_keys text[],
    p_window_ms bigint[], p_business bytea[], p_policy bytea[], p_payloads bytea[], p_headers jsonb[],
    p_max_executions integer[], p_max_unstarted integer[], p_timeouts bigint[], p_retry jsonb[], p_unstarted jsonb[],
    p_scopes jsonb[], p_rates jsonb[],
    p_faults jsonb DEFAULT NULL)
RETURNS runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_reject runwick.rw_reply_v1;
    v_core record;
    v_count integer := COALESCE(pg_catalog.cardinality(p_occ_keys), 0);
    v_found boolean;
    v_key text;
    v_definition bigint;
    v_control bigint;
    v_cursor bigint;
    v_enabled boolean;
    v_paused boolean;
    v_review boolean;
    v_through timestamptz;
    v_episode uuid;
    v_payload_length bigint;
    v_steps integer := 0;
    v_clock timestamptz;
    v_now timestamptz;
    v_items jsonb := '[]'::jsonb;
    v_n integer;
    v_occurrences jsonb;
    v_result jsonb;
BEGIN
    IF NOT COALESCE((SELECT c.required_capabilities ? 'scheduler' FROM runwick.application_controls AS c
                     WHERE c.application = p_application), false) THEN
        -- No decision time: the client reports these refusals at its own clock, as the statement path did.
        RETURN runwick.rw_no_effect_v1('UnsupportedVersion', 'scheduler_not_active', NULL);
    END IF;

    -- The schedule row (rank: after the receipt, before queue controls; 13 §8) with its current definition.
    SELECT s.schedule_key, s.definition_revision, s.control_revision, s.cursor_revision, s.enabled, s.paused, s.needs_review,
           s.evaluated_through, s.episode_id, pg_catalog.length(d.payload_template)
    INTO v_key, v_definition, v_control, v_cursor, v_enabled, v_paused, v_review, v_through, v_episode, v_payload_length
    FROM runwick.schedules AS s
    JOIN runwick.schedule_definitions AS d
      ON d.application = s.application AND d.schedule_id = s.schedule_id AND d.revision = s.definition_revision
    WHERE s.application = p_application AND s.schedule_id = p_schedule_id
    FOR UPDATE OF s;
    v_found := FOUND;

    -- Plan checks precede the dispatch core, so a stale plan is always reported as stale (13 §4.2).
    v_reject := CASE
        WHEN NOT v_found THEN runwick.rw_no_effect_v1('NotFound', 'schedule_not_found', NULL)
        WHEN NOT v_enabled THEN runwick.rw_no_effect_v1('InvalidState', 'schedule_disabled', NULL)
        WHEN v_paused THEN runwick.rw_no_effect_v1('InvalidState', 'schedule_paused', NULL)
        WHEN v_review THEN runwick.rw_no_effect_v1('InvalidState', 'schedule_needs_review', NULL)
        WHEN v_definition <> p_expected_definition OR v_control <> p_expected_control OR v_cursor <> p_expected_cursor
             OR v_episode IS DISTINCT FROM p_expected_episode
            THEN runwick.rw_no_effect_v1('VersionConflict', 'revision_conflict', NULL,
                     pg_catalog.format('actual=%s/%s/%s', v_definition, v_control, v_cursor))
        WHEN v_count::bigint * v_payload_length > p_max_plan_bytes THEN runwick.rw_no_effect_v1('InvalidRequest', 'plan_too_large', NULL)
        WHEN v_through IS NOT NULL AND EXISTS (SELECT 1 FROM unnest(p_occ_scheduled) AS o(at) WHERE o.at <= v_through)
            THEN runwick.rw_no_effect_v1('InvariantFault', 'occurrence_below_high_water', NULL)
        WHEN v_through IS NOT NULL AND (p_after_through IS NULL OR p_after_through < v_through)
            THEN runwick.rw_no_effect_v1('InvariantFault', 'cursor_rewind', NULL)
    END;
    IF v_reject.status IS NOT NULL THEN
        RETURN v_reject;
    END IF;

    IF v_count > 0 AND COALESCE(pg_catalog.cardinality(p_job_ids), 0) <> v_count THEN
        RAISE EXCEPTION 'dispatch items missing for the plan' USING ERRCODE = 'RWI04';
    END IF;

    IF v_count = 0 THEN
        PERFORM runwick.rw_fault('AfterLocksBeforeFreshTime', 0, p_faults); -- rw:fault
        v_clock := pg_catalog.clock_timestamp();
        v_now := runwick.ms_floor(v_clock);
        IF v_clock >= p_not_after THEN
            RETURN runwick.rw_no_effect_v1('CommandExpired', 'command_expired', v_now);
        END IF;
    ELSE
        SELECT c.p_steps, c.p_now, c.o_reject, c.o_items INTO v_core
        FROM runwick.rw_dispatch_core_v3(p_application, p_command_id, p_not_after, false, 'scheduled', p_invalid,
            p_job_ids, p_job_names, p_payload_versions, p_profile_ids, p_profile_versions, p_queues, p_priorities, p_not_before_ms,
            p_active_keys, p_window_keys, p_window_ms, p_business, p_policy, p_payloads, p_headers, p_max_executions, p_max_unstarted,
            p_timeouts, p_retry, p_unstarted, p_scopes, p_rates, p_faults, v_steps, NULL) AS c;
        v_steps := v_core.p_steps;
        v_now := v_core.p_now;
        v_reject := v_core.o_reject;
        v_items := v_core.o_items;
        IF v_reject.status IS NOT NULL THEN
            RETURN v_reject;
        END IF;
    END IF;

    -- The clock went backward after planning: reject and replan (13 §4.2).
    IF v_now < p_cutoff THEN
        RETURN runwick.rw_no_effect_v1('InvalidState', 'cutoff_in_future', v_now);
    END IF;

    IF v_steps = 0 THEN
        PERFORM runwick.rw_fault('BeforeFirstMutation', 0, p_faults); -- rw:fault
    END IF;

    IF v_count > 0 THEN
        INSERT INTO runwick.schedule_occurrences (
            application, schedule_id, occurrence_kind, occurrence_key, scheduled_at, definition_revision, job_id, disposition,
            coalesced, covered_from, covered_through, materialized_at, command_id)
        SELECT p_application, p_schedule_id, 'scheduled', o.key, o.at, v_definition, (v_items -> (o.i - 1)::integer ->> 'jobId')::uuid,
               CASE WHEN v_items -> (o.i - 1)::integer ->> 'disposition' = 'Created' THEN 'created' ELSE 'linked_existing' END,
               o.coalesced, o.covered_from, o.covered_through, v_now, p_command_id
        FROM unnest(p_occ_keys, p_occ_scheduled, p_occ_coalesced, p_occ_from, p_occ_through) WITH ORDINALITY
             AS o(key, at, coalesced, covered_from, covered_through, i)
        ORDER BY o.key
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS v_n = ROW_COUNT;
        IF v_n <> v_count THEN
            RETURN runwick.rw_no_effect_v1('InvariantFault', 'occurrence_exists', v_now);
        END IF;
    END IF;

    UPDATE runwick.schedules
    SET evaluated_through = p_after_through, next_due_at = p_after_next, scan_after_at = p_after_scan,
        episode_id = p_after_episode, episode_cutoff = p_after_episode_cutoff, episode_emitted = p_after_episode_emitted,
        episode_limit = p_after_episode_limit, needs_review = p_review_reason IS NOT NULL, review_reason = p_review_reason,
        resume_policy = CASE WHEN p_clear THEN NULL ELSE resume_policy END,
        resume_cutoff = CASE WHEN p_clear THEN NULL ELSE resume_cutoff END,
        cursor_revision = cursor_revision + 1, updated_at = v_now
    WHERE application = p_application AND schedule_id = p_schedule_id;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n <> 1 THEN
        RAISE EXCEPTION 'rw:invalid_transition' USING ERRCODE = 'RWI01';
    END IF;

    IF COALESCE(pg_catalog.cardinality(p_audit_reasons), 0) > 0 THEN
        INSERT INTO runwick.schedule_audit_ranges (
            application, schedule_id, definition_revision, command_id, from_exclusive, through_inclusive, reason, created_at)
        SELECT p_application, p_schedule_id, v_definition, p_command_id, r.f, r.t, r.reason, v_now
        FROM unnest(p_audit_from, p_audit_through, p_audit_reasons) AS r(f, t, reason);
        GET DIAGNOSTICS v_n = ROW_COUNT;
        IF v_n <> pg_catalog.cardinality(p_audit_reasons) THEN
            RAISE EXCEPTION 'rw:invalid_transition' USING ERRCODE = 'RWI01';
        END IF;
    END IF;

    INSERT INTO runwick.queue_events (application, job_id, attempt_id, command_id, event_kind, occurred_at, actor_id, details)
    VALUES (p_application, NULL, NULL, p_command_id, 'schedule_materialized', v_now, 'svc:scheduler',
            pg_catalog.jsonb_build_object('cursorRevision', v_cursor + 1, 'cutoff', runwick.rw_ms_v1(p_cutoff), 'jobs', v_count,
                'skippedRanges', COALESCE(pg_catalog.cardinality(p_audit_reasons), 0), 'episodeId', p_after_episode,
                'needsReview', p_review_reason, 'scheduleId', p_schedule_id, 'scheduleKey', v_key));
    IF p_review_reason IS NOT NULL THEN
        INSERT INTO runwick.queue_events (application, job_id, attempt_id, command_id, event_kind, occurred_at, actor_id, details)
        VALUES (p_application, NULL, NULL, p_command_id, 'schedule_needs_review', v_now, 'svc:scheduler',
                pg_catalog.jsonb_build_object('reason', p_review_reason, 'scheduleId', p_schedule_id, 'scheduleKey', v_key));
    END IF;

    v_steps := v_steps + 1;
    PERFORM runwick.rw_fault('AfterMutation', v_steps, p_faults); -- rw:fault

    v_occurrences := COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('kind', 'scheduled', 'key', o.key,
            'jobId', v_items -> (o.i - 1)::integer ->> 'jobId',
            'disposition', CASE WHEN v_items -> (o.i - 1)::integer ->> 'disposition' = 'Created' THEN 'created' ELSE 'linked_existing' END)
            ORDER BY o.i)
        FROM unnest(p_occ_keys) WITH ORDINALITY AS o(key, i)), '[]'::jsonb);
    v_result := pg_catalog.jsonb_build_object('scheduleId', p_schedule_id, 'cursorRevision', v_cursor + 1, 'occurrences', v_occurrences);
    PERFORM runwick.rw_finalize_v1(p_application, p_class, p_command_id, 'Applied', v_result, v_now);
    RETURN runwick.rw_applied_v1('Applied', NULL, v_now, v_result);
END
$fn$;

CREATE FUNCTION runwick.materialize_occurrences_v4(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
    p_fingerprint bytea, p_class text, p_deadline_cap interval, p_store_id uuid, p_revisions bigint[], p_epoch integer,
    p_capabilities jsonb, p_is_creation boolean, p_allow_drain boolean, p_staged boolean,
    p_schedule_id uuid, p_expected_definition bigint, p_expected_control bigint, p_expected_cursor bigint, p_expected_episode uuid,
    p_cutoff timestamptz, p_max_plan_bytes bigint,
    p_occ_scheduled timestamptz[], p_occ_keys text[], p_occ_from timestamptz[], p_occ_through timestamptz[], p_occ_coalesced boolean[],
    p_after_through timestamptz, p_after_next timestamptz, p_after_scan timestamptz, p_after_episode uuid,
    p_after_episode_cutoff timestamptz, p_after_episode_emitted integer, p_after_episode_limit integer, p_review_reason text,
    p_clear boolean, p_audit_from timestamptz[], p_audit_through timestamptz[], p_audit_reasons text[],
    p_invalid text,
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
    v_reply := runwick.rw_frame_v1(p_application, p_command_id, p_operation, p_not_after, p_fingerprint, p_class, p_deadline_cap,
        p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain, p_staged);
    IF v_reply.status IS NOT NULL THEN
        RETURN v_reply;
    END IF;

    RETURN runwick.rw_materialize_core_v1(p_application, p_command_id, p_not_after, p_class, p_schedule_id, p_expected_definition, p_expected_control,
        p_expected_cursor, p_expected_episode, p_cutoff, p_max_plan_bytes, p_occ_scheduled, p_occ_keys, p_occ_from,
        p_occ_through, p_occ_coalesced, p_after_through, p_after_next, p_after_scan, p_after_episode,
        p_after_episode_cutoff, p_after_episode_emitted, p_after_episode_limit, p_review_reason, p_clear, p_audit_from,
        p_audit_through, p_audit_reasons, p_invalid, p_job_ids, p_job_names, p_payload_versions, p_profile_ids,
        p_profile_versions, p_queues, p_priorities, p_not_before_ms, p_active_keys, p_window_keys, p_window_ms,
        p_business, p_policy, p_payloads, p_headers, p_max_executions, p_max_unstarted, p_timeouts, p_retry, p_unstarted,
        p_scopes, p_rates, p_faults);
END
$fn$;

CREATE TYPE runwick.rw_schedule_group_plan_v1 AS (
    p_command_id uuid, p_not_after timestamptz, p_fingerprint bytea,
    p_schedule_id uuid, p_expected_definition bigint, p_expected_control bigint, p_expected_cursor bigint, p_expected_episode uuid,
    p_cutoff timestamptz, p_max_plan_bytes bigint,
    p_occ_scheduled timestamptz[], p_occ_keys text[], p_occ_from timestamptz[], p_occ_through timestamptz[], p_occ_coalesced boolean[],
    p_after_through timestamptz, p_after_next timestamptz, p_after_scan timestamptz, p_after_episode uuid,
    p_after_episode_cutoff timestamptz, p_after_episode_emitted integer, p_after_episode_limit integer, p_review_reason text,
    p_clear boolean, p_audit_from timestamptz[], p_audit_through timestamptz[], p_audit_reasons text[],
    p_invalid text,
    p_job_ids uuid[], p_job_names text[], p_payload_versions integer[], p_profile_ids text[], p_profile_versions integer[],
    p_queues text[], p_priorities text[], p_not_before_ms bigint[], p_active_keys text[], p_window_keys text[],
    p_window_ms bigint[], p_business bytea[], p_policy bytea[], p_payloads bytea[], p_headers jsonb[],
    p_max_executions integer[], p_max_unstarted integer[], p_timeouts bigint[], p_retry jsonb[], p_unstarted jsonb[]
);

CREATE FUNCTION runwick.materialize_schedule_group_v1(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
    p_fingerprint bytea, p_class text, p_deadline_cap interval, p_store_id uuid, p_revisions bigint[], p_epoch integer,
    p_capabilities jsonb, p_is_creation boolean, p_allow_drain boolean, p_staged boolean,
    p_plans jsonb, p_faults jsonb DEFAULT NULL)
RETURNS runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_gate runwick.rw_reply_v1;
    v_reply runwick.rw_reply_v1;
    v_plan runwick.rw_schedule_group_plan_v1;
    v_plans runwick.rw_schedule_group_plan_v1[];
    v_replies jsonb := '{}'::jsonb;
    v_pending uuid[] := '{}';
BEGIN
    IF p_staged OR p_plans IS NULL OR pg_catalog.jsonb_array_length(p_plans) NOT BETWEEN 2 AND 4 THEN
        RETURN runwick.rw_no_effect_v1('InvalidRequest', 'invalid_schedule_group', NULL);
    END IF;
    v_gate := runwick.rw_gate_v1(p_application, p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain);
    IF v_gate.status IS NOT NULL THEN
        RETURN v_gate;
    END IF;

    SELECT pg_catalog.array_agg(x ORDER BY x.p_command_id) INTO v_plans
    FROM pg_catalog.jsonb_populate_recordset(NULL::runwick.rw_schedule_group_plan_v1, p_plans) AS x;

    -- This path accepts plans without dispatch-key or shared scope locks. The client also rejects these shapes
    -- before constructing the group. Reject malformed groups before creating any receipt.
    IF EXISTS (SELECT 1 FROM unnest(v_plans) AS p
               WHERE pg_catalog.cardinality(p.p_occ_keys) IS DISTINCT FROM 1
                  OR pg_catalog.cardinality(p.p_job_ids) IS DISTINCT FROM 1
                  OR p.p_expected_episode IS NOT NULL OR p.p_after_episode IS NOT NULL OR p.p_clear IS DISTINCT FROM false
                  OR p.p_review_reason IS NOT NULL OR pg_catalog.cardinality(p.p_audit_reasons) IS DISTINCT FROM 0
                  OR p.p_occ_from[1] IS NOT NULL OR p.p_occ_through[1] IS NOT NULL OR p.p_occ_coalesced[1] IS DISTINCT FROM false
                  OR p.p_active_keys[1] IS NOT NULL OR p.p_window_keys[1] IS NOT NULL)
       OR (SELECT count(DISTINCT p.p_schedule_id) FROM unnest(v_plans) AS p) <> pg_catalog.cardinality(v_plans)
       OR (SELECT count(DISTINCT p.p_command_id) FROM unnest(v_plans) AS p) <> pg_catalog.cardinality(v_plans)
       OR (SELECT count(DISTINCT p.p_job_ids[1]) FROM unnest(v_plans) AS p) <> pg_catalog.cardinality(v_plans)
       OR (SELECT sum(pg_catalog.length(p.p_payloads[1])) FROM unnest(v_plans) AS p) > 2097152 THEN
        RETURN runwick.rw_no_effect_v1('InvalidRequest', 'invalid_schedule_group', NULL);
    END IF;

    FOREACH v_plan IN ARRAY v_plans LOOP
        BEGIN
            v_reply := runwick.rw_receipt_v1(p_application, p_class, v_plan.p_command_id, p_operation,
                v_plan.p_fingerprint, v_plan.p_not_after, p_deadline_cap);
            IF v_reply.status IS NULL THEN
                v_pending := pg_catalog.array_append(v_pending, v_plan.p_command_id);
            ELSIF v_reply.effect <> 'Applied' THEN
                RAISE SQLSTATE 'RWS32';
            ELSE
                v_replies := v_replies || pg_catalog.jsonb_build_object(v_plan.p_command_id::text, pg_catalog.to_jsonb(v_reply));
            END IF;
        EXCEPTION WHEN SQLSTATE 'RWS32' THEN
            -- Expiry or conflict can leave an undecided receipt in the subtransaction.
            v_replies := v_replies || pg_catalog.jsonb_build_object(v_plan.p_command_id::text, pg_catalog.to_jsonb(v_reply));
        END;
    END LOOP;

    PERFORM 1 FROM runwick.schedules AS s
    JOIN unnest(v_plans) AS p ON p.p_schedule_id = s.schedule_id
    WHERE s.application = p_application AND p.p_command_id = ANY (v_pending)
    ORDER BY s.schedule_id FOR UPDATE OF s;

    -- Take receipts in command order, schedule rows in schedule order, and unique job inserts in job order.
    -- Keep receipt locks across item savepoints so overlapping groups follow the same order.
    FOR v_plan IN
        SELECT p.* FROM unnest(v_plans) AS p
        WHERE p.p_command_id = ANY (v_pending)
        ORDER BY p.p_job_ids[1]
    LOOP
        BEGIN
            v_reply := runwick.rw_materialize_core_v1(p_application, v_plan.p_command_id, v_plan.p_not_after, p_class,
                v_plan.p_schedule_id,
                v_plan.p_expected_definition,
                v_plan.p_expected_control,
                v_plan.p_expected_cursor,
                v_plan.p_expected_episode,
                v_plan.p_cutoff,
                v_plan.p_max_plan_bytes,
                v_plan.p_occ_scheduled,
                v_plan.p_occ_keys,
                v_plan.p_occ_from,
                v_plan.p_occ_through,
                v_plan.p_occ_coalesced,
                v_plan.p_after_through,
                v_plan.p_after_next,
                v_plan.p_after_scan,
                v_plan.p_after_episode,
                v_plan.p_after_episode_cutoff,
                v_plan.p_after_episode_emitted,
                v_plan.p_after_episode_limit,
                v_plan.p_review_reason,
                v_plan.p_clear,
                v_plan.p_audit_from,
                v_plan.p_audit_through,
                v_plan.p_audit_reasons,
                v_plan.p_invalid,
                v_plan.p_job_ids,
                v_plan.p_job_names,
                v_plan.p_payload_versions,
                v_plan.p_profile_ids,
                v_plan.p_profile_versions,
                v_plan.p_queues,
                v_plan.p_priorities,
                v_plan.p_not_before_ms,
                v_plan.p_active_keys,
                v_plan.p_window_keys,
                v_plan.p_window_ms,
                v_plan.p_business,
                v_plan.p_policy,
                v_plan.p_payloads,
                v_plan.p_headers,
                v_plan.p_max_executions,
                v_plan.p_max_unstarted,
                v_plan.p_timeouts,
                v_plan.p_retry,
                v_plan.p_unstarted,
                ARRAY['[]'::jsonb],
                ARRAY['[]'::jsonb], p_faults);
            IF v_reply.effect <> 'Applied' THEN
                RAISE SQLSTATE 'RWS31';
            END IF;
        EXCEPTION WHEN SQLSTATE 'RWS31' THEN
            -- Roll back the item effect while preserving its response. The receipt was seeded outside the savepoint.
            DELETE FROM runwick.storage_commands
            WHERE application = p_application AND retention_class = p_class
              AND command_id = v_plan.p_command_id AND result IS NULL;
        END;
        v_replies := v_replies || pg_catalog.jsonb_build_object(v_plan.p_command_id::text, pg_catalog.to_jsonb(v_reply));
    END LOOP;

    RETURN runwick.rw_applied_v1('Applied', NULL, runwick.ms_floor(pg_catalog.clock_timestamp()), v_replies);
END
$fn$;
