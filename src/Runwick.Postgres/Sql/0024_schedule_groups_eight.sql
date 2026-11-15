-- Keep the revision 22 entry point intact. This coordinator widens only the group-size bound.
CREATE FUNCTION runwick.materialize_schedule_group_v2(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
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
    IF p_staged OR p_plans IS NULL OR pg_catalog.jsonb_array_length(p_plans) NOT BETWEEN 2 AND 8 THEN
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
