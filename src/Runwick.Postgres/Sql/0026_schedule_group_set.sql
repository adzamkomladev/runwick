-- 0026_schedule_group_set (13 Changes 2026-11-19): a schedule group writes its plans with one statement per table.
--
-- materialize_schedule_group_v2 runs the single-plan core once per plan: about fifteen statements and three nested function
-- calls each, roughly 6 ms of server time per plan on a quiet host. Version 3 accepts the same request and, for the common
-- case, makes the same decisions and the same rows with one statement per table for the whole group (rw_schedule_group_set_v1).
-- Every case that path does not decide by itself (an existing receipt for any command, an expired or too-distant deadline,
-- a client-side refusal, an unregistered queue, a clock behind the cutoff, an occurrence that already exists, a duplicate
-- job, a server fault hook) undoes what it wrote and runs version 2 unchanged, so those cases keep their tested outcomes.
-- Released functions stay; clients at earlier revisions keep calling version 1 and 2.
SET LOCAL lock_timeout = '5s';

-- The set path. Returns NULL before any write when version 2 must decide; raises RWS33 to undo its writes for the same reason.
-- Plans arrive sorted by command ID. Locks are taken as version 2 takes them: receipts in command order, schedule rows in
-- schedule order, new jobs in job order.
CREATE FUNCTION runwick.rw_schedule_group_set_v1(p_application text, p_operation text, p_class text, p_deadline_cap interval,
    p_plans runwick.rw_schedule_group_plan_v1[])
RETURNS runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_count integer := pg_catalog.cardinality(p_plans);
    v_clock timestamptz := pg_catalog.clock_timestamp();
    v_now timestamptz;
    v_rejects jsonb;
    v_accepted uuid[];
    v_replies jsonb;
    v_n integer;
BEGIN
    -- Cases that version 2 decides, found before anything is written.
    IF NOT COALESCE((SELECT c.required_capabilities ? 'scheduler' FROM runwick.application_controls AS c
                     WHERE c.application = p_application), false)
       OR EXISTS (SELECT 1 FROM unnest(p_plans) AS p
                  WHERE p.p_not_after <= v_clock OR p.p_not_after - v_clock > p_deadline_cap OR p.p_invalid IS NOT NULL
                     OR NOT EXISTS (SELECT 1 FROM runwick.queue_controls AS q
                                    WHERE q.application = p_application AND q.queue = p.p_queues[1]))
       OR EXISTS (SELECT 1 FROM runwick.storage_commands AS c
                  WHERE c.application = p_application AND c.command_id IN (SELECT p.p_command_id FROM unnest(p_plans) AS p)) THEN
        RETURN NULL;
    END IF;

    -- R1 for every plan (04 §6.3). A receipt that appeared since the check is a concurrent send of the same command.
    INSERT INTO runwick.storage_commands (
        application, retention_class, command_id, operation, operation_version, protocol_epoch,
        fingerprint, fingerprint_version, not_after, receipt_codec_version, retain_until)
    SELECT p_application, p_class, p.p_command_id, p_operation, 1, 1, p.p_fingerprint, 1, p.p_not_after, 1,
           p.p_not_after + CASE p_class WHEN 'ownership' THEN interval '10 minutes' ELSE interval '24 hours' END
    FROM unnest(p_plans) AS p
    ORDER BY p.p_command_id
    ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n <> v_count THEN
        RAISE SQLSTATE 'RWS33';
    END IF;

    PERFORM 1 FROM runwick.schedules AS s
    WHERE s.application = p_application AND s.schedule_id IN (SELECT p.p_schedule_id FROM unnest(p_plans) AS p)
    ORDER BY s.schedule_id FOR UPDATE OF s;

    -- The plan checks of rw_materialize_core_v1, in its order, against the locked rows (13 §4.2).
    SELECT pg_catalog.jsonb_object_agg(x.command_id::text, pg_catalog.to_jsonb(x.reply))
    INTO v_rejects
    FROM (SELECT p.p_command_id AS command_id, CASE
              WHEN s.schedule_id IS NULL THEN runwick.rw_no_effect_v1('NotFound', 'schedule_not_found', NULL)
              WHEN NOT s.enabled THEN runwick.rw_no_effect_v1('InvalidState', 'schedule_disabled', NULL)
              WHEN s.paused THEN runwick.rw_no_effect_v1('InvalidState', 'schedule_paused', NULL)
              WHEN s.needs_review THEN runwick.rw_no_effect_v1('InvalidState', 'schedule_needs_review', NULL)
              WHEN s.definition_revision <> p.p_expected_definition OR s.control_revision <> p.p_expected_control
                   OR s.cursor_revision <> p.p_expected_cursor OR s.episode_id IS DISTINCT FROM p.p_expected_episode
                  THEN runwick.rw_no_effect_v1('VersionConflict', 'revision_conflict', NULL,
                           pg_catalog.format('actual=%s/%s/%s', s.definition_revision, s.control_revision, s.cursor_revision))
              WHEN s.payload_length > p.p_max_plan_bytes THEN runwick.rw_no_effect_v1('InvalidRequest', 'plan_too_large', NULL)
              WHEN s.evaluated_through IS NOT NULL AND p.p_occ_scheduled[1] <= s.evaluated_through
                  THEN runwick.rw_no_effect_v1('InvariantFault', 'occurrence_below_high_water', NULL)
              WHEN s.evaluated_through IS NOT NULL AND (p.p_after_through IS NULL OR p.p_after_through < s.evaluated_through)
                  THEN runwick.rw_no_effect_v1('InvariantFault', 'cursor_rewind', NULL)
          END AS reply
          FROM unnest(p_plans) AS p
          LEFT JOIN (SELECT s.schedule_id, s.enabled, s.paused, s.needs_review, s.definition_revision, s.control_revision,
                            s.cursor_revision, s.episode_id, s.evaluated_through, pg_catalog.length(d.payload_template) AS payload_length
                     FROM runwick.schedules AS s
                     JOIN runwick.schedule_definitions AS d
                       ON d.application = s.application AND d.schedule_id = s.schedule_id AND d.revision = s.definition_revision
                     WHERE s.application = p_application) AS s
            ON s.schedule_id = p.p_schedule_id) AS x
    WHERE (x.reply).status IS NOT NULL;
    v_rejects := COALESCE(v_rejects, '{}'::jsonb);

    -- A refused plan keeps no receipt and no effect, as in version 2.
    IF v_rejects <> '{}'::jsonb THEN
        DELETE FROM runwick.storage_commands
        WHERE application = p_application AND retention_class = p_class AND result IS NULL
          AND command_id IN (SELECT k::uuid FROM pg_catalog.jsonb_object_keys(v_rejects) AS k);
    END IF;

    v_accepted := ARRAY(SELECT p.p_command_id FROM unnest(p_plans) AS p WHERE NOT v_rejects ? p.p_command_id::text);
    v_clock := pg_catalog.clock_timestamp();
    v_now := runwick.ms_floor(v_clock);
    IF EXISTS (SELECT 1 FROM unnest(p_plans) AS p
               WHERE p.p_command_id = ANY (v_accepted) AND (v_clock >= p.p_not_after OR v_now < p.p_cutoff)) THEN
        RAISE SQLSTATE 'RWS33';
    END IF;

    IF pg_catalog.cardinality(v_accepted) > 0 THEN
        -- The dispatch effect of rw_dispatch_core_v3 for one keyless item per plan, all plans in job order.
        INSERT INTO runwick.jobs (
            application, job_id, job_name, payload_version, serializer_profile_id, serializer_profile_version,
            queue, priority, state, eligible_at, created_at, updated_at, active_unique_key)
        SELECT p_application, p.p_job_ids[1], p.p_job_names[1], p.p_payload_versions[1], p.p_profile_ids[1], p.p_profile_versions[1],
               p.p_queues[1], p.p_priorities[1], 'pending',
               runwick.ms_ceil(GREATEST(COALESCE('epoch'::timestamptz + p.p_not_before_ms[1] * interval '1 millisecond', v_now), v_now)),
               v_now, v_now, NULL
        FROM unnest(p_plans) AS p
        WHERE p.p_command_id = ANY (v_accepted)
        ORDER BY p.p_job_ids[1];

        INSERT INTO runwick.job_payloads (
            application, job_id, payload_utf8, payload_encoding_version, serializer_profile_id, serializer_profile_version, headers)
        SELECT p_application, p.p_job_ids[1], p.p_payloads[1], 1, p.p_profile_ids[1], p.p_profile_versions[1],
               CASE WHEN p.p_headers[1] -> 'schedule' -> 'scheduledAt' = 'null'::jsonb
                    THEN pg_catalog.jsonb_set(p.p_headers[1], '{schedule}', (p.p_headers[1] -> 'schedule')
                         || pg_catalog.jsonb_build_object('scheduledAt', runwick.rw_ms_v1(v_now), 'coveredFrom', runwick.rw_ms_v1(v_now),
                                                          'coveredUntil', runwick.rw_ms_v1(v_now)))
                    ELSE p.p_headers[1] END
        FROM unnest(p_plans) AS p
        WHERE p.p_command_id = ANY (v_accepted)
        ORDER BY p.p_job_ids[1];

        INSERT INTO runwick.job_generations (
            application, job_id, generation, created_at, created_reason,
            max_executions, max_unstarted_abandonments, execution_timeout_ms, retry_policy, unstarted_retry_policy)
        SELECT p_application, p.p_job_ids[1], 1, v_now, 'scheduled', p.p_max_executions[1], p.p_max_unstarted[1],
               p.p_timeouts[1], p.p_retry[1], p.p_unstarted[1]
        FROM unnest(p_plans) AS p
        WHERE p.p_command_id = ANY (v_accepted)
        ORDER BY p.p_job_ids[1];

        INSERT INTO runwick.schedule_occurrences (
            application, schedule_id, occurrence_kind, occurrence_key, scheduled_at, definition_revision, job_id, disposition,
            coalesced, covered_from, covered_through, materialized_at, command_id)
        SELECT p_application, p.p_schedule_id, 'scheduled', p.p_occ_keys[1], p.p_occ_scheduled[1], p.p_expected_definition,
               p.p_job_ids[1], 'created', p.p_occ_coalesced[1], p.p_occ_from[1], p.p_occ_through[1], v_now, p.p_command_id
        FROM unnest(p_plans) AS p
        WHERE p.p_command_id = ANY (v_accepted)
        ORDER BY p.p_job_ids[1]
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS v_n = ROW_COUNT;
        IF v_n <> pg_catalog.cardinality(v_accepted) THEN
            RAISE SQLSTATE 'RWS33';
        END IF;

        UPDATE runwick.schedules AS s
        SET evaluated_through = p.p_after_through, next_due_at = p.p_after_next, scan_after_at = p.p_after_scan,
            episode_id = p.p_after_episode, episode_cutoff = p.p_after_episode_cutoff, episode_emitted = p.p_after_episode_emitted,
            episode_limit = p.p_after_episode_limit, needs_review = false, review_reason = NULL,
            cursor_revision = s.cursor_revision + 1, updated_at = v_now
        FROM unnest(p_plans) AS p
        WHERE s.application = p_application AND s.schedule_id = p.p_schedule_id AND p.p_command_id = ANY (v_accepted);
        GET DIAGNOSTICS v_n = ROW_COUNT;
        IF v_n <> pg_catalog.cardinality(v_accepted) THEN
            RAISE EXCEPTION 'rw:invalid_transition' USING ERRCODE = 'RWI01';
        END IF;

        -- Per plan in job order: the dispatched event, then the materialized event, as version 2 writes them.
        INSERT INTO runwick.queue_events (application, job_id, attempt_id, command_id, event_kind, occurred_at, actor_id, details)
        SELECT p_application, e.job_id, NULL, e.command_id, e.kind, v_now, e.actor, e.details
        FROM (SELECT p.p_job_ids[1] AS o, 1 AS part, p.p_job_ids[1] AS job_id, p.p_command_id AS command_id, 'dispatched' AS kind,
                     NULL::text AS actor,
                     pg_catalog.jsonb_build_object('generation', 1, 'queue', p.p_queues[1], 'priority', p.p_priorities[1], 'ordinal', 0)
                         AS details
              FROM unnest(p_plans) AS p WHERE p.p_command_id = ANY (v_accepted)
              UNION ALL
              SELECT p.p_job_ids[1], 2, NULL, p.p_command_id, 'schedule_materialized', 'svc:scheduler',
                     pg_catalog.jsonb_build_object('cursorRevision', p.p_expected_cursor + 1, 'cutoff', runwick.rw_ms_v1(p.p_cutoff),
                         'jobs', 1, 'skippedRanges', 0, 'episodeId', p.p_after_episode, 'needsReview', NULL::text,
                         'scheduleId', p.p_schedule_id, 'scheduleKey', s.schedule_key)
              FROM unnest(p_plans) AS p
              JOIN runwick.schedules AS s ON s.application = p_application AND s.schedule_id = p.p_schedule_id
              WHERE p.p_command_id = ANY (v_accepted)) AS e
        ORDER BY e.o, e.part;

        -- The wake (05 §7.2): one per distinct queue with a job eligible now, delivered only if this transaction commits.
        PERFORM pg_catalog.pg_notify('runwick_wake', p_application || '/' || q.queue)
        FROM (SELECT p.p_queues[1] AS queue, min(p.p_job_ids[1]::text) AS first
              FROM unnest(p_plans) AS p
              WHERE p.p_command_id = ANY (v_accepted)
                AND (p.p_not_before_ms[1] IS NULL OR p.p_not_before_ms[1] <= runwick.rw_ms_v1(runwick.ms_ceil(v_now)))
              GROUP BY p.p_queues[1]) AS q
        ORDER BY q.first;
    END IF;

    -- R4 for every applied plan, with the reply version 2 would store.
    SELECT pg_catalog.jsonb_object_agg(p.p_command_id::text, pg_catalog.jsonb_build_object('scheduleId', p.p_schedule_id,
               'cursorRevision', p.p_expected_cursor + 1,
               'occurrences', pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('kind', 'scheduled', 'key', p.p_occ_keys[1],
                   'jobId', p.p_job_ids[1]::text, 'disposition', 'created'))))
    INTO v_replies
    FROM unnest(p_plans) AS p
    WHERE p.p_command_id = ANY (v_accepted);
    v_replies := COALESCE(v_replies, '{}'::jsonb);

    UPDATE runwick.storage_commands AS c
    SET status = 'Applied', result = r.value, decided_at = v_now
    FROM pg_catalog.jsonb_each(v_replies) AS r
    WHERE c.application = p_application AND c.retention_class = p_class AND c.command_id = r.key::uuid AND c.result IS NULL;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n <> pg_catalog.cardinality(v_accepted) THEN
        RAISE EXCEPTION 'rw:receipt_finalization' USING ERRCODE = 'RWI03';
    END IF;

    -- Final deadline guard, as in the dispatch core: a lock wait after the decision can pass a deadline.
    IF EXISTS (SELECT 1 FROM unnest(p_plans) AS p WHERE p.p_command_id = ANY (v_accepted) AND pg_catalog.clock_timestamp() >= p.p_not_after) THEN
        RAISE SQLSTATE 'RWS33';
    END IF;

    SELECT v_rejects || COALESCE(pg_catalog.jsonb_object_agg(r.key, pg_catalog.to_jsonb(runwick.rw_applied_v1('Applied', NULL, v_now, r.value))),
                                 '{}'::jsonb)
    INTO v_replies
    FROM pg_catalog.jsonb_each(v_replies) AS r;
    RETURN runwick.rw_applied_v1('Applied', NULL, runwick.ms_floor(pg_catalog.clock_timestamp()), v_replies);
END
$fn$;

CREATE FUNCTION runwick.materialize_schedule_group_v3(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
    p_fingerprint bytea, p_class text, p_deadline_cap interval, p_store_id uuid, p_revisions bigint[], p_epoch integer,
    p_capabilities jsonb, p_is_creation boolean, p_allow_drain boolean, p_staged boolean,
    p_plans jsonb, p_faults jsonb DEFAULT NULL)
RETURNS runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_reply runwick.rw_reply_v1;
    v_plans runwick.rw_schedule_group_plan_v1[];
BEGIN
    -- Server fault hooks name the steps of version 2, so a call that arms one runs version 2.
    IF p_faults IS NULL AND NOT p_staged AND p_plans IS NOT NULL AND pg_catalog.jsonb_array_length(p_plans) BETWEEN 2 AND 8 THEN
        v_reply := runwick.rw_gate_v1(p_application, p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain);
        IF v_reply.status IS NOT NULL THEN
            RETURN v_reply;
        END IF;

        SELECT pg_catalog.array_agg(x ORDER BY x.p_command_id) INTO v_plans
        FROM pg_catalog.jsonb_populate_recordset(NULL::runwick.rw_schedule_group_plan_v1, p_plans) AS x;

        -- The shape version 2 accepts; anything else goes to version 2 for its refusal.
        IF NOT EXISTS (SELECT 1 FROM unnest(v_plans) AS p
                       WHERE pg_catalog.cardinality(p.p_occ_keys) IS DISTINCT FROM 1
                          OR pg_catalog.cardinality(p.p_job_ids) IS DISTINCT FROM 1
                          OR p.p_expected_episode IS NOT NULL OR p.p_after_episode IS NOT NULL OR p.p_clear IS DISTINCT FROM false
                          OR p.p_review_reason IS NOT NULL OR pg_catalog.cardinality(p.p_audit_reasons) IS DISTINCT FROM 0
                          OR p.p_occ_from[1] IS NOT NULL OR p.p_occ_through[1] IS NOT NULL OR p.p_occ_coalesced[1] IS DISTINCT FROM false
                          OR p.p_active_keys[1] IS NOT NULL OR p.p_window_keys[1] IS NOT NULL)
           AND (SELECT count(DISTINCT p.p_schedule_id) FROM unnest(v_plans) AS p) = pg_catalog.cardinality(v_plans)
           AND (SELECT count(DISTINCT p.p_command_id) FROM unnest(v_plans) AS p) = pg_catalog.cardinality(v_plans)
           AND (SELECT count(DISTINCT p.p_job_ids[1]) FROM unnest(v_plans) AS p) = pg_catalog.cardinality(v_plans)
           AND (SELECT sum(pg_catalog.length(p.p_payloads[1])) FROM unnest(v_plans) AS p) <= 2097152 THEN
            BEGIN
                v_reply := runwick.rw_schedule_group_set_v1(p_application, p_operation, p_class, p_deadline_cap, v_plans);
            EXCEPTION WHEN SQLSTATE 'RWS33' OR unique_violation THEN
                v_reply := NULL;
            END;
            IF v_reply.status IS NOT NULL THEN
                RETURN v_reply;
            END IF;
        END IF;
    END IF;

    RETURN runwick.materialize_schedule_group_v2(p_application, p_command_id, p_operation, p_not_after, p_fingerprint, p_class,
        p_deadline_cap, p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain, p_staged, p_plans, p_faults);
END
$fn$;
