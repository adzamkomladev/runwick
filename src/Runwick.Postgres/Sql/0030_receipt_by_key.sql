-- 0030_receipt_by_key (04 Changes 2026-11-23, receipt compaction): fewer and cheaper writes per command receipt, with the
-- same rows, results and replay. Schema revision 30.
--
-- A receipt is written twice: R1 inserts the placeholder, R4 updates it with the decision. Each written row version had an
-- entry in three indexes: the primary key (application, retention_class, command_id), command_retention (application,
-- retain_until, command_id) and command_by_id (application, command_id). Two changes:
--
-- (1) command_by_id is dropped. It existed only for reads that name a command without its class: the other-class conflict
--     probe (R3) and the schedule group's existing-receipt check. A retention class is one of two values, so each of those
--     reads now names the class (the other class for R3, both classes for the schedule check) and is a primary-key lookup
--     in one partition. The three functions below are their earlier bodies with only that predicate changed; clients at
--     earlier revisions call the same names with the same arguments.
-- (2) The receipt partitions get fillfactor 70. The decided row (about 460 bytes for a Finish, 2 kB for a claim) is larger
--     than its placeholder, and on a full page the R4 update could not stay on the page, so about half of the durable
--     updates wrote a new entry into every index. With room left on each page nearly all R4 updates are HOT (no index
--     entry). It applies to pages written from now on; existing pages are not rewritten.
SET LOCAL lock_timeout = '5s';

DROP INDEX runwick.command_by_id;

ALTER TABLE runwick.storage_commands_ownership SET (fillfactor = 70);

ALTER TABLE runwick.storage_commands_durable SET (fillfactor = 70);

DROP FUNCTION runwick.rw_receipt_v1(text, text, uuid, text, bytea, timestamptz, interval);

DROP FUNCTION runwick.rw_schedule_group_set_v1(text, text, text, interval, runwick.rw_schedule_group_plan_v1[]);

DROP FUNCTION runwick.dispatch_group_v1(text, uuid, text, timestamptz, bytea, text, interval, uuid, bigint[], integer, jsonb,
    boolean, boolean, boolean, uuid[], timestamptz[], bytea[], text[], uuid[], text[], integer[], text[], integer[], text[],
    text[], bigint[], bytea[], jsonb[], integer[], integer[], bigint[], jsonb[], jsonb[], jsonb);

-- R1, then R3 only when R1 inserted, otherwise R2 FOR UPDATE; then the receipt clock, CommandExpired and
-- deadline_too_far (04 §6.3, 03 §8.2 steps 1-3). NULL = new command; a replay row has replayed = true.
CREATE FUNCTION runwick.rw_receipt_v1(p_application text, p_class text, p_command_id uuid, p_operation text,
    p_fingerprint bytea, p_not_after timestamptz, p_deadline_cap interval)
RETURNS runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_inserted boolean;
    v_operation text;
    v_version integer;
    v_fingerprint bytea;
    v_status text;
    v_result jsonb;
    v_decided timestamptz;
    v_clock timestamptz;
    v_reply runwick.rw_reply_v1;
BEGIN
    -- R1 (04 §6.3)
    INSERT INTO runwick.storage_commands (
        application, retention_class, command_id, operation, operation_version, protocol_epoch,
        fingerprint, fingerprint_version, not_after, receipt_codec_version, retain_until)
    VALUES (p_application, p_class, p_command_id, p_operation, 1, 1, p_fingerprint, 1, p_not_after, 1,
            p_not_after + CASE p_class WHEN 'ownership' THEN interval '10 minutes' ELSE interval '24 hours' END)
    ON CONFLICT DO NOTHING
    RETURNING true INTO v_inserted;

    IF v_inserted THEN
        -- R3: the same ID under the other retention class is a different operation.
        IF EXISTS (SELECT 1 FROM runwick.storage_commands AS c
                   WHERE c.application = p_application
                     AND c.retention_class = CASE p_class WHEN 'ownership' THEN 'durable' ELSE 'ownership' END
                     AND c.command_id = p_command_id) THEN
            RETURN runwick.rw_no_effect_v1('CommandConflict', 'command_conflict', NULL);
        END IF;
    ELSE
        -- R2: a new statement, so it sees the holder's committed row.
        SELECT c.operation, c.operation_version, c.fingerprint, c.status, c.result, c.decided_at
        INTO v_operation, v_version, v_fingerprint, v_status, v_result, v_decided
        FROM runwick.storage_commands AS c
        WHERE c.application = p_application AND c.retention_class = p_class AND c.command_id = p_command_id
        FOR UPDATE;
        IF NOT FOUND OR v_operation <> p_operation OR v_version <> 1 OR v_fingerprint <> p_fingerprint THEN
            RETURN runwick.rw_no_effect_v1('CommandConflict', 'command_conflict', NULL);
        END IF;

        IF v_result IS NULL OR v_status IS NULL OR v_decided IS NULL THEN
            RETURN runwick.rw_no_effect_v1('InvariantFault', 'receipt_incomplete', NULL);
        END IF;

        v_reply := runwick.rw_applied_v1(v_status, NULL, v_decided, v_result);
        v_reply.replayed := true;
        RETURN v_reply;
    END IF;

    v_clock := pg_catalog.clock_timestamp();
    IF v_clock >= p_not_after THEN
        RETURN runwick.rw_no_effect_v1('CommandExpired', 'command_expired', runwick.ms_floor(v_clock));
    END IF;

    IF p_not_after - v_clock > p_deadline_cap THEN
        RETURN runwick.rw_no_effect_v1('InvalidRequest', 'deadline_too_far', runwick.ms_floor(v_clock));
    END IF;

    RETURN NULL;
END
$fn$;

-- The set path. Returns NULL before any write when version 2 must decide; raises RWS33 to undo its writes for the same reason.
-- Plans arrive sorted by command ID. Locks are taken as version 2 takes them: receipts in command order, schedule rows in
-- schedule order, new jobs in job order. Every statement names its rows by key (= ANY of an array), so the plan stays an index
-- lookup whatever the table statistics say.
CREATE FUNCTION runwick.rw_schedule_group_set_v1(p_application text, p_operation text, p_class text, p_deadline_cap interval,
    p_plans runwick.rw_schedule_group_plan_v1[])
RETURNS runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_count integer := pg_catalog.cardinality(p_plans);
    v_clock timestamptz := pg_catalog.clock_timestamp();
    v_now timestamptz;
    v_rejects jsonb;
    v_commands uuid[] := ARRAY(SELECT p.p_command_id FROM unnest(p_plans) AS p);
    v_schedules uuid[] := ARRAY(SELECT p.p_schedule_id FROM unnest(p_plans) AS p);
    v_accepted uuid[];
    v_accepted_schedules uuid[];
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
                  WHERE c.application = p_application AND c.retention_class IN ('ownership', 'durable')
                    AND c.command_id = ANY (v_commands)) THEN
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
    WHERE s.application = p_application AND s.schedule_id = ANY (v_schedules)
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
                     WHERE s.application = p_application AND s.schedule_id = ANY (v_schedules)) AS s
            ON s.schedule_id = p.p_schedule_id) AS x
    WHERE (x.reply).status IS NOT NULL;
    v_rejects := COALESCE(v_rejects, '{}'::jsonb);

    -- A refused plan keeps no receipt and no effect, as in version 2.
    IF v_rejects <> '{}'::jsonb THEN
        DELETE FROM runwick.storage_commands
        WHERE application = p_application AND retention_class = p_class AND result IS NULL
          AND command_id = ANY (ARRAY(SELECT k::uuid FROM pg_catalog.jsonb_object_keys(v_rejects) AS k));
    END IF;

    v_accepted := ARRAY(SELECT p.p_command_id FROM unnest(p_plans) AS p WHERE NOT v_rejects ? p.p_command_id::text);
    v_accepted_schedules := ARRAY(SELECT p.p_schedule_id FROM unnest(p_plans) AS p WHERE p.p_command_id = ANY (v_accepted));
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
        WHERE s.application = p_application AND s.schedule_id = ANY (v_accepted_schedules)
          AND s.schedule_id = p.p_schedule_id AND p.p_command_id = ANY (v_accepted);
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
              JOIN runwick.schedules AS s
                ON s.application = p_application AND s.schedule_id = ANY (v_accepted_schedules) AND s.schedule_id = p.p_schedule_id
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
    WHERE c.application = p_application AND c.retention_class = p_class AND c.command_id = ANY (v_accepted)
      AND c.command_id = r.key::uuid AND c.result IS NULL;
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

CREATE FUNCTION runwick.dispatch_group_v1(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
    p_fingerprint bytea, p_class text, p_deadline_cap interval, p_store_id uuid, p_revisions bigint[], p_epoch integer,
    p_capabilities jsonb, p_is_creation boolean, p_allow_drain boolean, p_staged boolean,
    p_command_ids uuid[], p_not_afters timestamptz[], p_fingerprints bytea[], p_invalid_items text[],
    p_job_ids uuid[], p_job_names text[], p_payload_versions integer[], p_profile_ids text[], p_profile_versions integer[],
    p_queues text[], p_priorities text[], p_not_before_ms bigint[], p_payloads bytea[], p_headers jsonb[],
    p_max_executions integer[], p_max_unstarted integer[], p_timeouts bigint[], p_retry jsonb[], p_unstarted jsonb[],
    p_faults jsonb DEFAULT NULL)
RETURNS SETOF runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_n integer := pg_catalog.cardinality(p_command_ids);
    v_gate runwick.rw_reply_v1;
    v_receipt runwick.rw_reply_v1;
    v_replies runwick.rw_reply_v1[];
    v_mine integer[];
    v_other integer[];
    v_fresh integer[] := '{}';
    v_survivors integer[] := '{}';
    v_unregistered integer[];
    v_i integer;
    v_clock timestamptz;
    v_now timestamptz;
    v_count integer;
BEGIN
    IF p_staged OR v_n IS NULL OR v_n NOT BETWEEN 2 AND 8
       OR pg_catalog.array_ndims(p_command_ids) <> 1 OR pg_catalog.array_lower(p_command_ids, 1) <> 1
       OR pg_catalog.cardinality(p_not_afters) <> v_n OR pg_catalog.cardinality(p_fingerprints) <> v_n
       OR pg_catalog.cardinality(p_invalid_items) <> v_n OR pg_catalog.cardinality(p_job_ids) <> v_n
       OR pg_catalog.cardinality(p_job_names) <> v_n OR pg_catalog.cardinality(p_payload_versions) <> v_n
       OR pg_catalog.cardinality(p_profile_ids) <> v_n OR pg_catalog.cardinality(p_profile_versions) <> v_n
       OR pg_catalog.cardinality(p_queues) <> v_n OR pg_catalog.cardinality(p_priorities) <> v_n
       OR pg_catalog.cardinality(p_not_before_ms) <> v_n OR pg_catalog.cardinality(p_payloads) <> v_n
       OR pg_catalog.cardinality(p_headers) <> v_n OR pg_catalog.cardinality(p_max_executions) <> v_n
       OR pg_catalog.cardinality(p_max_unstarted) <> v_n OR pg_catalog.cardinality(p_timeouts) <> v_n
       OR pg_catalog.cardinality(p_retry) <> v_n OR pg_catalog.cardinality(p_unstarted) <> v_n
       OR (SELECT pg_catalog.count(DISTINCT x) FROM unnest(p_command_ids) AS u(x)) <> v_n
       OR (SELECT pg_catalog.count(DISTINCT x) FROM unnest(p_job_ids) AS u(x)) <> v_n
       OR EXISTS (SELECT 1 FROM unnest(p_not_afters, p_fingerprints, p_headers) AS u(na, fp, h)
                  WHERE u.na IS NULL OR u.fp IS NULL OR u.h ? 'schedule')
    THEN
        RETURN NEXT runwick.rw_no_effect_v1('InvalidRequest', 'invalid_dispatch_group', NULL);
        RETURN;
    END IF;

    -- The gates once for the whole group. A refusal is the group's only row; each command then hears it alone.
    v_gate := runwick.rw_gate_v1(p_application, p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain);
    IF v_gate.status IS NOT NULL THEN
        RETURN NEXT v_gate;
        RETURN;
    END IF;

    v_replies := pg_catalog.array_fill(NULL::runwick.rw_reply_v1, ARRAY[v_n]);

    -- R1 for the whole group in command-ID order, the order every group and singleton takes receipt rows.
    WITH ins AS (
        INSERT INTO runwick.storage_commands (
            application, retention_class, command_id, operation, operation_version, protocol_epoch,
            fingerprint, fingerprint_version, not_after, receipt_codec_version, retain_until)
        SELECT p_application, p_class, a.id, 'Dispatch', 1, 1, a.fp, 1, a.na, 1,
               a.na + CASE p_class WHEN 'ownership' THEN interval '10 minutes' ELSE interval '24 hours' END
        FROM unnest(p_command_ids, p_fingerprints, p_not_afters) AS a(id, fp, na)
        ORDER BY a.id
        ON CONFLICT DO NOTHING
        RETURNING command_id)
    SELECT COALESCE(pg_catalog.array_agg(u.o::integer ORDER BY u.o), '{}') INTO v_mine
    FROM ins JOIN unnest(p_command_ids) WITH ORDINALITY AS u(id, o) ON u.id = ins.command_id;

    -- R3 for the inserted rows: the same ID under the other retention class is a different operation.
    v_other := ARRAY(SELECT u.o::integer FROM unnest(p_command_ids) WITH ORDINALITY AS u(id, o)
                     WHERE u.o = ANY (v_mine)
                       AND EXISTS (SELECT 1 FROM runwick.storage_commands AS c
                                   WHERE c.application = p_application
                                     AND c.retention_class = CASE p_class WHEN 'ownership' THEN 'durable' ELSE 'ownership' END
                                     AND c.command_id = u.id));

    -- R2 for rows that already existed (a replay, a conflict, or a holder that rolled back meanwhile), one by one in
    -- command-ID order, exactly as a single dispatch sees them.
    FOR v_i IN SELECT u.o::integer FROM unnest(p_command_ids) WITH ORDINALITY AS u(id, o)
               WHERE NOT (u.o = ANY (v_mine)) ORDER BY u.id LOOP
        v_receipt := runwick.rw_receipt_v1(p_application, p_class, p_command_ids[v_i], 'Dispatch',
            p_fingerprints[v_i], p_not_afters[v_i], p_deadline_cap);
        IF v_receipt.status IS NULL THEN
            v_mine := v_mine || v_i; -- the holder rolled back and this call inserted the placeholder
            v_fresh := v_fresh || v_i;
        ELSE
            v_replies[v_i] := v_receipt;
        END IF;
    END LOOP;

    -- The receipt clock for the rows inserted by the set statement (rw_receipt_v1 checked its own).
    v_clock := pg_catalog.clock_timestamp();
    FOR v_i IN 1 .. v_n LOOP
        CONTINUE WHEN NOT (v_i = ANY (v_mine)) OR v_i = ANY (v_fresh);
        IF v_i = ANY (v_other) THEN
            v_replies[v_i] := runwick.rw_no_effect_v1('CommandConflict', 'command_conflict', NULL);
        ELSIF v_clock >= p_not_afters[v_i] THEN
            v_replies[v_i] := runwick.rw_no_effect_v1('CommandExpired', 'command_expired', runwick.ms_floor(v_clock));
        ELSIF p_not_afters[v_i] - v_clock > p_deadline_cap THEN
            v_replies[v_i] := runwick.rw_no_effect_v1('InvalidRequest', 'deadline_too_far', runwick.ms_floor(v_clock));
        ELSE
            v_fresh := v_fresh || v_i;
        END IF;
    END LOOP;

    -- Queue registration for the fresh commands: one join for the group.
    v_unregistered := ARRAY(SELECT f.i FROM unnest(v_fresh) AS f(i)
                            WHERE NOT EXISTS (SELECT 1 FROM runwick.queue_controls AS q
                                              WHERE q.application = p_application AND q.queue = p_queues[f.i]));

    -- One decision time for the group, read after the gate and receipt waits. Each command keeps its own deadline.
    v_clock := pg_catalog.clock_timestamp();
    v_now := runwick.ms_floor(v_clock);
    FOREACH v_i IN ARRAY v_fresh LOOP
        IF p_invalid_items[v_i] IS NOT NULL THEN
            v_replies[v_i] := runwick.rw_no_effect_v1('InvalidRequest', 'invalid_request', NULL, p_invalid_items[v_i]);
        ELSIF v_i = ANY (v_unregistered) THEN
            v_replies[v_i] := runwick.rw_no_effect_v1('InvalidRequest', 'queue_not_registered', NULL);
        ELSIF v_clock >= p_not_afters[v_i] THEN
            v_replies[v_i] := runwick.rw_no_effect_v1('CommandExpired', 'command_expired', v_now);
        ELSE
            v_survivors := v_survivors || v_i;
            v_replies[v_i] := runwick.rw_applied_v1('Applied', NULL, v_now, pg_catalog.jsonb_build_object(
                'jobId', p_job_ids[v_i], 'disposition', 'Created', 'match', 'None', 'policyDrift', false));
        END IF;
    END LOOP;

    IF pg_catalog.cardinality(v_survivors) > 0 THEN
        PERFORM runwick.rw_fault('BeforeFirstMutation', 0, p_faults); -- rw:fault

        INSERT INTO runwick.jobs (application, job_id, job_name, payload_version, serializer_profile_id,
            serializer_profile_version, queue, priority, state, eligible_at, created_at, updated_at)
        SELECT p_application, a.job_id, a.job_name, a.payload_version, a.profile_id, a.profile_version, a.queue, a.priority,
               'pending',
               runwick.ms_ceil(GREATEST(COALESCE('epoch'::timestamptz + a.not_before_ms * interval '1 millisecond', v_now), v_now)),
               v_now, v_now
        FROM unnest(p_job_ids, p_job_names, p_payload_versions, p_profile_ids, p_profile_versions, p_queues, p_priorities,
                    p_not_before_ms) WITH ORDINALITY
             AS a(job_id, job_name, payload_version, profile_id, profile_version, queue, priority, not_before_ms, o)
        WHERE a.o = ANY (v_survivors)
        ORDER BY a.job_id;

        INSERT INTO runwick.job_payloads (application, job_id, payload_utf8, payload_encoding_version,
            serializer_profile_id, serializer_profile_version, headers)
        SELECT p_application, a.job_id, a.payload, 1, a.profile_id, a.profile_version, a.headers
        FROM unnest(p_job_ids, p_payloads, p_profile_ids, p_profile_versions, p_headers) WITH ORDINALITY
             AS a(job_id, payload, profile_id, profile_version, headers, o)
        WHERE a.o = ANY (v_survivors)
        ORDER BY a.job_id;

        INSERT INTO runwick.job_generations (application, job_id, generation, created_at, created_reason,
            max_executions, max_unstarted_abandonments, execution_timeout_ms, retry_policy, unstarted_retry_policy)
        SELECT p_application, a.job_id, 1, v_now, 'initial', a.max_executions, a.max_unstarted, a.timeout, a.retry, a.unstarted
        FROM unnest(p_job_ids, p_max_executions, p_max_unstarted, p_timeouts, p_retry, p_unstarted) WITH ORDINALITY
             AS a(job_id, max_executions, max_unstarted, timeout, retry, unstarted, o)
        WHERE a.o = ANY (v_survivors)
        ORDER BY a.job_id;

        -- The event of each command carries that command's ID and ordinal 0, as a single dispatch's does.
        INSERT INTO runwick.queue_events (application, job_id, command_id, event_kind, occurred_at, details)
        SELECT p_application, a.job_id, a.command_id, 'dispatched', v_now,
               pg_catalog.jsonb_build_object('generation', 1, 'queue', a.queue, 'priority', a.priority, 'ordinal', 0)
        FROM unnest(p_job_ids, p_command_ids, p_queues, p_priorities) WITH ORDINALITY
             AS a(job_id, command_id, queue, priority, o)
        WHERE a.o = ANY (v_survivors)
        ORDER BY a.o;

        PERFORM runwick.rw_fault('AfterMutation', 1, p_faults); -- rw:fault

        -- A write above may have waited: no command may be reported applied after its own deadline.
        v_clock := pg_catalog.clock_timestamp();
        IF EXISTS (SELECT 1 FROM unnest(v_survivors) AS s(i) WHERE v_clock >= p_not_afters[s.i]) THEN
            RAISE EXCEPTION 'plain dispatch member expired after writes' USING ERRCODE = 'RWB01';
        END IF;

        -- R4 for the group.
        UPDATE runwick.storage_commands AS c
        SET status = 'Applied', result = (v_replies[s.i]).result, decided_at = v_now
        FROM unnest(v_survivors) AS s(i)
        WHERE c.application = p_application AND c.retention_class = p_class
          AND c.command_id = p_command_ids[s.i] AND c.result IS NULL;
        GET DIAGNOSTICS v_count = ROW_COUNT;
        IF v_count <> pg_catalog.cardinality(v_survivors) THEN
            RAISE EXCEPTION 'rw:receipt_finalization' USING ERRCODE = 'RWI03';
        END IF;
    END IF;

    -- Placeholders this call inserted for refused commands must not commit (a single dispatch rolls its own back).
    IF EXISTS (SELECT 1 FROM unnest(v_mine) AS m(i) WHERE NOT (m.i = ANY (v_survivors))) THEN
        DELETE FROM runwick.storage_commands AS c
        USING unnest(v_mine) AS m(i)
        WHERE NOT (m.i = ANY (v_survivors))
          AND c.application = p_application AND c.retention_class = p_class AND c.command_id = p_command_ids[m.i]
          AND c.result IS NULL;
    END IF;

    RETURN NEXT runwick.rw_applied_v1('Applied', NULL, v_now, NULL);
    RETURN QUERY SELECT (v_replies[a.i]).* FROM pg_catalog.generate_series(1, v_n) AS a(i) ORDER BY a.i;
END
$fn$;
