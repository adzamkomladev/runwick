-- 0029_dispatch_group (04 Changes 2026-11-23, PF2-B1 redesign): a driver-only entry point that commits 2 to 8 independent
-- plain Dispatch/1 commands in one transaction. It adds no protocol operation. Each command keeps its own command ID,
-- fingerprint, deadline, receipt and reply; there is no group receipt. Plain means one job, no uniqueness key, no
-- concurrency or rate scope and no schedule header, so the writes are the "no key, no scope" shapes of rw_dispatch_core_v4.
-- The common path is set-based: one receipt insert, one other-class check, one queue check, one insert per table, one
-- event insert and one receipt finalize for the whole group. Only a command whose receipt row already exists (a replay or
-- a conflict) goes through rw_receipt_v1 on its own.
-- The first row returned is the group's reply; then one reply per command, in input order. A group-level refusal (gates,
-- malformed group) is the only row and commits nothing; the client then sends each command alone. A raise (a member that
-- expired after the writes, a job ID collision, a lock timeout) rolls the whole group back, and the client sends each
-- command alone with its original identity, so a rare late failure never reaches the set path.
SET LOCAL lock_timeout = '5s';

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
                                   WHERE c.application = p_application AND c.command_id = u.id AND c.retention_class <> p_class));

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
