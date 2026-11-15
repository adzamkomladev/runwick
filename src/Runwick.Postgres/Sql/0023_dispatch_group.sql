-- 0023_dispatch_group (04 Changes 2026-11-15, PF2-B1): a driver-only entry point that commits 1 to 8 independent plain
-- Dispatch/1 commands in one transaction. Each command keeps its own receipt, fingerprint, deadline and result; there is
-- no group receipt. Plain means no uniqueness keys, no concurrency or rate scopes and no schedule header, so the writes
-- are the "no key, no scope" shapes of rw_dispatch_core_v4, run once over the set.
-- The function either returns one reply per command in input order, or raises. A raise (a member that expired after the
-- writes, an id collision, a lock timeout) rolls the whole transaction back and the client retries each command alone
-- through dispatch_v4 with its original identity, so a rare late failure never reaches the set fast path.
SET LOCAL lock_timeout = '5s';

CREATE FUNCTION runwick.dispatch_group_v1(
    p_application text, p_store_id uuid, p_revisions bigint[], p_epoch integer, p_capabilities jsonb,
    p_command_ids uuid[], p_not_afters timestamptz[], p_fingerprints bytea[], p_invalid text[],
    p_job_ids uuid[], p_job_names text[], p_payload_versions integer[], p_profile_ids text[], p_profile_versions integer[],
    p_queues text[], p_priorities text[], p_not_before_ms bigint[], p_payloads bytea[], p_headers jsonb[],
    p_max_executions integer[], p_max_unstarted integer[], p_timeouts bigint[], p_retry jsonb[], p_unstarted jsonb[])
RETURNS SETOF runwick.rw_reply_v1
LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_n integer := pg_catalog.cardinality(p_command_ids);
    v_gate runwick.rw_reply_v1;
    v_receipt runwick.rw_reply_v1;
    v_replies runwick.rw_reply_v1[] := '{}';
    v_fresh integer[] := '{}';
    v_registered integer[];
    v_survivors integer[] := '{}';
    v_i integer;
    v_clock timestamptz;
    v_now timestamptz;
    v_count integer;
BEGIN
    IF v_n NOT BETWEEN 1 AND 8
       OR pg_catalog.array_ndims(p_command_ids) <> 1 OR pg_catalog.array_lower(p_command_ids, 1) <> 1
       OR pg_catalog.cardinality(p_not_afters) <> v_n OR pg_catalog.cardinality(p_fingerprints) <> v_n
       OR pg_catalog.cardinality(p_invalid) <> v_n OR pg_catalog.cardinality(p_job_ids) <> v_n
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
        RAISE EXCEPTION 'unsupported plain dispatch group' USING ERRCODE = 'RWI04';
    END IF;

    -- The gates once for the whole set. A refusal is every command's reply; a replay still has to pass the gate.
    v_gate := runwick.rw_gate_v1(p_application, p_store_id, p_revisions, p_epoch, p_capabilities, true, false);
    IF v_gate.status IS NOT NULL THEN
        RETURN QUERY SELECT (v_gate).*  FROM pg_catalog.generate_series(1, v_n);
        RETURN;
    END IF;

    -- Receipts in ascending command id: every group and every singleton takes the same order, so no two of them wait on
    -- each other for receipt rows.
    FOR v_i IN SELECT a.o::integer FROM unnest(p_command_ids) WITH ORDINALITY AS a(id, o) ORDER BY a.id LOOP
        v_receipt := runwick.rw_receipt_v1(p_application, 'durable', p_command_ids[v_i], 'Dispatch',
            p_fingerprints[v_i], p_not_afters[v_i], interval '10 minutes');
        v_replies[v_i] := v_receipt;
        IF v_receipt.status IS NULL THEN
            v_fresh := pg_catalog.array_append(v_fresh, v_i);
        END IF;
    END LOOP;

    -- Queue registration for the members whose request bounds hold: one join for the set.
    v_registered := ARRAY(SELECT f.i FROM unnest(v_fresh) AS f(i)
                          JOIN runwick.queue_controls AS q ON q.application = p_application AND q.queue = p_queues[f.i]
                          WHERE p_invalid[f.i] IS NULL ORDER BY f.i);

    -- One decision time for the survivors, read after the gate and receipt waits. Each member keeps its own deadline.
    v_clock := pg_catalog.clock_timestamp();
    v_now := runwick.ms_floor(v_clock);
    FOREACH v_i IN ARRAY v_fresh LOOP
        IF p_invalid[v_i] IS NOT NULL THEN
            v_replies[v_i] := runwick.rw_no_effect_v1('InvalidRequest', 'invalid_request', NULL, p_invalid[v_i]);
        ELSIF NOT (v_i = ANY (v_registered)) THEN
            v_replies[v_i] := runwick.rw_no_effect_v1('InvalidRequest', 'queue_not_registered', NULL);
        ELSIF v_clock >= p_not_afters[v_i] THEN
            v_replies[v_i] := runwick.rw_no_effect_v1('CommandExpired', 'command_expired', v_now);
        ELSE
            v_survivors := pg_catalog.array_append(v_survivors, v_i);
            v_replies[v_i] := runwick.rw_applied_v1('Applied', NULL, v_now, pg_catalog.jsonb_build_object(
                'jobId', p_job_ids[v_i], 'disposition', 'Created', 'match', 'None', 'policyDrift', false));
        END IF;
    END LOOP;

    IF pg_catalog.cardinality(v_survivors) > 0 THEN
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

        -- The event of each command carries that command's id and ordinal 0, as a singleton's does.
        INSERT INTO runwick.queue_events (application, job_id, command_id, event_kind, occurred_at, details)
        SELECT p_application, a.job_id, a.command_id, 'dispatched', v_now,
               pg_catalog.jsonb_build_object('generation', 1, 'queue', a.queue, 'priority', a.priority, 'ordinal', 0)
        FROM unnest(p_job_ids, p_command_ids, p_queues, p_priorities) WITH ORDINALITY
             AS a(job_id, command_id, queue, priority, o)
        WHERE a.o = ANY (v_survivors)
        ORDER BY a.o;

        -- A write above may have waited: no member may be reported applied after its own deadline.
        v_clock := pg_catalog.clock_timestamp();
        IF EXISTS (SELECT 1 FROM unnest(v_survivors) AS s(i) WHERE v_clock >= p_not_afters[s.i]) THEN
            RAISE EXCEPTION 'plain dispatch member expired after writes' USING ERRCODE = 'RWB01';
        END IF;
    END IF;

    -- Placeholders this transaction inserted for refused members must not commit (a singleton rolls its own back).
    DELETE FROM runwick.storage_commands AS c
    USING pg_catalog.generate_series(1, v_n) AS a(i)
    WHERE NOT (a.i = ANY (v_survivors))
      AND c.application = p_application AND c.retention_class = 'durable' AND c.command_id = p_command_ids[a.i]
      AND c.result IS NULL AND c.xmin = pg_catalog.xid(pg_catalog.pg_current_xact_id());

    IF pg_catalog.cardinality(v_survivors) > 0 THEN
        UPDATE runwick.storage_commands AS c
        SET status = 'Applied', result = (v_replies[s.i]).result, decided_at = v_now
        FROM unnest(v_survivors) AS s(i)
        WHERE c.application = p_application AND c.retention_class = 'durable'
          AND c.command_id = p_command_ids[s.i] AND c.result IS NULL;
        GET DIAGNOSTICS v_count = ROW_COUNT;
        IF v_count <> pg_catalog.cardinality(v_survivors) THEN
            RAISE EXCEPTION 'rw:receipt_finalization' USING ERRCODE = 'RWI03';
        END IF;
    END IF;

    RETURN QUERY SELECT (v_replies[a.i]).* FROM pg_catalog.generate_series(1, v_n) AS a(i) ORDER BY a.i;
END
$fn$;
