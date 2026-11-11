-- 0021_client_wake (04 Changes 2026-11-11, A1): direct dispatch commits without a transactional NOTIFY.
-- dispatch_v4 and rw_dispatch_core_v4 keep revision 20's dispatch writes, decision time, receipts and fault hooks.
-- Caller-owned transactions, outbox delivery, scheduled jobs and management dispatch retain the transactional v3 path.
SET LOCAL lock_timeout = '5s';

CREATE FUNCTION runwick.rw_dispatch_core_v4(p_application text, p_command_id uuid, p_not_after timestamptz,
    p_continue boolean, p_created_reason text, p_invalid text,
    p_job_ids uuid[], p_job_names text[], p_payload_versions integer[], p_profile_ids text[], p_profile_versions integer[],
    p_queues text[], p_priorities text[], p_not_before_ms bigint[], p_active_keys text[], p_window_keys text[],
    p_window_ms bigint[], p_business bytea[], p_policy bytea[], p_payloads bytea[], p_headers jsonb[],
    p_max_executions integer[], p_max_unstarted integer[], p_timeouts bigint[], p_retry jsonb[], p_unstarted jsonb[],
    p_scopes jsonb[], p_rates jsonb[], p_faults jsonb,
    INOUT p_steps integer, INOUT p_now timestamptz, OUT o_reject runwick.rw_reply_v1, OUT o_items jsonb)
LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_n integer := pg_catalog.cardinality(p_job_ids);
    v_i integer;
    v_j integer;
    v_clock timestamptz;
    v_now timestamptz;
    k_names text[];
    k_kinds text[];
    k_keys text[];
    k_ids text[];
    k_bound uuid[];
    k_business bytea[];
    k_policy bytea[];
    k_version integer[];
    k_expires timestamptz[];
    k_dirty boolean[];
    v_ai integer;
    v_wi integer;
    v_match text;
    v_drift boolean;
    v_job uuid;
    v_results jsonb := '[]'::jsonb;
    v_created integer[] := '{}';
    v_drift_ords integer[] := '{}';
    v_drift_jobs uuid[] := '{}';
    v_drift_matches text[] := '{}';
    v_conflict_ords integer[] := '{}';
    v_conflict_codes text[] := '{}';
    v_conflict_jobs uuid[] := '{}';
    v_detail text;
    v_wrote boolean := false;
    v_seeds integer;
    v_locked integer;
BEGIN
    p_now := NULL;
    o_items := NULL;
    o_reject := NULL;

    -- Validation bounds (03 I-6), computed by the client from the request alone; reported before any write.
    IF p_invalid IS NOT NULL THEN
        o_reject := runwick.rw_no_effect_v1('InvalidRequest', 'invalid_request', NULL, p_invalid);
        RETURN;
    END IF;

    -- Rate scopes are refused until the application activated rate_admission (10 §6.1).
    IF EXISTS (SELECT 1 FROM unnest(p_rates) AS r WHERE pg_catalog.jsonb_array_length(r) > 0)
       AND NOT COALESCE((SELECT c.required_capabilities ? 'rate_admission' FROM runwick.application_controls AS c
                         WHERE c.application = p_application), false) THEN
        o_reject := runwick.rw_no_effect_v1('UnsupportedVersion', 'rate_admission_not_active', NULL);
        RETURN;
    END IF;

    -- 3 (computed first). Key rows, sorted once over the whole batch (bytewise UTF-8 order).
    SELECT COALESCE(pg_catalog.array_agg(x.n ORDER BY x.n COLLATE "C", x.k COLLATE "C", x.v COLLATE "C"), '{}'),
           COALESCE(pg_catalog.array_agg(x.k ORDER BY x.n COLLATE "C", x.k COLLATE "C", x.v COLLATE "C"), '{}'),
           COALESCE(pg_catalog.array_agg(x.v ORDER BY x.n COLLATE "C", x.k COLLATE "C", x.v COLLATE "C"), '{}')
    INTO k_names, k_kinds, k_keys
    FROM (SELECT DISTINCT y.n, y.k, y.v FROM (
              SELECT p_job_names[i] AS n, 'active' AS k, p_active_keys[i] AS v
              FROM pg_catalog.generate_series(1, v_n) AS i WHERE p_active_keys[i] IS NOT NULL
              UNION ALL
              SELECT p_job_names[i], 'window', p_window_keys[i]
              FROM pg_catalog.generate_series(1, v_n) AS i WHERE p_window_keys[i] IS NOT NULL) AS y) AS x;
    k_ids := ARRAY(SELECT u.n || chr(31) || u.k || chr(31) || u.v FROM unnest(k_names, k_kinds, k_keys) WITH ORDINALITY AS u(n, k, v, o)
                   ORDER BY u.o);

    -- 2. Queue registration (plain read).
    IF EXISTS (SELECT 1 FROM unnest(p_queues) AS q(queue)
               WHERE NOT EXISTS (SELECT 1 FROM runwick.queue_controls AS c WHERE c.application = p_application AND c.queue = q.queue)) THEN
        o_reject := runwick.rw_no_effect_v1('InvalidRequest', 'queue_not_registered', NULL);
        RETURN;
    END IF;

    -- 3. Seed the key rows if absent, then lock them in sorted order.
    IF pg_catalog.cardinality(k_names) > 0 THEN
        IF p_steps = 0 THEN
            PERFORM runwick.rw_fault('BeforeFirstMutation', 0, p_faults); -- rw:fault
        END IF;

        -- Retention (17 §3.3) may delete a key row between the seed and the lock: the seed and the lock repeat for the
        -- missing rows, at most 3 times, before the decision time is read.
        v_seeds := 0;
        LOOP
            INSERT INTO runwick.dispatch_keys (application, job_name, key_kind, key, created_at, updated_at)
            SELECT p_application, k.job_name, k.key_kind, k.key,
                   runwick.ms_floor(pg_catalog.clock_timestamp()), runwick.ms_floor(pg_catalog.clock_timestamp())
            FROM unnest(k_names, k_kinds, k_keys) AS k(job_name, key_kind, key)
            ORDER BY k.job_name COLLATE "C", k.key_kind COLLATE "C", k.key COLLATE "C"
            ON CONFLICT DO NOTHING;
            IF v_seeds = 0 THEN
                p_steps := p_steps + 1;
                PERFORM runwick.rw_fault('AfterMutation', p_steps, p_faults); -- rw:fault
            END IF;

            PERFORM 1
            FROM runwick.dispatch_keys AS d
            JOIN unnest(k_names, k_kinds, k_keys) AS k(job_name, key_kind, key)
              ON d.job_name = k.job_name AND d.key_kind = k.key_kind AND d.key = k.key
            WHERE d.application = p_application
            ORDER BY d.job_name, d.key_kind, d.key
            FOR UPDATE OF d;
            GET DIAGNOSTICS v_locked = ROW_COUNT;
            EXIT WHEN v_locked = pg_catalog.cardinality(k_names);
            v_seeds := v_seeds + 1;
            IF v_seeds > 3 THEN
                RAISE EXCEPTION 'rw:key_row_missing' USING ERRCODE = 'RWI05';
            END IF;
        END LOOP;

        SELECT pg_catalog.array_agg(d.bound_job_id ORDER BY u.o), pg_catalog.array_agg(d.business_fingerprint ORDER BY u.o),
               pg_catalog.array_agg(d.policy_fingerprint ORDER BY u.o), pg_catalog.array_agg(d.fingerprint_version ORDER BY u.o),
               pg_catalog.array_agg(d.expires_at ORDER BY u.o), pg_catalog.array_agg(false ORDER BY u.o)
        INTO k_bound, k_business, k_policy, k_version, k_expires, k_dirty
        FROM unnest(k_names, k_kinds, k_keys) WITH ORDINALITY AS u(n, k, v, o)
        LEFT JOIN runwick.dispatch_keys AS d
          ON d.application = p_application AND d.job_name = u.n AND d.key_kind = u.k AND d.key = u.v;
    END IF;

    -- 4. Decide.
    PERFORM runwick.rw_fault('AfterLocksBeforeFreshTime', 0, p_faults); -- rw:fault
    v_clock := pg_catalog.clock_timestamp();
    v_now := runwick.ms_floor(v_clock);
    p_now := v_now;
    IF v_clock >= p_not_after THEN
        o_reject := runwick.rw_no_effect_v1('CommandExpired', 'command_expired', v_now);
        RETURN;
    END IF;

    -- 5. Resolve items in ordinal order against the in-memory key state. A batch without any key creates every item,
    -- so it is resolved in one statement instead of the loop.
    IF pg_catalog.cardinality(k_names) = 0 THEN
        v_created := ARRAY(SELECT pg_catalog.generate_series(1, v_n));
        v_results := COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('ordinal', a.o - 1,
                'jobId', a.job_id, 'disposition', 'Created', 'match', 'None', 'policyDrift', false) ORDER BY a.o)
            FROM unnest(p_job_ids) WITH ORDINALITY AS a(job_id, o)), '[]'::jsonb);
    END IF;

    FOR v_i IN 1 .. CASE WHEN pg_catalog.cardinality(k_names) = 0 THEN 0 ELSE v_n END LOOP
        v_ai := CASE WHEN p_active_keys[v_i] IS NOT NULL
                     THEN pg_catalog.array_position(k_ids, p_job_names[v_i] || chr(31) || 'active' || chr(31) || p_active_keys[v_i]) END;
        v_wi := CASE WHEN p_window_keys[v_i] IS NOT NULL
                     THEN pg_catalog.array_position(k_ids, p_job_names[v_i] || chr(31) || 'window' || chr(31) || p_window_keys[v_i]) END;

        IF v_wi IS NOT NULL AND k_bound[v_wi] IS NOT NULL AND k_expires[v_wi] > v_now THEN
            IF k_version[v_wi] IS DISTINCT FROM 1 THEN
                o_reject := runwick.rw_no_effect_v1('UnsupportedVersion', 'fingerprint_version_unsupported', v_now);
                RETURN;
            END IF;

            IF k_business[v_wi] IS DISTINCT FROM p_business[v_i] THEN
                v_conflict_ords := v_conflict_ords || (v_i - 1);
                v_conflict_codes := v_conflict_codes || 'window_fingerprint_mismatch'::text;
                v_conflict_jobs := v_conflict_jobs || k_bound[v_wi];
                v_results := v_results || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
                    'ordinal', v_i - 1, 'failureCode', 'uniqueness_conflict', 'conflictingJobId', k_bound[v_wi]));
                CONTINUE;
            END IF;

            IF v_ai IS NOT NULL AND k_bound[v_ai] IS NOT NULL AND k_bound[v_ai] <> k_bound[v_wi] THEN
                v_conflict_ords := v_conflict_ords || (v_i - 1);
                v_conflict_codes := v_conflict_codes || 'uniqueness_composition_conflict'::text;
                v_conflict_jobs := v_conflict_jobs || k_bound[v_ai];
                v_results := v_results || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
                    'ordinal', v_i - 1, 'failureCode', 'uniqueness_composition_conflict', 'conflictingJobId', k_bound[v_ai]));
                CONTINUE;
            END IF;

            v_match := CASE WHEN v_ai IS NOT NULL AND k_bound[v_ai] = k_bound[v_wi] THEN 'ActiveAndWindow' ELSE 'Window' END;
            v_drift := k_policy[v_wi] IS DISTINCT FROM p_policy[v_i];
            v_results := v_results || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('ordinal', v_i - 1,
                'jobId', k_bound[v_wi], 'disposition', 'Reused', 'match', v_match, 'policyDrift', v_drift));
            IF v_drift THEN
                v_drift_ords := v_drift_ords || (v_i - 1);
                v_drift_jobs := v_drift_jobs || k_bound[v_wi];
                v_drift_matches := v_drift_matches || v_match;
            END IF;

            CONTINUE;
        END IF;

        IF v_ai IS NOT NULL AND k_bound[v_ai] IS NOT NULL THEN
            v_job := k_bound[v_ai];
            IF v_wi IS NOT NULL THEN
                k_bound[v_wi] := v_job;
                k_business[v_wi] := p_business[v_i];
                k_policy[v_wi] := p_policy[v_i];
                k_version[v_wi] := 1;
                k_expires[v_wi] := runwick.ms_floor(v_now + p_window_ms[v_i] * interval '1 millisecond');
                k_dirty[v_wi] := true;
            END IF;

            v_results := v_results || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('ordinal', v_i - 1,
                'jobId', v_job, 'disposition', 'Reused', 'match', 'Active', 'policyDrift', false));
            CONTINUE;
        END IF;

        v_job := p_job_ids[v_i];
        IF v_ai IS NOT NULL THEN
            k_bound[v_ai] := v_job;
            k_dirty[v_ai] := true;
        END IF;

        IF v_wi IS NOT NULL THEN
            k_bound[v_wi] := v_job;
            k_business[v_wi] := p_business[v_i];
            k_policy[v_wi] := p_policy[v_i];
            k_version[v_wi] := 1;
            k_expires[v_wi] := runwick.ms_floor(v_now + p_window_ms[v_i] * interval '1 millisecond');
            k_dirty[v_wi] := true;
        END IF;

        v_created := v_created || v_i;
        v_results := v_results || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('ordinal', v_i - 1,
            'jobId', v_job, 'disposition', 'Created', 'match', 'None', 'policyDrift', false));
    END LOOP;

    IF pg_catalog.cardinality(v_conflict_ords) > 0 AND NOT p_continue THEN
        IF v_n > 1 THEN
            v_detail := '[' || (SELECT pg_catalog.string_agg(pg_catalog.format('{"ordinal":%s,"code":"%s","conflictingJobId":%s}',
                    c.o, CASE c.code WHEN 'window_fingerprint_mismatch' THEN 'uniqueness_conflict' ELSE c.code END,
                    COALESCE('"' || c.job::text || '"', 'null')), ',' ORDER BY c.idx)
                FROM unnest(v_conflict_ords, v_conflict_codes, v_conflict_jobs) WITH ORDINALITY AS c(o, code, job, idx)) || ']';
        END IF;

        o_reject := runwick.rw_no_effect_v1('UniquenessConflict', v_conflict_codes[1], v_now, v_detail, v_conflict_jobs[1]);
        RETURN;
    END IF;

    v_wrote := true = ANY (k_dirty) OR pg_catalog.cardinality(v_created) > 0 OR pg_catalog.cardinality(v_drift_ords) > 0;
    IF v_wrote AND p_steps = 0 THEN
        PERFORM runwick.rw_fault('BeforeFirstMutation', 0, p_faults); -- rw:fault
    END IF;

    -- 6. Key writes (rows already locked), in sorted order.
    FOR v_j IN 1 .. COALESCE(pg_catalog.cardinality(k_names), 0) LOOP
        CONTINUE WHEN NOT k_dirty[v_j];
        UPDATE runwick.dispatch_keys
        SET bound_job_id = k_bound[v_j], business_fingerprint = k_business[v_j], policy_fingerprint = k_policy[v_j],
            fingerprint_version = k_version[v_j], expires_at = k_expires[v_j], updated_at = v_now
        WHERE application = p_application AND job_name = k_names[v_j] AND key_kind = k_kinds[v_j] AND key = k_keys[v_j];
    END LOOP;

    -- 7. New jobs, payloads, generations in ascending job_id. Each statement reads the items as rows (one unnest of
    -- the parallel arrays) instead of indexing every array once per item.
    IF pg_catalog.cardinality(v_created) > 0 THEN
        INSERT INTO runwick.jobs (
            application, job_id, job_name, payload_version, serializer_profile_id, serializer_profile_version,
            queue, priority, state, eligible_at, created_at, updated_at, active_unique_key)
        SELECT p_application, a.job_id, a.job_name, a.payload_version, a.profile_id, a.profile_version,
               a.queue, a.priority, 'pending',
               runwick.ms_ceil(GREATEST(COALESCE('epoch'::timestamptz + a.not_before_ms * interval '1 millisecond', v_now), v_now)),
               v_now, v_now, a.active_key
        FROM unnest(p_job_ids, p_job_names, p_payload_versions, p_profile_ids, p_profile_versions, p_queues, p_priorities,
                    p_not_before_ms, p_active_keys) WITH ORDINALITY
             AS a(job_id, job_name, payload_version, profile_id, profile_version, queue, priority, not_before_ms, active_key, o)
        WHERE a.o = ANY (v_created)
        ORDER BY a.job_id;

        INSERT INTO runwick.job_payloads (
            application, job_id, payload_utf8, payload_encoding_version, serializer_profile_id, serializer_profile_version, headers)
        SELECT p_application, a.job_id, a.payload, 1, a.profile_id, a.profile_version,
               CASE WHEN a.headers -> 'schedule' -> 'scheduledAt' = 'null'::jsonb
                    -- A manual trigger: its occurrence is the decision time of the command that creates the job.
                    THEN pg_catalog.jsonb_set(a.headers, '{schedule}', (a.headers -> 'schedule')
                         || pg_catalog.jsonb_build_object('scheduledAt', runwick.rw_ms_v1(v_now), 'coveredFrom', runwick.rw_ms_v1(v_now),
                                                          'coveredUntil', runwick.rw_ms_v1(v_now)))
                    ELSE a.headers END
        FROM unnest(p_job_ids, p_payloads, p_profile_ids, p_profile_versions, p_headers) WITH ORDINALITY
             AS a(job_id, payload, profile_id, profile_version, headers, o)
        WHERE a.o = ANY (v_created)
        ORDER BY a.job_id;

        INSERT INTO runwick.job_generations (
            application, job_id, generation, created_at, created_reason,
            max_executions, max_unstarted_abandonments, execution_timeout_ms, retry_policy, unstarted_retry_policy)
        SELECT p_application, a.job_id, 1, v_now, p_created_reason, a.max_executions, a.max_unstarted,
               a.timeout, a.retry, a.unstarted
        FROM unnest(p_job_ids, p_max_executions, p_max_unstarted, p_timeouts, p_retry, p_unstarted) WITH ORDINALITY
             AS a(job_id, max_executions, max_unstarted, timeout, retry, unstarted, o)
        WHERE a.o = ANY (v_created)
        ORDER BY a.job_id;

        -- 8. Scope provisioning (seed-if-absent in (kind, key) order; the first item's capacity in job order wins),
        -- then requirements in ascending scope_id.
        IF EXISTS (SELECT 1 FROM unnest(v_created) AS c(i) WHERE pg_catalog.jsonb_array_length(p_scopes[c.i]) > 0) THEN
            INSERT INTO runwick.concurrency_scopes (
                application, scope_id, scope_kind, scope_key, capacity, fencing_enabled, created_at, updated_at)
            SELECT p_application, runwick.rw_uuid_v7(pg_catalog.clock_timestamp()), s.kind, s.key, s.capacity,
                   (s.kind = 'resource' AND s.capacity = 1),
                   runwick.ms_floor(pg_catalog.clock_timestamp()), runwick.ms_floor(pg_catalog.clock_timestamp())
            FROM (SELECT DISTINCT ON (e ->> 'kind', e ->> 'key') e ->> 'kind' AS kind, e ->> 'key' AS key, (e ->> 'capacity')::integer AS capacity
                  FROM unnest(v_created) AS c(i)
                  CROSS JOIN LATERAL pg_catalog.jsonb_array_elements(p_scopes[c.i]) WITH ORDINALITY AS x(e, o)
                  ORDER BY e ->> 'kind', e ->> 'key', p_job_ids[c.i], x.o) AS s
            ORDER BY s.kind COLLATE "C", s.key COLLATE "C"
            ON CONFLICT (application, scope_kind, scope_key) DO NOTHING;

            INSERT INTO runwick.job_concurrency_requirements (application, job_id, scope_id)
            SELECT DISTINCT p_application, p_job_ids[c.i], cs.scope_id
            FROM unnest(v_created) AS c(i)
            CROSS JOIN LATERAL pg_catalog.jsonb_array_elements(p_scopes[c.i]) AS x(e)
            JOIN runwick.concurrency_scopes AS cs
              ON cs.application = p_application AND cs.scope_kind = x.e ->> 'kind' AND cs.scope_key = x.e ->> 'key'
            ORDER BY cs.scope_id, p_job_ids[c.i];
        END IF;

        IF EXISTS (SELECT 1 FROM unnest(v_created) AS c(i) WHERE pg_catalog.jsonb_array_length(p_rates[c.i]) > 0) THEN
            INSERT INTO runwick.rate_scopes (
                application, rate_scope_id, scope_kind, scope_key, limit_count, window_ms, created_at, updated_at)
            SELECT p_application, runwick.rw_uuid_v7(pg_catalog.clock_timestamp()), s.kind, s.key, s.lim, s.win,
                   runwick.ms_floor(pg_catalog.clock_timestamp()), runwick.ms_floor(pg_catalog.clock_timestamp())
            FROM (SELECT DISTINCT ON (e ->> 'kind', e ->> 'key') e ->> 'kind' AS kind, e ->> 'key' AS key,
                         (e ->> 'limit')::integer AS lim, (e ->> 'windowMs')::bigint AS win
                  FROM unnest(v_created) AS c(i)
                  CROSS JOIN LATERAL pg_catalog.jsonb_array_elements(p_rates[c.i]) WITH ORDINALITY AS x(e, o)
                  ORDER BY e ->> 'kind', e ->> 'key', p_job_ids[c.i], x.o) AS s
            ORDER BY s.kind COLLATE "C", s.key COLLATE "C"
            ON CONFLICT (application, scope_kind, scope_key) DO NOTHING;

            INSERT INTO runwick.job_rate_requirements (application, job_id, rate_scope_id)
            SELECT DISTINCT p_application, p_job_ids[c.i], rs.rate_scope_id
            FROM unnest(v_created) AS c(i)
            CROSS JOIN LATERAL pg_catalog.jsonb_array_elements(p_rates[c.i]) AS x(e)
            JOIN runwick.rate_scopes AS rs
              ON rs.application = p_application AND rs.scope_kind = x.e ->> 'kind' AND rs.scope_key = x.e ->> 'key'
            ORDER BY rs.rate_scope_id, p_job_ids[c.i];
        END IF;
    END IF;

    -- 9. Events. The driver may wake each due queue after it confirms COMMIT.
    IF pg_catalog.cardinality(v_created) > 0 OR pg_catalog.cardinality(v_drift_ords) > 0 THEN
        INSERT INTO runwick.queue_events (application, job_id, command_id, event_kind, occurred_at, details)
        SELECT p_application, e.job_id, p_command_id, e.kind, v_now, e.details
        FROM (SELECT 1 AS part, a.o AS o, a.job_id, 'dispatched' AS kind,
                     pg_catalog.jsonb_build_object('generation', 1, 'queue', a.queue, 'priority', a.priority,
                                                   'ordinal', a.o - 1) AS details
              FROM unnest(p_job_ids, p_queues, p_priorities) WITH ORDINALITY AS a(job_id, queue, priority, o)
              WHERE a.o = ANY (v_created)
              UNION ALL
              SELECT 2, d.o, d.job, 'policy_drift', pg_catalog.jsonb_build_object('ordinal', d.ord, 'match', d.m)
              FROM unnest(v_drift_ords, v_drift_jobs, v_drift_matches) WITH ORDINALITY AS d(ord, job, m, o)) AS e
        ORDER BY e.part, e.o;
    END IF;

    IF v_wrote THEN
        p_steps := p_steps + 1;
        PERFORM runwick.rw_fault('AfterMutation', p_steps, p_faults); -- rw:fault
    END IF;

    -- 10. Final deadline guard: provisioning may have waited after the decision.
    IF pg_catalog.clock_timestamp() >= p_not_after THEN
        o_reject := runwick.rw_no_effect_v1('CommandExpired', 'command_expired', v_now);
        RETURN;
    END IF;

    o_items := v_results;
END
$fn$;

-- ------------------------------------------------------------------ Dispatch/1, DispatchMany/1 (04 §7.2)

CREATE FUNCTION runwick.dispatch_v4(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
    p_fingerprint bytea, p_class text, p_deadline_cap interval, p_store_id uuid, p_revisions bigint[], p_epoch integer,
    p_capabilities jsonb, p_is_creation boolean, p_allow_drain boolean, p_staged boolean,
    p_many boolean, p_continue boolean, p_invalid text,
    p_job_ids uuid[], p_job_names text[], p_payload_versions integer[], p_profile_ids text[], p_profile_versions integer[],
    p_queues text[], p_priorities text[], p_not_before_ms bigint[], p_active_keys text[], p_window_keys text[],
    p_window_ms bigint[], p_business bytea[], p_policy bytea[], p_payloads bytea[], p_headers jsonb[],
    p_max_executions integer[], p_max_unstarted integer[], p_timeouts bigint[], p_retry jsonb[], p_unstarted jsonb[],
    p_scopes jsonb[], p_rates jsonb[],
    p_faults jsonb DEFAULT NULL)
RETURNS runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_reply runwick.rw_reply_v1;
    v_steps integer := 0;
    v_now timestamptz;
    v_reject runwick.rw_reply_v1;
    v_core record;
    v_items jsonb;
    v_result jsonb;
BEGIN
    v_reply := runwick.rw_frame_v1(p_application, p_command_id, p_operation, p_not_after, p_fingerprint, p_class, p_deadline_cap,
        p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain, p_staged);
    IF v_reply.status IS NOT NULL THEN
        RETURN v_reply;
    END IF;

    SELECT c.p_steps, c.p_now, c.o_reject, c.o_items INTO v_core
    FROM runwick.rw_dispatch_core_v4(p_application, p_command_id, p_not_after, p_continue, 'initial', p_invalid,
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

    v_result := CASE WHEN p_many THEN pg_catalog.jsonb_build_object('items', v_items) ELSE (v_items -> 0) - 'ordinal' END;
    PERFORM runwick.rw_finalize_v1(p_application, p_class, p_command_id, 'Applied', v_result, v_now);
    RETURN runwick.rw_applied_v1('Applied', NULL, v_now, v_result);
END
$fn$;
