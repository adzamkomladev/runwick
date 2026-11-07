-- 0014_set_dispatch (set-based dispatch, 04 Changes 2026-11-07, set dispatch): the dispatch core reads a batch's items
-- as rows, once per statement, and resolves a batch without uniqueness keys in one statement instead of a loop over its
-- items. Nothing changes on the wire, in the receipt, in the rows written, in their values or order, or in the lock
-- order; only how the database computes them.
--
-- A batch without any active or window key creates every item, in ordinal order, so step 5 builds the created list and
-- the per-item results in one statement. A batch with a key keeps the ordinal loop (an item can reuse the job an earlier
-- item of the same batch bound). Steps 7 and 9 select each item's fields from one unnest of the parallel arrays
-- (WITH ORDINALITY, filtered to the created ordinals) instead of subscripting each array per item. A released function
-- is never replaced in place (04 §7.17, PF-05), so rw_dispatch_core_v1 gives way to rw_dispatch_core_v2 and its three
-- callers to dispatch_v2, receive_outbox_v2 and materialize_occurrences_v2, which differ from their v1 only in the
-- core they call. Stop-and-migrate: no binary of revision 13 runs against revision 14.
--
-- Rules and fault hooks: as 0009_functions (LANGUAGE plpgsql VOLATILE, no SET clauses, no EXCEPTION blocks; lines ending
-- in "-- rw:fault" exist only in the fault-hooks variant).
SET LOCAL lock_timeout = '5s';

DROP FUNCTION runwick.dispatch_v1(text, uuid, text, timestamptz, bytea, text, interval, uuid, bigint[], integer, jsonb, boolean, boolean, boolean,
    boolean, boolean, text, uuid[], text[], integer[], text[], integer[], text[], text[], bigint[], text[], text[], bigint[], bytea[], bytea[], bytea[], jsonb[], integer[], integer[],
    bigint[], jsonb[], jsonb[], jsonb[], jsonb[], jsonb);
DROP FUNCTION runwick.receive_outbox_v1(text, uuid, text, timestamptz, bytea, text, interval, uuid, bigint[], integer, jsonb, boolean, boolean, boolean,
    uuid, uuid, bytea, timestamptz, text, text, uuid[], text[], integer[], text[], integer[], text[], text[], bigint[], text[], text[], bigint[], bytea[], bytea[], bytea[], jsonb[], integer[], integer[],
    bigint[], jsonb[], jsonb[], jsonb[], jsonb[], jsonb);
DROP FUNCTION runwick.materialize_occurrences_v1(text, uuid, text, timestamptz, bytea, text, interval, uuid, bigint[], integer, jsonb, boolean, boolean, boolean,
    uuid, bigint, bigint, bigint, uuid, timestamptz, bigint, timestamptz[], text[], timestamptz[], timestamptz[], boolean[],
    timestamptz, timestamptz, timestamptz, uuid, timestamptz, integer, integer, text, boolean, timestamptz[], timestamptz[], text[],
    text, uuid[], text[], integer[], text[], integer[], text[], text[], bigint[], text[], text[], bigint[], bytea[], bytea[], bytea[], jsonb[], integer[], integer[],
    bigint[], jsonb[], jsonb[], jsonb[], jsonb[], jsonb);
DROP FUNCTION runwick.rw_dispatch_core_v1(text, uuid, timestamptz, boolean, text, text, uuid[], text[], integer[], text[], integer[], text[], text[], bigint[], text[], text[], bigint[], bytea[], bytea[], bytea[], jsonb[], integer[], integer[],
    bigint[], jsonb[], jsonb[], jsonb[], jsonb[], jsonb, integer, timestamptz);

-- ------------------------------------------------------------------ dispatch core (04 §7.2 steps 2-10)

CREATE FUNCTION runwick.rw_dispatch_core_v2(p_application text, p_command_id uuid, p_not_after timestamptz,
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
    v_wake text[];
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

        INSERT INTO runwick.dispatch_keys (application, job_name, key_kind, key, created_at, updated_at)
        SELECT p_application, k.job_name, k.key_kind, k.key,
               runwick.ms_floor(pg_catalog.clock_timestamp()), runwick.ms_floor(pg_catalog.clock_timestamp())
        FROM unnest(k_names, k_kinds, k_keys) AS k(job_name, key_kind, key)
        ORDER BY k.job_name COLLATE "C", k.key_kind COLLATE "C", k.key COLLATE "C"
        ON CONFLICT DO NOTHING;
        p_steps := p_steps + 1;
        PERFORM runwick.rw_fault('AfterMutation', p_steps, p_faults); -- rw:fault

        PERFORM 1
        FROM runwick.dispatch_keys AS d
        JOIN unnest(k_names, k_kinds, k_keys) AS k(job_name, key_kind, key)
          ON d.job_name = k.job_name AND d.key_kind = k.key_kind AND d.key = k.key
        WHERE d.application = p_application
        ORDER BY d.job_name, d.key_kind, d.key
        FOR UPDATE OF d;

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

    -- 9. Events, with the wake notification (05 §7.2, PF-07): one per distinct queue with a job eligible now,
    -- delivered by PostgreSQL only if this transaction commits.
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

        v_wake := ARRAY(SELECT q.queue FROM (
                      SELECT a.queue, min(a.o) AS first
                      FROM unnest(p_queues, p_not_before_ms) WITH ORDINALITY AS a(queue, not_before_ms, o)
                      WHERE a.o = ANY (v_created)
                        AND (a.not_before_ms IS NULL OR a.not_before_ms <= runwick.rw_ms_v1(runwick.ms_ceil(v_now)))
                      GROUP BY a.queue) AS q ORDER BY q.first);
        PERFORM pg_catalog.pg_notify('runwick_wake', p_application || '/' || w.queue) FROM unnest(v_wake) AS w(queue);
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

CREATE FUNCTION runwick.dispatch_v2(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
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
    FROM runwick.rw_dispatch_core_v2(p_application, p_command_id, p_not_after, p_continue, 'initial', p_invalid,
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

-- ------------------------------------------------------------------ ReceiveOutbox/1 (14 §5)

-- Lock order: gates -> receipt -> delivery identity (transaction advisory lock) -> dispatch keys -> new jobs/scopes.
CREATE FUNCTION runwick.receive_outbox_v2(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
    p_fingerprint bytea, p_class text, p_deadline_cap interval, p_store_id uuid, p_revisions bigint[], p_epoch integer,
    p_capabilities jsonb, p_is_creation boolean, p_allow_drain boolean, p_staged boolean,
    p_source_store_id uuid, p_message_id uuid, p_delivery_fingerprint bytea, p_accept_until timestamptz, p_profile text,
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
    v_found boolean;
    v_job uuid;
    v_disposition text;
    v_match text;
    v_drift boolean;
    v_delivered timestamptz;
    v_accept timestamptz;
    v_confirmed text;
    v_stored_fingerprint bytea;
    v_clock timestamptz;
    v_now timestamptz;
    v_steps integer := 0;
    v_reject runwick.rw_reply_v1;
    v_core record;
    v_items jsonb;
    v_n integer;
    v_result jsonb;
BEGIN
    v_reply := runwick.rw_frame_v1(p_application, p_command_id, p_operation, p_not_after, p_fingerprint, p_class, p_deadline_cap,
        p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain, p_staged);
    IF v_reply.status IS NOT NULL THEN
        RETURN v_reply;
    END IF;

    -- The delivery identity: serializes every transport command for one identity without a placeholder row.
    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
        'runwick.outbox/' || p_application || '/' || p_source_store_id::text || '/' || p_message_id::text, 0));
    SELECT d.job_id, d.disposition, d.duplicate_match, d.policy_drift, d.delivered_at, d.accept_until, d.confirmed_profile,
           d.delivery_fingerprint
    INTO v_job, v_disposition, v_match, v_drift, v_delivered, v_accept, v_confirmed, v_stored_fingerprint
    FROM runwick.outbox_deliveries AS d
    WHERE d.application = p_application AND d.source_store_id = p_source_store_id AND d.message_id = p_message_id
    FOR UPDATE;
    v_found := FOUND;

    PERFORM runwick.rw_fault('AfterLocksBeforeFreshTime', 0, p_faults); -- rw:fault
    v_clock := pg_catalog.clock_timestamp();
    v_now := runwick.ms_floor(v_clock);
    IF v_clock >= p_not_after THEN
        RETURN runwick.rw_no_effect_v1('CommandExpired', 'command_expired', v_now);
    END IF;

    IF p_not_after - v_now > p_deadline_cap THEN
        RETURN runwick.rw_no_effect_v1('InvalidRequest', 'deadline_too_far', v_now);
    END IF;

    -- A retained mapping answers every later transport command for the identity, even after AcceptUntil.
    IF v_found THEN
        IF v_stored_fingerprint <> p_delivery_fingerprint THEN
            RETURN runwick.rw_no_effect_v1('CommandConflict', 'delivery_conflict', v_now, NULL, v_job);
        END IF;

        v_result := pg_catalog.jsonb_build_object('jobId', v_job, 'disposition', v_disposition, 'match', v_match,
            'policyDrift', v_drift, 'firstDelivery', false, 'deliveredAt', runwick.rw_ms_v1(v_delivered),
            'acceptUntil', runwick.rw_ms_v1(v_accept), 'confirmedProfile', v_confirmed);
        PERFORM runwick.rw_finalize_v1(p_application, p_class, p_command_id, 'Applied', v_result, v_now);
        RETURN runwick.rw_applied_v1('Applied', NULL, v_now, v_result);
    END IF;

    IF v_now >= p_accept_until THEN
        RETURN runwick.rw_no_effect_v1('CommandExpired', 'delivery_horizon_passed', v_now);
    END IF;

    SELECT c.p_steps, c.p_now, c.o_reject, c.o_items INTO v_core
    FROM runwick.rw_dispatch_core_v2(p_application, p_command_id, p_not_after, false, 'initial', p_invalid,
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

    -- The dispatch core re-read the clock after its own locks: the cutoff is checked at that decision instant.
    IF v_now >= p_accept_until THEN
        RETURN runwick.rw_no_effect_v1('CommandExpired', 'delivery_horizon_passed', v_now);
    END IF;

    v_job := (v_items -> 0 ->> 'jobId')::uuid;
    INSERT INTO runwick.outbox_deliveries (
        application, source_store_id, message_id, delivery_fingerprint, fingerprint_version, accept_until, retain_until,
        job_id, disposition, duplicate_match, policy_drift, confirmed_profile, first_command_id, delivered_at)
    VALUES (p_application, p_source_store_id, p_message_id, p_delivery_fingerprint, 1, p_accept_until, p_accept_until + interval '24 hours',
            v_job, v_items -> 0 ->> 'disposition', v_items -> 0 ->> 'match', (v_items -> 0 ->> 'policyDrift')::boolean,
            p_profile, p_command_id, v_now);
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n <> 1 THEN
        RAISE EXCEPTION 'rw:invalid_transition' USING ERRCODE = 'RWI01';
    END IF;

    v_steps := v_steps + 1;
    PERFORM runwick.rw_fault('AfterMutation', v_steps, p_faults); -- rw:fault
    v_result := pg_catalog.jsonb_build_object('jobId', v_job, 'disposition', v_items -> 0 ->> 'disposition',
        'match', v_items -> 0 ->> 'match', 'policyDrift', (v_items -> 0 ->> 'policyDrift')::boolean, 'firstDelivery', true,
        'deliveredAt', runwick.rw_ms_v1(v_now), 'acceptUntil', runwick.rw_ms_v1(p_accept_until), 'confirmedProfile', p_profile);
    PERFORM runwick.rw_finalize_v1(p_application, p_class, p_command_id, 'Applied', v_result, v_now);
    RETURN runwick.rw_applied_v1('Applied', NULL, v_now, v_result);
END
$fn$;

-- ------------------------------------------------------------------ MaterializeOccurrences/1 (13 §4.2, §6.2)

-- The dispatch items are built by the client from the definition at p_expected_definition (immutable per revision);
-- the function checks the plan against the locked schedule row before it uses them.
CREATE FUNCTION runwick.materialize_occurrences_v2(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
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
    v_reply := runwick.rw_frame_v1(p_application, p_command_id, p_operation, p_not_after, p_fingerprint, p_class, p_deadline_cap,
        p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain, p_staged);
    IF v_reply.status IS NOT NULL THEN
        RETURN v_reply;
    END IF;

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
        FROM runwick.rw_dispatch_core_v2(p_application, p_command_id, p_not_after, false, 'scheduled', p_invalid,
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
