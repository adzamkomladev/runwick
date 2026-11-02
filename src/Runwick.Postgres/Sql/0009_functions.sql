-- 0009_functions (PF-05, 04 Changes 2026-11-02): one versioned PL/pgSQL function per hot-path operation (04 §7.17).
-- Additive only: the store_metadata.fault_hooks column, the reply type and the functions. The protocol does not change,
-- so the manifest lists the same operations and versions as revision 8.
--
-- Rules (04 §7.17): LANGUAGE plpgsql VOLATILE SECURITY INVOKER, no SET clauses, no EXCEPTION blocks, explicit column
-- lists into scalar variables, PostgreSQL 16 syntax. The statements are the 04 §7 statements in their written order.
-- A released function is never replaced: a changed function is a new versioned name in a new migration.
--
-- Fault hooks (07 §2.3): every line ending in "-- rw:fault" and every block between "-- rw:fault-begin" and
-- "-- rw:fault-end" exists only in the fault-hooks variant that the test harness migrates. The production variant
-- (RunwickPostgresMigrations.MigrateAsync) strips them before executing, so production bodies contain no hook and
-- runwick.rw_fault does not exist.
SET LOCAL lock_timeout = '5s';

ALTER TABLE runwick.store_metadata ADD COLUMN fault_hooks boolean NOT NULL DEFAULT false;

-- rw:fault-begin
UPDATE runwick.store_metadata SET fault_hooks = true WHERE singleton_id = 1;
-- rw:fault-end

-- One reply row per call (04 §7.17). status/effect are StoreStatus/StoreEffect names; result is the receipt codec v1
-- result the call wrote (or replayed), or the operation's value for the operations without a receipt.
CREATE TYPE runwick.rw_reply_v1 AS (
    status              text,
    effect              text,
    code                text,
    detail              text,
    conflicting_job_id  uuid,
    blocked_scope_id    uuid,
    resume_eligible_at  timestamptz,
    resume_job_id       uuid,
    decided_at          timestamptz,
    replayed            boolean,
    result              jsonb,
    payload             bytea,
    headers             jsonb,
    created_at          timestamptz
);

-- ------------------------------------------------------------------ shared internal functions

CREATE FUNCTION runwick.rw_no_effect_v1(p_status text, p_code text, p_decided_at timestamptz, p_detail text DEFAULT NULL,
    p_conflicting_job_id uuid DEFAULT NULL, p_blocked_scope_id uuid DEFAULT NULL, p_resume_eligible_at timestamptz DEFAULT NULL,
    p_resume_job_id uuid DEFAULT NULL)
RETURNS runwick.rw_reply_v1 LANGUAGE sql IMMUTABLE AS $fn$
    SELECT ROW(p_status, 'ObservedNoEffect', p_code, p_detail, p_conflicting_job_id, p_blocked_scope_id, p_resume_eligible_at,
               p_resume_job_id, p_decided_at, false, NULL::jsonb, NULL::bytea, NULL::jsonb, NULL::timestamptz)::runwick.rw_reply_v1
$fn$;

CREATE FUNCTION runwick.rw_applied_v1(p_status text, p_code text, p_decided_at timestamptz, p_result jsonb, p_detail text DEFAULT NULL)
RETURNS runwick.rw_reply_v1 LANGUAGE sql IMMUTABLE AS $fn$
    SELECT ROW(p_status, 'Applied', p_code, p_detail, NULL::uuid, NULL::uuid, NULL::timestamptz, NULL::uuid, p_decided_at, false,
               p_result, NULL::bytea, NULL::jsonb, NULL::timestamptz)::runwick.rw_reply_v1
$fn$;

-- Epoch milliseconds of an instant (receipt codec v1, 03 §8.5).
CREATE FUNCTION runwick.rw_ms_v1(p_t timestamptz) RETURNS bigint LANGUAGE sql IMMUTABLE AS $fn$
    SELECT (pg_catalog.date_part('epoch', p_t)::numeric * 1000)::bigint
$fn$;

-- A version 7 UUID from the decision time, with the layout of Guid.CreateVersion7(DateTimeOffset): 48-bit Unix
-- milliseconds, version 7, variant 10, the rest random. PostgreSQL 16-17 have no uuidv7().
CREATE FUNCTION runwick.rw_uuid_v7(p_ts timestamptz) RETURNS uuid LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_bytes bytea;
BEGIN
    v_bytes := pg_catalog.decode(pg_catalog.lpad(pg_catalog.to_hex(runwick.rw_ms_v1(runwick.ms_floor(p_ts))), 12, '0'), 'hex')
        || pg_catalog.substr(pg_catalog.uuid_send(pg_catalog.gen_random_uuid()), 7, 10);
    v_bytes := pg_catalog.set_byte(v_bytes, 6, (pg_catalog.get_byte(v_bytes, 6) & 15) | 112);
    v_bytes := pg_catalog.set_byte(v_bytes, 8, (pg_catalog.get_byte(v_bytes, 8) & 63) | 128);
    RETURN pg_catalog.encode(v_bytes, 'hex')::uuid;
END
$fn$;

-- exponential-v1 with equal jitter (RetryEvaluator.Delay): b = min(max, initial * 2^(n-1)) saturating, then a delay drawn
-- uniformly from ceil(b/2) through b. The entropy is the client's unsigned 64-bit draw, sent as its two's complement.
CREATE FUNCTION runwick.rw_retry_delay_v1(p_n integer, p_initial_ms bigint, p_max_ms bigint, p_entropy bigint)
RETURNS bigint LANGUAGE plpgsql IMMUTABLE AS $fn$
DECLARE
    v_b numeric;
    v_low numeric;
    v_u numeric;
BEGIN
    IF p_n < 1 THEN
        RAISE EXCEPTION 'rw:retry_n' USING ERRCODE = 'RWI04';
    END IF;

    IF p_n - 1 >= 62 OR p_initial_ms > (9223372036854775807::bigint >> (p_n - 1)) THEN
        v_b := p_max_ms;
    ELSE
        v_b := LEAST(p_max_ms, p_initial_ms << (p_n - 1));
    END IF;

    v_low := pg_catalog.div(v_b + 1, 2);
    v_u := CASE WHEN p_entropy < 0 THEN p_entropy::numeric + 18446744073709551616 ELSE p_entropy::numeric END;
    RETURN (v_low + pg_catalog.mod(v_u, v_b - v_low + 1))::bigint;
END
$fn$;

-- rw:fault-begin
-- Test hook (07 §2.3, PF-05): when the harness planned this point for the call (p_faults.points), the backend reports
-- the point and waits on the call's gate lock, which the harness holds while its in-process injector handles the point
-- (Throw, Delay, Barrier, KillProcess exactly as at a client point). The harness then marks "proceed" or "throw" and
-- releases the gate. No mark (the harness process died) aborts the transaction.
CREATE FUNCTION runwick.rw_fault(p_point text, p_step integer, p_faults jsonb) RETURNS void
LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_nonce integer;
    v_seq integer;
    v_state text;
    v_lock text;
BEGIN
    IF p_faults IS NULL OR NOT (p_faults -> 'points') ? p_point THEN
        RETURN;
    END IF;

    v_nonce := (p_faults ->> 'nonce')::integer;
    v_state := pg_catalog.current_setting('runwick.fault_seq', true);
    v_seq := CASE WHEN v_state LIKE v_nonce::text || ':%' THEN pg_catalog.split_part(v_state, ':', 2)::integer + 1 ELSE 1 END;
    PERFORM pg_catalog.set_config('runwick.fault_seq', v_nonce::text || ':' || v_seq::text, true);
    PERFORM pg_catalog.pg_advisory_xact_lock_shared(v_nonce, 1000000 + v_seq * 10000
        + pg_catalog.array_position(ARRAY['AfterLocksBeforeFreshTime', 'BeforeFirstMutation', 'AfterMutation'], p_point) * 100 + p_step);
    v_lock := pg_catalog.current_setting('lock_timeout');
    PERFORM pg_catalog.set_config('lock_timeout', '0', true);
    PERFORM pg_catalog.pg_advisory_xact_lock_shared(v_nonce, 10000 + v_seq);
    PERFORM pg_catalog.set_config('lock_timeout', v_lock, true);
    IF NOT pg_catalog.pg_try_advisory_xact_lock_shared(v_nonce, 40000 + v_seq) THEN
        RAISE EXCEPTION 'rw:fault injected' USING ERRCODE = 'RWF01';
    END IF;

    IF pg_catalog.pg_try_advisory_xact_lock_shared(v_nonce, 30000 + v_seq) THEN
        RAISE EXCEPTION 'rw:fault abandoned' USING ERRCODE = 'RWF01';
    END IF;
END
$fn$;
-- rw:fault-end

-- G1 and G2 (04 §6.2) with every 03 §10.1 check in the order of PgRunner.CheckGatesAsync. NULL = pass.
CREATE FUNCTION runwick.rw_gate_v1(p_application text, p_store_id uuid, p_revisions bigint[], p_epoch integer,
    p_capabilities jsonb, p_is_creation boolean, p_allow_drain boolean)
RETURNS runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_store uuid;
    v_revision bigint;
    v_epoch integer;
    v_mode text;
    v_hooks boolean;
    v_app_epoch integer;
    v_caps jsonb;
    v_admission text;
BEGIN
    -- G1 (04 §6.2)
    SELECT m.store_id, m.schema_revision, m.protocol_epoch, m.migration_mode, m.fault_hooks
    INTO v_store, v_revision, v_epoch, v_mode, v_hooks
    FROM runwick.store_metadata AS m WHERE m.singleton_id = 1 FOR SHARE;
    IF NOT FOUND THEN
        RETURN runwick.rw_no_effect_v1('UnsupportedVersion', 'schema_incompatible', NULL);
    END IF;

    IF p_store_id IS NOT NULL AND p_store_id <> v_store THEN
        RETURN runwick.rw_no_effect_v1('UnsupportedVersion', 'store_identity_mismatch', NULL);
    END IF;

    IF v_mode <> 'normal' OR v_revision = 0 THEN
        RETURN runwick.rw_no_effect_v1('AdmissionClosed', 'store_maintenance', NULL);
    END IF;

    IF NOT (v_revision = ANY (p_revisions)) OR v_epoch <> p_epoch THEN
        RETURN runwick.rw_no_effect_v1('UnsupportedVersion', 'schema_incompatible', NULL);
    END IF;

    -- A fault-hooks store is a test store: only the harness may run against it (04 §8.1, PF-05).
    IF v_hooks AND NOT (p_capabilities ? 'rw:harness') THEN
        RETURN runwick.rw_no_effect_v1('UnsupportedVersion', 'schema_incompatible', NULL, 'fault_hooks');
    END IF;

    -- G2 (04 §6.2)
    SELECT a.required_protocol_epoch, a.required_capabilities, a.admission_mode
    INTO v_app_epoch, v_caps, v_admission
    FROM runwick.application_controls AS a WHERE a.application = p_application FOR SHARE;
    IF NOT FOUND THEN
        RETURN runwick.rw_no_effect_v1('AdmissionClosed', 'application_not_registered', NULL);
    END IF;

    IF p_is_creation AND (v_app_epoch > 1 OR EXISTS (
        SELECT 1 FROM pg_catalog.jsonb_each(v_caps) AS c(name, need)
        WHERE NOT (p_capabilities ? c.name)
           OR pg_catalog.jsonb_typeof(c.need) <> 'number'
           OR c.need::numeric <> pg_catalog.trunc(c.need::numeric)
           OR c.need::numeric > 2147483647 OR c.need::numeric < -2147483648
           OR c.need::numeric > (p_capabilities ->> c.name)::numeric)) THEN
        RETURN runwick.rw_no_effect_v1('UnsupportedVersion', 'capability_unsupported', NULL);
    END IF;

    IF v_admission = 'open' OR (v_admission = 'drain' AND p_allow_drain) THEN
        RETURN NULL;
    END IF;

    RETURN runwick.rw_no_effect_v1('AdmissionClosed',
        CASE WHEN v_admission = 'drain' THEN 'application_draining' ELSE 'application_maintenance' END, NULL);
END
$fn$;

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
                   WHERE c.application = p_application AND c.command_id = p_command_id AND c.retention_class <> p_class) THEN
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

-- R4 (04 §6.3), then its row-count check.
CREATE FUNCTION runwick.rw_finalize_v1(p_application text, p_class text, p_command_id uuid, p_status text, p_result jsonb,
    p_decided_at timestamptz)
RETURNS void LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_n integer;
BEGIN
    UPDATE runwick.storage_commands
    SET status = p_status, result = p_result, decided_at = p_decided_at
    WHERE application = p_application AND retention_class = p_class AND command_id = p_command_id AND result IS NULL;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n <> 1 THEN
        RAISE EXCEPTION 'rw:receipt_finalization' USING ERRCODE = 'RWI03';
    END IF;
END
$fn$;

-- The frame of every operation function: the staging read-only guard, gates, then (receipt-bearing operations) the
-- receipt protocol. NULL = proceed with the body.
CREATE FUNCTION runwick.rw_frame_v1(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
    p_fingerprint bytea, p_class text, p_deadline_cap interval, p_store_id uuid, p_revisions bigint[], p_epoch integer,
    p_capabilities jsonb, p_is_creation boolean, p_allow_drain boolean, p_staged boolean)
RETURNS runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_reply runwick.rw_reply_v1;
BEGIN
    IF p_staged AND pg_catalog.current_setting('transaction_read_only') = 'on' THEN
        RETURN runwick.rw_no_effect_v1('TransactionInvalid', 'transaction_invalid', NULL);
    END IF;

    v_reply := runwick.rw_gate_v1(p_application, p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain);
    IF v_reply.status IS NOT NULL OR p_fingerprint IS NULL THEN
        RETURN v_reply;
    END IF;

    RETURN runwick.rw_receipt_v1(p_application, p_class, p_command_id, p_operation, p_fingerprint, p_not_after, p_deadline_cap);
END
$fn$;

-- The ownership check (03 §6.6), branch for branch as PgStore.Ownership. NULL = owned.
CREATE FUNCTION runwick.rw_ownership_v1(p_found boolean, p_state text, p_attempt_id uuid, p_token uuid, p_worker_id uuid,
    p_generation integer, p_lease_expires_at timestamptz, p_lease_attempt_id uuid, p_lease_token uuid, p_lease_worker_id uuid,
    p_lease_generation integer, p_now timestamptz, p_states text[])
RETURNS runwick.rw_reply_v1 LANGUAGE plpgsql IMMUTABLE AS $fn$
BEGIN
    IF NOT p_found THEN
        RETURN runwick.rw_no_effect_v1('NotFound', 'job_not_found', p_now);
    END IF;

    IF p_attempt_id IS DISTINCT FROM p_lease_attempt_id THEN
        RETURN CASE WHEN p_state IN ('succeeded', 'failed', 'cancelled')
                    THEN runwick.rw_no_effect_v1('InvalidState', 'job_terminal', p_now)
                    ELSE runwick.rw_no_effect_v1('StaleLease', 'stale_lease', p_now) END;
    END IF;

    IF p_token IS DISTINCT FROM p_lease_token OR p_worker_id IS DISTINCT FROM p_lease_worker_id OR p_generation <> p_lease_generation THEN
        RETURN runwick.rw_no_effect_v1('StaleLease', 'ownership_mismatch', p_now);
    END IF;

    IF NOT coalesce(p_lease_expires_at > p_now, false) THEN
        RETURN runwick.rw_no_effect_v1('StaleLease', 'lease_expired', p_now);
    END IF;

    IF NOT (p_state = ANY (p_states)) THEN
        RETURN runwick.rw_no_effect_v1('InvalidState', 'invalid_state', p_now);
    END IF;

    RETURN NULL;
END
$fn$;

-- A -> P -> J' -> K' -> E (04 §7.10), each with its row-count check. Returns the job's new revision.
CREATE FUNCTION runwick.rw_close_v1(p_application text, p_command_id uuid, p_now timestamptz, p_job_id uuid, p_attempt_id uuid,
    p_token uuid, p_generation integer, p_revision bigint, p_job_name text, p_active_key text, p_outcome text, p_new_state text,
    p_reason text, p_event_kind text, p_elapsed_ms bigint, p_retry_delay_ms bigint, p_next_eligible_at timestamptz,
    p_error jsonb, p_event_details jsonb)
RETURNS bigint LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_n integer;
    v_revision bigint;
BEGIN
    -- A
    UPDATE runwick.job_attempts
    SET finished_at = p_now, outcome = p_outcome, disposition = p_new_state, applied_reason = p_reason,
        execution_elapsed_ms = p_elapsed_ms, retry_delay_ms = p_retry_delay_ms, next_eligible_at = p_next_eligible_at,
        error = p_error, closed_by_command_id = p_command_id
    WHERE application = p_application AND attempt_id = p_attempt_id AND job_id = p_job_id AND finished_at IS NULL;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n <> 1 THEN
        RAISE EXCEPTION 'rw:invalid_transition' USING ERRCODE = 'RWI01';
    END IF;

    -- P
    DELETE FROM runwick.concurrency_permits
    WHERE application = p_application AND job_id = p_job_id AND attempt_id = p_attempt_id AND ownership_token = p_token;

    -- J'
    UPDATE runwick.jobs
    SET state = p_new_state,
        eligible_at = COALESCE(p_next_eligible_at, eligible_at),
        current_attempt_id = NULL, worker_instance_id = NULL, ownership_token = NULL, lease_expires_at = NULL,
        terminal_at = CASE WHEN p_new_state IN ('succeeded', 'failed', 'cancelled') THEN p_now END,
        terminal_reason = p_reason,
        revision = revision + 1,
        updated_at = p_now
    WHERE application = p_application AND job_id = p_job_id AND current_attempt_id = p_attempt_id AND ownership_token = p_token
    RETURNING revision INTO v_revision;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n <> 1 OR v_revision <> p_revision + 1 THEN
        RAISE EXCEPTION 'rw:invalid_transition' USING ERRCODE = 'RWI01';
    END IF;

    -- K'
    IF p_new_state IN ('succeeded', 'failed', 'cancelled') AND p_active_key IS NOT NULL THEN
        UPDATE runwick.dispatch_keys
        SET bound_job_id = NULL, updated_at = p_now
        WHERE application = p_application AND job_name = p_job_name AND key_kind = 'active' AND key = p_active_key
          AND bound_job_id = p_job_id;
    END IF;

    -- E
    INSERT INTO runwick.queue_events (application, job_id, attempt_id, command_id, event_kind, occurred_at, details)
    VALUES (p_application, p_job_id, p_attempt_id, p_command_id, p_event_kind, p_now,
            COALESCE(p_event_details, '{}'::jsonb)
            || pg_catalog.jsonb_build_object('generation', p_generation, 'outcome', p_outcome, 'disposition', p_new_state)
            || CASE WHEN p_reason IS NULL THEN '{}'::jsonb ELSE pg_catalog.jsonb_build_object('terminalReason', p_reason) END
            || CASE WHEN p_next_eligible_at IS NULL THEN '{}'::jsonb
                    ELSE pg_catalog.jsonb_build_object('eligibleAt', runwick.rw_ms_v1(p_next_eligible_at)) END);
    RETURN v_revision;
END
$fn$;

-- ------------------------------------------------------------------ Renew/1 (04 §7.6)

CREATE FUNCTION runwick.renew_v1(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
    p_fingerprint bytea, p_class text, p_deadline_cap interval, p_store_id uuid, p_revisions bigint[], p_epoch integer,
    p_capabilities jsonb, p_is_creation boolean, p_allow_drain boolean, p_staged boolean,
    p_job_id uuid, p_attempt_id uuid, p_worker_id uuid, p_token uuid, p_generation integer, p_lease interval,
    p_faults jsonb DEFAULT NULL)
RETURNS runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_reply runwick.rw_reply_v1;
    v_found boolean;
    v_state text;
    v_generation integer;
    v_attempt uuid;
    v_worker uuid;
    v_token uuid;
    v_lease timestamptz;
    v_cancel timestamptz;
    v_scopes uuid[];
    v_renewed uuid[];
    v_clock timestamptz;
    v_now timestamptz;
    v_expiry timestamptz;
    v_n integer;
BEGIN
    v_reply := runwick.rw_frame_v1(p_application, p_command_id, p_operation, p_not_after, p_fingerprint, p_class, p_deadline_cap,
        p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain, p_staged);
    IF v_reply.status IS NOT NULL THEN
        RETURN v_reply;
    END IF;

    -- J (04 §7.6)
    SELECT j.state, j.generation, j.current_attempt_id, j.worker_instance_id, j.ownership_token, j.lease_expires_at, j.cancel_requested_at
    INTO v_state, v_generation, v_attempt, v_worker, v_token, v_lease, v_cancel
    FROM runwick.jobs AS j
    WHERE j.application = p_application AND j.job_id = p_job_id
    FOR NO KEY UPDATE;
    v_found := FOUND;

    -- S: the job's required scopes in UUID order (no rows when there is no job).
    SELECT COALESCE(pg_catalog.array_agg(x.scope_id ORDER BY x.scope_id), '{}') INTO v_scopes
    FROM (SELECT s.scope_id
          FROM runwick.concurrency_scopes AS s
          WHERE s.application = p_application
            AND s.scope_id IN (SELECT r.scope_id FROM runwick.job_concurrency_requirements AS r
                               WHERE r.application = p_application AND r.job_id = p_job_id)
          ORDER BY s.scope_id
          FOR UPDATE OF s) AS x;

    PERFORM runwick.rw_fault('AfterLocksBeforeFreshTime', 0, p_faults); -- rw:fault
    v_clock := pg_catalog.clock_timestamp();
    v_now := runwick.ms_floor(v_clock);
    IF v_clock >= p_not_after THEN
        RETURN runwick.rw_no_effect_v1('CommandExpired', 'command_expired', v_now);
    END IF;

    v_reply := runwick.rw_ownership_v1(v_found, v_state, v_attempt, v_token, v_worker, v_generation, v_lease,
        p_attempt_id, p_token, p_worker_id, p_generation, v_now, ARRAY['reserved', 'running']);
    IF v_reply.status IS NOT NULL THEN
        RETURN v_reply;
    END IF;

    v_expiry := runwick.ms_floor(v_now + p_lease);
    PERFORM runwick.rw_fault('BeforeFirstMutation', 0, p_faults); -- rw:fault
    UPDATE runwick.jobs
    SET lease_expires_at = v_expiry
    WHERE application = p_application AND job_id = p_job_id
      AND current_attempt_id = p_attempt_id AND ownership_token = p_token AND worker_instance_id = p_worker_id
      AND generation = p_generation AND state IN ('reserved', 'running') AND lease_expires_at > v_now;
    GET DIAGNOSTICS v_n = ROW_COUNT;

    WITH u AS (
        UPDATE runwick.concurrency_permits
        SET lease_expires_at = v_expiry
        WHERE application = p_application AND job_id = p_job_id AND attempt_id = p_attempt_id AND ownership_token = p_token
          AND lease_expires_at = v_lease
        RETURNING scope_id)
    SELECT COALESCE(pg_catalog.array_agg(u.scope_id ORDER BY u.scope_id), '{}') INTO v_renewed FROM u;
    PERFORM runwick.rw_fault('AfterMutation', 1, p_faults); -- rw:fault

    IF v_n <> 1 THEN
        RETURN runwick.rw_no_effect_v1('StaleLease', 'lease_expired', v_now);
    END IF;

    IF v_renewed <> v_scopes THEN
        RETURN runwick.rw_no_effect_v1('InvariantFault', 'permit_set_mismatch', v_now);
    END IF;

    RETURN runwick.rw_applied_v1('Applied', NULL, v_now,
        pg_catalog.jsonb_build_object('leaseExpiresAt', runwick.rw_ms_v1(v_expiry), 'cancelRequested', v_cancel IS NOT NULL));
END
$fn$;
