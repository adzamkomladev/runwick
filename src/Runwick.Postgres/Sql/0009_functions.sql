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

-- ------------------------------------------------------------------ Reserve/1 (04 §7.3)

CREATE FUNCTION runwick.reserve_v1(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
    p_fingerprint bytea, p_class text, p_deadline_cap interval, p_store_id uuid, p_revisions bigint[], p_epoch integer,
    p_capabilities jsonb, p_is_creation boolean, p_allow_drain boolean, p_staged boolean,
    p_queue text, p_priority text, p_after_eligible_at timestamptz, p_after_job_id uuid,
    p_names text[], p_versions integer[], p_profile_ids text[], p_profile_versions integer[],
    p_lease interval, p_worker_id uuid, p_token uuid,
    p_faults jsonb DEFAULT NULL)
RETURNS runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_reply runwick.rw_reply_v1;
    v_paused boolean;
    v_job_id uuid;
    v_eligible timestamptz;
    v_generation integer;
    v_revision bigint;
    v_reservations bigint;
    v_name text;
    v_payload_version integer;
    v_profile_id text;
    v_profile_version integer;
    v_max_executions integer;
    v_max_unstarted integer;
    v_timeout bigint;
    v_retry jsonb;
    v_unstarted jsonb;
    v_payload bytea;
    v_headers jsonb;
    v_created timestamptz;
    v_scope_ids uuid[];
    v_capacities integer[];
    v_fencing boolean[];
    v_counters bigint[];
    v_fence_ids uuid[];
    v_fence_values bigint[];
    v_blocked uuid;
    v_clock timestamptz;
    v_now timestamptz;
    v_expiry timestamptz;
    v_attempt uuid;
    v_new_revision bigint;
    v_n integer;
    v_result jsonb;
BEGIN
    v_reply := runwick.rw_frame_v1(p_application, p_command_id, p_operation, p_not_after, p_fingerprint, p_class, p_deadline_cap,
        p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain, p_staged);
    IF v_reply.replayed THEN
        -- The reservation's payload row is immutable; the receipt names the job.
        SELECT p.payload_utf8, p.headers, j.created_at INTO v_payload, v_headers, v_created
        FROM runwick.job_payloads AS p JOIN runwick.jobs AS j USING (application, job_id)
        WHERE p.application = p_application AND p.job_id = (v_reply.result ->> 'jobId')::uuid;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'payload row missing for a waiting job' USING ERRCODE = 'RWI04';
        END IF;

        v_reply.payload := v_payload;
        v_reply.headers := v_headers;
        v_reply.created_at := v_created;
        RETURN v_reply;
    END IF;

    IF v_reply.status IS NOT NULL THEN
        RETURN v_reply;
    END IF;

    -- Q (04 §7.3): a paused or unregistered queue claims nothing.
    SELECT q.paused INTO v_paused FROM runwick.queue_controls AS q
    WHERE q.application = p_application AND q.queue = p_queue
    FOR SHARE;
    IF NOT FOUND THEN
        RETURN runwick.rw_no_effect_v1('NoWork', 'queue_not_registered', NULL);
    END IF;

    IF v_paused THEN
        RETURN runwick.rw_no_effect_v1('QueuePaused', 'queue_paused', NULL);
    END IF;

    -- C: the candidate claim.
    SELECT j.job_id, j.eligible_at, j.generation, j.revision, j.reservation_count,
           j.job_name, j.payload_version, j.serializer_profile_id, j.serializer_profile_version
    INTO v_job_id, v_eligible, v_generation, v_revision, v_reservations, v_name, v_payload_version, v_profile_id, v_profile_version
    FROM runwick.jobs AS j
    WHERE j.application = p_application
      AND j.queue = p_queue
      AND j.priority = p_priority
      AND j.state IN ('pending', 'retrying')
      AND j.eligible_at <= pg_catalog.statement_timestamp()
      AND (p_after_job_id IS NULL OR (j.eligible_at, j.job_id) > (p_after_eligible_at, p_after_job_id))
      AND EXISTS (
          SELECT 1
          FROM unnest(p_names, p_versions, p_profile_ids, p_profile_versions) AS c(name, version, profile_id, profile_version)
          WHERE c.name = j.job_name AND c.version = j.payload_version
            AND c.profile_id = j.serializer_profile_id
            AND c.profile_version = j.serializer_profile_version)
    ORDER BY j.eligible_at, j.job_id
    LIMIT 1
    FOR NO KEY UPDATE OF j SKIP LOCKED;
    IF NOT FOUND THEN
        RETURN runwick.rw_no_effect_v1('NoWork', 'no_work', NULL);
    END IF;

    -- G: a separate statement, so its snapshot is taken after C's lock.
    SELECT g.max_executions, g.max_unstarted_abandonments, g.execution_timeout_ms, g.retry_policy, g.unstarted_retry_policy
    INTO v_max_executions, v_max_unstarted, v_timeout, v_retry, v_unstarted
    FROM runwick.job_generations AS g
    WHERE g.application = p_application AND g.job_id = v_job_id AND g.generation = v_generation;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'generation row missing' USING ERRCODE = 'RWI04';
    END IF;

    SELECT p.payload_utf8, p.headers, j.created_at INTO v_payload, v_headers, v_created
    FROM runwick.job_payloads AS p JOIN runwick.jobs AS j USING (application, job_id)
    WHERE p.application = p_application AND p.job_id = v_job_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'payload row missing for a waiting job' USING ERRCODE = 'RWI04';
    END IF;

    -- S: the job's scopes in UUID order, the last blocking lock.
    SELECT COALESCE(pg_catalog.array_agg(x.scope_id ORDER BY x.scope_id), '{}'),
           COALESCE(pg_catalog.array_agg(x.capacity ORDER BY x.scope_id), '{}'),
           COALESCE(pg_catalog.array_agg(x.fencing_enabled ORDER BY x.scope_id), '{}'),
           COALESCE(pg_catalog.array_agg(x.fencing_counter ORDER BY x.scope_id), '{}')
    INTO v_scope_ids, v_capacities, v_fencing, v_counters
    FROM (SELECT s.scope_id, s.capacity, s.fencing_enabled, s.fencing_counter
          FROM runwick.concurrency_scopes AS s
          WHERE s.application = p_application
            AND s.scope_id IN (SELECT r.scope_id FROM runwick.job_concurrency_requirements AS r
                               WHERE r.application = p_application AND r.job_id = v_job_id)
          ORDER BY s.scope_id
          FOR UPDATE OF s) AS x;

    PERFORM runwick.rw_fault('AfterLocksBeforeFreshTime', 0, p_faults); -- rw:fault
    v_clock := pg_catalog.clock_timestamp();
    v_now := runwick.ms_floor(v_clock);
    IF v_clock >= p_not_after THEN
        RETURN runwick.rw_no_effect_v1('CommandExpired', 'command_expired', v_now);
    END IF;

    -- P: live permits per scope at the decision time; the first full scope (UUID order) blocks.
    IF pg_catalog.cardinality(v_scope_ids) > 0 THEN
        SELECT r.scope_id INTO v_blocked
        FROM unnest(v_scope_ids, v_capacities) WITH ORDINALITY AS r(scope_id, capacity, ord)
        WHERE (SELECT count(*) FROM runwick.concurrency_permits AS p
               WHERE p.application = p_application AND p.scope_id = r.scope_id AND p.lease_expires_at > v_now) >= r.capacity
        ORDER BY r.ord
        LIMIT 1;
        IF v_blocked IS NOT NULL THEN
            RETURN runwick.rw_no_effect_v1('CapacityBlocked', 'capacity_blocked', v_now, NULL, NULL, v_blocked, v_eligible, v_job_id);
        END IF;
    END IF;

    v_expiry := runwick.ms_floor(v_now + p_lease);
    v_attempt := runwick.rw_uuid_v7(v_now);
    PERFORM runwick.rw_fault('BeforeFirstMutation', 0, p_faults); -- rw:fault

    -- Fences: the returned counters are the permits' fencing values (count checked).
    IF true = ANY (v_fencing) THEN
        WITH u AS (
            UPDATE runwick.concurrency_scopes
            SET fencing_counter = fencing_counter + 1
            WHERE application = p_application AND fencing_enabled
              AND scope_id = ANY (ARRAY(SELECT f.id FROM unnest(v_scope_ids, v_fencing) AS f(id, on_) WHERE f.on_))
            RETURNING scope_id, fencing_counter)
        SELECT COALESCE(pg_catalog.array_agg(u.scope_id ORDER BY u.scope_id), '{}'),
               COALESCE(pg_catalog.array_agg(u.fencing_counter ORDER BY u.scope_id), '{}')
        INTO v_fence_ids, v_fence_values FROM u;
        IF v_fence_ids <> ARRAY(SELECT f.id FROM unnest(v_scope_ids, v_fencing) AS f(id, on_) WHERE f.on_ ORDER BY f.id)
           OR v_fence_values <> ARRAY(SELECT f.c + 1 FROM unnest(v_scope_ids, v_fencing, v_counters) AS f(id, on_, c) WHERE f.on_ ORDER BY f.id) THEN
            RAISE EXCEPTION 'rw:invalid_transition' USING ERRCODE = 'RWI01';
        END IF;
    ELSE
        v_fence_ids := '{}';
        v_fence_values := '{}';
    END IF;

    INSERT INTO runwick.job_attempts (
        application, attempt_id, job_id, generation, reservation_number, worker_instance_id, ownership_token, reserved_at)
    VALUES (p_application, v_attempt, v_job_id, v_generation, v_reservations + 1, p_worker_id, p_token, v_now);

    UPDATE runwick.jobs
    SET state = 'reserved', reservation_count = reservation_count + 1, revision = revision + 1,
        current_attempt_id = v_attempt, worker_instance_id = p_worker_id, ownership_token = p_token,
        lease_expires_at = v_expiry, updated_at = v_now
    WHERE application = p_application AND job_id = v_job_id AND state IN ('pending', 'retrying')
    RETURNING revision INTO v_new_revision;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n <> 1 OR v_new_revision <> v_revision + 1 THEN
        RETURN runwick.rw_no_effect_v1('InvariantFault', 'invalid_transition', v_now);
    END IF;

    IF pg_catalog.cardinality(v_scope_ids) > 0 THEN
        INSERT INTO runwick.concurrency_permits (
            application, scope_id, attempt_id, job_id, ownership_token, lease_expires_at, fencing_value)
        SELECT p_application, s.scope_id, v_attempt, v_job_id, p_token, v_expiry,
               v_fence_values[pg_catalog.array_position(v_fence_ids, s.scope_id)]
        FROM unnest(v_scope_ids) AS s(scope_id)
        ORDER BY s.scope_id;
    END IF;

    INSERT INTO runwick.queue_events (application, job_id, attempt_id, command_id, event_kind, occurred_at, details)
    VALUES (p_application, v_job_id, v_attempt, p_command_id, 'attempt_reserved', v_now,
            pg_catalog.jsonb_build_object('generation', v_generation, 'reservationNumber', v_reservations + 1));

    -- R4: receipt codec v1 (ReceiptCodec.Reserve).
    v_result := pg_catalog.jsonb_build_object(
        'jobId', v_job_id, 'generation', v_generation, 'revision', v_new_revision, 'state', 'reserved',
        'attemptId', v_attempt, 'workerInstanceId', p_worker_id, 'ownershipToken', p_token,
        'leaseExpiresAt', runwick.rw_ms_v1(v_expiry), 'reservationNumber', v_reservations + 1,
        'contract', pg_catalog.jsonb_build_object('name', v_name, 'payloadVersion', v_payload_version,
            'serializerProfileId', v_profile_id, 'serializerProfileVersion', v_profile_version),
        'queue', p_queue, 'priority', p_priority,
        'cursor', pg_catalog.jsonb_build_object('eligibleAt', runwick.rw_ms_v1(v_eligible), 'jobId', v_job_id),
        'policy', pg_catalog.jsonb_build_object('executionTimeout', v_timeout, 'fingerprintVersion', 1,
            'maxConsecutiveUnstartedAbandonments', v_max_unstarted, 'maxExecutions', v_max_executions,
            'retry', v_retry, 'unstartedRetry', v_unstarted),
        'fences', COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('scopeId', f.id, 'value', f.v) ORDER BY f.ord)
                            FROM unnest(v_fence_ids, v_fence_values) WITH ORDINALITY AS f(id, v, ord)), '[]'::jsonb));
    PERFORM runwick.rw_finalize_v1(p_application, p_class, p_command_id, 'Applied', v_result, v_now);
    PERFORM runwick.rw_fault('AfterMutation', 1, p_faults); -- rw:fault

    v_reply := runwick.rw_applied_v1('Applied', NULL, v_now, v_result);
    v_reply.payload := v_payload;
    v_reply.headers := v_headers;
    v_reply.created_at := v_created;
    RETURN v_reply;
END
$fn$;

-- ------------------------------------------------------------------ Begin/1 (04 §7.5, 10 §4)

CREATE FUNCTION runwick.begin_v1(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
    p_fingerprint bytea, p_class text, p_deadline_cap interval, p_store_id uuid, p_revisions bigint[], p_epoch integer,
    p_capabilities jsonb, p_is_creation boolean, p_allow_drain boolean, p_staged boolean,
    p_job_id uuid, p_attempt_id uuid, p_worker_id uuid, p_token uuid, p_generation integer,
    p_min_defer interval, p_defer_jitter_ms integer,
    p_faults jsonb DEFAULT NULL)
RETURNS runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_reply runwick.rw_reply_v1;
    v_found boolean;
    v_state text;
    v_generation integer;
    v_revision bigint;
    v_attempt uuid;
    v_worker uuid;
    v_token uuid;
    v_lease timestamptz;
    v_cancel timestamptz;
    v_active_key text;
    v_job_name text;
    v_max_executions integer;
    v_started integer;
    v_executions bigint;
    v_rated boolean;
    v_plan text;
    v_rate_ids uuid[];
    v_limits integer[];
    v_windows bigint[];
    v_rate_revisions bigint[];
    v_last_decisions timestamptz[];
    v_not_befores timestamptz[];
    v_counts integer[];
    v_blocked uuid[];
    v_clock timestamptz;
    v_now timestamptz;
    v_t timestamptz;
    v_hint timestamptz;
    v_oldest timestamptz;
    v_logical timestamptz;
    v_eligible timestamptz;
    v_outcome text;
    v_reason text;
    v_new_state text;
    v_code text;
    v_status text;
    v_new_revision bigint;
    v_n integer;
    v_i integer;
    v_execution_number integer;
    v_total bigint;
    v_result jsonb;
BEGIN
    v_reply := runwick.rw_frame_v1(p_application, p_command_id, p_operation, p_not_after, p_fingerprint, p_class, p_deadline_cap,
        p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain, p_staged);
    IF v_reply.replayed THEN
        -- A closed or deferred Begin replays its code with its receipt.
        IF v_reply.result ? 'deferredUntil' THEN
            v_reply.code := 'rate_limited';
            v_reply.detail := v_reply.result ->> 'deferredUntil';
        ELSIF v_reply.result ? 'attemptOutcome' THEN
            v_reply.code := v_reply.result ->> 'errorCode';
        END IF;

        RETURN v_reply;
    END IF;

    IF v_reply.status IS NOT NULL THEN
        RETURN v_reply;
    END IF;

    -- J
    SELECT j.state, j.generation, j.revision, j.current_attempt_id, j.worker_instance_id, j.ownership_token,
           j.lease_expires_at, j.cancel_requested_at, j.active_unique_key, j.job_name
    INTO v_state, v_generation, v_revision, v_attempt, v_worker, v_token, v_lease, v_cancel, v_active_key, v_job_name
    FROM runwick.jobs AS j
    WHERE j.application = p_application AND j.job_id = p_job_id
    FOR NO KEY UPDATE;
    v_found := FOUND;

    -- G behind J: a separate statement, so its snapshot is taken after the job lock.
    IF v_found THEN
        SELECT g.max_executions, g.started_count, j.execution_count,
               EXISTS (SELECT 1 FROM runwick.job_rate_requirements AS rr WHERE rr.application = j.application AND rr.job_id = j.job_id)
        INTO v_max_executions, v_started, v_executions, v_rated
        FROM runwick.jobs AS j
        JOIN runwick.job_generations AS g ON g.application = j.application AND g.job_id = j.job_id AND g.generation = j.generation
        WHERE j.application = p_application AND j.job_id = p_job_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'generation row missing' USING ERRCODE = 'RWI04';
        END IF;

        v_plan := CASE WHEN v_cancel IS NOT NULL THEN 'cancel' WHEN v_started >= v_max_executions THEN 'exhausted' ELSE 'authorize' END;
        v_rated := v_plan = 'authorize' AND v_rated;
    ELSE
        v_plan := 'none';
        v_rated := false;
    END IF;

    -- K (closing plans only), S, then R (rated plans only), in lock order (04 §5, 10 §4).
    IF v_found AND (v_plan <> 'authorize' OR v_rated) THEN
        IF v_plan IN ('cancel', 'exhausted') AND v_active_key IS NOT NULL THEN
            PERFORM 1
            FROM runwick.dispatch_keys AS k
            JOIN runwick.jobs AS j ON j.application = k.application AND j.job_name = k.job_name AND j.active_unique_key = k.key
            WHERE j.application = p_application AND j.job_id = p_job_id AND k.key_kind = 'active'
            FOR UPDATE OF k;
        END IF;

        PERFORM 1
        FROM runwick.concurrency_scopes AS s
        WHERE s.application = p_application
          AND s.scope_id IN (SELECT r.scope_id FROM runwick.job_concurrency_requirements AS r
                             WHERE r.application = p_application AND r.job_id = p_job_id)
        ORDER BY s.scope_id
        FOR UPDATE OF s;

        IF v_rated THEN
            SELECT COALESCE(pg_catalog.array_agg(x.rate_scope_id ORDER BY x.rate_scope_id), '{}'),
                   COALESCE(pg_catalog.array_agg(x.limit_count ORDER BY x.rate_scope_id), '{}'),
                   COALESCE(pg_catalog.array_agg(x.window_ms ORDER BY x.rate_scope_id), '{}'),
                   COALESCE(pg_catalog.array_agg(x.policy_revision ORDER BY x.rate_scope_id), '{}'),
                   COALESCE(pg_catalog.array_agg(x.last_decision_at ORDER BY x.rate_scope_id), '{}'),
                   COALESCE(pg_catalog.array_agg(x.not_before ORDER BY x.rate_scope_id), '{}')
            INTO v_rate_ids, v_limits, v_windows, v_rate_revisions, v_last_decisions, v_not_befores
            FROM (SELECT s.rate_scope_id, s.limit_count, s.window_ms, s.policy_revision, s.last_decision_at, s.not_before
                  FROM runwick.rate_scopes AS s
                  WHERE s.application = p_application
                    AND s.rate_scope_id IN (SELECT r.rate_scope_id FROM runwick.job_rate_requirements AS r
                                            WHERE r.application = p_application AND r.job_id = p_job_id)
                  ORDER BY s.rate_scope_id
                  FOR UPDATE OF s) AS x;
        END IF;
    END IF;

    PERFORM runwick.rw_fault('AfterLocksBeforeFreshTime', 0, p_faults); -- rw:fault
    v_clock := pg_catalog.clock_timestamp();
    v_now := runwick.ms_floor(v_clock);
    IF v_clock >= p_not_after THEN
        RETURN runwick.rw_no_effect_v1('CommandExpired', 'command_expired', v_now);
    END IF;

    v_reply := runwick.rw_ownership_v1(v_found, v_state, v_attempt, v_token, v_worker, v_generation, v_lease,
        p_attempt_id, p_token, p_worker_id, p_generation, v_now, ARRAY['reserved']);
    IF v_reply.status IS NOT NULL THEN
        RETURN v_reply;
    END IF;

    -- Cancellation and an exhausted budget close the attempt before any charge.
    IF v_plan IN ('cancel', 'exhausted') THEN
        v_outcome := CASE v_plan WHEN 'cancel' THEN 'cancelled_before_start' ELSE 'abandoned_before_start' END;
        v_reason := CASE v_plan WHEN 'cancel' THEN 'cancelled_before_start' ELSE 'execution_budget_exhausted' END;
        v_new_state := CASE v_plan WHEN 'cancel' THEN 'cancelled' ELSE 'failed' END;
        v_status := CASE v_plan WHEN 'cancel' THEN 'InvalidState' ELSE 'BudgetExhausted' END;
        v_code := v_reason;
        PERFORM runwick.rw_fault('BeforeFirstMutation', 0, p_faults); -- rw:fault
        v_new_revision := runwick.rw_close_v1(p_application, p_command_id, v_now, p_job_id, p_attempt_id, p_token, v_generation,
            v_revision, v_job_name, v_active_key, v_outcome, v_new_state, v_reason,
            CASE v_plan WHEN 'cancel' THEN 'attempt_cancelled_before_start' ELSE 'attempt_finished' END,
            NULL, NULL, NULL, NULL, NULL);
        PERFORM runwick.rw_fault('AfterMutation', 1, p_faults); -- rw:fault
        v_result := pg_catalog.jsonb_build_object('jobId', p_job_id, 'generation', v_generation, 'revision', v_new_revision,
            'state', v_new_state, 'attemptOutcome', v_outcome, 'terminalReason', v_reason, 'errorCode', v_code);
        PERFORM runwick.rw_finalize_v1(p_application, p_class, p_command_id, v_status, v_result, v_now);
        RETURN runwick.rw_applied_v1(v_status, v_code, v_now, v_result);
    END IF;

    PERFORM runwick.rw_fault('BeforeFirstMutation', 0, p_faults); -- rw:fault

    -- Rate step (10 §4): all-or-none at logical time t = max(now, the scopes' last decisions).
    IF pg_catalog.cardinality(v_rate_ids) > 0 THEN
        v_t := GREATEST(v_now, (SELECT max(d) FROM unnest(v_last_decisions) AS d));
        SELECT pg_catalog.array_agg((SELECT count(*)::integer FROM runwick.rate_grants AS g
                                     WHERE g.application = p_application AND g.rate_scope_id = r.id
                                       AND g.authorized_at > v_t - r.w * interval '1 millisecond') ORDER BY r.ord)
        INTO v_counts
        FROM unnest(v_rate_ids, v_windows) WITH ORDINALITY AS r(id, w, ord);

        SELECT COALESCE(pg_catalog.array_agg(r.id ORDER BY r.ord), '{}') INTO v_blocked
        FROM unnest(v_rate_ids, v_limits, v_not_befores, v_counts) WITH ORDINALITY AS r(id, lim, nb, c, ord)
        WHERE r.c >= r.lim OR (r.nb IS NOT NULL AND v_t < r.nb);

        -- The scopes' logical time never goes backward, whichever way the decision goes.
        UPDATE runwick.rate_scopes SET last_decision_at = v_t
        WHERE application = p_application AND rate_scope_id = ANY (v_rate_ids)
          AND (last_decision_at IS NULL OR last_decision_at < v_t);

        IF pg_catalog.cardinality(v_blocked) = 0 THEN
            INSERT INTO runwick.rate_grants (application, rate_scope_id, attempt_id, job_id, authorized_at, policy_revision)
            SELECT p_application, s.id, p_attempt_id, p_job_id, v_t, s.revision
            FROM unnest(v_rate_ids, v_rate_revisions) AS s(id, revision)
            ORDER BY s.id;
            GET DIAGNOSTICS v_n = ROW_COUNT;
            IF v_n <> pg_catalog.cardinality(v_rate_ids) THEN
                RAISE EXCEPTION 'rw:invalid_transition' USING ERRCODE = 'RWI01';
            END IF;
        ELSE
            PERFORM runwick.rw_fault('AfterMutation', 1, p_faults); -- rw:fault

            -- Earliest logical instant at which every blocked scope could admit (capacity-control §4); not a reservation.
            v_hint := v_t;
            FOR v_i IN 1 .. pg_catalog.cardinality(v_rate_ids) LOOP
                CONTINUE WHEN NOT (v_rate_ids[v_i] = ANY (v_blocked));
                IF v_counts[v_i] >= v_limits[v_i] THEN
                    SELECT g.authorized_at INTO v_oldest
                    FROM runwick.rate_grants AS g
                    WHERE g.application = p_application AND g.rate_scope_id = v_rate_ids[v_i]
                      AND g.authorized_at > v_t - v_windows[v_i] * interval '1 millisecond'
                    ORDER BY g.authorized_at
                    OFFSET v_counts[v_i] - v_limits[v_i] LIMIT 1;
                    IF FOUND AND v_oldest + v_windows[v_i] * interval '1 millisecond' > v_hint THEN
                        v_hint := v_oldest + v_windows[v_i] * interval '1 millisecond';
                    END IF;
                END IF;

                IF v_not_befores[v_i] IS NOT NULL AND v_not_befores[v_i] > v_hint THEN
                    v_hint := v_not_befores[v_i];
                END IF;
            END LOOP;

            -- Logical hint to storage wall time: the same offset, so a backward clock defers longer, never shorter.
            v_logical := GREATEST(v_hint, v_t + p_min_defer);
            v_eligible := v_now + (v_logical - v_t) + p_defer_jitter_ms * interval '1 millisecond';
            IF runwick.ms_floor(v_eligible) <> v_eligible THEN
                v_eligible := runwick.ms_floor(v_eligible) + interval '1 millisecond';
            END IF;

            v_new_state := CASE WHEN v_started = 0 THEN 'pending' ELSE 'retrying' END;
            v_new_revision := runwick.rw_close_v1(p_application, p_command_id, v_now, p_job_id, p_attempt_id, p_token, v_generation,
                v_revision, v_job_name, v_active_key, 'admission_deferred', v_new_state, NULL, 'attempt_admission_deferred',
                NULL, GREATEST(1, runwick.rw_ms_v1(v_eligible) - runwick.rw_ms_v1(v_now)), v_eligible, NULL,
                pg_catalog.jsonb_build_object('blockingScopes', pg_catalog.to_jsonb(v_blocked), 'rateTime', runwick.rw_ms_v1(v_t)));
            PERFORM runwick.rw_fault('AfterMutation', 2, p_faults); -- rw:fault
            v_result := pg_catalog.jsonb_build_object('jobId', p_job_id, 'generation', v_generation, 'revision', v_new_revision,
                'state', v_new_state, 'deferredUntil', runwick.rw_ms_v1(v_eligible), 'blockingScopes', pg_catalog.to_jsonb(v_blocked));
            PERFORM runwick.rw_finalize_v1(p_application, p_class, p_command_id, 'RateLimited', v_result, v_now);
            RETURN runwick.rw_applied_v1('RateLimited', 'rate_limited', v_now, v_result, (runwick.rw_ms_v1(v_eligible))::text);
        END IF;
    END IF;

    -- The effects: the job is locked, so the execution number, total and new revision are known and checked.
    v_execution_number := v_started + 1;
    v_total := v_executions + 1;
    UPDATE runwick.job_generations
    SET started_count = started_count + 1, consecutive_unstarted_count = 0
    WHERE application = p_application AND job_id = p_job_id AND generation = p_generation AND started_count < max_executions
    RETURNING started_count INTO v_n;
    IF v_n IS DISTINCT FROM v_execution_number THEN
        RETURN runwick.rw_no_effect_v1('InvariantFault', 'invalid_transition', v_now);
    END IF;

    UPDATE runwick.jobs
    SET state = 'running', execution_count = execution_count + 1, revision = revision + 1, updated_at = v_now
    WHERE application = p_application AND job_id = p_job_id AND state = 'reserved'
      AND current_attempt_id = p_attempt_id AND ownership_token = p_token
    RETURNING execution_count, revision INTO v_executions, v_new_revision;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n <> 1 OR v_executions <> v_total OR v_new_revision <> v_revision + 1 THEN
        RETURN runwick.rw_no_effect_v1('InvariantFault', 'invalid_transition', v_now);
    END IF;

    UPDATE runwick.job_attempts
    SET execution_number = v_execution_number, authorized_at = v_now
    WHERE application = p_application AND attempt_id = p_attempt_id AND finished_at IS NULL AND execution_number IS NULL;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n <> 1 THEN
        RETURN runwick.rw_no_effect_v1('InvariantFault', 'invalid_transition', v_now);
    END IF;

    INSERT INTO runwick.queue_events (application, job_id, attempt_id, command_id, event_kind, occurred_at, details)
    VALUES (p_application, p_job_id, p_attempt_id, p_command_id, 'attempt_started', v_now,
            pg_catalog.jsonb_build_object('generation', p_generation, 'executionNumber', v_execution_number));

    v_result := pg_catalog.jsonb_build_object('jobId', p_job_id, 'generation', p_generation, 'revision', v_new_revision,
        'state', 'running', 'attemptId', p_attempt_id, 'workerInstanceId', p_worker_id, 'ownershipToken', p_token,
        'leaseExpiresAt', runwick.rw_ms_v1(v_lease), 'executionNumber', v_execution_number, 'totalExecutionNumber', v_total,
        'authorizedAt', runwick.rw_ms_v1(v_now));
    PERFORM runwick.rw_finalize_v1(p_application, p_class, p_command_id, 'Applied', v_result, v_now);
    PERFORM runwick.rw_fault('AfterMutation', 1, p_faults); -- rw:fault
    RETURN runwick.rw_applied_v1('Applied', NULL, v_now, v_result);
END
$fn$;

-- ------------------------------------------------------------------ Finish/1 (04 §7.7)

CREATE FUNCTION runwick.finish_v1(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
    p_fingerprint bytea, p_class text, p_deadline_cap interval, p_store_id uuid, p_revisions bigint[], p_epoch integer,
    p_capabilities jsonb, p_is_creation boolean, p_allow_drain boolean, p_staged boolean,
    p_job_id uuid, p_attempt_id uuid, p_worker_id uuid, p_token uuid, p_generation integer,
    p_outcome text, p_elapsed_ms bigint, p_requested_delay_ms bigint, p_error jsonb, p_entropy bigint,
    p_faults jsonb DEFAULT NULL)
RETURNS runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_reply runwick.rw_reply_v1;
    v_found boolean;
    v_state text;
    v_generation integer;
    v_revision bigint;
    v_attempt uuid;
    v_worker uuid;
    v_token uuid;
    v_lease timestamptz;
    v_cancel timestamptz;
    v_active_key text;
    v_job_name text;
    v_max_executions integer;
    v_started integer;
    v_retry jsonb;
    v_new_state text;
    v_reason text;
    v_clock timestamptz;
    v_now timestamptz;
    v_delay bigint;
    v_eligible timestamptz;
    v_new_revision bigint;
    v_result jsonb;
BEGIN
    v_reply := runwick.rw_frame_v1(p_application, p_command_id, p_operation, p_not_after, p_fingerprint, p_class, p_deadline_cap,
        p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain, p_staged);
    IF v_reply.status IS NOT NULL THEN
        RETURN v_reply;
    END IF;

    -- J, then G behind it (a separate statement: its snapshot follows the job lock).
    SELECT j.state, j.generation, j.revision, j.current_attempt_id, j.worker_instance_id, j.ownership_token,
           j.lease_expires_at, j.cancel_requested_at, j.active_unique_key, j.job_name
    INTO v_state, v_generation, v_revision, v_attempt, v_worker, v_token, v_lease, v_cancel, v_active_key, v_job_name
    FROM runwick.jobs AS j
    WHERE j.application = p_application AND j.job_id = p_job_id
    FOR NO KEY UPDATE;
    v_found := FOUND;

    IF v_found THEN
        SELECT g.max_executions, g.started_count, g.retry_policy INTO v_max_executions, v_started, v_retry
        FROM runwick.jobs AS j
        JOIN runwick.job_generations AS g ON g.application = j.application AND g.job_id = j.job_id AND g.generation = j.generation
        WHERE j.application = p_application AND j.job_id = p_job_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'generation row missing' USING ERRCODE = 'RWI04';
        END IF;
    END IF;

    SELECT x.s, x.r INTO v_new_state, v_reason
    FROM (VALUES (CASE
        WHEN p_outcome = 'succeeded' THEN 'succeeded'
        WHEN p_outcome = 'user_cancelled' THEN 'cancelled'
        WHEN v_cancel IS NOT NULL THEN 'cancelled'
        WHEN p_outcome = 'permanent_failure' THEN 'failed'
        WHEN v_found AND v_started < v_max_executions THEN 'retrying'
        ELSE 'failed' END,
        CASE
        WHEN p_outcome = 'succeeded' THEN 'succeeded'
        WHEN p_outcome = 'user_cancelled' THEN 'user_cancelled'
        WHEN v_cancel IS NOT NULL THEN 'cancellation_requested'
        WHEN p_outcome = 'permanent_failure' THEN 'permanent_failure'
        WHEN v_found AND v_started < v_max_executions THEN NULL
        ELSE 'execution_budget_exhausted' END)) AS x(s, r);

    -- K (a terminal close only, found through the locked row), then S (04 §5).
    IF v_found THEN
        IF v_new_state IN ('succeeded', 'failed', 'cancelled') AND v_active_key IS NOT NULL THEN
            PERFORM 1
            FROM runwick.dispatch_keys AS k
            JOIN runwick.jobs AS j ON j.application = k.application AND j.job_name = k.job_name AND j.active_unique_key = k.key
            WHERE j.application = p_application AND j.job_id = p_job_id AND k.key_kind = 'active'
            FOR UPDATE OF k;
        END IF;

        PERFORM 1
        FROM runwick.concurrency_scopes AS s
        WHERE s.application = p_application
          AND s.scope_id IN (SELECT r.scope_id FROM runwick.job_concurrency_requirements AS r
                             WHERE r.application = p_application AND r.job_id = p_job_id)
        ORDER BY s.scope_id
        FOR UPDATE OF s;
    END IF;

    PERFORM runwick.rw_fault('AfterLocksBeforeFreshTime', 0, p_faults); -- rw:fault
    v_clock := pg_catalog.clock_timestamp();
    v_now := runwick.ms_floor(v_clock);
    IF v_clock >= p_not_after THEN
        RETURN runwick.rw_no_effect_v1('CommandExpired', 'command_expired', v_now);
    END IF;

    v_reply := runwick.rw_ownership_v1(v_found, v_state, v_attempt, v_token, v_worker, v_generation, v_lease,
        p_attempt_id, p_token, p_worker_id, p_generation, v_now, ARRAY['running']);
    IF v_reply.status IS NOT NULL THEN
        RETURN v_reply;
    END IF;

    IF v_new_state = 'retrying' THEN
        v_delay := CASE WHEN p_outcome = 'retryable_failure' AND p_requested_delay_ms IS NOT NULL
                        THEN LEAST(p_requested_delay_ms, (v_retry ->> 'maximumDelay')::bigint)
                        ELSE runwick.rw_retry_delay_v1(v_started, (v_retry ->> 'initialDelay')::bigint,
                                                       (v_retry ->> 'maximumDelay')::bigint, p_entropy) END;
        v_eligible := v_now + v_delay * interval '1 millisecond';
    END IF;

    PERFORM runwick.rw_fault('BeforeFirstMutation', 0, p_faults); -- rw:fault
    v_new_revision := runwick.rw_close_v1(p_application, p_command_id, v_now, p_job_id, p_attempt_id, p_token, v_generation,
        v_revision, v_job_name, v_active_key, p_outcome, v_new_state, v_reason, 'attempt_finished',
        p_elapsed_ms, v_delay, v_eligible, p_error, NULL);
    v_result := pg_catalog.jsonb_build_object('jobId', p_job_id, 'generation', v_generation, 'revision', v_new_revision,
        'state', v_new_state, 'attemptId', p_attempt_id, 'attemptOutcome', p_outcome, 'disposition', v_new_state,
        'terminalReason', v_reason, 'eligibleAt', runwick.rw_ms_v1(v_eligible), 'retryDelay', v_delay);
    PERFORM runwick.rw_finalize_v1(p_application, p_class, p_command_id, 'Applied', v_result, v_now);
    PERFORM runwick.rw_fault('AfterMutation', 1, p_faults); -- rw:fault
    RETURN runwick.rw_applied_v1('Applied', NULL, v_now, v_result);
END
$fn$;

-- ------------------------------------------------------------------ Recover/1 (04 §7.8)

CREATE FUNCTION runwick.recover_v1(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
    p_fingerprint bytea, p_class text, p_deadline_cap interval, p_store_id uuid, p_revisions bigint[], p_epoch integer,
    p_capabilities jsonb, p_is_creation boolean, p_allow_drain boolean, p_staged boolean,
    p_job_id uuid, p_attempt_id uuid, p_token uuid, p_entropy bigint,
    p_faults jsonb DEFAULT NULL)
RETURNS runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_reply runwick.rw_reply_v1;
    v_state text;
    v_generation integer;
    v_revision bigint;
    v_attempt uuid;
    v_token uuid;
    v_lease timestamptz;
    v_cancel boolean;
    v_active_key text;
    v_job_name text;
    v_max_executions integer;
    v_max_unstarted integer;
    v_started integer;
    v_unstarted integer;
    v_retry jsonb;
    v_unstarted_retry jsonb;
    v_reserved boolean;
    v_k integer;
    v_new_state text;
    v_reason text;
    v_outcome text;
    v_policy jsonb;
    v_clock timestamptz;
    v_now timestamptz;
    v_delay bigint;
    v_eligible timestamptz;
    v_new_revision bigint;
    v_step integer := 0;
    v_n integer;
    v_result jsonb;
BEGIN
    v_reply := runwick.rw_frame_v1(p_application, p_command_id, p_operation, p_not_after, p_fingerprint, p_class, p_deadline_cap,
        p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain, p_staged);
    IF v_reply.status IS NOT NULL THEN
        RETURN v_reply;
    END IF;

    -- J
    SELECT j.state, j.generation, j.revision, j.current_attempt_id, j.ownership_token, j.lease_expires_at,
           j.cancel_requested_at IS NOT NULL, j.active_unique_key, j.job_name
    INTO v_state, v_generation, v_revision, v_attempt, v_token, v_lease, v_cancel, v_active_key, v_job_name
    FROM runwick.jobs AS j
    WHERE j.application = p_application AND j.job_id = p_job_id
    FOR NO KEY UPDATE;
    IF NOT FOUND THEN
        RETURN runwick.rw_no_effect_v1('NotFound', 'job_not_found', NULL);
    END IF;

    IF v_attempt IS DISTINCT FROM p_attempt_id OR v_token IS DISTINCT FROM p_token OR v_state NOT IN ('reserved', 'running') THEN
        RETURN runwick.rw_no_effect_v1('StaleLease', 'stale_lease', NULL);
    END IF;

    -- G
    SELECT g.max_executions, g.max_unstarted_abandonments, g.started_count, g.consecutive_unstarted_count,
           g.retry_policy, g.unstarted_retry_policy
    INTO v_max_executions, v_max_unstarted, v_started, v_unstarted, v_retry, v_unstarted_retry
    FROM runwick.job_generations AS g
    WHERE g.application = p_application AND g.job_id = p_job_id AND g.generation = v_generation;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'generation row missing' USING ERRCODE = 'RWI04';
    END IF;

    v_reserved := v_state = 'reserved';
    v_k := v_unstarted + 1;
    IF v_reserved AND v_cancel THEN
        v_new_state := 'cancelled'; v_reason := 'cancelled_before_start';
    ELSIF v_reserved AND v_k >= v_max_unstarted THEN
        v_new_state := 'failed'; v_reason := 'reservation_abandonment_exhausted';
    ELSIF v_reserved THEN
        v_new_state := CASE WHEN v_started = 0 THEN 'pending' ELSE 'retrying' END;
    ELSIF v_cancel THEN
        v_new_state := 'cancelled'; v_reason := 'cancellation_requested';
    ELSIF v_started < v_max_executions THEN
        v_new_state := 'retrying';
    ELSE
        v_new_state := 'failed'; v_reason := 'execution_budget_exhausted';
    END IF;

    -- K (a terminal close only), then S.
    IF v_new_state IN ('succeeded', 'failed', 'cancelled') AND v_active_key IS NOT NULL THEN
        PERFORM 1 FROM runwick.dispatch_keys
        WHERE application = p_application AND job_name = v_job_name AND key_kind = 'active' AND key = v_active_key
        FOR UPDATE;
    END IF;

    PERFORM 1
    FROM runwick.concurrency_scopes AS s
    WHERE s.application = p_application
      AND s.scope_id IN (SELECT r.scope_id FROM runwick.job_concurrency_requirements AS r
                         WHERE r.application = p_application AND r.job_id = p_job_id)
    ORDER BY s.scope_id
    FOR UPDATE OF s;

    PERFORM runwick.rw_fault('AfterLocksBeforeFreshTime', 0, p_faults); -- rw:fault
    v_clock := pg_catalog.clock_timestamp();
    v_now := runwick.ms_floor(v_clock);
    IF v_clock >= p_not_after THEN
        RETURN runwick.rw_no_effect_v1('CommandExpired', 'command_expired', v_now);
    END IF;

    IF v_lease > v_now THEN
        RETURN runwick.rw_no_effect_v1('StaleLease', 'lease_renewed', v_now);
    END IF;

    PERFORM runwick.rw_fault('BeforeFirstMutation', 0, p_faults); -- rw:fault
    IF v_reserved AND NOT v_cancel THEN
        UPDATE runwick.job_generations
        SET consecutive_unstarted_count = consecutive_unstarted_count + 1
        WHERE application = p_application AND job_id = p_job_id AND generation = v_generation;
        GET DIAGNOSTICS v_n = ROW_COUNT;
        IF v_n <> 1 THEN
            RAISE EXCEPTION 'rw:invalid_transition' USING ERRCODE = 'RWI01';
        END IF;

        v_step := v_step + 1;
        PERFORM runwick.rw_fault('AfterMutation', v_step, p_faults); -- rw:fault
    END IF;

    IF v_new_state NOT IN ('succeeded', 'failed', 'cancelled') THEN
        v_policy := CASE WHEN v_reserved THEN v_unstarted_retry ELSE v_retry END;
        v_delay := runwick.rw_retry_delay_v1(CASE WHEN v_reserved THEN v_k ELSE v_started END,
            (v_policy ->> 'initialDelay')::bigint, (v_policy ->> 'maximumDelay')::bigint, p_entropy);
        v_eligible := v_now + v_delay * interval '1 millisecond';
    END IF;

    v_outcome := CASE WHEN v_reserved THEN 'abandoned_before_start' ELSE 'abandoned_running' END;
    v_new_revision := runwick.rw_close_v1(p_application, p_command_id, v_now, p_job_id, p_attempt_id, p_token, v_generation,
        v_revision, v_job_name, v_active_key, v_outcome, v_new_state, v_reason, 'attempt_recovered',
        NULL, v_delay, v_eligible, NULL, NULL);
    v_step := v_step + 1;
    PERFORM runwick.rw_fault('AfterMutation', v_step, p_faults); -- rw:fault
    v_result := pg_catalog.jsonb_build_object('jobId', p_job_id, 'generation', v_generation, 'revision', v_new_revision,
        'state', v_new_state, 'attemptId', p_attempt_id, 'attemptOutcome', v_outcome, 'disposition', v_new_state,
        'terminalReason', v_reason, 'eligibleAt', runwick.rw_ms_v1(v_eligible), 'retryDelay', v_delay);
    PERFORM runwick.rw_finalize_v1(p_application, p_class, p_command_id, 'Applied', v_result, v_now);
    RETURN runwick.rw_applied_v1('Applied', NULL, v_now, v_result);
END
$fn$;

