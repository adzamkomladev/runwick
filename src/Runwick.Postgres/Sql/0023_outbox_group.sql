-- 0023_outbox_group (D4 grouped destination delivery): a driver-only function that runs several ReceiveOutbox/1 transport
-- commands in one transaction. It adds no protocol operation. Each item keeps its own command ID, fingerprint, deadline,
-- receipt and delivery mapping, because the item runs through receive_outbox_v3 unchanged; the only shared work is the
-- connection, the transaction, the commit and the post-commit wake. An item that is refused rolls back in its own
-- subtransaction (receipt placeholder included) and reports its own reply; the other items commit.
-- NOTE for the integrator: this file was written next to the B1 migration and may need a new number at merge.
SET LOCAL lock_timeout = '5s';

CREATE TYPE runwick.rw_outbox_group_item_v1 AS (
    p_command_id uuid, p_not_after timestamptz, p_fingerprint bytea,
    p_source_store_id uuid, p_message_id uuid, p_delivery_fingerprint bytea, p_accept_until timestamptz, p_profile text,
    p_invalid text,
    p_job_ids uuid[], p_job_names text[], p_payload_versions integer[], p_profile_ids text[], p_profile_versions integer[],
    p_queues text[], p_priorities text[], p_not_before_ms bigint[], p_active_keys text[], p_window_keys text[],
    p_window_ms bigint[], p_business bytea[], p_policy bytea[], p_payloads bytea[], p_headers jsonb[],
    p_max_executions integer[], p_max_unstarted integer[], p_timeouts bigint[], p_retry jsonb[], p_unstarted jsonb[]
);

CREATE FUNCTION runwick.receive_outbox_group_v1(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
    p_fingerprint bytea, p_class text, p_deadline_cap interval, p_store_id uuid, p_revisions bigint[], p_epoch integer,
    p_capabilities jsonb, p_is_creation boolean, p_allow_drain boolean, p_staged boolean,
    p_items jsonb, p_faults jsonb DEFAULT NULL)
RETURNS runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_gate runwick.rw_reply_v1;
    v_reply runwick.rw_reply_v1;
    v_item runwick.rw_outbox_group_item_v1;
    v_items runwick.rw_outbox_group_item_v1[];
    v_replies jsonb := '{}'::jsonb;
BEGIN
    IF p_staged OR p_items IS NULL OR pg_catalog.jsonb_array_length(p_items) NOT BETWEEN 2 AND 16 THEN
        RETURN runwick.rw_no_effect_v1('InvalidRequest', 'invalid_outbox_group', NULL);
    END IF;
    v_gate := runwick.rw_gate_v1(p_application, p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain);
    IF v_gate.status IS NOT NULL THEN
        RETURN v_gate;
    END IF;

    -- Command-ID order keeps receipt locks in one order across overlapping groups and singleton retries.
    SELECT pg_catalog.array_agg(x ORDER BY x.p_command_id) INTO v_items
    FROM pg_catalog.jsonb_populate_recordset(NULL::runwick.rw_outbox_group_item_v1, p_items) AS x;

    -- Plain items only (one job, no keys, no scopes, no rates), distinct commands and messages. Checked before any receipt.
    IF EXISTS (SELECT 1 FROM unnest(v_items) AS p
               WHERE pg_catalog.cardinality(p.p_job_ids) IS DISTINCT FROM 1
                  OR p.p_active_keys[1] IS NOT NULL OR p.p_window_keys[1] IS NOT NULL)
       OR (SELECT count(DISTINCT p.p_command_id) FROM unnest(v_items) AS p) <> pg_catalog.cardinality(v_items)
       OR (SELECT count(DISTINCT (p.p_source_store_id, p.p_message_id)) FROM unnest(v_items) AS p) <> pg_catalog.cardinality(v_items)
       OR (SELECT sum(pg_catalog.length(p.p_payloads[1])) FROM unnest(v_items) AS p) > 2097152 THEN
        RETURN runwick.rw_no_effect_v1('InvalidRequest', 'invalid_outbox_group', NULL);
    END IF;

    FOREACH v_item IN ARRAY v_items LOOP
        BEGIN
            v_reply := runwick.receive_outbox_v3(p_application, v_item.p_command_id, 'ReceiveOutbox', v_item.p_not_after,
                v_item.p_fingerprint, p_class, p_deadline_cap, p_store_id, p_revisions, p_epoch, p_capabilities, false,
                p_allow_drain, false,
                v_item.p_source_store_id, v_item.p_message_id, v_item.p_delivery_fingerprint, v_item.p_accept_until,
                v_item.p_profile, v_item.p_invalid,
                v_item.p_job_ids, v_item.p_job_names, v_item.p_payload_versions, v_item.p_profile_ids, v_item.p_profile_versions,
                v_item.p_queues, v_item.p_priorities, v_item.p_not_before_ms, v_item.p_active_keys, v_item.p_window_keys,
                v_item.p_window_ms, v_item.p_business, v_item.p_policy, v_item.p_payloads, v_item.p_headers,
                v_item.p_max_executions, v_item.p_max_unstarted, v_item.p_timeouts, v_item.p_retry, v_item.p_unstarted,
                ARRAY['[]'::jsonb], ARRAY['[]'::jsonb], p_faults);
            IF v_reply.effect <> 'Applied' THEN
                RAISE SQLSTATE 'RWS41';
            END IF;
        EXCEPTION WHEN SQLSTATE 'RWS41' THEN
            -- The subtransaction rolled the item back, its receipt placeholder included; the reply is kept.
            NULL;
        END;
        v_replies := v_replies || pg_catalog.jsonb_build_object(v_item.p_command_id::text, pg_catalog.to_jsonb(v_reply));
    END LOOP;

    RETURN runwick.rw_applied_v1('Applied', NULL, runwick.ms_floor(pg_catalog.clock_timestamp()), v_replies);
END
$fn$;
