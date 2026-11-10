-- 0019_worker_detail (16 Changes 2026-11-10): what each worker instance reports about itself with every presence publication
-- (05 §14: host figures, pools, settings, the attempts it runs and the last ones it finished), returned by ListWorkers.
-- Schema revision 19.
--
-- One nullable jsonb column; NULL = not reported (an older worker, or a row written before this migration). The store sends at
-- most 16 KiB of compact JSON (a larger detail is dropped, never the presence); the check is a backstop with room for the
-- spaces jsonb's text form adds.
-- Adding a nullable column without a default rewrites nothing. Released functions are never replaced in place (04 §7.17):
-- publish_presence_v2 runs publish_presence_v1 unchanged, then, when it applied, stores the detail on the row it wrote (the
-- same instance and presence sequence; v1 already holds that row's lock). A publication without detail clears the old one.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE runwick.worker_instances ADD COLUMN detail jsonb
    CONSTRAINT worker_instances_detail_ck CHECK (detail IS NULL OR (jsonb_typeof(detail) = 'object' AND octet_length(detail::text) <= 32768));

CREATE FUNCTION runwick.publish_presence_v2(p_application text, p_command_id uuid, p_operation text, p_not_after timestamptz,
    p_fingerprint bytea, p_class text, p_deadline_cap interval, p_store_id uuid, p_revisions bigint[], p_epoch integer,
    p_capabilities jsonb, p_is_creation boolean, p_allow_drain boolean, p_staged boolean,
    p_instance_id uuid, p_process_id integer, p_host_name text, p_build_id text, p_status text, p_capability_revision bigint,
    p_presence_sequence bigint, p_subscriptions jsonb, p_pool_targets jsonb, p_expiry interval,
    p_names text[], p_versions integer[], p_profile_ids text[], p_profile_versions integer[], p_queues text[],
    p_detail jsonb, p_faults jsonb DEFAULT NULL)
RETURNS runwick.rw_reply_v1 LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
    v_reply runwick.rw_reply_v1;
BEGIN
    v_reply := runwick.publish_presence_v1(p_application, p_command_id, p_operation, p_not_after, p_fingerprint, p_class, p_deadline_cap,
        p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain, p_staged,
        p_instance_id, p_process_id, p_host_name, p_build_id, p_status, p_capability_revision,
        p_presence_sequence, p_subscriptions, p_pool_targets, p_expiry,
        p_names, p_versions, p_profile_ids, p_profile_versions, p_queues, p_faults);
    IF v_reply.effect = 'Applied' THEN
        UPDATE runwick.worker_instances SET detail = p_detail
        WHERE application = p_application AND instance_id = p_instance_id AND presence_sequence = p_presence_sequence;
    END IF;
    RETURN v_reply;
END
$fn$;
