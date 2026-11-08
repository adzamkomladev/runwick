-- 0017_management_dispatch (SP-032, 19 §6.2, §10): audited dispatch by job name. Schema revision 17; it applies after
-- 0016_retention. ManagementDispatch/1 runs the shared dispatch core (rw_dispatch_core_v2, unchanged) and writes one
-- management_audit row with operation 'Dispatch' in the same transaction. No new table, index or function: the only
-- change is the widened audit operation check. The list keeps every operation of 0015 and of 0016_retention
-- (EditRetentionPolicy, RetentionSummary); IF EXISTS drops whichever predecessor check this store has, so the same
-- migration applies whether 0016 widened the check (v6) or not (v5).
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE runwick.management_audit DROP CONSTRAINT IF EXISTS management_audit_operation_v5_ck;

ALTER TABLE runwick.management_audit DROP CONSTRAINT IF EXISTS management_audit_operation_v6_ck;

ALTER TABLE runwick.management_audit
    ADD CONSTRAINT management_audit_operation_v7_ck CHECK (operation IN (
        'Cancel', 'ManualRetry', 'RerunSucceeded', 'SetQueueState', 'EditScopePolicy',
        'EditRatePolicy', 'ActivateCapabilities',
        'SetScheduleState', 'UpdateSchedule', 'TriggerSchedule', 'ResolveCatchUp', 'RevalidateSchedule',
        'RecordScheduleEvaluationFailure',
        'SetPoolOverride',
        'RevealPayload', 'RevealError',
        'EditRetentionPolicy', 'RetentionSummary',
        'Dispatch'));
