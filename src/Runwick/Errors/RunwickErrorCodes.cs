namespace Runwick;

/// <summary>Stable error codes. Never renamed once released.</summary>
public static class RunwickErrorCodes
{
    // Configuration (RunwickConfigurationException.Errors[i].Code)
    public const string ConfigurationInvalid = "configuration_invalid";
    public const string RunwickAlreadyAdded = "runwick_already_added";
    public const string MissingApplication = "missing_application";
    public const string InvalidApplicationName = "invalid_application_name";
    public const string MissingStore = "missing_store";
    public const string DuplicateStore = "duplicate_store";
    public const string InvalidSchemaName = "invalid_schema_name";
    public const string MissingJobName = "missing_job_name";
    public const string InvalidJobName = "invalid_job_name";
    public const string IdentityConflict = "identity_conflict";
    public const string DuplicateJobName = "duplicate_job_name";
    public const string InvalidPayloadVersion = "invalid_payload_version";
    public const string InvalidQueueName = "invalid_queue_name";
    public const string UnknownQueue = "unknown_queue";
    public const string InvalidPriority = "invalid_priority";
    public const string SameLayerConflict = "same_layer_conflict";
    public const string InvalidPolicy = "invalid_policy";
    public const string JobNotRegistered = "job_not_registered";
    public const string DuplicateHandler = "duplicate_handler";
    public const string UpgradeNotAdjacent = "upgrade_not_adjacent";
    public const string UpgradeGap = "upgrade_gap";
    public const string UpgradeCycle = "upgrade_cycle";
    public const string UpgradeDuplicateRoute = "upgrade_duplicate_route";
    public const string UpgradeTypeConflict = "upgrade_type_conflict";
    public const string InvalidMiddlewareAlias = "invalid_middleware_alias";
    public const string UnknownMiddlewareAlias = "unknown_middleware_alias";
    public const string DuplicateMiddleware = "duplicate_middleware";
    public const string MiddlewareAliasConflict = "middleware_alias_conflict";
    public const string InvalidScope = "invalid_scope";
    public const string TooManyScopes = "too_many_scopes";
    public const string InvalidRatePolicy = "invalid_rate_policy";
    public const string TooManyRateScopes = "too_many_rate_scopes";
    public const string RateAdmissionNotActive = "rate_admission_not_active";
    public const string DuplicateWorker = "duplicate_worker";
    public const string WorkerWithoutPool = "worker_without_pool";
    public const string PoolWithoutQueue = "pool_without_queue";
    public const string DuplicatePool = "duplicate_pool";
    public const string InvalidPool = "invalid_pool";
    public const string PoolBudgetExceeded = "pool_budget_exceeded";
    public const string InvalidTiming = "invalid_timing";
    public const string CapabilitySetTooLarge = "capability_set_too_large";
    public const string FeatureNotAvailable = "feature_not_available";
    public const string UnsupportedCapability = "unsupported_capability";
    public const string RatePoliciesUnsupported = "rate_policies_unsupported";

    // Dispatch — known not applied
    public const string InvalidUniqueness = "invalid_uniqueness";
    public const string InvalidKey = "invalid_key";
    public const string UniquenessKeyMismatch = "uniqueness_key_mismatch";
    public const string UniquenessRequired = "uniqueness_required";
    public const string KeySelectorFailed = "key_selector_failed";
    public const string ScopeCapacityConflict = "scope_capacity_conflict";
    public const string InvalidMetadata = "invalid_metadata";
    public const string MetadataTooLarge = "metadata_too_large";
    public const string PayloadSerializationFailed = "payload_serialization_failed";
    public const string PayloadTooLarge = "payload_too_large";
    public const string PayloadInvalid = "payload_invalid";
    // Fine-grained Prepare/import codes (02-serialization-and-wire §11; spec 01 I-22).
    public const string PayloadInvalidUtf8 = "payload_invalid_utf8";
    public const string PayloadInvalidJson = "payload_invalid_json";
    public const string PayloadRootNotObject = "payload_root_not_object";
    public const string PayloadDepthExceeded = "payload_depth_exceeded";
    public const string PayloadStringTooLong = "payload_string_too_long";
    public const string PayloadPropertyNameTooLong = "payload_property_name_too_long";
    public const string PayloadTooManyProperties = "payload_too_many_properties";
    public const string PayloadArrayTooLong = "payload_array_too_long";
    public const string PayloadInvalidNumber = "payload_invalid_number";
    public const string PayloadNumberTooLong = "payload_number_too_long";
    public const string PayloadDuplicateProperty = "payload_duplicate_property";
    public const string PayloadUnmappedMember = "payload_unmapped_member";
    public const string PayloadMissingRequired = "payload_missing_required";
    public const string PayloadInvalidValue = "payload_invalid_value";
    public const string PreparedEnvelopeTooLarge = "prepared_envelope_too_large";
    public const string PreparedEnvelopeInvalid = "prepared_envelope_invalid";
    public const string PreparedEnvelopeUnsupportedVersion = "prepared_envelope_unsupported_version";
    public const string PreparedEnvelopeStoreMismatch = "prepared_envelope_store_mismatch";
    public const string PreparedEnvelopeApplicationMismatch = "prepared_envelope_application_mismatch";
    public const string PreparedEnvelopeDigestMismatch = "prepared_envelope_digest_mismatch";
    public const string EnvelopeTooLarge = "envelope_too_large";
    public const string EnvelopeInvalid = "envelope_invalid";
    public const string WrongStore = "wrong_store";
    public const string BatchEmpty = "batch_empty";
    public const string BatchTooLarge = "batch_too_large";
    public const string BatchEnvelopeTooLarge = "batch_envelope_too_large";
    public const string UniquenessConflict = "uniqueness_conflict";
    public const string UniquenessCompositionConflict = "uniqueness_composition_conflict";
    public const string CommandConflict = "command_conflict";
    public const string CommandExpired = "command_expired";
    public const string LocalCapacityExceeded = "local_capacity_exceeded";
    public const string StoreUnavailable = "store_unavailable";
    public const string StoreIncompatible = "store_incompatible";
    public const string AdmissionClosed = "admission_closed";
    public const string NotADispatchOperation = "not_a_dispatch_operation";
    public const string TransactionStoreMismatch = "transaction_store_mismatch";
    public const string TransactionInvalid = "transaction_invalid";

    // Dispatch — outcome unknown
    public const string OutcomeUnknown = "outcome_unknown";
    public const string CancelledAfterSend = "cancelled_after_send";
    public const string ExpiredUnknown = "expired_unknown";

    // Execution (attempt outcome codes assigned by the engine; R6 errors)
    public const string PayloadMalformed = "payload_malformed";
    public const string PayloadUpgradeFailed = "payload_upgrade_failed";
    public const string PipelineFault = "pipeline_fault";
    public const string MiddlewareContractFault = "middleware_contract_fault";
    public const string StagedCompletionNotCommitted = "staged_completion_not_committed";
    public const string ReservationAbandonmentExhausted = "reservation_abandonment_exhausted";
    public const string NoActiveAttempt = "no_active_attempt";
    public const string CompletionAlreadyStaged = "completion_already_staged";
    public const string OwnershipLost = "ownership_lost";

    // Schedules (13)
    public const string DuplicateScheduler = "duplicate_scheduler";
    public const string DuplicateSchedule = "duplicate_schedule";
    public const string ScheduleDefinitionMissing = "schedule_definition_missing";
    public const string InvalidSchedulePolicy = "invalid_schedule_policy";
    public const string ScheduleDelayUnsupported = "schedule_delay_unsupported";
    public const string ScheduleWindowUnsupported = "schedule_window_unsupported";
    public const string ScheduleOccurrenceReuseRequired = "schedule_occurrence_reuse_required";
    public const string SchedulerNotActive = "scheduler_not_active";
    public const string SchedulePaused = "schedule_paused";
    public const string EpisodeUnresolved = "episode_unresolved";
    public const string PlanTooLarge = "plan_too_large";
    public const string OccurrenceBelowHighWater = "occurrence_below_high_water";
    public const string CursorRewind = "cursor_rewind";
    public const string CutoffInFuture = "cutoff_in_future";
    public const string CatchUpLimit = "catch_up_limit";
    public const string CatchUpTooOld = "catch_up_too_old";
}
