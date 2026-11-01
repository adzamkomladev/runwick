# G-2 case table

Generated on 2026-11-02 by `Runwick.Workloads report --gate g2` from run 12 (`artifacts/g2/run12-suites/results`, commit `d2e0530`) and the combined workloads (`artifacts/g2/final/perf`). See [g2.md](g2.md) §3.

| Document | Case | Title | SP | Host project | Category | Result | Reason / evidence |
|---|---|---|---|---|---|---|---|
| driver-contract-suite | END-02 | Cancel_of_a_waiting_job_closes_it_releases_the_active_key_and_audits_once | SP-011 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| driver-contract-suite | END-03 | Cancel_of_an_owned_job_records_the_request_only_and_precedence_closes_it | SP-011 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| management-and-dashboard | MGMT-04 | Audit_effect_and_receipt_commit_together_or_not_at_all | SP-011 | Runwick.Postgres.Tests | Fault | passed | 1/1 tests passed |
| driver-contract-suite | UNI-04 | Manual_retry_creates_a_new_generation_and_reacquires_the_active_key | SP-011 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| postgres-schema-and-operations | PGX-04 | Manual_retry_conflicts_when_another_job_holds_the_active_key | SP-011 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| postgres-schema-and-operations | PGX-06 | Missing_payload_blocks_manual_retry | SP-011 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| driver-contract-suite | ADM-04 | Public_pause_blocks_reserve_only_and_resume_restores_it | SP-011 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| driver-contract-suite | ISO-01 | Controls_and_events_are_isolated_by_application | SP-011 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| driver-contract-suite | CFG-01 | Every_m3_operation_is_rejected_before_any_write_at_another_schema_revision; Each_m3_operation_version_is_listed_from_its_revision_and_never_before | SP-011 | Runwick.Postgres.Tests | Contract | passed | 3/3 tests passed |
| driver-contract-suite | OPS-03 | Control_receipts_are_administrative_durable_class | SP-011 | Runwick.Postgres.Tests | Contract | passed | 5/5 tests passed |
| 15 (I-1) | CTL-01 | A_capacity_decrease_drains_without_revoking_live_permits | SP-011 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| 15 (I-1) | CTL-02 | Membership_cannot_be_edited_and_unknown_scopes_are_rejected | SP-011 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| 15 (I-1) | CTL-03 | A_stale_position_is_a_conflict_with_no_change_receipt_or_audit | SP-011 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| 15 (I-1) | CTL-04 | One_receipt_and_one_audit_per_success_none_on_replay_or_rejection; Rerun_applies_only_to_succeeded_jobs; Actor_bounds_are_validated_before_submission | SP-011 | Runwick.Postgres.Tests | Contract | passed | 3/3 tests passed |
| 15 (I-1) | CTL-05 | Bulk_cancel_returns_per_item_results_each_with_its_own_receipt | SP-011 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| 15 (I-1) | EVT-01 | Rereading_from_a_cursor_returns_the_same_ordered_rows | SP-011 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| 15 (I-1) | EVT-02 | A_slow_writer_is_never_skipped_and_pages_stay_bounded | SP-011 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| 15 (I-1) | TXC-01 | Caller_rollback_leaves_the_attempt_owned_and_the_normal_path_settles | SP-011 | Runwick.Postgres.Tests | Fault | passed | 1/1 tests passed |
| 15 (I-1) | TXC-02 | Caller_commit_finalizes_exactly_once_without_an_engine_finish; Outside_an_attempt_completion_is_no_active_attempt | SP-011 | Runwick.Postgres.Tests | Contract | passed | 2/2 tests passed |
| 15 (I-1) | TXC-03 | A_lost_caller_commit_reply_resolves_by_receipt_without_rerunning_the_handler | SP-011 | Runwick.Postgres.Tests | Fault | passed | 1/1 tests passed |
| 15 (I-1) | TXC-04 | A_stale_owner_gets_ownership_lost_and_must_roll_back | SP-011 | Runwick.Postgres.Tests | Fault | passed | 1/1 tests passed |
| 15 (I-1) | TXC-05 | A_staged_completion_that_never_commits_fails_the_attempt | SP-011 | Runwick.Postgres.Tests | Fault | passed | 2/2 tests passed |
| 15 (I-1) | TXC-06 | A_handler_exception_after_a_committed_completion_leaves_the_commit_standing | SP-011 | Runwick.Postgres.Tests | Fault | passed | 1/1 tests passed |
| 15 (I-1) | MODE-01 | One_process_produces_works_and_controls | SP-011 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| 15 (I-1) | MODE-02 | A_producer_only_process_feeds_separate_workers | SP-011 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| 15 (I-1) | MODE-03 | Worker_only_processes_on_disjoint_queues_take_only_their_own_lane | SP-011 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| 15 (I-1) | MODE-04 | A_control_only_process_steers_a_separate_worker | SP-011 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| 15 (I-1) | MODE-05 | Producer_staged_completion_worker_and_control_run_together | SP-011 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| driver-contract-suite | CMD-01 | Same_control_concurrently_and_sequentially_applies_once | SP-011 | Runwick.Postgres.Tests | Contract | passed | 5/5 tests passed |
| driver-contract-suite | CMD-02 | Same_control_id_with_a_changed_body_or_deadline_conflicts | SP-011 | Runwick.Postgres.Tests | Contract | passed | 5/5 tests passed |
| driver-contract-suite | CMD-03 | A_lost_control_reply_is_outcome_unknown_and_the_same_command_replays; The_preview_service_resends_the_same_command_after_a_lost_reply | SP-011 | Runwick.Postgres.Tests | Fault | passed | 6/6 tests passed |
| driver-contract-suite | CMD-04 | A_control_whose_deadline_passes_during_a_lock_wait_expires_without_effect | SP-011 | Runwick.Postgres.Tests | Fault | passed | 1/1 tests passed |
| driver-contract-suite | CMD-05 | A_pruned_control_receipt_replays_as_expired_and_never_reapplies | SP-011 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| capacity-defaults-and-budgets | CAP-01 | Fixed_pools_keep_their_own_maxima_and_the_group_limit_under_hung_handlers; Invalid_fixed_pools_fail_startup_before_any_store_write; A_pool_with_no_mode_is_fixed_four_against_the_real_store | SP-012 | Runwick.Postgres.Tests | Contract | passed | 3/3 tests passed |
| capacity-defaults-and-budgets | CAP-02 | Queue_and_lane_shares_hold_against_the_real_store_and_a_returning_queue_banks_no_credit; Lane_shares_are_six_three_one_against_the_real_store | SP-012 | Runwick.Postgres.Tests | Contract | passed | 2/2 tests passed |
| capacity-defaults-and-budgets | CAP-03 | Strict_starves_an_aged_lane_and_strict_with_aging_takes_it_every_ninth_admission | SP-012 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| capacity-defaults-and-budgets | CAP-04 | A_saturated_queue_does_not_delay_another_queue_and_a_control_change_rechecks_at_once | SP-012 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| driver-contract-suite | OBS-01 | With_every_wake_dropped_polling_finds_work_controls_and_recovery | SP-012 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| worker-engine-internals | WRK-15 | A_saturated_queue_sends_no_reserves_and_writes_nothing_per_job; A_full_type_scope_over_ten_thousand_jobs_costs_no_per_job_transaction | SP-012 | Runwick.Postgres.Tests | Contract | passed | 2/2 tests passed |
| worker-engine-internals | WRK-19 | Expired_or_pruned_presence_changes_no_ownership_and_renewals_continue | SP-012 | Runwick.Postgres.Tests | Fault | passed | 1/1 tests passed |
| postgres-schema-and-operations | PGX-07 | Probe_reserve_and_diagnostic_plans_use_the_ready_index_with_bounded_buffers_at_a_million_rows | SP-012 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| 15 (I-1) | RDY-01 | A_saturated_queue_sends_no_reserves_and_writes_nothing_per_job; A_full_type_scope_over_ten_thousand_jobs_costs_no_per_job_transaction | SP-012 | Runwick.Postgres.Tests | Contract | passed | 2/2 tests passed |
| 15 (I-1) | RDY-02 | Expired_or_pruned_presence_changes_no_ownership_and_renewals_continue | SP-012 | Runwick.Postgres.Tests | Fault | passed | 1/1 tests passed |
| 15 (I-1) | RDY-03 | Unsupported_backlog_stays_visible_and_is_diagnosed_without_claims | SP-012 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| 15 (I-1) | MODE-01 | Each_worker_role_publishes_its_subscriptions_and_capabilities_and_producers_publish_none; One_peer_process_produces_works_and_controls | SP-012 | Runwick.Postgres.Tests | Contract | passed | 2/2 tests passed |
| 15 (I-1) | MODE-02 | Each_worker_role_publishes_its_subscriptions_and_capabilities_and_producers_publish_none; A_producer_only_process_feeds_three_worker_processes | SP-012 | Runwick.Postgres.Tests | Contract | passed | 2/2 tests passed |
| 15 (I-1) | MODE-03 | Each_worker_role_publishes_its_subscriptions_and_capabilities_and_producers_publish_none; Worker_processes_on_disjoint_queues_take_only_their_own_lane | SP-012 | Runwick.Postgres.Tests | Contract | passed | 2/2 tests passed |
| 15 (I-1) | MODE-04 | Each_worker_role_publishes_its_subscriptions_and_capabilities_and_producers_publish_none; A_control_only_process_steers_a_worker_process | SP-012 | Runwick.Postgres.Tests | Contract | passed | 2/2 tests passed |
| 15 (I-1) | MODE-05 | Each_worker_role_publishes_its_subscriptions_and_capabilities_and_producers_publish_none; Producer_control_and_three_staged_completion_worker_processes_run_together | SP-012 | Runwick.Postgres.Tests | Contract | passed | 2/2 tests passed |
| driver-contract-suite | CMD-06 | A_replayed_deferral_returns_the_same_decision_without_a_second_close | SP-013 | Runwick.Postgres.Tests | Fault | passed | 1/1 tests passed |
| capacity-defaults-and-budgets | CAP-05 | Rate_policy_bounds_are_validated_at_registration_and_on_edit | SP-013 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| postgres-schema-and-operations | PGX-01 | Gate_changes_race_every_command_family_at_the_current_revision | SP-013 | Runwick.Postgres.Tests | Fault | passed | 11/11 tests passed |
| postgres-schema-and-operations | PGX-03 | Rate_scopes_are_refused_until_the_application_activates_rate_admission | SP-013 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| execution-defaults-and-timing | TIME-06 | The_worker_retires_a_deferred_begin_and_runs_the_job_once_admitted | SP-013 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| postgres-upgrades | UPG-02 | After_activation_an_old_binary_cannot_begin_and_keeps_only_settlement_paths | SP-013 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| postgres-upgrades | UPG-06 | Rollback_after_activation_is_rejected | SP-013 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| capacity-control | RATE-01 | Competing_begins_get_at_most_n_grants_per_rolling_window | SP-013 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| capacity-control | RATE-02 | One_full_scope_defers_without_a_partial_charge_and_releases_permits | SP-013 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| capacity-control | RATE-03 | A_failed_execution_keeps_its_grant_and_the_retry_needs_a_new_one | SP-013 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| capacity-control | RATE-04 | Window_edits_freeze_admission_and_logical_time_never_goes_backward | SP-013 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| capacity-control | RATE-05 | Three_replica_processes_never_grant_more_than_n_per_rolling_window | SP-013 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| capacity-control | RATE-06 | A_rate_rejection_after_reservation_releases_exact_permits_and_sets_eligibility | SP-013 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| capacity-control | RATE-07 | A_lost_begin_reply_replays_without_a_second_grant_or_start | SP-013 | Runwick.Postgres.Tests | Fault | passed | 1/1 tests passed |
| capacity-control | RATE-08 | A_crash_after_begin_keeps_the_grant_and_the_retry_needs_a_new_one | SP-013 | Runwick.Postgres.Tests | Fault | passed | 1/1 tests passed |
| capacity-control | RATE-09 | Opposite_order_rate_scopes_lock_in_uuid_order_and_never_deadlock | SP-013 | Runwick.Postgres.Tests | Fault | passed | 1/1 tests passed |
| capacity-control | RATE-10 | A_backward_store_clock_never_reverses_rate_time_or_refills_early | SP-013 | Runwick.Postgres.Tests | Fault | passed | 1/1 tests passed |
| 15 (I-1) | SCL-01 | Sustained_idleness_shrinks_by_bounded_steps_to_the_minimum; Changes_are_at_least_one_cooldown_apart_and_need_consecutive_evidence; Sustained_demand_grows_by_bounded_steps_to_the_maximum | SP-014 | Runwick.Core.Tests | Pure | passed | 3/3 tests passed |
| 15 (I-1) | SCL-02 | Two_workers_on_one_queue_share_the_backlog_and_the_survivor_takes_it_all | SP-014 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| 15 (I-1) | SCL-03 | An_oscillating_backlog_and_long_jobs_never_move_the_target; Automatic_targets_bound_hung_handlers_and_reductions_cancel_nothing | SP-014 | Runwick.Core.Tests, Runwick.Postgres.Tests | Contract, Pure | passed | 2/2 tests passed |
| 15 (I-1) | SCL-04 | A_store_outage_freezes_automatic_targets_with_pending_finishes | SP-014 | Runwick.Postgres.Tests | Fault | passed | 1/1 tests passed |
| 15 (I-1) | SCL-05 | A_zero_minimum_keeps_discovery_through_missing_metrics_and_recovers_from_zero | SP-014 | Runwick.Core.Tests | Pure | passed | 1/1 tests passed |
| capacity-defaults-and-budgets | CAP-01 | Automatic_targets_bound_hung_handlers_and_reductions_cancel_nothing | SP-014 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| capacity-defaults-and-budgets | CAP-06 | Occupancy_is_a_time_weighted_mean_over_the_window; The_service_estimate_is_cold_then_an_ewma_with_a_live_lower_bound; Each_lane_counts_at_most_2000_due_rows_and_a_full_lane_sets_truncated | SP-014 | Runwick.Core.Tests, Runwick.Postgres.Tests | Contract, Pure | passed | 3/3 tests passed |
| capacity-defaults-and-budgets | CAP-07 | Demand_inside_the_band_holds_and_interrupted_evidence_restarts; Unknown_demand_holds_and_is_never_read_as_zero; An_override_applies_at_once_stops_automatic_changes_and_is_clamped_to_max; Minimum_zero_bootstraps_one_ticket_until_trusted_idle_evidence; The_boundary_waiver_reaches_max_and_min_inside_the_band; Growth_needs_due_backlog_and_freezes_on_pressure_signals; Desired_is_occupancy_plus_drain_rate_over_utilization; A_membership_change_restarts_evidence; A_late_estimate_is_unknown_demand_and_never_overlaps_the_next; Pool_overrides_are_revisioned_replayable_and_audited; Unchanged_targets_are_never_recorded_and_unknown_demand_is_not_zero | SP-014 | Runwick.Core.Tests, Runwick.Postgres.Tests | Contract, Fault, Pure | passed | 14/14 tests passed |
| capacity-defaults-and-budgets | CAP-08 | The_blocked_ratio_needs_twenty_cycles_and_three_quarters_blocked; Two_workers_on_one_queue_share_the_backlog_and_the_survivor_takes_it_all; The_estimate_shares_backlog_and_an_override_to_zero_cancels_nothing | SP-014 | Runwick.Core.Tests, Runwick.Postgres.Tests | Contract, Pure | passed | 3/3 tests passed |
| capacity-defaults-and-budgets | CAP-10 | A_pressure_stop_freezes_growth_and_resets_its_evidence | SP-014 | Runwick.Postgres.Tests | Fault | passed | 1/1 tests passed |
| capacity-defaults-and-budgets | CAP-11 | A_late_estimate_is_unknown_demand_and_never_overlaps_the_next; A_store_outage_freezes_automatic_targets_with_pending_finishes | SP-014 | Runwick.Postgres.Tests | Fault | passed | 2/2 tests passed |
| capacity-defaults-and-budgets | CAP-12 | Automatic_targets_bound_hung_handlers_and_reductions_cancel_nothing; The_estimate_shares_backlog_and_an_override_to_zero_cancels_nothing | SP-014 | Runwick.Postgres.Tests | Contract | passed | 2/2 tests passed |
| worker-engine-internals | WRK-02 | Automatic_targets_bound_hung_handlers_and_reductions_cancel_nothing; The_estimate_shares_backlog_and_an_override_to_zero_cancels_nothing | SP-014 | Runwick.Postgres.Tests | Contract | passed | 2/2 tests passed |
| scheduler-evaluator | SCHED-01 | Unsupported_syntax_is_rejected_by_the_grammar; A_field_selecting_its_whole_domain_is_unrestricted; Cronos_receives_only_the_normalized_rendering; Five_field_values_ranges_steps_and_names_normalize_to_sets; Six_field_dialect_is_explicit_and_adds_seconds; Equivalent_text_has_one_fingerprint; Leap_second_sixty_is_rejected_in_the_seconds_field; Day_of_week_expands_in_0_to_7_before_seven_becomes_sunday | SP-015 | Runwick.Core.Tests | Pure | passed | 76/76 tests passed |
| scheduler-evaluator | SCHED-02 | A_monday_that_is_also_the_first_emits_once; Restricted_day_fields_match_either_day_of_month_or_day_of_week; Both_day_fields_restricted_compile_to_an_or_of_two_streams | SP-015 | Runwick.Core.Tests | Pure | passed | 3/3 tests passed |
| scheduler-evaluator | SCHED-03 | February_29_skips_2100_and_finds_2104_after_2096; A_calendar_possible_in_some_selected_month_is_accepted; An_impossible_day_of_month_branch_does_not_invalidate_a_weekday_branch; An_impossible_calendar_without_a_weekday_alternative_is_rejected | SP-015 | Runwick.Core.Tests | Pure | passed | 9/9 tests passed |
| scheduler-evaluator | SCHED-04 | Berlin_spring_gap_shift_emits_the_first_valid_instant_not_0330; Berlin_spring_gap_skip_emits_nothing_that_day | SP-015 | Runwick.Core.Tests | Pure | passed | 2/2 tests passed |
| scheduler-evaluator | SCHED-05 | Berlin_autumn_fold_follows_the_policy_in_utc_order; Fold_occurrences_carry_their_side_of_the_fold | SP-015 | Runwick.Core.Tests | Pure | passed | 4/4 tests passed |
| scheduler-evaluator | SCHED-06 | Every_minute_over_the_repeated_hour_with_both_yields_120_ordered_instants; An_excluded_fold_side_is_jumped_not_scanned; A_cursor_inside_the_fold_finds_a_later_match_with_an_earlier_local_label | SP-015 | Runwick.Core.Tests | Pure | passed | 3/3 tests passed |
| scheduler-evaluator | SCHED-07 | A_gap_shift_colliding_with_a_normal_boundary_match_yields_one_occurrence; Many_gap_matches_are_one_bounded_query | SP-015 | Runwick.Core.Tests | Pure | passed | 2/2 tests passed |
| scheduler-evaluator | SCHED-08 | Output_is_strictly_increasing_across_transitions; Lord_Howe_half_hour_fold; Sao_Paulo_midnight_gap; Second_valued_offsets_are_not_rounded; Lord_Howe_half_hour_gap; Apia_shift_reports_a_skipped_whole_day; Six_field_matches_have_second_resolution; Sao_Paulo_midnight_fold_keeps_half_open_boundaries; Apia_skipped_2011_12_30_entirely | SP-015 | Runwick.Core.Tests | Pure | passed | 14/14 tests passed |
| scheduler-evaluator | SCHED-14 | Each_lookup_searches_at_most_the_horizon; A_dense_six_field_expression_stays_inside_the_bounds; An_elapsed_budget_is_local_pressure_not_invalidity; Dates_past_the_supported_range_overflow_instead_of_wrapping; Input_is_bounded_to_64_list_terms; One_evaluator_task_per_group_and_no_replacement_on_cancellation; A_sparse_expression_is_a_few_steps_not_a_scan; Input_is_bounded_to_256_characters; The_plan_step_bound_is_deterministic_and_reviewable; Preview_lists_the_next_ten_with_labels | SP-015 | Runwick.Core.Tests | Pure | passed | 10/10 tests passed |
| provider-dependency-plan | DEP-01 | The_graph_check_passes_with_the_evaluator_packages_allowed_for_runwick_only; Central_pins_lock_files_and_the_graph_agree_exactly; No_public_api_exposes_a_cronos_or_nodatime_type | SP-015 | Runwick.Core.Tests | Pure | passed | 3/3 tests passed |
| identity-and-serialization | SER-11 | The_vectors_carry_the_spec_12_golden_fingerprints; Definition_vector_bytes_and_fingerprint_match | SP-015 | Runwick.Core.Tests | Pure | passed | 9/9 tests passed |
| scheduler-evaluator | SCHED-09 | Catch_up_emits_all_eleven_and_closes_the_episode; Run_once_emits_the_earliest_missed_minute_covering_through_the_cutoff; Grace_equality_counts_as_misfired; Skip_emits_nothing_and_audits_the_missed_range; A_schedule_not_yet_due_only_moves_its_scan_position; Misfire_policies_cover_the_missed_range; The_hosted_scheduler_seeds_once_materializes_and_jobs_see_their_occurrence | SP-016 | Runwick.Core.Tests, Runwick.Postgres.Tests | Contract, Pure | passed | 10/10 tests passed |
| scheduler-evaluator | SCHED-10 | An_episode_of_exactly_the_limit_completes_and_one_more_needs_review; A_backlog_older_than_the_catch_up_age_needs_review_with_the_cursor_kept | SP-016 | Runwick.Core.Tests, Runwick.Postgres.Tests | Contract, Pure | passed | 5/5 tests passed |
| scheduler-evaluator | SCHED-11 | A_missing_profile_is_a_local_diagnostic_only | SP-016 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| scheduler-evaluator | SCHED-12 | A_lost_materialization_reply_keeps_the_same_command | SP-016 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| scheduler-evaluator | SCHED-13 | Plans_carry_the_revisions_they_were_made_against_and_never_rewind; An_edit_cuts_over_forward_and_never_rewinds; Registration_is_gated_seeds_once_and_reports_drift; The_hosted_scheduler_seeds_once_materializes_and_jobs_see_their_occurrence; A_plan_cut_off_after_store_time_is_rejected_and_the_cursor_waits | SP-016 | Runwick.Core.Tests, Runwick.Postgres.Tests | Contract, Pure | passed | 5/5 tests passed |
| scheduler-evaluator | SCHED-14 | An_elapsed_evaluation_budget_backs_off_locally_and_writes_nothing | SP-016 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| scheduler-evaluator | SCHED-15 | A_pending_resume_policy_covers_through_its_cutoff_then_normal_rules_apply; A_one_time_schedule_runs_once_late_or_on_time_and_is_then_exhausted; Manual_triggers_are_idempotent_and_ignore_the_cursor; The_hosted_scheduler_seeds_once_materializes_and_jobs_see_their_occurrence; Serialized_catch_up_jobs_never_run_concurrently | SP-016 | Runwick.Core.Tests, Runwick.Postgres.Tests | Contract, Pure | passed | 8/8 tests passed |
| scheduling-internals | SCH-01 | Competing_materializations_of_one_plan_create_one_set_of_jobs; Cross_occurrence_uniqueness_needs_explicit_reuse_and_then_links | SP-016 | Runwick.Postgres.Tests | Contract | passed | 2/2 tests passed |
| scheduling-internals | SCH-02 | A_fault_inside_materialization_leaves_nothing | SP-016 | Runwick.Postgres.Tests | Contract | passed | 2/2 tests passed |
| scheduling-internals | SCH-03 | Pause_racing_materialization_has_one_serial_outcome; A_paused_queue_keeps_materializing_while_execution_waits | SP-016 | Runwick.Postgres.Tests | Contract | passed | 2/2 tests passed |
| scheduling-internals | SCH-04 | Catch_up_runs_32_32_6_under_one_cutoff_with_pacing; A_normal_backlog_over_one_batch_is_partial_and_resumes_at_the_first_unemitted; Catch_up_runs_in_batches_under_a_fixed_cutoff | SP-016 | Runwick.Core.Tests, Runwick.Postgres.Tests | Contract, Pure | passed | 3/3 tests passed |
| scheduling-internals | SCH-05 | Pruned_history_never_redispatches_below_the_high_water_mark | SP-016 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| postgres-schema-and-operations | PGX-02 | Scheduled_jobs_carry_the_template_profile_tuple | SP-016 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| postgres-schema-and-operations | PGX-04 | Materialization_versus_scope_creation_and_edits_in_both_orders; Materialization_and_scope_creation_in_opposite_orders_never_deadlock | SP-016 | Runwick.Postgres.Tests | Fault | passed | 2/2 tests passed |
| driver-contract-suite | ISO-01 | The_same_schedule_key_in_two_applications_never_interacts | SP-016 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| driver-contract-suite | CMD-01 | Schedule_controls_follow_the_receipt_protocol | SP-016 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| driver-contract-suite | CMD-02 | Schedule_controls_follow_the_receipt_protocol | SP-016 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| driver-contract-suite | CMD-03 | Schedule_controls_follow_the_receipt_protocol | SP-016 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| driver-contract-suite | CMD-04 | A_schedule_command_whose_deadline_passes_during_a_lock_wait_expires_without_effect | SP-016 | Runwick.Postgres.Tests | Fault | passed | 1/1 tests passed |
| driver-contract-suite | CMD-05 | Pruned_schedule_receipts_replay_as_expired_and_never_reapply | SP-016 | Runwick.Postgres.Tests | Contract | passed | 1/1 tests passed |
| transactional-outbox | OUTBOX-01 | Rollback_removes_business_and_outbox_writes_and_Stage_never_contacts_the_destination | SP-017 | Runwick.Outbox.Tests | Contract | passed | 2/2 tests passed |
| transactional-outbox | OUTBOX-02 | Lost_delivered_mark_maps_the_retry_to_the_original_job | SP-017 | Runwick.Outbox.Tests | Fault | passed | 2/2 tests passed |
| transactional-outbox | OUTBOX-03 | Two_owners_a_stale_token_and_an_expired_transport_request_leave_one_mapping | SP-017 | Runwick.Outbox.Tests | Fault | passed | 2/2 tests passed |
| transactional-outbox | OUTBOX-04 | Nothing_is_newly_delivered_at_or_after_the_absolute_cutoff | SP-017 | Runwick.Outbox.Tests | Fault | passed | 2/2 tests passed |
| transactional-outbox | OUTBOX-05 | Unavailable_destination_policy_drift_and_malformed_intent_are_visible_without_false_success | SP-017 | Runwick.Outbox.Tests | Fault | passed | 2/2 tests passed |
| transactional-outbox | OUTBOX-06 | Restores_show_the_finite_horizon_and_the_history_loss_limit | SP-017 | Runwick.Outbox.Tests | Fault | passed | 2/2 tests passed |
| transactional-outbox | OUTBOX-07 | Connection_task_retry_and_shutdown_bounds_hold_through_a_long_outage | SP-017 | Runwick.Outbox.Tests | Fault | passed | 1/1 tests passed |
| driver-contract-suite | OUT-01 | Concurrent_relays_with_lost_marks_deduplicate_by_message_identity_within_the_horizon | SP-017 | Runwick.Outbox.Tests | Fault | passed | 2/2 tests passed |
| driver-contract-suite | OUT-02 | Replay_outside_the_retained_history_is_an_explicit_limit_not_a_silent_dispatch | SP-017 | Runwick.Outbox.Tests | Contract | passed | 2/2 tests passed |
| driver-contract-suite | TX-01 | Business_row_and_staged_message_roll_back_together | SP-017 | Runwick.Outbox.Tests | Contract | passed | 2/2 tests passed |
| driver-contract-suite | TX-02 | Stage_leaves_the_caller_transaction_open_and_commits_nothing_itself | SP-017 | Runwick.Outbox.Tests | Contract | passed | 2/2 tests passed |
| driver-contract-suite | TX-03 | A_stage_cancelled_after_its_insert_leaves_nothing_staged_in_the_caller_transaction; A_sqlite_error_inside_the_savepoint_surfaces_unchanged_and_the_caller_keeps_its_transaction; Caller_transaction_failure_surfaces_unchanged_and_a_whole_retry_stages_once | SP-017 | Runwick.Outbox.Tests | Fault | passed | 5/5 tests passed |
| driver-contract-suite | TX-04 | Lost_caller_commit_reply_makes_no_success_claim_and_the_relay_resolves_by_message_identity | SP-017 | Runwick.Outbox.Tests | Fault | passed | 1/1 tests passed |
| driver-contract-suite | CMD-01 | Same_command_concurrent_and_sequential_replay_applies_once | SP-017 | Runwick.Outbox.Tests | Contract | passed | 1/1 tests passed |
| driver-contract-suite | CMD-02 | Same_command_id_with_changed_body_deadline_operation_or_identity_conflicts | SP-017 | Runwick.Outbox.Tests | Contract | passed | 1/1 tests passed |
| driver-contract-suite | CMD-03 | Committed_receive_with_a_dropped_reply_replays_and_the_relay_resends_the_same_command | SP-017 | Runwick.Outbox.Tests | Fault | passed | 1/1 tests passed |
| driver-contract-suite | CMD-04 | Deadline_expiring_while_waiting_for_the_delivery_identity_lock_applies_nothing | SP-017 | Runwick.Outbox.Tests | Fault | passed | 1/1 tests passed |
| driver-contract-suite | CMD-05 | Pruned_transport_receipt_never_reapplies_and_a_new_command_resolves_through_the_mapping | SP-017 | Runwick.Outbox.Tests | Contract | passed | 1/1 tests passed |
| driver-contract-suite | ISO-01 | Delivery_identities_are_isolated_per_application | SP-017 | Runwick.Outbox.Tests | Contract | passed | 1/1 tests passed |
| identity-and-serialization | SER-11 | Delivery_and_command_vectors_match; Ob0_freezes_the_gv_b0_intent_and_the_transport_is_outside_the_delivery_identity; Stored_fingerprints_match_the_independent_encoder_on_both_sources | SP-017 | Runwick.Core.Tests, Runwick.Outbox.Tests | Contract, Pure | passed | 12/12 tests passed |
| management-and-dashboard | MGMT-04 (deferred part) | HTTP half, authorization | SP-020 | — | — | not-applicable | deferred by 15 to SP-020 |
| driver-contract-suite | UNI-04 (deferred part) | window survival across retention cleanup | SP-021 | — | — | not-applicable | deferred by 15 to SP-021 |
| 15 (I-1) | TXC-01 (deferred part) | SQLite twin of transactional completion (TXC-01 … TXC-06) | SP-019 | — | — | not-applicable | deferred by 15 to SP-019 |
| capacity-control | RATE-04 (deferred part) | enlargement/cleanup race (capacity-control §6) | SP-021 | — | — | not-applicable | deferred by 15 to SP-021 |
| capacity-defaults-and-budgets | CAP-11 (deferred part) | dashboard/management lanes | SP-020, SP-024 | — | — | not-applicable | deferred by 15 to SP-020, SP-024 |
| scheduling-internals | SCH-05 (deferred part) | schedule delete and tombstones | SP-020, SP-021 | — | — | not-applicable | deferred by 15 to SP-020, SP-021 |
| 15 (I-1) | SCL-01 (deferred part) | scaling-event retention (06 §1.13) | SP-021 | — | — | not-applicable | deferred by 15 to SP-021 |
| 07 §14.3 (inferred) | G1-M1 | workload (15 §11) | SP-012 | Runwick.Workloads | Perf | passed | 1 run(s), 0 correctness violation(s), 0 run failure(s); 20261101-042726-G1-M1/result.json |
| 07 §14.3 (inferred) | G1-M3 | workload (15 §11) | SP-012 | Runwick.Workloads | Perf | passed | 1 run(s), 0 correctness violation(s), 0 run failure(s); 20261101-043853-G1-M3/result.json |
| performance-acceptance-plan | P-04 | workload (15 §11) | SP-012 | Runwick.Workloads | Perf | passed | 4 run(s), 0 correctness violation(s), 0 run failure(s); 20261101-045633-P-04/result.json |
| performance-acceptance-plan | P-05 | workload (15 §11) | SP-012 | Runwick.Workloads | Perf | passed | 6 run(s), 0 correctness violation(s), 0 run failure(s); 20261101-065826-P-05/result.json |
| performance-acceptance-plan | P-06r | workload (15 §11) | SP-013 | Runwick.Workloads | Perf | passed | 3 run(s), 0 correctness violation(s), 0 run failure(s); 20261101-151441-P-06r/result.json |
| performance-acceptance-plan | P-06s | workload (15 §11) | SP-016 | Runwick.Workloads | Perf | passed | 1 run(s), 0 correctness violation(s), 0 run failure(s); 20261101-111357-P-06s/result.json |
| performance-acceptance-plan | P-09 | workload (15 §11) | SP-017 | Runwick.Workloads | Perf | passed | 1 run(s), 0 correctness violation(s), 0 run failure(s); 20261101-191032-P-09/result.json |

## Acceptance checklist (15 §12.2 rule 6)

| G-2 box | Rows | Passed | Not applicable | Other | Satisfied |
|---|---|---|---|---|---|
| Five deployments | 10 | 10 | 0 | 0 | yes |
| Controls and audit races | 9 | 8 | 1 | 0 | yes |
| Bounded readiness and diagnostics | 6 | 6 | 0 | 0 | yes |
| Rates across replicas with old-writer rejection | 13 | 12 | 1 | 0 | yes |
| Scaler traces | 5 | 4 | 1 | 0 | yes |
| Cron/DST, materialization, misfire, edit, retention | 22 | 21 | 1 | 0 | yes |
| Outbox contract | 9 | 9 | 0 | 0 | yes |
