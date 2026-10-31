namespace Runwick.Scheduling.Evaluation;

/// <summary>The evaluator error codes of spec 12 §10. Wire names are the snake_case strings in <see cref="EvaluationErrors"/>.</summary>
internal enum EvaluationErrorCode
{
    InvalidGrammar,
    InvalidCalendar,
    InvalidZone,
    NoMatchWithinHorizon,
    StepLimitExceeded,
    RangeOverflow,
    MissingProfile,
    MissingArtifact,
    ProviderFailure,
    BudgetElapsed,
}

/// <summary>What a caller may conclude from an evaluator error (spec 12 §10.1).</summary>
internal enum EvaluationErrorClass
{
    /// <summary>The input is not a valid definition; reject it at registration or edit.</summary>
    Invalid,

    /// <summary>Deterministic under the definition's own profile: a candidate for <c>needs_review</c>.</summary>
    Deterministic,

    /// <summary>This process lacks the profile or artifact; another replica may be capable.</summary>
    LocalIncompatibility,

    /// <summary>The cooperative work budget elapsed: defer and report, never invalidate.</summary>
    LocalPressure,

    /// <summary>A library or provider failed unexpectedly; retry later, never invalidate.</summary>
    Transient,
}

internal static class EvaluationErrors
{
    public static string WireName(EvaluationErrorCode code) => code switch
    {
        EvaluationErrorCode.InvalidGrammar => "schedule_invalid_grammar",
        EvaluationErrorCode.InvalidCalendar => "schedule_invalid_calendar",
        EvaluationErrorCode.InvalidZone => "schedule_invalid_zone",
        EvaluationErrorCode.NoMatchWithinHorizon => "schedule_no_match_within_horizon",
        EvaluationErrorCode.StepLimitExceeded => "schedule_step_limit_exceeded",
        EvaluationErrorCode.RangeOverflow => "schedule_range_overflow",
        EvaluationErrorCode.MissingProfile => "schedule_missing_profile",
        EvaluationErrorCode.MissingArtifact => "schedule_missing_artifact",
        EvaluationErrorCode.ProviderFailure => "schedule_provider_failure",
        EvaluationErrorCode.BudgetElapsed => "schedule_budget_elapsed",
        _ => throw new ArgumentOutOfRangeException(nameof(code)),
    };

    public static EvaluationErrorClass Classify(EvaluationErrorCode code) => code switch
    {
        EvaluationErrorCode.InvalidGrammar or EvaluationErrorCode.InvalidCalendar or EvaluationErrorCode.InvalidZone
            => EvaluationErrorClass.Invalid,
        EvaluationErrorCode.NoMatchWithinHorizon or EvaluationErrorCode.StepLimitExceeded or EvaluationErrorCode.RangeOverflow
            => EvaluationErrorClass.Deterministic,
        EvaluationErrorCode.MissingProfile or EvaluationErrorCode.MissingArtifact
            => EvaluationErrorClass.LocalIncompatibility,
        EvaluationErrorCode.BudgetElapsed => EvaluationErrorClass.LocalPressure,
        _ => EvaluationErrorClass.Transient,
    };

    public static ScheduleEvaluationException Grammar(string message) => new(EvaluationErrorCode.InvalidGrammar, message);
}

/// <summary>Raised by the grammar, the zone mapper and the planner. Messages are bounded and never echo more than one token.</summary>
internal sealed class ScheduleEvaluationException(EvaluationErrorCode code, string message, Exception? inner = null)
    : Exception(message, inner)
{
    public EvaluationErrorCode Code { get; } = code;

    public EvaluationErrorClass Class => EvaluationErrors.Classify(Code);

    public string WireName => EvaluationErrors.WireName(Code);
}
