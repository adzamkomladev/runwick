using NodaTime.Text;

namespace Runwick.Scheduling.Evaluation;

/// <summary>
/// One preview row (spec 12 §9): the UTC instant, the local wall-clock time and offset at it, and the adjustment label.
/// Text forms are ISO-8601 and culture-invariant; <see cref="Skipped"/> is set only for a shifted gap.
/// </summary>
internal sealed record PreviewItem(long UtcEpochMilliseconds, string Utc, string Local, string Offset, string Label, string? Skipped)
{
    static readonly LocalDateTimePattern LocalPattern = LocalDateTimePattern.ExtendedIso;
    static readonly OffsetPattern OffsetText = OffsetPattern.GeneralInvariantWithZ;

    public static string LabelOf(OccurrenceKind kind) => kind switch
    {
        OccurrenceKind.Exact => "exact",
        OccurrenceKind.FoldEarlier => "fold-earlier",
        OccurrenceKind.FoldLater => "fold-later",
        OccurrenceKind.GapShifted => "gap-shifted",
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    public static PreviewItem From(Occurrence o) => new(
        o.UtcEpochMilliseconds,
        InstantPattern.ExtendedIso.Format(o.At),
        LocalPattern.Format(o.Local),
        OffsetText.Format(o.Offset),
        LabelOf(o.Kind),
        o is { SkippedFrom: { } from, SkippedTo: { } to } ? $"{LocalPattern.Format(from)}/{LocalPattern.Format(to)}" : null);
}
