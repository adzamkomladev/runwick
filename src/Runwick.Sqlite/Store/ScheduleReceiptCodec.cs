using System.Text.Json.Nodes;
using Runwick.Protocol;

namespace Runwick.Sqlite.Store;

/// <summary>Receipt results of the schedule operations (13 §4.2, §6); replay returns exactly these values.</summary>
internal static class ScheduleReceiptCodec
{
    static string U(Guid g) => g.ToString("D");

    static Guid G(JsonNode? n) => Guid.Parse(n!.GetValue<string>());

    public static JsonObject Registration(ScheduleRegistration r) => new()
    {
        ["scheduleId"] = U(r.ScheduleId),
        ["outcome"] = r.Outcome,
        ["definitionRevision"] = r.DefinitionRevision,
        ["storedFingerprint"] = Convert.ToHexStringLower(r.StoredFingerprint),
    };

    public static ScheduleRegistration ReadRegistration(JsonObject o) =>
        new(G(o["scheduleId"]), o["outcome"]!.GetValue<string>(), o["definitionRevision"]!.GetValue<long>(),
            Convert.FromHexString(o["storedFingerprint"]!.GetValue<string>()));

    public static JsonObject Materialization(ScheduleMaterialization m) => new()
    {
        ["scheduleId"] = U(m.ScheduleId),
        ["cursorRevision"] = m.CursorRevision,
        ["occurrences"] = new JsonArray([.. m.Occurrences.Select(o => (JsonNode)new JsonObject
        {
            ["kind"] = o.Kind,
            ["key"] = o.Key,
            ["jobId"] = o.JobId is { } j ? U(j) : null,
            ["disposition"] = o.Disposition,
        })]),
    };

    public static ScheduleMaterialization ReadMaterialization(JsonObject o) =>
        new(G(o["scheduleId"]), o["cursorRevision"]!.GetValue<long>(),
            [.. o["occurrences"]!.AsArray().Select(x => new MaterializedOccurrence(
                x!["kind"]!.GetValue<string>(), x["key"]!.GetValue<string>(),
                x["jobId"] is { } j ? Guid.Parse(j.GetValue<string>()) : null, x["disposition"]!.GetValue<string>()))]);

    public static JsonObject State(ScheduleState s) => new()
    {
        ["key"] = s.Key,
        ["paused"] = s.Paused,
        ["needsReview"] = s.NeedsReview,
        ["definitionRevision"] = s.DefinitionRevision,
        ["controlRevision"] = s.ControlRevision,
        ["cursorRevision"] = s.CursorRevision,
    };

    public static ScheduleState ReadState(JsonObject o) =>
        new(o["key"]!.GetValue<string>(), o["paused"]!.GetValue<bool>(), o["needsReview"]!.GetValue<bool>(),
            o["definitionRevision"]!.GetValue<long>(), o["controlRevision"]!.GetValue<long>(), o["cursorRevision"]!.GetValue<long>());
}
