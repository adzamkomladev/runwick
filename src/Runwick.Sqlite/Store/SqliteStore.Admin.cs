using System.Text;
using System.Text.Json.Nodes;
using Runwick.Protocol;

namespace Runwick.Sqlite.Store;

/// <summary>Helpers shared by the administrative operations (08 §2, §5): actor bounds, the control event and the audit row.</summary>
internal sealed partial class SqliteStore
{
    /// <summary>08 §2 bounds (PgStore.ValidateActor); returns the offending field or null.</summary>
    internal static string? ValidateActor(ControlActor actor)
    {
        static bool Bytes(string? s, int min, int max) => s is null ? min == 0 : Encoding.UTF8.GetByteCount(s) is var n && n >= min && n <= max;

        if (actor is null || !Bytes(actor.Subject, 1, 256) || actor.Subject.Any(char.IsControl))
        {
            return "actor.subject";
        }

        if (!Bytes(actor.Display, 0, 256))
        {
            return "actor.display";
        }

        if (string.IsNullOrEmpty(actor.Scheme) || actor.Scheme.Length > 64 || !char.IsAsciiLetterOrDigit(actor.Scheme[0])
            || actor.Scheme.Any(c => !(char.IsAsciiLetterOrDigit(c) || c is '.' or '_' or ':' or '-')))
        {
            return "actor.scheme";
        }

        return Bytes(actor.Reason, 0, 512) ? null : "reason";
    }

    static SqOutcome<T> Rollback<T>(SqTx tx, StoreStatus status, string code) where T : class =>
        new SqOutcome<T>.Rollback(tx.Reject<T>(status, code));

    static SqOutcome<T> RevisionConflict<T>(SqTx tx, long actual) where T : class =>
        new SqOutcome<T>.Rollback(StoreReply<T>.NoEffect(StoreStatus.VersionConflict,
            new StoreError("revision_conflict", ActualRevision: actual, Detail: $"actual={actual}"), tx.Now));

    static void ControlEvent(SqTx tx, Guid? jobId, Guid? attemptId, string kind, ControlActor actor, JsonObject details) => tx.Exec(tx.Sql("""
        INSERT INTO runwick_queue_events (application, job_id, attempt_id, command_id, event_kind, occurred_at, actor_id, details)
        VALUES (@application, @job_id, @attempt_id, @command_id, @kind, @now, @actor, @details)
        """).P("job_id", jobId).P("attempt_id", attemptId).P("command_id", tx.Op.CommandId).P("kind", kind).P("now", tx.Now)
            .P("actor", actor.Subject).P("details", details));

    /// <summary>The management audit row (08 §5), written only on success, in the effect's transaction.</summary>
    static void Audit(SqTx tx, string targetKind, string targetId, ControlActor actor, long? expectedRevision, long? resultingRevision, JsonObject details,
        string? operation = null) => Expect(tx.Exec(tx.Sql("""
        INSERT INTO runwick_management_audit (
            application, command_id, operation, target_kind, target_id, actor_subject, actor_display, actor_scheme, reason,
            expected_revision, resulting_revision, details, occurred_at)
        VALUES (@application, @command_id, @operation, @target_kind, @target_id, @subject, @display, @scheme, @reason,
                @expected, @resulting, @details, @now)
        """).P("command_id", tx.Op.CommandId).P("operation", operation ?? tx.Op.Operation).P("target_kind", targetKind).P("target_id", targetId)
            .P("subject", actor.Subject).P("display", actor.Display).P("scheme", actor.Scheme).P("reason", actor.Reason)
            .P("expected", expectedRevision).P("resulting", resultingRevision).P("details", details).P("now", tx.Now)) == 1);
}
