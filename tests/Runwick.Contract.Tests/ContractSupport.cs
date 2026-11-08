using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Faults;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.ReferenceModel;
using Runwick.Redis.Store;
using Runwick.Serialization;

namespace Runwick.Contract.Tests;

/// <summary>
/// Helpers the ported driver-suite cases share (G-4 gap closing). Counts come from the committed store-neutral snapshot, which every
/// driver can produce, so the same assertion runs on PostgreSQL, SQLite and Redis.
/// </summary>
internal static class Cx
{
    public static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    public static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    public static async Task<StoreSnapshot> Model(DriverFixture f) => (await f.ReadCommittedSnapshot()).Model;

    public static async Task<long> Jobs(DriverFixture f) => (await Model(f)).Jobs.Count;

    public static async Task<long> Attempts(DriverFixture f) => (await Model(f)).Attempts.Count;

    public static async Task<long> Permits(DriverFixture f) => (await Model(f)).Permits.Count;

    public static async Task<long> Receipts(DriverFixture f, Guid commandId) => (await Model(f)).Receipts.Count(r => r.CommandId == commandId);

    /// <summary>The store command for a prepared dispatch (same honest fingerprints as Prepare).</summary>
    public static StoreCommand<DispatchRequest> Command(PreparedDispatch p, Guid? commandId = null, DateTimeOffset? notAfter = null, string? application = null)
    {
        var id = commandId ?? p.OperationId.Value;
        var na = notAfter ?? p.NotAfter;
        var app = application ?? p.Application;
        var digests = id == p.OperationId.Value && na == p.NotAfter && app == p.Application
            ? p.Digests
            : Fingerprints.ForDispatch(p.StoreId, app, id, na.ToUnixTimeMilliseconds(), p.Intent);
        return new StoreCommand<DispatchRequest>(app, id, na, new DispatchRequest(new DispatchItem(p.Intent, p.Scopes, digests), p.StoreId, digests.Command));
    }

    public static Task<StoreReply<DispatchResult>> SendAsync(TestClient c, PreparedDispatch p, Guid? commandId = null, DateTimeOffset? notAfter = null) =>
        c.Driver.DispatchAsync(Command(p, commandId, notAfter, c.Application), CancellationToken.None);

    /// <summary>40P01 rollbacks the PostgreSQL runner observed; the other drivers have one writer and no lock-order deadlock to count, so 0.</summary>
    public static long Deadlocks(TestClient c) =>
        c.Services.GetService(typeof(Runwick.Postgres.Store.PgRunner)) is Runwick.Postgres.Store.PgRunner r ? r.Deadlocks : 0;

    public static async Task WithTimeout(Task task) => await task.WaitAsync(Wait);

    public static async Task<T> WithTimeout<T>(Task<T> task) => await task.WaitAsync(Wait);

    public static DateTimeOffset NotAfter(TimeSpan lifetime) =>
        DateTimeOffset.FromUnixTimeMilliseconds((DateTimeOffset.UtcNow + lifetime).ToUnixTimeMilliseconds());

    /// <summary>Admin SQL text for the store: the PostgreSQL form, with SQLite's table names.</summary>
    public static string Q(DriverFixture f, string sql) => f.Kind == StoreKind.Sqlite ? sql.Replace("runwick.", "runwick_", StringComparison.Ordinal) : sql;

    /// <summary>
    /// A reply lost after the store did its work. PostgreSQL and SQLite: AfterCommitBeforeReply + DropReply for the command. Redis has no fault
    /// points (22 §12), so the stand-in is its write-prefix seam: the client stops after <paramref name="redisWrites"/> writes and the
    /// outcome is unknown; the resend recovers the stored intent (REDIS-01). Dispose resets the seam.
    /// </summary>
    public sealed class ReplyLoss : IDisposable
    {
        readonly FaultHandle? _handle;
        readonly RedisStore? _redis;

        internal ReplyLoss(DriverFixture f, TestClient c, Guid commandId, int redisWrites)
        {
            if (f.Kind == StoreKind.Redis)
            {
                _redis = c.Services.GetRequiredService<RedisStore>();
                _redis.Runner.Client.FailAfter = redisWrites;
            }
            else
            {
                _handle = f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Command(commandId));
            }
        }

        /// <summary>Times the PostgreSQL/SQLite fault fired; -1 on Redis (the seam has no counter).</summary>
        public int Fired => _handle?.Fired ?? -1;

        public void Dispose()
        {
            if (_redis is not null)
            {
                _redis.Runner.Client.FailAfter = 0;
            }
        }
    }

    public static ReplyLoss LoseReply(DriverFixture f, TestClient c, Guid commandId, int redisWrites = 2) => new(f, c, commandId, redisWrites);

    /// <summary>The names of snapshot tables that hold jobs, payloads, generations and dispatch keys in this store's snapshot.</summary>
    public static IEnumerable<string> JobDataTables(DriverFixture f, IEnumerable<string> changed) => f.Kind == StoreKind.Redis
        ? changed.Where(t => t.StartsWith("job:", StringComparison.Ordinal) || t.StartsWith("pay:", StringComparison.Ordinal) || t.StartsWith("keys:", StringComparison.Ordinal))
        : changed.Where(t => t is "jobs" or "job_payloads" or "job_generations" or "dispatch_keys");
}
