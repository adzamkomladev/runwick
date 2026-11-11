using Npgsql;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using StackExchange.Redis;

namespace Runwick.Contract.Tests;

/// <summary>
/// Redis server-wide stalls and layout overrides affect every application on the shared fixture. xUnit runs this
/// collection separately so tests of other applications only observe the normal server.
/// </summary>
[CollectionDefinition(nameof(RedisServerWideCollection), DisableParallelization = true)]
public sealed class RedisServerWideCollection;

/// <summary>
/// Makes protocol commands wait on the store the way a contended lock does, then lets them go. This is how the "waits on a lock" cases
/// (CMD-04, OWN-03) get their wait on every driver:
/// PostgreSQL holds the resource-scope rows <c>FOR UPDATE</c> in an open transaction (what the original PostgreSQL cases do);
/// SQLite holds its single writer inside another command at a <c>BeforeCommit</c> barrier, so later commands queue behind it;
/// Redis has no locks, so <c>CLIENT PAUSE ... WRITE</c> keeps every script waiting in the server until it is released (the server then
/// decides each one with its own clock, which is the property the cases check).
/// </summary>
internal sealed class WriteHold : IAsyncDisposable
{
    readonly DriverFixture _f;
    NpgsqlConnection? _pg;
    NpgsqlTransaction? _tx;
    FaultHandle? _barrier;
    Task? _holder;
    bool _released;

    WriteHold(DriverFixture f) => _f = f;

    public static async Task<WriteHold> AcquireAsync(DriverFixture f, TestClient c)
    {
        var hold = new WriteHold(f);
        switch (f.Kind)
        {
            case StoreKind.Postgres:
                hold._pg = new NpgsqlConnection(f.ClientConnectionString("app", "business"));
                await hold._pg.OpenAsync();
                hold._tx = await hold._pg.BeginTransactionAsync();
                await using (var cmd = new NpgsqlCommand("SELECT 1 FROM runwick.concurrency_scopes WHERE scope_kind = 'resource' FOR UPDATE", hold._pg, hold._tx))
                {
                    await cmd.ExecuteNonQueryAsync();
                }

                break;
            case StoreKind.Sqlite:
                hold._barrier = f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Op(Operations.Dispatch));
                hold._holder = Task.Run(() => c.Dispatcher.DispatchAsync(new PingJob("write-holder")));
                await DriverFixture.WaitAtBarrier(hold._barrier, Cx.Wait);
                break;
            default:
                var mux = await RedisServer.AdminAsync();
                await mux.GetServer(mux.GetEndPoints()[0]).ExecuteAsync("CLIENT", "PAUSE", 60_000, "WRITE");
                break;
        }

        return hold;
    }

    /// <summary>Lets the waiting commands run. Safe to call twice.</summary>
    public async Task ReleaseAsync()
    {
        if (_released)
        {
            return;
        }

        _released = true;
        switch (_f.Kind)
        {
            case StoreKind.Postgres:
                await _tx!.RollbackAsync();
                await _pg!.DisposeAsync();
                break;
            case StoreKind.Sqlite:
                DriverFixture.ReleaseBarrier(_barrier!);
                await _holder!.WaitAsync(Cx.Wait);
                break;
            default:
                var mux = await RedisServer.AdminAsync();
                await mux.GetServer(mux.GetEndPoints()[0]).ExecuteAsync("CLIENT", "UNPAUSE");
                break;
        }
    }

    public async ValueTask DisposeAsync() => await ReleaseAsync();

    /// <summary>The store's clock as the case needs it: PostgreSQL's server time, otherwise this host's (SQLite and Redis run on the same host).</summary>
    public static async Task<DateTimeOffset> NowAsync(DriverFixture f) => f.Kind == StoreKind.Postgres ? await f.StoreNow() : DateTimeOffset.UtcNow;
}
