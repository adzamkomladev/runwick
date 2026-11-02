using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using System.Text.Json.Nodes;
using Npgsql;
using Runwick.Protocol;

namespace Runwick.Postgres.Store;

/// <summary>
/// The harness side of the server fault hooks (07 §2.3, PF-05). Created only when the injector is an
/// <see cref="IFaultPlanner"/> with a fault armed at a server point for this call; production calls never create one and
/// pass <c>p_faults = NULL</c>.
/// <para>
/// Protocol, over transaction-scoped advisory locks keyed (nonce, n): this session holds the gate lock of hook s
/// (10000 + s) before the backend reaches it. At a planned hook the backend takes (1000000 + s·10000 + point·100 + step),
/// which names the point, and waits on the gate. The session sees the waiter in <c>pg_locks</c>, runs the in-process
/// injector for that context (so Throw, Delay, Barrier and KillProcess behave as at a client point, and fire counts are
/// the injector's own), then holds "proceed" (30000 + s), plus "throw" (40000 + s) when the injector threw, takes the
/// next gate and releases this one. A backend that finds no "proceed" mark (this process died) aborts.
/// </para>
/// <para>
/// The session connection is used by one thread at a time. Npgsql connections are not thread-safe: cancelling the
/// watcher's command through a token, or disposing the connection while the watcher used it, could hang the call that
/// owns the session after its own work had finished (ADM-03). So the watcher's statements take no cancellation token,
/// every use of the connection holds <see cref="_io"/>, and <see cref="DisposeAsync"/> takes it before closing. The
/// injector call itself (a barrier may wait indefinitely) runs outside it, so closing never waits for a barrier.
/// </para>
/// </summary>
internal sealed class PgFaultSession : IAsyncDisposable
{
    static readonly FaultPoint[] ServerPoints = [FaultPoint.AfterLocksBeforeFreshTime, FaultPoint.BeforeFirstMutation, FaultPoint.AfterMutation];

    readonly NpgsqlConnection _connection;
    readonly IFaultInjector _faults;
    readonly PgOp _op;
    readonly int _nonce;
    readonly CancellationTokenSource _stop = new();
    readonly SemaphoreSlim _io = new(1, 1);
    bool _closed;
    Task? _watch;

    PgFaultSession(NpgsqlConnection connection, IFaultInjector faults, PgOp op, int nonce, string[] points)
    {
        _connection = connection;
        _faults = faults;
        _op = op;
        _nonce = nonce;
        Json = new JsonObject { ["nonce"] = nonce, ["points"] = new JsonArray([.. points.Select(p => (JsonNode)p)]) };
    }

    /// <summary>The <c>p_faults</c> argument of the call.</summary>
    public JsonObject Json { get; }

    /// <summary>What the injector threw at a server point, rethrown in place of the server's RWF01.</summary>
    public ExceptionDispatchInfo? Thrown { get; private set; }

    public static async Task<PgFaultSession?> StartAsync(IFaultInjector faults, PgOp op, string connectionString, CancellationToken ct)
    {
        if (faults is not IFaultPlanner planner)
        {
            return null;
        }

        var points = ServerPoints.Where(p => planner.IsArmed(p, op.Operation, op.CommandId, op.Application)).Select(p => p.ToString()).ToArray();
        if (points.Length == 0)
        {
            return null;
        }

        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct).ConfigureAwait(false);
        var session = new PgFaultSession(connection, faults, op, RandomNumberGenerator.GetInt32(1, int.MaxValue), points);
        await session.LockAsync(10001).ConfigureAwait(false);
        return session;
    }

    /// <summary>Starts watching the backend <paramref name="pid"/> that runs the call.</summary>
    public void Watch(int pid) => _watch = Task.Run(async () =>
    {
        try
        {
            await WatchAsync(pid, _stop.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is OperationCanceledException or NpgsqlException or InvalidOperationException or ObjectDisposedException)
        {
            // The call ended (or the session closed) while the watcher was waiting.
        }
        catch (SessionClosedException)
        {
            // The session closed while the injector handled a point (an abandoned barrier): nothing left to mark.
        }
    });

    async Task WatchAsync(int pid, CancellationToken ct)
    {
        var seq = 1;
        while (!ct.IsCancellationRequested)
        {
            int? code = null;
            var waiting = false;
            await _io.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            try
            {
                if (_closed)
                {
                    return;
                }

                using var q = new NpgsqlCommand("""
                SELECT objid::bigint, granted FROM pg_locks
                WHERE locktype = 'advisory' AND objsubid = 2 AND classid::bigint = @nonce AND pid = @pid
                """, _connection);
                q.Parameters.AddWithValue("nonce", (long)_nonce);
                q.Parameters.AddWithValue("pid", pid);
                var reader = await q.ExecuteReaderAsync(CancellationToken.None).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    while (await reader.ReadAsync(CancellationToken.None).ConfigureAwait(false))
                    {
                        var key = reader.GetInt64(0);
                        var granted = reader.GetBoolean(1);
                        if (key == 10000 + seq && !granted)
                        {
                            waiting = true;
                        }
                        else if (granted && key >= 1_000_000 + (seq * 10_000) && key < 1_000_000 + ((seq + 1) * 10_000))
                        {
                            code = (int)(key % 10_000);
                        }
                    }
                }
            }
            finally
            {
                _io.Release();
            }

            if (!waiting || code is not { } c)
            {
                await Task.Delay(2, ct).ConfigureAwait(false);
                continue;
            }

            var point = ServerPoints[(c / 100) - 1];
            var thrown = false;
            try
            {
                await _faults.OnAsync(new FaultContext(point, _op.Operation, _op.CommandId, _op.Application, c % 100), CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Thrown = ExceptionDispatchInfo.Capture(ex);
                thrown = true;
                await LockAsync(40000 + seq).ConfigureAwait(false);
            }

            await LockAsync(30000 + seq).ConfigureAwait(false);
            await LockAsync(10000 + seq + 1).ConfigureAwait(false);
            await UnlockAsync(10000 + seq).ConfigureAwait(false);
            seq++;
            if (thrown)
            {
                return;
            }
        }
    }

    Task LockAsync(int key) => AdvisoryAsync("SELECT pg_advisory_lock(@nonce, @key)", key);

    Task UnlockAsync(int key) => AdvisoryAsync("SELECT pg_advisory_unlock(@nonce, @key)", key);

    /// <summary>One advisory-lock statement under <see cref="_io"/>; never cancelled, never after close.</summary>
    async Task AdvisoryAsync(string sql, int key)
    {
        await _io.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            if (_closed)
            {
                throw new SessionClosedException();
            }

            using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("nonce", _nonce);
            cmd.Parameters.AddWithValue("key", key);
            await cmd.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            _io.Release();
        }
    }

    sealed class SessionClosedException : Exception;

    public async ValueTask DisposeAsync()
    {
        // Only the watcher's delay observes the token; a statement in flight runs to its end (a few milliseconds).
        await _stop.CancelAsync().ConfigureAwait(false);

        // Wait for the connection to be free, then close it. An injector still blocked in a barrier or KillProcess is not
        // waited for (it holds no connection): closing aborts the backend's wait (no "proceed" mark), and the watcher
        // stops at its next use of the connection.
        await _io.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            _closed = true;

            // Closing the session releases every lock it holds.
            await _connection.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _io.Release();
        }

        // A watcher that wakes after this point meets ObjectDisposedException, which it treats as the session's end.
        _stop.Dispose();
    }
}
