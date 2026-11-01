using System.Text.Json.Nodes;
using Npgsql;

namespace Runwick.Workloads;

internal sealed record DbSample(
    DateTimeOffset At,
    IReadOnlyDictionary<string, long> States,
    long WalBytes,
    long Transactions,
    IReadOnlyDictionary<string, long> Inserts,
    IReadOnlyDictionary<string, long> Connections,
    long LockWaiters)
{
    public long State(string s) => States.TryGetValue(s, out var v) ? v : 0;

    /// <summary>Accepted work not yet reserved.</summary>
    public long Backlog => State("pending") + State("retrying");

    public long Open => Backlog + State("reserved") + State("running");

    public JsonObject ToJson() => new()
    {
        ["t"] = At.ToString("O"),
        ["states"] = new JsonObject(States.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value))),
        ["walBytes"] = WalBytes,
        ["transactions"] = Transactions,
        ["inserts"] = new JsonObject(Inserts.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value))),
        ["connections"] = new JsonObject(Connections.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value))),
        ["lockWaiters"] = LockWaiters,
    };
}

/// <summary>Samples the store every 5 s (07 §14.4): job states, WAL position, commits, inserts per table, connections.</summary>
internal sealed class DbSampler(string connectionString, string database, string path) : IAsyncDisposable
{
    readonly List<DbSample> _samples = [];
    readonly CancellationTokenSource _stop = new();
    Task? _loop;

    public IReadOnlyList<DbSample> Samples
    {
        get
        {
            lock (_samples)
            {
                return [.. _samples];
            }
        }
    }

    public int Failures { get; private set; }

    public void Start() => _loop = LoopAsync();

    async Task LoopAsync()
    {
        await using var file = new StreamWriter(path, append: false) { AutoFlush = true };
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));
        do
        {
            try
            {
                var s = await ReadAsync(connectionString, database, _stop.Token).ConfigureAwait(false);
                lock (_samples)
                {
                    _samples.Add(s);
                }

                await file.WriteLineAsync(s.ToJson().ToJsonString()).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested)
            {
                // Disposed while a sample was being read: the last sample is simply not taken.
                break;
            }
            catch (Exception ex) when (ex is NpgsqlException or TimeoutException or InvalidOperationException or System.Net.Sockets.SocketException)
            {
                Failures++;
                NpgsqlConnection.ClearAllPools();
            }
        }
        while (await Tick(timer).ConfigureAwait(false));
    }

    async Task<bool> Tick(PeriodicTimer timer)
    {
        try
        {
            return await timer.WaitForNextTickAsync(_stop.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    public static async Task<DbSample> ReadAsync(string connectionString, string database, CancellationToken ct = default)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync(ct).ConfigureAwait(false);
        var states = new Dictionary<string, long>(StringComparer.Ordinal);
        await using (var cmd = new NpgsqlCommand("SELECT state, count(*) FROM runwick.jobs GROUP BY state", conn))
        await using (var r = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false))
        {
            while (await r.ReadAsync(ct).ConfigureAwait(false))
            {
                states[r.GetString(0)] = r.GetInt64(1);
            }
        }

        long wal, tx, lockWaiters;
        await using (var cmd = new NpgsqlCommand("""
            SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint,
                   (SELECT xact_commit + xact_rollback FROM pg_stat_database WHERE datname = @db),
                   (SELECT count(*) FROM pg_stat_activity WHERE datname = @db AND wait_event_type = 'Lock')
            """, conn))
        {
            cmd.Parameters.AddWithValue("db", database);
            await using var r = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            await r.ReadAsync(ct).ConfigureAwait(false);
            (wal, tx, lockWaiters) = (r.GetInt64(0), r.GetInt64(1), r.GetInt64(2));
        }

        var inserts = new Dictionary<string, long>(StringComparer.Ordinal);
        await using (var cmd = new NpgsqlCommand(
            "SELECT relname, n_tup_ins FROM pg_stat_user_tables WHERE schemaname = 'runwick'", conn))
        await using (var r = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false))
        {
            while (await r.ReadAsync(ct).ConfigureAwait(false))
            {
                inserts[r.GetString(0)] = r.GetInt64(1);
            }
        }

        var connections = new Dictionary<string, long>(StringComparer.Ordinal);
        await using (var cmd = new NpgsqlCommand(
            "SELECT coalesce(application_name, ''), count(*) FROM pg_stat_activity WHERE datname = @db GROUP BY 1", conn))
        {
            cmd.Parameters.AddWithValue("db", database);
            await using var r = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await r.ReadAsync(ct).ConfigureAwait(false))
            {
                connections[r.GetString(0)] = r.GetInt64(1);
            }
        }

        return new DbSample(DateTimeOffset.UtcNow, states, wal, tx, inserts, connections, lockWaiters);
    }

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync().ConfigureAwait(false);
        if (_loop is not null)
        {
            await _loop.ConfigureAwait(false);
        }

        _stop.Dispose();
    }
}
