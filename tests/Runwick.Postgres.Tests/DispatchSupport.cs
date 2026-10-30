using Npgsql;
using Runwick.Harness.Store;
using Runwick.Postgres.Store;
using Runwick.Protocol;
using Runwick.Serialization;

namespace Runwick.Postgres.Tests;

/// <summary>Store-level dispatch commands built from a <see cref="PreparedDispatch"/> with honest fingerprints.</summary>
internal static class DispatchSupport
{
    public static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The prepared dispatch as a store command; a different command ID, NotAfter or application recomputes the
    /// digests exactly as Prepare would have, so the fingerprint is the one an honest client would send.
    /// </summary>
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
        c.Store.DispatchAsync(Command(p, commandId, notAfter, c.Application), CancellationToken.None);

    public static IPostgresJobDispatcher Staging(TestClient c) =>
        (IPostgresJobDispatcher)c.Services.GetService(typeof(IPostgresJobDispatcher))!;

    public static async Task<NpgsqlConnection> OpenBusinessAsync(DriverFixture f, string identity = "app")
    {
        var conn = new NpgsqlConnection(f.ClientConnectionString(identity, "business"));
        await conn.OpenAsync();
        return conn;
    }

    public static async Task ExecAsync(NpgsqlConnection conn, NpgsqlTransaction tx, string sql)
    {
        using var cmd = new NpgsqlCommand(sql, conn, tx);
        await cmd.ExecuteNonQueryAsync();
    }

    public static Task<long> Jobs(DriverFixture f, string? application = null) =>
        f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE application = @a", ("a", application ?? f.Application));

    public static Task<long> Receipts(DriverFixture f, Guid commandId) =>
        f.CountAsync("SELECT count(*) FROM runwick.storage_commands WHERE command_id = @c", ("c", commandId));

    public static async Task<List<string>> TextAsync(DriverFixture f, string sql, params (string Name, object Value)[] parameters)
    {
        await using var conn = new NpgsqlConnection(f.AdminConnectionString);
        await conn.OpenAsync();
        using var cmd = new NpgsqlCommand(sql, conn);
        foreach (var (name, value) in parameters)
        {
            cmd.Parameters.AddWithValue(name, value);
        }

        var list = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            list.Add(reader.IsDBNull(0) ? "" : Convert.ToString(reader.GetValue(0), System.Globalization.CultureInfo.InvariantCulture)!);
        }

        return list;
    }

    public static long Conflicts(TestClient c) => ((PgRunner)c.Services.GetService(typeof(PgRunner))!).TransientConflicts;

    /// <summary>Reserves and begins the next job; the queue must hold only the intended candidate first.</summary>
    public static async Task<LeaseHandle> RunAsync(TestClient c, Guid? expectJob = null)
    {
        var reserved = await c.ReserveAsync();
        Assert.Equal(StoreStatus.Applied, reserved.Status);
        if (expectJob is { } j)
        {
            Assert.Equal(j, reserved.Value!.Position.JobId);
        }

        var lease = reserved.Value!.Lease.Handle;
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(lease)).Reply.Status);
        return lease;
    }

    public static async Task WithTimeout(Task task)
    {
        await task.WaitAsync(Wait);
    }

    public static async Task<T> WithTimeout<T>(Task<T> task) => await task.WaitAsync(Wait);
}
