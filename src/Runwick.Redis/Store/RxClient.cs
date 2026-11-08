using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using Runwick.Protocol;
using StackExchange.Redis;

namespace Runwick.Redis.Store;

/// <summary>Call classes of 04 §9.1, kept as logical in-flight limits on the two multiplexers (22 §5.1).</summary>
internal enum RxLane
{
    Renewal,
    Settlement,
    Admission,
    Producer,
    Background,
}

/// <summary>A profile, topology or configuration refusal (22 §2.1, §2.3) with its stable code.</summary>
internal sealed class RxProfileException(string code, string message) : Exception(message)
{
    public string Code { get; } = code;
}

/// <summary>One embedded Lua script: shebang, prelude, shared job steps and the operation body; called by SHA (22 §4.1).</summary>
internal sealed class RxScript
{
    RxScript(string name, string text)
    {
        Name = name;
        Text = text;
#pragma warning disable CA5350 // Redis names scripts by SHA-1 (EVALSHA); not a security use.
        Sha = SHA1.HashData(Encoding.UTF8.GetBytes(text));
#pragma warning restore CA5350
    }

    public string Name { get; }

    public string Text { get; }

    public byte[] Sha { get; }

    static readonly ConcurrentDictionary<string, RxScript> Cache = new(StringComparer.Ordinal);

    internal static string Resource(string name)
    {
        using var stream = typeof(RxScript).Assembly.GetManifestResourceStream("Runwick.Redis.Lua." + name)
            ?? throw new InvalidOperationException("Embedded Lua script missing: " + name);
        using var reader = new StreamReader(stream, Encoding.UTF8);
        return reader.ReadToEnd().Replace("\r\n", "\n", StringComparison.Ordinal);
    }

    /// <summary>A script that may write (Redis checks memory and MISCONF before it runs any of it).</summary>
    public static RxScript Write(string op) =>
        Cache.GetOrAdd("w:" + op, _ => new RxScript(op, "#!lua\n" + Resource("prelude.lua") + "\n" + Resource("jobs.lua") + Lib(op) + "\n" + Resource("ops." + op + ".lua")));

    /// <summary>A read-only script (<c>flags=no-writes</c>, 22 §4.6).</summary>
    public static RxScript Read(string op) =>
        Cache.GetOrAdd("r:" + op, _ => new RxScript(op, "#!lua flags=no-writes\n" + Resource("prelude.lua") + "\n" + Resource("jobs.lua") + Lib(op) + "\n" + Resource("ops." + op + ".lua")));

    /// <summary>The helper library an operation family shares (the schedule scripts: sched.lua).</summary>
    static string Lib(string op) => op.StartsWith("sched_", StringComparison.Ordinal) ? "\n" + Resource("sched.lua") : "";

    /// <summary>Every Lua source file of the bundle (REDIS-08/11 source checks).</summary>
    internal static IEnumerable<string> AllSources() =>
        typeof(RxScript).Assembly.GetManifestResourceNames().Where(n => n.EndsWith(".lua", StringComparison.Ordinal))
            .Select(n => Resource(n["Runwick.Redis.Lua.".Length..]));
}

/// <summary>The command frame a write script receives (prelude ARGV 1–11).</summary>
internal sealed record RxOp(
    string Application, Guid CommandId, string Operation, DateTimeOffset NotAfter, byte[]? Fingerprint, RxLane Lane, char Gate,
    Guid? ExpectedStoreId = null)
{
    public bool HasReceipt => Fingerprint is not null;
}

/// <summary>
/// Connections, script calls and the 22 §4.7 error mapping. One control and one work multiplexer with the 22 §5.2 settings,
/// plus an admin connection used only for the profile check (22 §2.3).
/// </summary>
internal sealed class RxClient : IAsyncDisposable
{
    readonly ConfigurationOptions _options;
    readonly string _application;
    readonly SemaphoreSlim[] _lanes;
    readonly Lazy<Task<ConnectionMultiplexer>> _control;
    readonly Lazy<Task<ConnectionMultiplexer>> _work;
    readonly Lazy<Task<ConnectionMultiplexer>> _admin;

    public RxClient(ConfigurationOptions options, string application, int producerCalls)
    {
        _options = options;
        _application = application;
        _lanes = [new(4, 4), new(4, 4), new(4, 4), new(Math.Max(1, producerCalls), Math.Max(1, producerCalls)), new(2, 2)];
        _control = new(() => ConnectAsync("control", admin: false));
        _work = new(() => ConnectAsync("work", admin: false));
        _admin = new(() => ConnectAsync("admin", admin: true));
    }

    /// <summary>Test seam (22 §12): writes the scripts fail after; production always sends 0.</summary>
    internal int FailAfter { get; set; }

    /// <summary>Raised after a reconnect so the profile and history checks run again (22 §2.3, §2.4).</summary>
    public event Action? Reconnected;

    async Task<ConnectionMultiplexer> ConnectAsync(string role, bool admin)
    {
        var o = _options.Clone();
        o.AllowAdmin = admin;
        o.ClientName = "runwick:" + _application + ":" + role;
        var mux = await ConnectionMultiplexer.ConnectAsync(o).ConfigureAwait(false);
        mux.ConnectionRestored += (_, _) => Reconnected?.Invoke();
        return mux;
    }

    public async Task<IDatabase> DbAsync(RxLane lane)
    {
        var mux = await (lane is RxLane.Renewal or RxLane.Settlement ? _control.Value : _work.Value).ConfigureAwait(false);
        return mux.GetDatabase();
    }

    public async Task<(IServer Server, IDatabase Db)> AdminAsync()
    {
        var mux = await _admin.Value.ConfigureAwait(false);
        var ep = mux.GetEndPoints(configuredOnly: true).Single();
        return (mux.GetServer(ep), mux.GetDatabase());
    }

    public static RedisKey Meta(string application) => KeyPrefix(application) + "meta";

    /// <summary>22 §2.2: the application's UTF-8 bytes as lowercase hex inside the hash tag.</summary>
    public static string KeyPrefix(string application) => "rw:{" + Convert.ToHexStringLower(Encoding.UTF8.GetBytes(application)) + "}:";

    static string Ms(DateTimeOffset t) => t.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);

    /// <summary>The prelude's ARGV 1–11 for a write command.</summary>
    public RedisValue[] Frame(RxOp op, Guid storeId, params RedisValue[] args)
    {
        var floor = op.HasReceipt ? (long)Operations.RetentionFloor(op.Operation).TotalMilliseconds : 0;
        var cap = (long)Operations.DeadlineCap(op.Operation).TotalMilliseconds;
        RedisValue[] frame =
        [
            FailAfter, Hex(op.ExpectedStoreId ?? storeId), op.Operation, Hex(op.CommandId),
            op.Fingerprint is null ? "" : Convert.ToHexStringLower(op.Fingerprint), op.NotAfter.ToUnixTimeMilliseconds(), cap, floor,
            op.HasReceipt ? Operations.RetentionClass(op.Operation) : "", op.Gate.ToString(), KnownCapabilities(),
        ];
        return [.. frame, .. args];
    }

    /// <summary>The prelude's ARGV for a read (no command; store id only).</summary>
    public static RedisValue[] ReadFrame(Guid? storeId, params RedisValue[] args)
    {
        RedisValue[] frame = [0, storeId is { } id ? Hex(id) : "", "", "", "", 0, 0, 0, "", "r", ""];
        return [.. frame, .. args];
    }

    public static string Hex(Guid g) => g.ToString("N");

    internal static bool HarnessMarker { get; set; }

    static string KnownCapabilities()
    {
        var known = Hosting.RunwickRuntime.KnownCapabilities.Select(c => c.Key + "=" + c.Value.ToString(CultureInfo.InvariantCulture));
        return string.Join(';', HarnessMarker ? known.Append("rw:harness=1") : known);
    }

    public async Task<RedisResult[]> EvalAsync(RxLane lane, RxScript script, RedisKey meta, RedisValue[] args, RxOp? op, CancellationToken ct)
    {
        var gate = _lanes[(int)lane];
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var db = await DbAsync(lane).ConfigureAwait(false);
            for (var attempt = 0; ; attempt++)
            {
                try
                {
                    var r = await db.ScriptEvaluateAsync(script.Sha, [meta], args).ConfigureAwait(false);
                    return (RedisResult[])r!;
                }
                catch (RedisServerException ex) when (attempt == 0 && ex.Message.StartsWith("NOSCRIPT", StringComparison.Ordinal))
                {
                    // Not run: load it and resend once (22 §4.1).
                    await db.ExecuteAsync("SCRIPT", "LOAD", script.Text).ConfigureAwait(false);
                }
            }
        }
        catch (RedisServerException ex) when (RefusedBeforeRun(ex.Message))
        {
            if (ex.Message.StartsWith("READONLY", StringComparison.Ordinal) || ex.Message.StartsWith("NOPERM", StringComparison.Ordinal))
            {
                Reconnected?.Invoke();
            }

            throw new StoreNotAppliedException("store_unavailable", ex);
        }
        catch (RedisServerException ex)
        {
            // The script ran and may have stored an intent: only the guard can say what is visible.
            throw op is null ? new StoreNotAppliedException("store_unavailable", ex)
                : new StoreOutcomeUnknownException("commit_outcome_unknown", op.CommandId, op.Operation, ex);
        }
        catch (RedisConnectionException ex) when (op is null || ex.CommandStatus is CommandStatus.WaitingToBeSent or CommandStatus.WaitingInBacklog
            or CommandStatus.Unknown || ex.FailureType == ConnectionFailureType.UnableToResolvePhysicalConnection)
        {
            throw new StoreNotAppliedException("store_unavailable", ex);
        }
        catch (Exception ex) when (ex is RedisConnectionException or RedisTimeoutException)
        {
            throw op is null ? new StoreNotAppliedException("store_unavailable", ex)
                : new StoreOutcomeUnknownException("commit_outcome_unknown", op.CommandId, op.Operation, ex);
        }
        finally
        {
            gate.Release();
        }
    }

    static bool RefusedBeforeRun(string message) =>
        message.StartsWith("OOM", StringComparison.Ordinal) || message.StartsWith("MISCONF", StringComparison.Ordinal)
        || message.StartsWith("READONLY", StringComparison.Ordinal) || message.StartsWith("LOADING", StringComparison.Ordinal)
        || message.StartsWith("BUSY", StringComparison.Ordinal) || message.StartsWith("NOPERM", StringComparison.Ordinal);

    public async ValueTask DisposeAsync()
    {
        foreach (var lazy in new[] { _control, _work, _admin })
        {
            if (lazy.IsValueCreated)
            {
                try
                {
                    var mux = await lazy.Value.ConfigureAwait(false);
                    await mux.DisposeAsync().ConfigureAwait(false);
                }
                catch (RedisConnectionException)
                {
                    // Never connected.
                }
            }
        }
    }

    // ------------------------------------------------------------------ configuration (22 §2.1, §5.2)

    /// <summary>
    /// Takes only the endpoint, user, password, TLS settings, defaultDatabase and connectTimeout (capped at 5 s) from the
    /// caller's string and fixes everything else. Never logs the string.
    /// </summary>
    public static ConfigurationOptions Options(string configuration, TimeSpan asyncTimeout)
    {
        if (configuration.Contains('$', StringComparison.Ordinal))
        {
            throw new RxProfileException("invalid_policy", "Command renames are not accepted in the Redis configuration.");
        }

        ConfigurationOptions parsed;
        try
        {
            parsed = ConfigurationOptions.Parse(configuration);
        }
        catch (ArgumentException ex)
        {
            throw new RxProfileException("invalid_policy", "The Redis configuration string could not be parsed: " + ex.GetType().Name);
        }

        if (parsed.EndPoints.Count != 1 || !string.IsNullOrEmpty(parsed.ServiceName))
        {
            throw new RxProfileException("topology_unsupported", "Exactly one Redis primary endpoint is supported; Sentinel and several endpoints are refused.");
        }

        var ep = parsed.EndPoints[0];
        var o = new ConfigurationOptions
        {
            User = parsed.User,
            Password = parsed.Password,
            Ssl = parsed.Ssl,
            SslHost = parsed.SslHost,
            SslProtocols = parsed.SslProtocols,
            DefaultDatabase = parsed.DefaultDatabase,
            ConnectTimeout = Math.Min(parsed.ConnectTimeout, 5000),
            AbortOnConnectFail = false,
            BacklogPolicy = BacklogPolicy.FailFast,
            AsyncTimeout = (int)asyncTimeout.TotalMilliseconds,
            SyncTimeout = (int)asyncTimeout.TotalMilliseconds,
            AllowAdmin = false,
            ConnectRetry = 1,
        };
        o.EndPoints.Add(ep);
        return o;
    }
}
