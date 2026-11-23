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
        Cache.GetOrAdd("w:" + op, _ => new RxScript(op, "#!lua\n" + (Lean.Contains(op)
            ? "local function GENERIC()\n" + Generic(op) + "\nend\n" + Resource("lean.frame.lua") + "\n" + Resource("lean." + op + ".lua")
            : Generic(op))));

    /// <summary>
    /// Worker operations with a lean path (22 §4.1, REDIS-22): the script defines the generic frame as <c>GENERIC()</c> without running
    /// it, then runs the hand-written path, which calls <c>GENERIC()</c> before any write when it cannot decide the command itself.
    /// </summary>
    internal static readonly HashSet<string> Lean = new(StringComparer.Ordinal) { "reserve_and_begin", "finish", "finish_many", "renew" };

    /// <summary>The generic frame of a write operation: prelude, shared job steps and the operation body.</summary>
    static string Generic(string op) => Resource("prelude.lua") + "\n" + Resource("jobs.lua") + Lib(op) + "\n" + Resource("ops." + op + ".lua");

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
/// plus an admin connection for profile checks (22 §2.3) and read-only management diagnostics.
/// </summary>
internal sealed class RxClient : IAsyncDisposable
{
    readonly ConfigurationOptions _options;
    readonly string _application;
    readonly SemaphoreSlim[] _lanes;
    Generation? _gen;
    RxAuthority? _authority;

    public RxClient(ConfigurationOptions options, string application, int producerCalls)
    {
        _options = options;
        _application = application;
        _lanes = [new(4, 4), new(4, 4), new(4, 4), new(Math.Max(1, producerCalls), Math.Max(1, producerCalls)), new(2, 2)];
        if (options.EndPoints.Count > 0)
        {
            _gen = new Generation(this, null);
        }
    }

    /// <summary>Cluster mode (22 §2.6): the authority verifies the owner of the application's slot, and the connections route by slot.</summary>
    internal bool Cluster { get; set; }

    /// <summary>Sentinel or Cluster mode (22 §2.5, §2.6): commands go only to the primary this authority has verified; null for a standalone primary.</summary>
    internal RxAuthority? Authority
    {
        get => _authority;
        set
        {
            _authority = value;
            _authority?.Attach(this);
            if (value is not null)
            {
                _gen = null; // nothing is bound until a primary is verified
            }
        }
    }

    /// <summary>The three connections of one bound primary: control and work lanes plus the admin connection (22 §5.1).</summary>
    sealed class Generation
    {
        public Generation(RxClient owner, EndPoint? endpoint)
        {
            Endpoint = endpoint;
            Control = new(() => owner.ConnectAsync("control", admin: false, endpoint));
            Work = new(() => owner.ConnectAsync("work", admin: false, endpoint));
            Admin = new(() => owner.ConnectAsync("admin", admin: true, endpoint));
        }

        public EndPoint? Endpoint { get; }

        public Lazy<Task<ConnectionMultiplexer>> Control { get; }

        public Lazy<Task<ConnectionMultiplexer>> Work { get; }

        public Lazy<Task<ConnectionMultiplexer>> Admin { get; }

        public async ValueTask DisposeAsync()
        {
            foreach (var lazy in new[] { Control, Work, Admin })
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
    }

    /// <summary>Called by the authority after a verification: connections follow the verified primary and the old ones are dropped.</summary>
    internal void Bind(EndPoint endpoint)
    {
        var old = _gen;
        if (old?.Endpoint is { } e && e.ToString() == endpoint.ToString())
        {
            return;
        }

        _gen = new Generation(this, endpoint);
        if (old is not null)
        {
            _ = Task.Run(async () => await old.DisposeAsync().ConfigureAwait(false));
        }
    }

    /// <summary>The admin connection of the bound generation if it is bound to <paramref name="endpoint"/> (the authority reuses it), else null.</summary>
    internal Task<(IServer Server, IDatabase Db)>? TryBoundAdmin(EndPoint endpoint) =>
        _gen is { Endpoint: { } e } && e.ToString() == endpoint.ToString() ? AdminAsync() : null;

    Generation Current()
    {
        _authority?.Require();
        return _gen ?? throw new StoreNotAppliedException("store_authority_unverified");
    }

    public string Application => _application;

    /// <summary>Test seam (22 §12): writes the scripts fail after; production always sends 0.</summary>
    internal int FailAfter { get; set; }

    /// <summary>Test seam (REDIS-01): sees each answered script call's arguments and raw reply; production leaves it null.</summary>
    internal Action<RxScript, RedisValue[], RedisResult[]>? Traced { get; set; }

    /// <summary>Raised after a reconnect so the profile and history checks run again (22 §2.3, §2.4).</summary>
    public event Action? Reconnected;

    async Task<ConnectionMultiplexer> ConnectAsync(string role, bool admin, EndPoint? endpoint)
    {
        var o = _options.Clone();
        if (endpoint is not null)
        {
            o.EndPoints.Clear();
            o.EndPoints.Add(endpoint);
        }

        o.AllowAdmin = admin;
        o.ClientName = "runwick:" + _application + ":" + role;
        var mux = await ConnectionMultiplexer.ConnectAsync(o).ConfigureAwait(false);
        mux.ConnectionRestored += (_, _) => Reconnected?.Invoke();
        if (_authority is { } authority && !admin)
        {
            // A lost connection to the verified primary is evidence that authority may have ended: close it and verify again.
            // Dropping an old generation on purpose, or an old primary's socket after a switch, is not.
            // A cluster connection also holds the other nodes: only the verified owner's socket counts.
            mux.ConnectionFailed += (_, e) =>
            {
                if (e.FailureType != ConnectionFailureType.ConnectionDisposed && _gen?.Endpoint?.ToString() == endpoint?.ToString()
                    && (!Cluster || e.EndPoint?.ToString() == endpoint?.ToString()))
                {
                    authority.Invalidate("connection_to_primary_failed");
                }
            };
            if (Cluster)
            {
                // A MOVED reply means the slot has a new owner: stop sending until that owner is verified (22 §2.6).
                mux.HashSlotMoved += (_, _) =>
                {
                    if (_gen?.Endpoint?.ToString() == endpoint?.ToString())
                    {
                        authority.Invalidate("hash_slot_moved");
                    }
                };
            }
        }

        return mux;
    }

    /// <summary>
    /// Listens to the application's wake channel (the scripts publish the queue name when a job falls due) until <paramref name="ct"/> ends.
    /// A hint only: <paramref name="onLost"/> reports a lost session, <paramref name="onSubscribed"/> every (re)subscription, and a failed
    /// subscribe is retried after a back-off (1 s doubling to 30 s).
    /// </summary>
    public async Task SubscribeWakeAsync(Action<string> onWake, Action onSubscribed, Action onLost, CancellationToken ct)
    {
        var channel = RedisChannel.Literal(KeyPrefix(_application) + "wake");
        var backoff = TimeSpan.FromSeconds(1);
        while (!ct.IsCancellationRequested)
        {
            ConnectionMultiplexer? mux = null;
            EventHandler<ConnectionFailedEventArgs> failed = (_, _) => onLost();
            EventHandler<ConnectionFailedEventArgs> restored = (_, _) => onSubscribed();
            try
            {
                mux = await Current().Control.Value.ConfigureAwait(false); // follows the verified primary in Sentinel mode
                mux.ConnectionFailed += failed;
                mux.ConnectionRestored += restored; // the library resubscribes by itself; the pool looks once more for what it missed
                await mux.GetSubscriber().SubscribeAsync(channel, (_, value) =>
                {
                    if (value.HasValue)
                    {
                        onWake((string)value!);
                    }
                }).ConfigureAwait(false);
                backoff = TimeSpan.FromSeconds(1);
                onSubscribed();
                await Task.Delay(Timeout.InfiniteTimeSpan, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex) when (ex is RedisException or IOException or TimeoutException or InvalidOperationException or StoreNotAppliedException)
            {
                onLost();
                try
                {
                    await Task.Delay(backoff, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }

                backoff = TimeSpan.FromTicks(Math.Min(backoff.Ticks * 2, TimeSpan.FromSeconds(30).Ticks));
            }
            finally
            {
                if (mux is not null)
                {
                    mux.ConnectionFailed -= failed;
                    mux.ConnectionRestored -= restored;
                    try
                    {
                        await mux.GetSubscriber().UnsubscribeAsync(channel).ConfigureAwait(false);
                    }
                    catch (Exception)
                    {
                        // Stopping or already disconnected: nothing is left to unsubscribe.
                    }
                }
            }
        }
    }

    public async Task<IDatabase> DbAsync(RxLane lane)
    {
        var g = Current();
        var mux = await (lane is RxLane.Renewal or RxLane.Settlement ? g.Control.Value : g.Work.Value).ConfigureAwait(false);
        return mux.GetDatabase();
    }

    internal async Task<string?> DiagnosticsInfoAsync(CancellationToken ct)
    {
        var gate = _lanes[(int)RxLane.Background];
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            // StackExchange.Redis requires its local AllowAdmin flag for INFO. Reuse the profile multiplexer and credentials.
            var (server, _) = await AdminAsync().WaitAsync(ct).ConfigureAwait(false);
            var result = await server.ExecuteAsync("INFO", ["memory", "clients", "stats", "persistence", "replication"],
                CommandFlags.None).WaitAsync(ct).ConfigureAwait(false);
            return (string?)result;
        }
        finally
        {
            gate.Release();
        }
    }

    internal async Task<RedisValue[]> DiagnosticsIntentAsync(CancellationToken ct)
    {
        var gate = _lanes[(int)RxLane.Background];
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var (_, db) = await AdminAsync().WaitAsync(ct).ConfigureAwait(false);
            return await db.HashGetAsync(Meta(_application), ["store", "guard", "ipos"], CommandFlags.DemandMaster)
                .WaitAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
        }
    }

    public async Task<(IServer Server, IDatabase Db)> AdminAsync()
    {
        var g = _gen ?? throw new StoreNotAppliedException("store_authority_unverified");
        var mux = await g.Admin.Value.ConfigureAwait(false);
        // A cluster connection knows every node: the admin reads belong to the verified owner.
        var ep = Cluster && g.Endpoint is { } owner ? owner : mux.GetEndPoints(configuredOnly: true).Single();
        return (mux.GetServer(ep), mux.GetDatabase());
    }

    public static RedisKey Meta(string application) => KeyPrefix(application) + "meta";

    /// <summary>The cluster hash slot of every key of <paramref name="application"/> (22 §2.6): CRC16 (XMODEM) of the hash tag, modulo 16384.</summary>
    public static int SlotOf(string application)
    {
        ushort crc = 0;
        foreach (var b in Encoding.ASCII.GetBytes(Convert.ToHexStringLower(Encoding.UTF8.GetBytes(application))))
        {
            crc ^= (ushort)(b << 8);
            for (var i = 0; i < 8; i++)
            {
                crc = (crc & 0x8000) != 0 ? (ushort)((crc << 1) ^ 0x1021) : (ushort)(crc << 1);
            }
        }

        return crc % 16384;
    }

    /// <summary>22 §2.2: the application's UTF-8 bytes as lowercase hex inside the hash tag.</summary>
    public static string KeyPrefix(string application) => "rw:{" + Convert.ToHexStringLower(Encoding.UTF8.GetBytes(application)) + "}:";

    static string Ms(DateTimeOffset t) => t.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);

    /// <summary>The prelude's ARGV 1–11 for a write command.</summary>
    public RedisValue[] Frame(RxOp op, Guid storeId, params RedisValue[] args)
    {
        // A grouped dispatch has no receipt of its own; its items get ordinary Dispatch receipts, so it carries Dispatch's limits.
        var grouped = op.Operation == RedisStore.DispatchGroupOperation;
        var limits = grouped ? Operations.Dispatch : op.Operation;
        var floor = op.HasReceipt || grouped ? (long)Operations.RetentionFloor(limits).TotalMilliseconds : 0;
        var cap = (long)Operations.DeadlineCap(limits).TotalMilliseconds;
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
                    var r = (RedisResult[])(await db.ScriptEvaluateAsync(script.Sha, [meta], args).ConfigureAwait(false))!;
                    Traced?.Invoke(script, args, r);
                    return r;
                }
                catch (RedisServerException ex) when (attempt == 0 && ex.Message.StartsWith("NOSCRIPT", StringComparison.Ordinal))
                {
                    if (Cluster)
                    {
                        // Not run. An unkeyed SCRIPT LOAD would go to any node; sending the text by key loads it on the slot's owner (22 §2.6).
                        var evaluated = (RedisResult[])(await db.ScriptEvaluateAsync(script.Text, [meta], args).ConfigureAwait(false))!;
                        Traced?.Invoke(script, args, evaluated);
                        return evaluated;
                    }

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
        if (_authority is not null)
        {
            await _authority.DisposeAsync().ConfigureAwait(false);
        }

        if (_gen is not null)
        {
            await _gen.DisposeAsync().ConfigureAwait(false);
        }
    }

    // ------------------------------------------------------------------ configuration (22 §2.1, §5.2)

    /// <summary>
    /// Sentinel mode (22 §2.5): the string lists the Sentinel endpoints and <c>serviceName</c>. Returns the data-node options (no endpoint:
    /// the authority supplies the verified primary) and the Sentinel seeds. Everything else is fixed as in <see cref="Options"/>.
    /// </summary>
    public static (ConfigurationOptions Data, RxSentinelSeeds Seeds) SentinelOptions(string configuration, TimeSpan asyncTimeout)
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

        if (parsed.EndPoints.Count == 0 || string.IsNullOrEmpty(parsed.ServiceName))
        {
            throw new RxProfileException("topology_unsupported", "Sentinel mode needs the Sentinel endpoints and a serviceName in the configuration string.");
        }

        var connectTimeout = Math.Min(parsed.ConnectTimeout, 5000);
        var data = new ConfigurationOptions
        {
            User = parsed.User,
            Password = parsed.Password,
            Ssl = parsed.Ssl,
            SslHost = parsed.SslHost,
            SslProtocols = parsed.SslProtocols,
            DefaultDatabase = parsed.DefaultDatabase,
            ConnectTimeout = connectTimeout,
            AbortOnConnectFail = false,
            BacklogPolicy = BacklogPolicy.FailFast,
            AsyncTimeout = (int)asyncTimeout.TotalMilliseconds,
            SyncTimeout = (int)asyncTimeout.TotalMilliseconds,
            AllowAdmin = false,
            ConnectRetry = 1,
        };
        var seeds = new RxSentinelSeeds([.. parsed.EndPoints], parsed.ServiceName, parsed.SentinelUser ?? parsed.User, parsed.SentinelPassword ?? parsed.Password,
            parsed.Ssl, parsed.SslHost, parsed.SslProtocols ?? default, connectTimeout);
        return (data, seeds);
    }

    /// <summary>
    /// Cluster mode (22 §2.6): the string lists the cluster's nodes (list every primary; replicas may be listed too). Returns the data options
    /// with every listed node as an endpoint and the same node list as the authority's voters. Everything else is fixed as in <see cref="Options"/>.
    /// </summary>
    public static (ConfigurationOptions Data, RxSentinelSeeds Seeds) ClusterOptions(string configuration, TimeSpan asyncTimeout)
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

        if (parsed.EndPoints.Count == 0 || !string.IsNullOrEmpty(parsed.ServiceName))
        {
            throw new RxProfileException("topology_unsupported", "Cluster mode needs the cluster's node endpoints in the configuration string and no serviceName.");
        }

        var connectTimeout = Math.Min(parsed.ConnectTimeout, 5000);
        var data = new ConfigurationOptions
        {
            User = parsed.User,
            Password = parsed.Password,
            Ssl = parsed.Ssl,
            SslHost = parsed.SslHost,
            SslProtocols = parsed.SslProtocols,
            DefaultDatabase = parsed.DefaultDatabase,
            ConnectTimeout = connectTimeout,
            AbortOnConnectFail = false,
            BacklogPolicy = BacklogPolicy.FailFast,
            AsyncTimeout = (int)asyncTimeout.TotalMilliseconds,
            SyncTimeout = (int)asyncTimeout.TotalMilliseconds,
            AllowAdmin = false,
            ConnectRetry = 1,
        };
        foreach (var ep in parsed.EndPoints)
        {
            data.EndPoints.Add(ep);
        }

        return (data, new RxSentinelSeeds([.. parsed.EndPoints], "", parsed.User, parsed.Password, parsed.Ssl, parsed.SslHost, parsed.SslProtocols ?? default, connectTimeout));
    }

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
