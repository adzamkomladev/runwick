using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Security.Authentication;
using Runwick.Protocol;
using StackExchange.Redis;

namespace Runwick.Redis.Store;

/// <summary>The Sentinel endpoints (or, in Cluster mode, the cluster nodes that vote) and their credentials from the configuration string (22 §2.5, §2.6).</summary>
internal sealed record RxSentinelSeeds(
    IReadOnlyList<EndPoint> Endpoints, string ServiceName, string? User, string? Password, bool Ssl, string? SslHost, SslProtocols SslProtocols, int ConnectTimeoutMs);

internal enum RxAuthorityState
{
    /// <summary>Nothing verified yet.</summary>
    Unverified,

    /// <summary>A verified primary may take commands until <see cref="RxAuthoritySnapshot.ExpiresAt"/>.</summary>
    Verified,

    /// <summary>No command goes out. The next check may resolve it (Sentinels disagree, failover running, primary unreachable, profile wrong).</summary>
    Unresolved,

    /// <summary>No command goes out until an operator reconciles the histories and restarts the process (continuity or identity could not be shown).</summary>
    Blocked,
}

internal sealed record RxAuthorityEvent(DateTimeOffset At, string Text);

/// <summary>One cluster node as a voting node reports it (22 §2.6): its address, role, health word and how many slots it owns.</summary>
internal sealed record RxClusterNode(string Address, string Role, string Health, int Slots, bool OwnsApplicationSlot, bool MovingApplicationSlot);

internal sealed record RxAuthoritySnapshot(
    RxAuthorityState State, string? Reason, string? Primary, string? RunId, long Epoch, int SentinelsAgreeing, int SentinelsConfigured,
    DateTimeOffset? VerifiedAt, DateTimeOffset? ExpiresAt, IReadOnlyList<RxAuthorityEvent> Recent,
    string Mode = "sentinel", int? Slot = null, IReadOnlyList<RxClusterNode>? Nodes = null);

/// <summary>
/// Strict write authority for a Sentinel-managed primary (22 §2.5) or for the owner of the application's slot in a Cluster (22 §2.6).
/// Commands are sent only to a primary that a majority of the configured Sentinels (or cluster nodes) named, whose profile was checked and whose history continues the one this process last verified; each success is good for
/// <see cref="RedisAuthorityOptions.AuthorityTtl"/>. Evidence of trouble (a different primary, failover flags, a lost connection) closes
/// authority at once; silence closes it when the lease runs out, so a process cut off from the Sentinels stops writing to a former primary.
/// </summary>
internal sealed class RxAuthority : IAsyncDisposable
{
    const int MaxEvents = 16;
    readonly RxSentinelSeeds _seeds;
    readonly RedisAuthorityOptions _options;
    readonly TimeProvider _time;
    readonly ConfigurationOptions _data;
    readonly RedisPersistence _persistence;
    readonly SemaphoreSlim _gate = new(1, 1);
    readonly object _lock = new();
    readonly ConcurrentDictionary<string, Lazy<Task<ConnectionMultiplexer>>> _sentinels = new(StringComparer.Ordinal);
    readonly List<RxAuthorityEvent> _events = [];
    readonly CancellationTokenSource _stop = new();
    readonly TimeSpan _fullEvery;
    readonly bool _cluster;
    readonly int _slot;
    Lazy<Task<ConnectionMultiplexer>>? _clusterMux;
    IReadOnlyList<RxClusterNode>? _nodes;
    RxClient? _client;
    Task? _loop;
    DateTimeOffset _lastFull = DateTimeOffset.MinValue;
    IReadOnlyDictionary<string, string> _lastFacts = new Dictionary<string, string>();
    RxAuthorityState _state;
    string? _reason;
    string? _primary;
    string? _runId;
    long _epoch;
    int _agreeing;
    DateTimeOffset? _verifiedAt;
    DateTimeOffset? _expiresAt;
    (string Primary, string RunId, string ReplId, long Offset)? _last;
    int _invalidations;
    readonly bool _functions;

    public RxAuthority(RxSentinelSeeds seeds, RedisAuthorityOptions options, TimeProvider time, ConfigurationOptions data, RedisPersistence persistence,
        TimeSpan? fullCheckEvery = null, int? clusterSlot = null, bool functions = false)
    {
        _functions = functions;
        _cluster = clusterSlot is not null;
        _slot = clusterSlot ?? 0;
        _fullEvery = fullCheckEvery ?? TimeSpan.FromSeconds(30);
        _seeds = seeds;
        _options = options;
        _time = time;
        _data = data;
        _persistence = persistence;
    }

    /// <summary>The history check on the candidate primary (22 §2.4): null when it continues this process's history, else the stable code.</summary>
    public Func<IDatabase, Task<string?>>? HistoryCheck { get; set; }

    /// <summary>Receives each profile report so Describe and health carry the facts of the primary that was checked.</summary>
    public Action<RxProfileReport>? OnReport { get; set; }

    /// <summary>Test seam: runs after the candidate passed every check and before commands are allowed.</summary>
    internal Func<Task>? BeforeAllow { get; set; }

    public void Attach(RxClient client) => _client = client;

    public RxAuthoritySnapshot Snapshot()
    {
        lock (_lock)
        {
            var now = _time.GetUtcNow();
            var state = _state == RxAuthorityState.Verified && now >= _expiresAt ? RxAuthorityState.Unresolved : _state;
            var reason = state != _state ? "authority_lease_expired" : _reason;
            return new(state, reason, _primary, _runId, _epoch, _agreeing, _seeds.Endpoints.Count, _verifiedAt, _expiresAt, [.. _events],
                _cluster ? "cluster" : "sentinel", _cluster ? _slot : null, _nodes);
        }
    }

    /// <summary>Throws unless a verified primary holds an unexpired lease.</summary>
    public void Require()
    {
        lock (_lock)
        {
            if (_state == RxAuthorityState.Blocked)
            {
                throw new StoreIncompatibleException(_reason ?? "authority_blocked");
            }

            if (_state != RxAuthorityState.Verified || _time.GetUtcNow() >= _expiresAt)
            {
                throw new StoreNotAppliedException("store_authority_unverified");
            }
        }
    }

    /// <summary>Evidence that the verified primary may no longer be the primary: close authority now and verify again.</summary>
    public void Invalidate(string reason)
    {
        lock (_lock)
        {
            _lastFull = DateTimeOffset.MinValue; // a lost connection may mean a restart: the next check is the full one
            _invalidations++;
            if (_state == RxAuthorityState.Verified)
            {
                _state = RxAuthorityState.Unresolved;
                _reason = reason;
                _expiresAt = _time.GetUtcNow();
                Note("authority closed: " + reason);
            }
        }

        _ = Task.Run(() => VerifyAsync(CancellationToken.None));
    }

    public void Start()
    {
        lock (_lock)
        {
            _loop ??= Task.Run(LoopAsync);
        }
    }

    async Task LoopAsync()
    {
        using var timer = new PeriodicTimer(_options.AuthorityRecheckInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(_stop.Token).ConfigureAwait(false))
            {
                try
                {
                    await VerifyAsync(_stop.Token).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    SetUnresolved("verification_failed: " + ex.GetType().Name);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Disposed.
        }
    }

    void Note(string text)
    {
        _events.Add(new(_time.GetUtcNow(), text));
        if (_events.Count > MaxEvents)
        {
            _events.RemoveAt(0);
        }
    }

    void SetUnresolved(string reason, bool closeNow = false, int agreeing = 0)
    {
        lock (_lock)
        {
            if (_state == RxAuthorityState.Blocked)
            {
                return;
            }

            if (_state != RxAuthorityState.Unresolved || _reason != reason)
            {
                Note("unresolved: " + reason);
            }

            _agreeing = agreeing;
            if (_state == RxAuthorityState.Verified && !closeNow)
            {
                _reason = reason; // the lease still runs; silence closes it when it ends
                return;
            }

            _state = RxAuthorityState.Unresolved;
            _reason = reason;
            _expiresAt = _time.GetUtcNow();
        }
    }

    void SetBlocked(string reason)
    {
        lock (_lock)
        {
            _state = RxAuthorityState.Blocked;
            _reason = reason;
            _expiresAt = _time.GetUtcNow();
            Note("blocked: " + reason);
        }
    }

    /// <summary>One verification: Sentinel majority, primary profile, history continuity, then the lease.</summary>
    public async Task<RxAuthoritySnapshot> VerifyAsync(CancellationToken ct)
    {
        await _gate.WaitAsync(ct).ConfigureAwait(false);
        Probe? probe = null; // the candidate's own connection, kept until the verification ends (the history check uses it)
        var invalidations = Volatile.Read(ref _invalidations);
        try
        {
            if (Snapshot().State == RxAuthorityState.Blocked)
            {
                return Snapshot();
            }

            var votes = _cluster ? await AskClusterAsync(ct).ConfigureAwait(false) : await AskSentinelsAsync(ct).ConfigureAwait(false);
            var needed = _seeds.Endpoints.Count / 2 + 1;
            var clean = votes.Where(v => v.Primary is not null && !v.Troubled).GroupBy(v => v.Primary!, StringComparer.Ordinal).OrderByDescending(g => g.Count()).FirstOrDefault();
            if (clean is null || clean.Count() < needed)
            {
                // Evidence (a Sentinel reports the primary down or a failover) closes authority now; mere silence waits for the lease.
                var troubled = votes.Any(v => v.Troubled);
                var split = votes.Select(v => v.Primary).Where(p => p is not null).Distinct(StringComparer.Ordinal).Count() > 1;
                SetUnresolved(_cluster
                    ? troubled ? "slot_owner_failing_or_moving" : split ? "cluster_nodes_disagree" : "cluster_majority_unreachable"
                    : troubled ? "primary_reported_down_or_failing_over" : split ? "sentinels_disagree" : "sentinel_majority_unreachable", troubled || split, clean?.Count() ?? 0);
                return Snapshot();
            }

            var primary = clean.Key;
            var current = Snapshot();
            if (current.State == RxAuthorityState.Verified && current.Primary != primary)
            {
                SetUnresolved(_cluster ? "slot_owner_changed" : "sentinels_name_a_different_primary", closeNow: true, clean.Count());
            }

            // The bound connection is reused while the same node stays primary; a new candidate gets a connection of its own and the full check.
            var endpoint = ParseEndPoint(primary);
            IReadOnlyDictionary<string, string> facts;
            IDatabase db;
            try
            {
                IServer server;
                if (_client?.TryBoundAdmin(endpoint) is { } bound)
                {
                    (server, db) = await bound.ConfigureAwait(false);
                }
                else
                {
                    probe = await ProbeAsync(primary, ct).ConfigureAwait(false);
                    (server, db) = (probe.Server, probe.Db);
                }

                var now = _time.GetUtcNow();
                if (probe is not null || now - _lastFull >= _fullEvery)
                {
                    var report = await RxProfile.CheckAsync(server, db, _persistence, _cluster ? RxTopology.Cluster : RxTopology.Sentinel, _data.User, _functions).ConfigureAwait(false);
                    OnReport?.Invoke(report);
                    if (!report.Verified)
                    {
                        SetUnresolved($"{report.FailureCode}: {report.Failure}", closeNow: true, clean.Count());
                        return Snapshot();
                    }

                    _lastFull = now;
                    _lastFacts = facts = report.Facts;
                }
                else
                {
                    // Between full checks: the node must still be a primary, and the lineage fields are read fresh.
                    var info = await RxProfile.InfoAsync(server, "replication").ConfigureAwait(false);
                    if (info.GetValueOrDefault("role") != "master")
                    {
                        SetUnresolved("topology_unsupported: role=" + info.GetValueOrDefault("role", "<missing>"), closeNow: true, clean.Count());
                        return Snapshot();
                    }

                    var merged = new Dictionary<string, string>(_lastFacts, StringComparer.Ordinal);
                    foreach (var key in new[] { "master_replid", "master_replid2", "master_repl_offset", "second_repl_offset", "connected_slaves" })
                    {
                        merged[key] = info.GetValueOrDefault(key, "");
                    }

                    merged["replicas"] = merged["connected_slaves"];
                    _lastFacts = facts = merged;
                    OnReport?.Invoke(new RxProfileReport(null, null, merged));
                }
            }
            catch (Exception ex) when (ex is RedisException or TimeoutException)
            {
                SetUnresolved(Unreachable(ex), closeNow: true, clean.Count());
                return Snapshot();
            }

            var runId = facts.GetValueOrDefault("run_id", "");
            var replId = facts.GetValueOrDefault("master_replid", "");
            var offset = long.TryParse(facts.GetValueOrDefault("master_repl_offset"), NumberStyles.None, CultureInfo.InvariantCulture, out var o) ? o : 0;
            // A slot moved by migration leaves the old owner a healthy primary that every voter still lists: the data moved key by key, so the
            // history check below (store identity, revisions) is the continuity proof. After a failover the old owner is down or demoted, and the
            // promoted node must descend from the verified replication history (22 §2.6).
            var migrated = _cluster && _last is { } was && was.Primary != primary
                && votes.Where(v => v.Primary == primary && v.Nodes is not null).All(v => v.Nodes!.Any(n => n.Address == was.Primary && n.Role == "primary" && n.Health == "ok"));
            if (_last is { } last && last.Primary != primary && !migrated)
            {
                // A different node: it must descend from the history this process last verified and cover everything seen then.
                var replId2 = facts.GetValueOrDefault("master_replid2", "");
                var second = long.TryParse(facts.GetValueOrDefault("second_repl_offset"), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var s) ? s : -1;
                if (replId2 != last.ReplId || second <= last.Offset)
                {
                    SetBlocked($"authority_continuity_unverified: the promoted primary does not continue the verified history (replid2 match {replId2 == last.ReplId}, shared offset {second}, verified offset {last.Offset})");
                    return Snapshot();
                }
            }

            string? historyFailure;
            try
            {
                historyFailure = HistoryCheck is { } history ? await history(db).ConfigureAwait(false) : null;
            }
            catch (Exception ex) when (ex is RedisException or TimeoutException or ObjectDisposedException)
            {
                SetUnresolved(Unreachable(ex), closeNow: true, clean.Count());
                return Snapshot();
            }

            if (historyFailure is not null)
            {
                SetBlocked(historyFailure);
                return Snapshot();
            }

            if (BeforeAllow is { } hook)
            {
                await hook().ConfigureAwait(false);
            }

            var changed = current.Primary != primary;
            lock (_lock)
            {
                if (_invalidations != invalidations)
                {
                    // Evidence (a MOVED reply, a lost connection) arrived while this check ran: its view predates the evidence, so it opens
                    // nothing. The verification Invalidate queued runs next and decides.
                    return Snapshot();
                }

                _client?.Bind(ParseEndPoint(primary));
                if (changed || _state != RxAuthorityState.Verified)
                {
                    _epoch++;
                    Note($"verified primary {primary} (epoch {_epoch})");
                }

                _state = RxAuthorityState.Verified;
                _reason = null;
                _primary = primary;
                _runId = runId;
                _agreeing = clean.Count();
                _verifiedAt = _time.GetUtcNow();
                _expiresAt = _verifiedAt + _options.AuthorityTtl;
                _last = (primary, runId, replId, offset);
                _nodes = clean.Select(v => v.Nodes).FirstOrDefault(n => n is not null);
            }

            return Snapshot();
        }
        finally
        {
            if (probe is not null)
            {
                await probe.DisposeAsync().ConfigureAwait(false);
            }

            _gate.Release();
        }
    }

    /// <summary>
    /// Why the candidate primary could not be checked. A refused login or a missing permission is named as such: unlike a timeout it will not
    /// heal by itself, and the operator needs to know it is the credentials, not the network.
    /// </summary>
    internal static string Unreachable(Exception ex)
    {
        var text = ex.ToString();
        return ex is RedisConnectionException { FailureType: ConnectionFailureType.AuthenticationFailure }
            || text.Contains("WRONGPASS", StringComparison.Ordinal) || text.Contains("NOAUTH", StringComparison.Ordinal)
            || text.Contains("AuthenticationFailure", StringComparison.Ordinal) ? "primary_authentication_failed"
            : text.Contains("NOPERM", StringComparison.Ordinal) ? "primary_permission_denied"
            : "primary_unreachable: " + ex.GetType().Name;
    }

    internal sealed record Vote(string? Primary, bool Troubled, IReadOnlyList<RxClusterNode>? Nodes = null);

    async Task<Vote[]> AskSentinelsAsync(CancellationToken ct)
    {
        var timeout = TimeSpan.FromMilliseconds(Math.Min(1500, Math.Max(250, _options.AuthorityTtl.TotalMilliseconds / 2)));
        return await Task.WhenAll(_seeds.Endpoints.Select(async ep =>
        {
            try
            {
                var lazy = _sentinels.GetOrAdd(ep.ToString()!, _ => new(() => ConnectSentinelAsync(ep)));
                var mux = await lazy.Value.WaitAsync(timeout, ct).ConfigureAwait(false);
                var server = mux.GetServer(ep);
                var addr = (RedisResult[]?)await server.ExecuteAsync("SENTINEL", "GET-MASTER-ADDR-BY-NAME", _seeds.ServiceName).WaitAsync(timeout, ct).ConfigureAwait(false);
                if (addr is not { Length: 2 })
                {
                    return new Vote(null, false);
                }

                var master = (RedisResult[])(await server.ExecuteAsync("SENTINEL", "MASTER", _seeds.ServiceName).WaitAsync(timeout, ct).ConfigureAwait(false))!;
                var flags = "";
                for (var i = 0; i + 1 < master.Length; i += 2)
                {
                    if ((string?)master[i] == "flags")
                    {
                        flags = (string?)master[i + 1] ?? "";
                    }
                }

                // "master" alone is healthy; s_down, o_down, disconnected or a failover in flight are evidence the primary may change.
                var troubled = flags != "master";
                return new Vote((string?)addr[0] + ":" + (string?)addr[1], troubled);
            }
            catch (Exception ex) when (ex is RedisException or TimeoutException or ObjectDisposedException)
            {
                _sentinels.TryRemove(ep.ToString()!, out _);
                return new Vote(null, false);
            }
        })).ConfigureAwait(false);
    }

    /// <summary>
    /// Asks every listed cluster node who owns the application's slot (22 §2.6). A node's answer is its own view, so each vote is one
    /// <c>CLUSTER NODES</c> reply read on that node; the authority needs the same answer from a majority of the listed nodes.
    /// </summary>
    async Task<Vote[]> AskClusterAsync(CancellationToken ct)
    {
        var timeout = TimeSpan.FromMilliseconds(Math.Min(1500, Math.Max(250, _options.AuthorityTtl.TotalMilliseconds / 2)));
        var lazy = _clusterMux ??= new(ConnectClusterAsync);
        ConnectionMultiplexer mux;
        try
        {
            mux = await lazy.Value.WaitAsync(timeout, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is RedisException or TimeoutException or ObjectDisposedException)
        {
            _clusterMux = null;
            return [.. _seeds.Endpoints.Select(_ => new Vote(null, false))];
        }

        return await Task.WhenAll(_seeds.Endpoints.Select(async ep =>
        {
            try
            {
                var text = await mux.GetServer(ep).ClusterNodesRawAsync().WaitAsync(timeout, ct).ConfigureAwait(false);
                return ParseClusterView(text ?? "", ep, _slot);
            }
            catch (Exception ex) when (ex is RedisException or TimeoutException or ObjectDisposedException or InvalidOperationException)
            {
                return new Vote(null, false);
            }
        })).ConfigureAwait(false);
    }

    async Task<ConnectionMultiplexer> ConnectClusterAsync()
    {
        var o = _data.Clone();
        o.AllowAdmin = true;
        o.ClientName = "runwick:cluster-authority";
        o.ConnectTimeout = Math.Min(o.ConnectTimeout, 2000);
        o.AsyncTimeout = 2000;
        o.SyncTimeout = 2000;
        return await ConnectionMultiplexer.ConnectAsync(o).ConfigureAwait(false);
    }

    /// <summary>
    /// One node's <c>CLUSTER NODES</c> text as a vote on the owner of <paramref name="slot"/>: the owner is the healthy-looking primary whose ranges
    /// hold the slot. The vote is troubled when the owner is flagged failing, or when any node lists the slot as migrating or importing.
    /// </summary>
    internal static Vote ParseClusterView(string text, EndPoint seed, int slot)
    {
        var nodes = new List<RxClusterNode>();
        string? owner = null;
        var ownerFailing = false;
        var moving = false;
        foreach (var line in text.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            var f = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (f.Length < 8)
            {
                continue;
            }

            var addr = f[1];
            var cut = addr.IndexOf('@', StringComparison.Ordinal);
            addr = cut >= 0 ? addr[..cut] : addr.Split(',')[0];
            if (addr.StartsWith(':'))
            {
                addr = (seed is IPEndPoint ip ? ip.Address.ToString() : ((DnsEndPoint)seed).Host) + addr;
            }

            var flags = f[2].Split(',');
            var primary = flags.Contains("master");
            var health = flags.Contains("fail") ? "fail" : flags.Contains("fail?") ? "fail?" : flags.Contains("handshake") || flags.Contains("noaddr") ? "unknown" : "ok";
            var owns = false;
            var slots = 0;
            var movingHere = false;
            foreach (var token in f.Skip(8))
            {
                if (token.StartsWith('['))
                {
                    var n = token.TrimStart('[').Split('-')[0];
                    if (int.TryParse(n, NumberStyles.None, CultureInfo.InvariantCulture, out var movingSlot) && movingSlot == slot)
                    {
                        movingHere = true;
                    }

                    continue;
                }

                var parts = token.Split('-');
                if (int.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out var lo)
                    && int.TryParse(parts[^1], NumberStyles.None, CultureInfo.InvariantCulture, out var hi))
                {
                    slots += hi - lo + 1;
                    owns |= slot >= lo && slot <= hi;
                }
            }

            moving |= movingHere;
            if (primary && owns)
            {
                owner = addr;
                ownerFailing = health != "ok";
            }

            nodes.Add(new RxClusterNode(addr, primary ? "primary" : "replica", health, slots, primary && owns, movingHere));
        }

        return new Vote(owner, ownerFailing || moving, nodes);
    }

    async Task<ConnectionMultiplexer> ConnectSentinelAsync(EndPoint ep)
    {
        var o = new ConfigurationOptions
        {
            CommandMap = CommandMap.Sentinel,
            TieBreaker = "",
            AbortOnConnectFail = false,
            AllowAdmin = true,
            User = _seeds.User,
            Password = _seeds.Password,
            Ssl = _seeds.Ssl,
            SslHost = _seeds.SslHost,
            SslProtocols = _seeds.SslProtocols,
            ConnectTimeout = Math.Min(_seeds.ConnectTimeoutMs, 2000),
            AsyncTimeout = 2000,
            SyncTimeout = 2000,
            ConnectRetry = 1,
            ClientName = "runwick:sentinel-authority",
            BacklogPolicy = BacklogPolicy.FailFast,
        };
        o.EndPoints.Add(ep);
        return await ConnectionMultiplexer.ConnectAsync(o).ConfigureAwait(false);
    }

    sealed class Probe(ConnectionMultiplexer mux, IServer server, IDatabase db) : IAsyncDisposable
    {
        public IServer Server { get; } = server;

        public IDatabase Db { get; } = db;

        public ValueTask DisposeAsync() => mux.DisposeAsync();
    }

    async Task<Probe> ProbeAsync(string primary, CancellationToken ct)
    {
        var o = _data.Clone();
        o.EndPoints.Clear();
        var ep = ParseEndPoint(primary);
        o.EndPoints.Add(ep);
        o.AllowAdmin = true;
        o.ClientName = "runwick:authority-probe";
        var mux = await ConnectionMultiplexer.ConnectAsync(o).WaitAsync(TimeSpan.FromSeconds(5), ct).ConfigureAwait(false);
        return new Probe(mux, mux.GetServer(ep), mux.GetDatabase());
    }

    internal static EndPoint ParseEndPoint(string hostPort)
    {
        var i = hostPort.LastIndexOf(':');
        var host = hostPort[..i];
        var port = int.Parse(hostPort[(i + 1)..], CultureInfo.InvariantCulture);
        return IPAddress.TryParse(host, out var ip) ? new IPEndPoint(ip, port) : new DnsEndPoint(host, port);
    }

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync().ConfigureAwait(false);
        if (_loop is not null)
        {
            try
            {
                await _loop.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Stopped.
            }
        }

        if (_clusterMux is { IsValueCreated: true } cm)
        {
            try
            {
                await (await cm.Value.ConfigureAwait(false)).DisposeAsync().ConfigureAwait(false);
            }
            catch (RedisException)
            {
                // Never connected.
            }
        }

        foreach (var lazy in _sentinels.Values)
        {
            if (lazy.IsValueCreated)
            {
                try
                {
                    await (await lazy.Value.ConfigureAwait(false)).DisposeAsync().ConfigureAwait(false);
                }
                catch (RedisException)
                {
                    // Never connected.
                }
            }
        }
    }
}
