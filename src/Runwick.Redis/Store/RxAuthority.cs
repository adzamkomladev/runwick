using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Security.Authentication;
using Runwick.Protocol;
using StackExchange.Redis;

namespace Runwick.Redis.Store;

/// <summary>The Sentinel endpoints, service name and Sentinel credentials taken from the configuration string (22 §2.5).</summary>
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

internal sealed record RxAuthoritySnapshot(
    RxAuthorityState State, string? Reason, string? Primary, string? RunId, long Epoch, int SentinelsAgreeing, int SentinelsConfigured,
    DateTimeOffset? VerifiedAt, DateTimeOffset? ExpiresAt, IReadOnlyList<RxAuthorityEvent> Recent);

/// <summary>
/// Strict write authority for a Sentinel-managed primary (22 §2.5). Commands are sent only to a primary that a majority of the configured
/// Sentinels named, whose profile was checked and whose history continues the one this process last verified; each success is good for
/// <see cref="RedisSentinelOptions.AuthorityTtl"/>. Evidence of trouble (a different primary, failover flags, a lost connection) closes
/// authority at once; silence closes it when the lease runs out, so a process cut off from the Sentinels stops writing to a former primary.
/// </summary>
internal sealed class RxAuthority : IAsyncDisposable
{
    const int MaxEvents = 16;
    readonly RxSentinelSeeds _seeds;
    readonly RedisSentinelOptions _options;
    readonly TimeProvider _time;
    readonly ConfigurationOptions _data;
    readonly RedisPersistence _persistence;
    readonly SemaphoreSlim _gate = new(1, 1);
    readonly object _lock = new();
    readonly ConcurrentDictionary<string, Lazy<Task<ConnectionMultiplexer>>> _sentinels = new(StringComparer.Ordinal);
    readonly List<RxAuthorityEvent> _events = [];
    readonly CancellationTokenSource _stop = new();
    RxClient? _client;
    Task? _loop;
    RxAuthorityState _state;
    string? _reason;
    string? _primary;
    string? _runId;
    long _epoch;
    int _agreeing;
    DateTimeOffset? _verifiedAt;
    DateTimeOffset? _expiresAt;
    (string Primary, string RunId, string ReplId, long Offset)? _last;

    public RxAuthority(RxSentinelSeeds seeds, RedisSentinelOptions options, TimeProvider time, ConfigurationOptions data, RedisPersistence persistence)
    {
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
            return new(state, reason, _primary, _runId, _epoch, _agreeing, _seeds.Endpoints.Count, _verifiedAt, _expiresAt, [.. _events]);
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
        try
        {
            if (Snapshot().State == RxAuthorityState.Blocked)
            {
                return Snapshot();
            }

            var votes = await AskSentinelsAsync(ct).ConfigureAwait(false);
            var needed = _seeds.Endpoints.Count / 2 + 1;
            var clean = votes.Where(v => v.Primary is not null && !v.Troubled).GroupBy(v => v.Primary!, StringComparer.Ordinal).OrderByDescending(g => g.Count()).FirstOrDefault();
            if (clean is null || clean.Count() < needed)
            {
                // Evidence (a Sentinel reports the primary down or a failover) closes authority now; mere silence waits for the lease.
                var troubled = votes.Any(v => v.Troubled);
                var split = votes.Select(v => v.Primary).Where(p => p is not null).Distinct(StringComparer.Ordinal).Count() > 1;
                SetUnresolved(troubled ? "primary_reported_down_or_failing_over" : split ? "sentinels_disagree" : "sentinel_majority_unreachable", troubled || split, clean?.Count() ?? 0);
                return Snapshot();
            }

            var primary = clean.Key;
            var current = Snapshot();
            if (current.State == RxAuthorityState.Verified && current.Primary != primary)
            {
                SetUnresolved("sentinels_name_a_different_primary", closeNow: true, clean.Count());
            }

            var probe = await ProbeAsync(primary, ct).ConfigureAwait(false);
            await using var _ = probe.ConfigureAwait(false);
            var report = await RxProfile.CheckAsync(probe.Server, probe.Db, _persistence, sentinel: true).ConfigureAwait(false);
            OnReport?.Invoke(report);
            if (!report.Verified)
            {
                SetUnresolved($"{report.FailureCode}: {report.Failure}", closeNow: true, clean.Count());
                return Snapshot();
            }

            var facts = report.Facts;
            var runId = facts.GetValueOrDefault("run_id", "");
            var replId = facts.GetValueOrDefault("master_replid", "");
            var offset = long.TryParse(facts.GetValueOrDefault("master_repl_offset"), NumberStyles.None, CultureInfo.InvariantCulture, out var o) ? o : 0;
            if (_last is { } last && last.Primary != primary)
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

            if (HistoryCheck is { } history && await history(probe.Db).ConfigureAwait(false) is { } failure)
            {
                SetBlocked(failure);
                return Snapshot();
            }

            if (BeforeAllow is { } hook)
            {
                await hook().ConfigureAwait(false);
            }

            var changed = current.Primary != primary;
            _client?.Bind(ParseEndPoint(primary));
            lock (_lock)
            {
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
            }

            return Snapshot();
        }
        finally
        {
            _gate.Release();
        }
    }

    sealed record Vote(string? Primary, bool Troubled);

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
