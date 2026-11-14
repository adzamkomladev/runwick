using System.Collections.Concurrent;
using System.Globalization;
using System.Numerics;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Nodes;
using Runwick.Protocol;
using Runwick.Redis.Migrations;
using StackExchange.Redis;

namespace Runwick.Redis.Store;

/// <summary>
/// The Redis implementation of the storage protocol (03) per 22: each operation is one Lua script that decides with the
/// server clock, stores its intent, projects and publishes (22 §4.1). This class builds the arguments and maps replies.
/// </summary>
internal sealed partial class RedisStore : IRunwickStore, IAsyncDisposable
{
    readonly RxRunner runner;
    readonly RedisOptions options;
    readonly TimeProvider time;
    readonly ConcurrentDictionary<string, BigInteger> _revisions = new(StringComparer.Ordinal);
    readonly SemaphoreSlim _profileGate = new(1, 1);
    ITimer? _recheck;
    string? _runId;
    (string Layout, string Epoch, string Mode, string Digest)? _storeGate;

    public RedisStore(RxRunner runner, RedisOptions options, TimeProvider time)
    {
        this.runner = runner;
        this.options = options;
        this.time = time;
        runner.Client.Reconnected += () => _ = Task.Run(() => CheckProfileAsync(CancellationToken.None));
    }

    public Guid StoreId => runner.StoreId ?? throw new InvalidOperationException("Describe has not run yet.");

    public string AcknowledgementProfile =>
        options.Persistence == RedisPersistence.AofEverySecond ? AcknowledgementProfiles.RedisAofEverySecond : AcknowledgementProfiles.RedisAofAlways;

    internal RxRunner Runner => runner;

    /// <summary>Describe's engine facts (22 §6): version, run_id, modules, profile status, replicas, maxmemory, limits.</summary>
    public IReadOnlyDictionary<string, string> EngineFacts { get; private set; } = new Dictionary<string, string>();

    public async ValueTask DisposeAsync()
    {
        if (_recheck is not null)
        {
            await _recheck.DisposeAsync().ConfigureAwait(false);
        }

        await runner.Client.DisposeAsync().ConfigureAwait(false);
    }

    static string Ms(DateTimeOffset t) => t.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);

    static string Hex(Guid g) => RxClient.Hex(g);

    async Task<Guid> EnsureStoreIdAsync(string application, CancellationToken ct) =>
        runner.StoreId ?? (await DescribeAsync(application, ct).ConfigureAwait(false)).StoreId;

    RxOp Op(string application, Guid commandId, string operation, DateTimeOffset notAfter, byte[]? fingerprint, RxLane lane, char gate,
        Guid? storeId = null) =>
        new(application, commandId, operation, notAfter, fingerprint, lane, gate, storeId);

    /// <summary>Creation operations (03 §10.1 capability gate); others are allowed while draining or not.</summary>
    static char GateOf(string operation) => operation switch
    {
        Operations.Dispatch or Operations.DispatchMany or Operations.Reserve or Operations.ReserveAndBegin or Operations.Begin
            or Operations.RegisterQueue or Operations.RegisterScope or OutboxOperations.ReceiveOutbox or Operations.ManagementDispatch => 'c',
        _ when ScheduleOperations.IsCreation(operation) => 'c',
        Operations.Renew or Operations.RenewMany or Operations.Finish or Operations.FinishMany or Operations.Recover or Operations.ReleaseReservation
            or "PublishPresence" or Operations.EventsSince => 'd',
        _ when Operations.IsAdministrative(operation) => 'd',
        _ => 'n',
    };

    Task<RxReply> Write(RxOp op, string script, CancellationToken ct, params RedisValue[] args) =>
        runner.WriteAsync(op, RxScript.Write(script), args, ct);

    // ------------------------------------------------------------------ Describe (22 §6) and the profile/history checks

    public async Task<StoreDescriptor> DescribeAsync(string application, CancellationToken ct)
    {
        if (!EngineFacts.ContainsKey("profile_status"))
        {
            await CheckProfileAsync(ct).ConfigureAwait(false);
            if (runner.ProfileFailure is { } failure)
            {
                throw new StoreIncompatibleException(failure);
            }

            _recheck ??= time.CreateTimer(_ => _ = CheckProfileAsync(CancellationToken.None), null, options.ProfileRecheckInterval, options.ProfileRecheckInterval);
        }

        HashEntry[] entries;
        try
        {
            var db = await runner.Client.DbAsync(RxLane.Producer).ConfigureAwait(false);
            entries = await db.HashGetAllAsync(RedisMigrator.StoreKey).ConfigureAwait(false);
        }
        catch (RedisException ex)
        {
            throw new StoreUnavailableException("Could not read the Redis store.", ex);
        }

        if (entries.Length == 0)
        {
            throw new StoreIncompatibleException("The Runwick Redis store is not installed (rw:store missing); run the migrator.");
        }

        var s = entries.ToDictionary(e => (string)e.Name!, e => (string)e.Value!, StringComparer.Ordinal);
        var storeId = Guid.Parse(s["store"]);
        if (s["layout"] != "1" || s["digest"] != Convert.ToHexStringLower(RedisMigrator.Manifest().Digest))
        {
            throw new StoreIncompatibleException("The Redis layout revision or manifest differs from this binary's: schema_incompatible.");
        }

        if (runner.StoreId is { } known && known != storeId)
        {
            throw new StoreIncompatibleException("store_identity_mismatch");
        }

        runner.StoreId ??= storeId;
        _storeGate = (s["layout"], s["epoch"], s["mode"], s["digest"]);
        var a = await runner.ReadAsync(application, RxScript.Read("describe"), RxLane.Producer, [], ct).ConfigureAwait(false);
        ApplicationGate? gate = null;
        if ((string)a[0]! == "OK")
        {
            Seen(application, (string)a[1]!);
            if ((string)a[2]! != Hex(storeId))
            {
                throw new StoreIncompatibleException("store_identity_mismatch");
            }

            gate = new ApplicationGate(long.Parse((string)a[3]!, CultureInfo.InvariantCulture), int.Parse((string)a[4]!, CultureInfo.InvariantCulture),
                ParseCapabilities((string)a[5]!), (string)a[6]!);
        }

        return new StoreDescriptor(storeId, 1, int.Parse(s["epoch"], CultureInfo.InvariantCulture), s["mode"],
            int.Parse(s["mversion"], CultureInfo.InvariantCulture), Convert.FromHexString(s["digest"]), gate, time.GetUtcNow());
    }

    static Dictionary<string, int> ParseCapabilities(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.TryGetInt32(out var v) ? v : int.MaxValue, StringComparer.Ordinal);
    }

    /// <summary>Remembers the highest application revision this process has read (22 §2.4).</summary>
    void Seen(string application, string rev)
    {
        var r = BigInteger.Parse(rev, CultureInfo.InvariantCulture);
        _revisions.AddOrUpdate(application, r, (_, old) => BigInteger.Max(old, r));
    }

    /// <summary>The 22 §2.3 profile check plus the §2.4 history check after a run_id change.</summary>
    internal async Task CheckProfileAsync(CancellationToken ct)
    {
        await _profileGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            RxProfileReport report;
            try
            {
                var (server, db) = await runner.Client.AdminAsync().ConfigureAwait(false);
                // A server still loading its AOF answers LOADING to some checks: that is not a profile verdict, so wait it out (bounded).
                for (var i = 0; ; i++)
                {
                    report = await RxProfile.CheckAsync(server, db, options.Persistence).ConfigureAwait(false);
                    if (report.Facts.GetValueOrDefault("loading") != "1" || i >= 150)
                    {
                        break;
                    }

                    await Task.Delay(200, ct).ConfigureAwait(false);
                }
            }
            catch (RedisException ex)
            {
                // Unreachable is not a profile verdict: calls fail fast meanwhile and the next check decides.
                if (!EngineFacts.ContainsKey("profile_status"))
                {
                    throw new StoreUnavailableException("Could not read the Redis profile.", ex);
                }

                return;
            }

            EngineFacts = report.Facts;
            runner.ProfileFailure = report.Verified ? null : $"{report.FailureCode}: {report.Failure}";
            var runId = report.Facts.GetValueOrDefault("run_id");
            if (_runId is not null && runId != _runId)
            {
                await CheckHistoryAsync().ConfigureAwait(false);
            }

            _runId = runId;
        }
        finally
        {
            _profileGate.Release();
        }
    }

    /// <summary>22 §2.4: same store identity and no revision lower than one this process saw; otherwise authority is invalidated.</summary>
    internal async Task CheckHistoryAsync()
    {
        if (runner.StoreId is not { } id)
        {
            return;
        }

        var db = await runner.Client.DbAsync(RxLane.Background).ConfigureAwait(false);
        var store = await db.HashGetAsync(RedisMigrator.StoreKey, "store").ConfigureAwait(false);
        if (store.IsNull || (string)store! != Hex(id))
        {
            runner.HistoryFailure = "store_identity_mismatch";
            return;
        }

        foreach (var (application, seen) in _revisions)
        {
            var m = await db.HashGetAsync(RxClient.Meta(application), ["store", "rev"]).ConfigureAwait(false);
            if (m[0].IsNull || (string)m[0]! != Hex(id))
            {
                runner.HistoryFailure = "store_identity_mismatch";
                return;
            }

            if (BigInteger.Parse(m[1].IsNull ? "0" : (string)m[1]!, CultureInfo.InvariantCulture) < seen)
            {
                runner.HistoryFailure = "history_discontinuity";
                return;
            }
        }
    }

    // ------------------------------------------------------------------ Register* (04 §7.1)

    public async Task<StoreReply<ApplicationSeedResult>> RegisterApplicationAsync(StoreCommand<RegisterApplicationRequest> command, CancellationToken ct)
    {
        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var g = _storeGate!.Value;
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.RegisterApplication, command.CommandId, command.NotAfter,
            CommandEnvelopes.RegisterApplication());
        var r = await Write(Op(command.Application, command.CommandId, Operations.RegisterApplication, command.NotAfter, fp, RxLane.Producer, 'a'),
            "register_application", ct, g.Layout, g.Epoch, g.Mode, g.Digest).ConfigureAwait(false);
        if (!r.HasValue)
        {
            return r.Reject<ApplicationSeedResult>();
        }

        // The receipt holds created/revision/admission; the full gate is read back (capabilities included).
        var seeded = ReceiptCodec.ReadApplication(r.Result!);
        var described = await DescribeAsync(command.Application, ct).ConfigureAwait(false);
        return r.Applied(new ApplicationSeedResult(seeded.Created, described.Application ?? seeded.Gate));
    }

    public async Task<StoreReply<QueueSeedResult>> RegisterQueuesAsync(StoreCommand<RegisterQueuesRequest> command, CancellationToken ct)
    {
        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.RegisterQueue, command.CommandId, command.NotAfter,
            CommandEnvelopes.RegisterQueues(command.Body));
        var r = await Write(Op(command.Application, command.CommandId, Operations.RegisterQueue, command.NotAfter, fp, RxLane.Producer, 'c'),
            "register_queue", ct, new JsonArray([.. command.Body.Queues.Select(q => (JsonNode?)q)]).ToJsonString()).ConfigureAwait(false);
        return r.HasValue ? r.Applied(ReceiptCodec.ReadQueues(r.Result!)) : r.Reject<QueueSeedResult>();
    }

    public async Task<StoreReply<ScopeSeedResult>> RegisterScopesAsync(StoreCommand<RegisterScopesRequest> command, CancellationToken ct)
    {
        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.RegisterScope, command.CommandId, command.NotAfter,
            CommandEnvelopes.RegisterScopes(command.Body));
        var scopes = new JsonArray([.. command.Body.Scopes.Select(s => (JsonNode?)new JsonObject
        {
            ["kind"] = s.Kind, ["key"] = s.Key, ["capacity"] = s.Capacity, ["id"] = Hex(Guid.CreateVersion7()),
        })]);
        var r = await Write(Op(command.Application, command.CommandId, Operations.RegisterScope, command.NotAfter, fp, RxLane.Producer, 'c'),
            "register_scope", ct, Json(scopes)).ConfigureAwait(false);
        return r.HasValue ? r.Applied(ReceiptCodec.ReadScopes(r.Result!)) : r.Reject<ScopeSeedResult>();
    }

    // ------------------------------------------------------------------ shared

    static readonly JsonSerializerOptions Relaxed = new() { Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    /// <summary>JSON text for a script argument, with non-ASCII kept as UTF-8 (cjson decodes it byte for byte).</summary>
    internal static string Json(JsonNode node) => node.ToJsonString(Relaxed);

    /// <summary>20 hex characters of entropy for a UUIDv7 the script stamps with its decision time.</summary>
    static string Entropy() => Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(10));

    static Guid NewToken() => new(RandomNumberGenerator.GetBytes(16), bigEndian: true);

    /// <summary>A uniform draw in [0, 1) for the retry jitter (rw_retry_delay_v1's equal jitter).</summary>
    static string Draw() => (RandomNumberGenerator.GetInt32(int.MaxValue) / (double)int.MaxValue).ToString("R", CultureInfo.InvariantCulture);

    static (Dictionary<string, string> Business, Dictionary<string, string> Diagnostic) ReadHeaders(string json)
    {
        var o = JsonNode.Parse(json)!.AsObject();
        static Dictionary<string, string> Map(JsonNode? n) =>
            n is JsonObject m ? m.ToDictionary(p => p.Key, p => p.Value!.GetValue<string>(), StringComparer.Ordinal) : new(StringComparer.Ordinal);
        return (Map(o["business"]), Map(o["diagnostic"]));
    }

    static JobScheduleInfo? ReadScheduleHeader(string json) =>
        JsonNode.Parse(json)!.AsObject()["schedule"] is JsonObject s
            ? new JobScheduleInfo(s["key"]!.GetValue<string>(), DateTimeOffset.FromUnixTimeMilliseconds(s["scheduledAt"]!.GetValue<long>()),
                DateTimeOffset.FromUnixTimeMilliseconds(s["coveredFrom"]!.GetValue<long>()), DateTimeOffset.FromUnixTimeMilliseconds(s["coveredUntil"]!.GetValue<long>()))
            : null;

    static ReservedJob ReadReserved(JsonObject o, RxPayload p)
    {
        var (business, diagnostic) = ReadHeaders(p.Headers);
        return ReceiptCodec.ReadReserve(o, p.Payload, business, diagnostic, p.CreatedAt) with { Schedule = ReadScheduleHeader(p.Headers) };
    }

    static StoreReply<T> Invalid<T>(string code, TimeProvider time) where T : class =>
        StoreReply<T>.NoEffect(StoreStatus.InvalidRequest, new StoreError(code), time.GetUtcNow());

    static JsonObject LeaseJson(LeaseHandle l) => new()
    {
        ["jobId"] = Hex(l.JobId), ["attemptId"] = Hex(l.AttemptId), ["workerInstanceId"] = Hex(l.WorkerInstanceId),
        ["ownershipToken"] = Hex(l.OwnershipToken), ["generation"] = l.Generation,
    };

    static JsonArray Contracts(IEnumerable<JobContract> contracts) =>
        new([.. contracts.Distinct().Select(c => (JsonNode?)new JsonArray(c.Name, c.PayloadVersion, c.SerializerProfileId, c.SerializerProfileVersion))]);
}
