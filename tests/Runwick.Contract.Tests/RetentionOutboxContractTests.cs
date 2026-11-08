using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Dispatching;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Protocol;

namespace Runwick.Contract.Tests;

public sealed record ContractWindowJob(string Id, string Key) : IQueueJob;

/// <summary>
/// Retention (17: RET-01, CLN-02, the policy edit) and the outbox destination (14: OUT-01, OUT-02) on both stores, ported from
/// Runwick.Postgres.Tests.RetentionTests and Runwick.Outbox.Tests.RelayTests. Barriers are left out: on SQLite one writer
/// thread holds the write lock for the whole batch, so live work waits instead of interleaving (20 §1).
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-019")]
public sealed class RetentionOutboxContractTests
{
    public static TheoryData<StoreKind> Kinds => CoreContractTests.Kinds;

    static readonly ControlActor Actor = new("user:42", "Ada", "host", "retention");

    static readonly CleanupLimits Limits = new(100, 1 << 20, TimeSpan.FromSeconds(5));

    /// <summary>Everything the policy and the fixed rules may delete; every other table must be unchanged by cleanup.</summary>
    static readonly HashSet<string> Deletable = new(StringComparer.Ordinal)
    {
        "jobs", "job_payloads", "job_generations", "job_attempts", "job_concurrency_requirements", "dispatch_keys", "storage_commands",
        "outbox_deliveries", "schedule_occurrences", "schedule_audit_ranges", "scaling_events", "queue_events", "rate_grants", "management_audit",
        "worker_instances", "cleanup_progress",
    };

    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static async Task<(DriverFixture F, TestClient C)> OpenAsync(StoreKind kind, string name)
    {
        var f = await DriverFixture.StartAsync(name + "_" + Guid.NewGuid().ToString("N")[..8], kind);
        var c = await f.OpenClient(configure: r =>
        {
            TestJobs.Register(r);
            r.AddJob<ContractWindowJob>().Named("test.window").ToQueue("default").UniqueWithinWindow(j => j.Key, TimeSpan.FromMinutes(10));
        });
        return (f, c);
    }

    /// <summary>A table name in the store's form.</summary>
    static string T(DriverFixture f, string table) => f.Kind == StoreKind.Sqlite ? "runwick_" + table : "runwick." + table;

    static long Ms(TimeSpan t) => (long)t.TotalMilliseconds;

    /// <summary>The ms-floored instant <paramref name="ms"/> milliseconds ago (stored instants are whole ms).</summary>
    static DateTimeOffset MsAgo(long ms) => DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - ms);

    /// <summary>Admin SQL (setup only): PostgreSQL through the fixture, SQLite on its own connection.</summary>
    static async Task ExecAsync(DriverFixture f, string pg, string sqlite, params (string Name, object Value)[] parameters)
    {
        if (f.Kind != StoreKind.Sqlite)
        {
            await f.ExecAsync(pg, parameters);
            return;
        }

        using var conn = new SqliteConnection(f.AdminConnectionString);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sqlite;
        foreach (var (name, value) in parameters)
        {
            cmd.Parameters.AddWithValue("@" + name, value switch
            {
                Guid g => g.ToByteArray(bigEndian: true),
                DateTimeOffset t => t.ToUnixTimeMilliseconds(),
                TimeSpan s => Ms(s),
                _ => value,
            });
        }

        cmd.ExecuteNonQuery();
    }

    static Task AgeJobsAsync(DriverFixture f, TimeSpan by) => ExecAsync(f,
        "UPDATE runwick.jobs SET terminal_at = terminal_at - @by WHERE terminal_at IS NOT NULL",
        "UPDATE runwick_jobs SET terminal_at = terminal_at - @by WHERE terminal_at IS NOT NULL", ("by", by));

    static async Task<LeaseHandle> RunAsync(TestClient c)
    {
        var reserved = await c.ReserveAsync();
        Assert.Equal(StoreStatus.Applied, reserved.Status);
        var lease = reserved.Value!.Lease.Handle;
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(lease)).Reply.Status);
        return lease;
    }

    /// <summary>Dispatches, runs and finishes one job; failures carry an error text that detail expiry must remove.</summary>
    static async Task<Guid> TerminalAsync(TestClient c, IQueueJob job, AttemptOutcome outcome = AttemptOutcome.PermanentFailure)
    {
        var id = (await c.Dispatcher.DispatchAsync(job, cancellationToken: Ct)).JobId.Value;
        var lease = await RunAsync(c);
        Assert.Equal(id, lease.JobId);
        var error = outcome == AttemptOutcome.Succeeded ? null : new AttemptError("boom", "System.Exception", "secret detail", null);
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(lease, outcome, error: error)).Status);
        return id;
    }

    static IRetentionStore Retention(TestClient c) => (IRetentionStore)c.Driver;

    static async Task<CleanupBatchResult> BatchAsync(TestClient c, string recordClass)
    {
        var r = await Retention(c).RunCleanupBatchAsync(c.Application, recordClass, Limits, CancellationToken.None);
        Assert.True(r.Status == "done", $"{recordClass}: {r.Status} {r.ErrorCode}");
        return r;
    }

    static async Task PassAsync(TestClient c, string recordClass)
    {
        for (var i = 0; i < 10_000; i++)
        {
            if ((await BatchAsync(c, recordClass)).PassComplete)
            {
                return;
            }
        }

        throw new InvalidOperationException("pass did not complete");
    }

    static async Task FullPassAsync(TestClient c)
    {
        foreach (var recordClass in RecordClasses.All)
        {
            await PassAsync(c, recordClass);
        }
    }

    static Task<long> Exists(DriverFixture f, Guid job) => f.CountAsync($"SELECT count(*) FROM {T(f, "jobs")} WHERE job_id = @j", ("j", job));

    static Task<long> Payloads(DriverFixture f, Guid job) => f.CountAsync($"SELECT count(*) FROM {T(f, "job_payloads")} WHERE job_id = @j", ("j", job));

    static Task<long> Expired(DriverFixture f, Guid job) =>
        f.CountAsync($"SELECT count(*) FROM {T(f, "jobs")} WHERE job_id = @j AND detail_expired_at IS NOT NULL AND NOT payload_retained", ("j", job));

    // ------------------------------------------------------------------ RET-01

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "RET-01")]
    public async Task Ret01a_a_live_job_is_never_selected_and_its_lease_and_finish_are_unaffected(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "ret01a");
        await using var _ = f;
        var old = await TerminalAsync(c, new PingJob("old"));
        await AgeJobsAsync(f, TimeSpan.FromDays(200));
        var live = (await c.Dispatcher.DispatchAsync(new PingJob("live"), cancellationToken: Ct)).JobId.Value;
        var lease = await RunAsync(c);
        Assert.Equal(live, lease.JobId);
        await ExecAsync(f, "UPDATE runwick.jobs SET created_at = created_at - interval '200 days' WHERE job_id = @j",
            "UPDATE runwick_jobs SET created_at = created_at - 17280000000 WHERE job_id = @j", ("j", live));

        Assert.Equal(1, (await BatchAsync(c, RecordClasses.JobDetailFailed)).Rows);
        Assert.Equal(StoreStatus.Applied, (await c.RenewAsync(lease)).Status);
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(lease, AttemptOutcome.Succeeded)).Status);

        await FullPassAsync(c);
        await FullPassAsync(c);
        Assert.Equal(0, await Exists(f, old));
        Assert.Equal((1, 1), (await Exists(f, live), await Payloads(f, live)));
        Assert.Equal(1, await f.CountAsync($"SELECT count(*) FROM {T(f, "jobs")} WHERE job_id = @j AND state = 'succeeded'", ("j", live)));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "RET-01")]
    public async Task Ret01d_a_receipt_still_inside_retain_until_survives_and_replays(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "ret01d");
        await using var _ = f;
        await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct);
        await c.Dispatcher.DispatchAsync(new PingJob("b"), cancellationToken: Ct);
        var gone = Guid.CreateVersion7();
        Assert.Equal(StoreStatus.Applied, (await c.ReserveAsync(commandId: gone)).Status);
        var kept = Guid.CreateVersion7();
        var notAfter = DateTimeOffset.UtcNow.AddSeconds(30);
        var first = await c.ReserveAsync(commandId: kept, notAfter: notAfter);
        Assert.Equal(StoreStatus.Applied, first.Status);

        // The gone receipt ends 1 ms ago (its deadline moved with it, as the retain check requires); the kept one is untouched.
        var end = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 1);
        await ExecAsync(f,
            "UPDATE runwick.storage_commands SET not_after = not_after - (retain_until - @t), retain_until = @t WHERE command_id = @gone",
            "UPDATE runwick_storage_commands SET not_after = not_after - (retain_until - @t), retain_until = @t WHERE command_id = @gone",
            ("t", end), ("gone", gone));

        await PassAsync(c, RecordClasses.ReceiptsOwnership);

        var receipts = $"SELECT count(*) FROM {T(f, "storage_commands")} WHERE command_id = @c";
        Assert.Equal((0, 1), (await f.CountAsync(receipts, ("c", gone)), await f.CountAsync(receipts, ("c", kept))));
        var replay = await c.ReserveAsync(commandId: kept, notAfter: notAfter);
        Assert.Equal((StoreStatus.Applied, true), (replay.Status, replay.Replayed));
        Assert.Equal(first.Value!.Lease.Handle, replay.Value!.Lease.Handle);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "RET-01")]
    public async Task Ret01e_expired_window_keys_race_dispatch_without_a_duplicate_job(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "ret01e");
        await using var _ = f;
        for (var round = 0; round < 10; round++)
        {
            var key = "k" + round;
            var first = (await c.Dispatcher.DispatchAsync(new ContractWindowJob("a" + round, key), cancellationToken: Ct)).JobId.Value;
            await ExecAsync(f, "UPDATE runwick.dispatch_keys SET expires_at = @t WHERE key_kind = 'window' AND key = @k",
                "UPDATE runwick_dispatch_keys SET expires_at = @t WHERE key_kind = 'window' AND key = @k",
                ("t", MsAgo(1)), ("k", key));
            var dispatch = c.Dispatcher.DispatchAsync(new ContractWindowJob("b" + round, key), cancellationToken: Ct);
            var cleanup = BatchAsync(c, RecordClasses.DispatchKeys);
            var second = (await dispatch.WaitAsync(TimeSpan.FromSeconds(30), Ct)).JobId.Value;
            await cleanup.WaitAsync(TimeSpan.FromSeconds(30), Ct);
            Assert.NotEqual(first, second);

            // The new dispatch either re-bound the row or made a fresh one after cleanup: one key row, bound to it.
            var keys = $"SELECT count(*) FROM {T(f, "dispatch_keys")} WHERE key_kind = 'window' AND key = @k";
            Assert.Equal(1, await f.CountAsync(keys, ("k", key)));
            Assert.Equal(1, await f.CountAsync(keys + " AND bound_job_id = @j", ("k", key), ("j", second)));
            var again = await c.Dispatcher.DispatchAsync(new ContractWindowJob("b" + round, key), cancellationToken: Ct);
            Assert.Equal(second, again.JobId.Value);
        }
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "RET-01")]
    public async Task Ret01f_a_terminal_job_with_a_live_window_binding_or_a_permit_is_never_purged(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "ret01f");
        await using var _ = f;
        var windowed = await TerminalAsync(c, new ContractWindowJob("w", "k"), AttemptOutcome.Succeeded);
        var permitted = await TerminalAsync(c, new ResourceJob("r", "res"), AttemptOutcome.Succeeded);
        await ExecAsync(f, """
            INSERT INTO runwick.concurrency_permits (application, scope_id, attempt_id, job_id, ownership_token, lease_expires_at)
            SELECT q.application, q.scope_id, a.attempt_id, q.job_id, gen_random_uuid(), runwick.ms_floor(clock_timestamp())
            FROM runwick.job_concurrency_requirements AS q JOIN runwick.job_attempts AS a USING (application, job_id)
            WHERE q.job_id = @j
            """, """
            INSERT INTO runwick_concurrency_permits (application, scope_id, attempt_id, job_id, ownership_token, lease_expires_at)
            SELECT q.application, q.scope_id, a.attempt_id, q.job_id, randomblob(16), @t
            FROM runwick_job_concurrency_requirements AS q JOIN runwick_job_attempts AS a USING (application, job_id)
            WHERE q.job_id = @j
            """, ("j", permitted), ("t", MsAgo(0)));
        await AgeJobsAsync(f, TimeSpan.FromDays(200));

        await FullPassAsync(c);
        await FullPassAsync(c);
        Assert.Equal((1, 1L), (await Exists(f, windowed), await Expired(f, windowed)));
        Assert.Equal((1, 1L), (await Exists(f, permitted), await Expired(f, permitted)));

        // Binding expired and permit gone: both purge.
        await ExecAsync(f, "UPDATE runwick.dispatch_keys SET expires_at = @t WHERE key_kind = 'window'",
            "UPDATE runwick_dispatch_keys SET expires_at = @t WHERE key_kind = 'window'", ("t", MsAgo(1)));
        await ExecAsync(f, "DELETE FROM runwick.concurrency_permits", "DELETE FROM runwick_concurrency_permits");
        await FullPassAsync(c);
        Assert.Equal((0, 0), (await Exists(f, windowed), await Exists(f, permitted)));
        Assert.Equal(0, await f.CountAsync($"SELECT count(*) FROM {T(f, "dispatch_keys")}"));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "RET-01")]
    public async Task Ret01g_fences_and_scope_rows_are_unchanged_after_a_full_pass_of_every_class(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "ret01g");
        await using var _ = f;
        await TerminalAsync(c, new PingJob("p"));
        await TerminalAsync(c, new ResourceJob("r", "res"), AttemptOutcome.Succeeded);
        await TerminalAsync(c, new KeyedJob("k", "key"), AttemptOutcome.Succeeded);
        await c.Dispatcher.DispatchAsync(new PingJob("pending"), cancellationToken: Ct);
        await AgeJobsAsync(f, TimeSpan.FromDays(400));
        await ExecAsync(f,
            "UPDATE runwick.storage_commands SET not_after = not_after - interval '400 days', retain_until = retain_until - interval '400 days'",
            "UPDATE runwick_storage_commands SET not_after = not_after - @by, retain_until = retain_until - @by", ("by", TimeSpan.FromDays(400)));
        var before = await f.ReadCommittedSnapshot();

        await FullPassAsync(c);
        await FullPassAsync(c);

        var changed = before.DiffTables(await f.ReadCommittedSnapshot());
        Assert.DoesNotContain(changed, t => !Deletable.Contains(t));
        Assert.Equal(1, await f.CountAsync($"SELECT count(*) FROM {T(f, "jobs")}"));
    }

    // ------------------------------------------------------------------ CLN-02 and the policy

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "CLN-02")]
    public async Task Cln02_a_closed_day_writes_one_summary_per_class(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "cln02");
        await using var _ = f;
        await BatchAsync(c, RecordClasses.JobEvents);
        await ExecAsync(f,
            "UPDATE runwick.cleanup_progress SET summary_day = (now() AT TIME ZONE 'UTC')::date - 1, day_rows = 5, day_batches = 2, day_failures = 1",
            "UPDATE runwick_cleanup_progress SET summary_day = date('now', '-1 day'), day_rows = 5, day_batches = 2, day_failures = 1");

        await BatchAsync(c, RecordClasses.JobEvents);
        await BatchAsync(c, RecordClasses.JobEvents);
        Assert.Equal(1, await f.CountAsync($"SELECT count(*) FROM {T(f, "management_audit")} WHERE operation = 'RetentionSummary' AND target_id = 'job_events'"));
        var progress = (await Retention(c).GetRetentionAsync(c.Application, Ct)).Value.Classes.Single(p => p.RecordClass == RecordClasses.JobEvents);
        Assert.Equal((2, 0), (progress.TodayBatches, progress.TodayFailures));
        Assert.NotNull(progress.CaughtUpAt);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    public async Task EditRetentionPolicy_is_revisioned_audited_and_replayed(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "retpol");
        await using var _ = f;
        var store = Retention(c);
        var initial = await store.GetRetentionAsync(c.Application, Ct);
        Assert.Equal((0, RecordClasses.All.Count), (initial.Value.Policy.Revision, initial.Value.Classes.Count));

        var policy = RetentionPolicy.Default with { CompletedDetail = TimeSpan.FromDays(3) };
        var command = c.Command(new RetentionPolicyRequest(policy, 0, Actor));
        var applied = await store.EditRetentionPolicyAsync(command, Ct);
        Assert.Equal((StoreStatus.Applied, 1L), (applied.Status, applied.Value!.Revision));
        var replay = await store.EditRetentionPolicyAsync(command, Ct);
        Assert.Equal((StoreStatus.Applied, true, 1L, policy), (replay.Status, replay.Replayed, replay.Value!.Revision, replay.Value.Policy));

        var stale = await store.EditRetentionPolicyAsync(c.Command(new RetentionPolicyRequest(RetentionPolicy.Default, 0, Actor)), Ct);
        Assert.Equal((StoreStatus.VersionConflict, 1L), (stale.Status, stale.Error!.ActualRevision));
        var same = await store.EditRetentionPolicyAsync(c.Command(new RetentionPolicyRequest(policy, 1, Actor)), Ct);
        Assert.Equal((StoreStatus.InvalidState, "no_change"), (same.Status, same.Error!.Code));
        var invalid = await store.EditRetentionPolicyAsync(
            c.Command(new RetentionPolicyRequest(policy with { Audit = TimeSpan.FromDays(1) }, 1, Actor)), Ct);
        Assert.Equal((StoreStatus.InvalidRequest, "invalid_retention_policy"), (invalid.Status, invalid.Error!.Code));

        var read = await store.GetRetentionAsync(c.Application, Ct);
        Assert.Equal((1L, policy), (read.Value.Policy.Revision, read.Value.Policy.Policy));
        Assert.Equal(1, await f.CountAsync($"SELECT count(*) FROM {T(f, "management_audit")} WHERE operation = 'EditRetentionPolicy' AND resulting_revision = 1"));
    }

    // ------------------------------------------------------------------ OUT-01, OUT-02 (store level)

    static StoreCommand<ReceiveOutboxRequest> Receive(TestClient c, PreparedDispatch prepared, OutboxSourceIdentity identity, DateTimeOffset acceptUntil)
    {
        var cutoff = DateTimeOffset.FromUnixTimeMilliseconds(acceptUntil.ToUnixTimeMilliseconds());
        var deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(20);
        deadline = DateTimeOffset.FromUnixTimeMilliseconds((deadline < cutoff ? deadline : cutoff).ToUnixTimeMilliseconds());
        var fingerprint = OutboxFingerprints.Delivery(prepared.StoreId, prepared.Application, identity, cutoff.ToUnixTimeMilliseconds(), prepared.Intent,
            prepared.Digests);
        return new StoreCommand<ReceiveOutboxRequest>(c.Application, Guid.CreateVersion7(), deadline,
            new ReceiveOutboxRequest(identity, new DispatchItem(prepared.Intent, prepared.Scopes, prepared.Digests), cutoff, fingerprint, prepared.StoreId));
    }

    static PreparedDispatch Prepare(TestClient c, IQueueJob job) =>
        c.Services.GetRequiredService<DispatchPreparer>().Prepare(c.Runtime.StoreId, job, null);

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "OUT-01")]
    public async Task Out01_concurrent_transport_commands_deduplicate_by_message_identity(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "out01");
        await using var _ = f;
        var destination = (IOutboxDestinationStore)c.Driver;
        var prepared = Prepare(c, new PingJob("m"));
        var identity = new OutboxSourceIdentity(Guid.NewGuid(), Guid.CreateVersion7());
        var acceptUntil = DateTimeOffset.UtcNow.AddDays(1);

        // Several relays send their own transport commands for one message: one job, one mapping, one first delivery.
        var commands = Enumerable.Range(0, 8).Select(_ => Receive(c, prepared, identity, acceptUntil)).ToList();
        var replies = await Task.WhenAll(commands.Select(cmd => destination.ReceiveOutboxAsync(cmd, Ct)));
        Assert.All(replies, r => Assert.Equal((StoreStatus.Applied, prepared.Intent.ProposedJobId), (r.Status, r.Value!.JobId)));
        Assert.Single(replies, r => r.Value!.FirstDelivery);
        var profile = kind == StoreKind.Sqlite ? AcknowledgementProfiles.SqliteWalFull : OutboxOperations.PostgresProfile;
        Assert.All(replies, r => Assert.Equal(profile, r.Value!.ConfirmedProfile));
        Assert.Equal(1, await f.CountAsync($"SELECT count(*) FROM {T(f, "jobs")}"));
        Assert.Equal(1, await f.CountAsync($"SELECT count(*) FROM {T(f, "outbox_deliveries")}"));

        // A resent command replays its receipt; different intent under the same identity is a conflict, not a dispatch.
        var resent = await destination.ReceiveOutboxAsync(commands[0], Ct);
        Assert.Equal((StoreStatus.Applied, true), (resent.Status, resent.Replayed));
        var other = Prepare(c, new PingJob("other"));
        var conflict = await destination.ReceiveOutboxAsync(Receive(c, other, identity, acceptUntil), Ct);
        Assert.Equal((StoreStatus.CommandConflict, "delivery_conflict", prepared.Intent.ProposedJobId),
            (conflict.Status, conflict.Error!.Code, conflict.Error.ConflictingJobId));

        var found = await destination.FindDeliveryAsync(c.Application, identity, commands[0].Body.DeliveryFingerprint, Ct);
        Assert.Equal((prepared.Intent.ProposedJobId, false), (found.Delivery!.JobId, found.Conflict));
        var mismatch = await destination.FindDeliveryAsync(c.Application, identity, new byte[32], Ct);
        Assert.Equal((null, true), (mismatch.Delivery, mismatch.Conflict));
        Assert.Equal(1, await f.CountAsync($"SELECT count(*) FROM {T(f, "jobs")}"));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "OUT-02")]
    public async Task Out02_a_message_outside_its_horizon_is_an_explicit_limit_not_a_silent_dispatch(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "out02");
        await using var _ = f;
        var destination = (IOutboxDestinationStore)c.Driver;
        var prepared = Prepare(c, new PingJob("x"));
        var identity = new OutboxSourceIdentity(Guid.NewGuid(), Guid.CreateVersion7());
        var late = Receive(c, prepared, identity, DateTimeOffset.UtcNow.AddMilliseconds(1500));
        await Task.Delay(2000, Ct);

        // Past AcceptUntil no command can apply (its NotAfter is never later), no mapping exists, and nothing was dispatched.
        Assert.Equal(StoreStatus.CommandExpired, (await destination.ReceiveOutboxAsync(late, Ct)).Status);
        var lookup = await destination.FindDeliveryAsync(c.Application, identity, late.Body.DeliveryFingerprint, Ct);
        Assert.Equal((null, false), (lookup.Delivery, lookup.Conflict));
        Assert.Equal(0, await f.CountAsync($"SELECT count(*) FROM {T(f, "jobs")}"));

        // A NotAfter beyond AcceptUntil is refused before the store is touched.
        var beyond = Receive(c, prepared, new OutboxSourceIdentity(identity.SourceStoreId, Guid.CreateVersion7()), DateTimeOffset.UtcNow.AddDays(1));
        var invalid = await destination.ReceiveOutboxAsync(beyond with { NotAfter = beyond.Body.AcceptUntil.AddSeconds(1) }, Ct);
        Assert.Equal(StoreStatus.InvalidRequest, invalid.Status);

        // A delivered mapping keeps answering after the horizon, and cleanup keeps it until retain_until.
        var delivered = Receive(c, prepared, new OutboxSourceIdentity(identity.SourceStoreId, Guid.CreateVersion7()), DateTimeOffset.UtcNow.AddMilliseconds(1500));
        Assert.Equal(StoreStatus.Applied, (await destination.ReceiveOutboxAsync(delivered, Ct)).Status);
        await Task.Delay(2000, Ct);
        await PassAsync(c, RecordClasses.OutboxDeliveries);
        var after = await destination.FindDeliveryAsync(c.Application, delivered.Body.Source, delivered.Body.DeliveryFingerprint, Ct);
        Assert.Equal(prepared.Intent.ProposedJobId, after.Delivery!.JobId);

        // Past retain_until the mapping is cleaned up.
        await ExecAsync(f, "UPDATE runwick.outbox_deliveries SET delivered_at = accept_until - interval '25 hours 1 millisecond', accept_until = accept_until - interval '25 hours', retain_until = accept_until - interval '1 hour'",
            "UPDATE runwick_outbox_deliveries SET delivered_at = accept_until - 90000001, accept_until = accept_until - 90000000, retain_until = accept_until - 3600000");
        await PassAsync(c, RecordClasses.OutboxDeliveries);
        Assert.Equal(0, await f.CountAsync($"SELECT count(*) FROM {T(f, "outbox_deliveries")}"));
    }
}
