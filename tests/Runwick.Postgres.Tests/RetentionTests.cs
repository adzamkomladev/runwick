using System.Diagnostics;
using Npgsql;
using Runwick.Harness.Engine;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Protocol;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

public sealed record WindowJob(string Id, string Key) : IQueueJob;

/// <summary>
/// Retention (17): RET-01, OPS-02, OPS-04, OPS-05 (scaled), OBS-10 and CLN-01 … CLN-04. Old records are made by moving
/// their timestamps back with admin SQL (17 §8 "seed"), not by moving store time.
/// </summary>
public sealed class RetentionTests(ITestOutputHelper output)
{
    static readonly ControlActor Actor = new("user:42", "Ada", "host", "retention");

    static readonly CleanupLimits Limits = new(100, 1 << 20, TimeSpan.FromSeconds(5));

    /// <summary>Everything the policy and the fixed rules may delete; every other table must be unchanged by cleanup.</summary>
    static readonly HashSet<string> Deletable = new(StringComparer.Ordinal)
    {
        "jobs", "job_payloads", "job_generations", "job_attempts", "job_concurrency_requirements", "dispatch_keys", "storage_commands",
        "outbox_deliveries", "schedule_occurrences", "schedule_audit_ranges", "scaling_events", "queue_events", "rate_grants", "management_audit",
        "worker_instances", "cleanup_progress",
    };

    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static void Register(RunwickBuilder r)
    {
        TestJobs.Register(r);
        r.AddJob<WindowJob>().Named("test.window").ToQueue("default").UniqueWithinWindow(j => j.Key, TimeSpan.FromMinutes(10));
        r.AddJob<RatedJob>().Named("test.rated").ToQueue("default").RollingWindow(100, TimeSpan.FromMinutes(1));
    }

    static Task<TestClient> OpenAsync(DriverFixture f) => f.OpenClient(configure: Register);

    /// <summary>Dispatches, runs and finishes one job; failures carry an error text that detail expiry must remove.</summary>
    static async Task<Guid> TerminalAsync(TestClient c, IQueueJob job, AttemptOutcome outcome = AttemptOutcome.PermanentFailure)
    {
        var id = (await c.Dispatcher.DispatchAsync(job)).JobId.Value;
        var lease = await RunAsync(c, id);
        var error = outcome == AttemptOutcome.Succeeded ? null : new AttemptError("boom", "System.Exception", "secret detail", null);
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(lease, outcome, error: error)).Status);
        return id;
    }

    static Task AgeAsync(DriverFixture f, TimeSpan by, Guid? job = null) =>
        f.ExecAsync("UPDATE runwick.jobs SET terminal_at = terminal_at - @by WHERE terminal_at IS NOT NULL AND (@j = '' OR job_id::text = @j)",
            ("by", by), ("j", job?.ToString() ?? ""));

    static async Task<CleanupBatchResult> BatchAsync(IRetentionStore store, string application, string recordClass, CleanupLimits? limits = null)
    {
        var r = await store.RunCleanupBatchAsync(application, recordClass, limits ?? Limits, CancellationToken.None);
        Assert.True(r.Status != "failed", $"{recordClass}: {r.ErrorCode}");
        return r;
    }

    /// <summary>Batches until the class's pass completes; returns every batch's result.</summary>
    static async Task<List<CleanupBatchResult>> PassAsync(IRetentionStore store, string application, string recordClass, CleanupLimits? limits = null)
    {
        var results = new List<CleanupBatchResult>();
        for (var i = 0; i < 100_000; i++)
        {
            var r = await BatchAsync(store, application, recordClass, limits);
            results.Add(r);
            if (r.PassComplete)
            {
                return results;
            }
        }

        throw new InvalidOperationException("pass did not complete");
    }

    static async Task FullPassAsync(TestClient c)
    {
        foreach (var recordClass in RecordClasses.All)
        {
            await PassAsync(c.Store, c.Application, recordClass);
        }
    }

    static async Task<JobPosition> PositionAsync(DriverFixture f, Guid jobId)
    {
        var row = (await TextAsync(f, "SELECT generation || '/' || revision || '/' || state FROM runwick.jobs WHERE job_id = @j", ("j", jobId))).Single().Split('/');
        return new JobPosition(jobId, int.Parse(row[0], System.Globalization.CultureInfo.InvariantCulture),
            long.Parse(row[1], System.Globalization.CultureInfo.InvariantCulture), row[2]);
    }

    static async Task<StoreReply<ControlledJob>> RetryAsync(DriverFixture f, TestClient c, Guid job, Guid? commandId = null)
    {
        var p = await PositionAsync(f, job);
        return await c.Store.ManualRetryAsync(c.Command(new RetryRequest(job, p.Generation, p.Revision, Actor), commandId), CancellationToken.None);
    }

    static Task<long> Payloads(DriverFixture f, Guid job) => f.CountAsync("SELECT count(*) FROM runwick.job_payloads WHERE job_id = @j", ("j", job));

    static Task<long> Exists(DriverFixture f, Guid job) => f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE job_id = @j", ("j", job));

    static Task<long> Expired(DriverFixture f, Guid job) =>
        f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE job_id = @j AND detail_expired_at IS NOT NULL AND NOT payload_retained", ("j", job));

    // ------------------------------------------------------------------ RET-01

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-021")]
    [Trait("Case", "RET-01")]
    public async Task Ret01a_a_live_job_is_never_selected_and_its_lease_and_finish_are_unaffected()
    {
        await using var f = await DriverFixture.StartAsync(Ns("ret01a"));
        var c = await OpenAsync(f);
        var old = await TerminalAsync(c, new PingJob("old"));
        await AgeAsync(f, TimeSpan.FromDays(200));
        var live = (await c.Dispatcher.DispatchAsync(new PingJob("live"))).JobId.Value;
        var lease = await RunAsync(c, live);
        await f.ExecAsync("UPDATE runwick.jobs SET created_at = created_at - interval '200 days' WHERE job_id = @j", ("j", live));

        // The batch holds the old job's row lock at its first deletion; the live job renews and finishes meanwhile.
        var barrier = f.ArmFault(FaultPoint.BeforeFirstMutation, FaultSpec.Barrier, FaultTarget.Op(Operations.CleanupBatch));
        var batch = BatchAsync(c.Store, c.Application, RecordClasses.JobDetailFailed);
        await DriverFixture.WaitAtBarrier(barrier);
        Assert.Equal(StoreStatus.Applied, (await WithTimeout(c.RenewAsync(lease))).Status);
        Assert.Equal(StoreStatus.Applied, (await WithTimeout(c.FinishAsync(lease, AttemptOutcome.Succeeded))).Status);
        DriverFixture.ReleaseBarrier(barrier);
        Assert.Equal(1, (await WithTimeout(batch)).Rows);

        await FullPassAsync(c);
        await FullPassAsync(c);
        Assert.Equal(0, await Exists(f, old));
        Assert.Equal((1, 1), (await Exists(f, live), await Payloads(f, live)));
        Assert.Equal("succeeded", (await PositionAsync(f, live)).State);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-021")]
    [Trait("Case", "RET-01")]
    public async Task Ret01b_manual_retry_before_and_after_detail_expiry_gets_the_ordered_answer()
    {
        await using var f = await DriverFixture.StartAsync(Ns("ret01b"));
        var c = await OpenAsync(f);
        var before = await TerminalAsync(c, new PingJob("before"));
        var after = await TerminalAsync(c, new PingJob("after"));
        var raced = await TerminalAsync(c, new PingJob("raced"));
        await AgeAsync(f, TimeSpan.FromDays(31));

        // Retry first: the job is no longer terminal, so cleanup does not select it and the payload stays.
        Assert.Equal(StoreStatus.Applied, (await RetryAsync(f, c, before)).Status);

        // Cleanup holds the raced job's row lock; the retry waits for it and then sees the expired detail.
        var barrier = f.ArmFault(FaultPoint.BeforeFirstMutation, FaultSpec.Barrier, FaultTarget.Op(Operations.CleanupBatch));
        var batch = BatchAsync(c.Store, c.Application, RecordClasses.JobDetailFailed);
        await DriverFixture.WaitAtBarrier(barrier);
        var p = await PositionAsync(f, raced);
        var retry = c.Store.ManualRetryAsync(c.Command(new RetryRequest(raced, p.Generation, p.Revision, Actor)), CancellationToken.None);
        await Task.Delay(200);
        Assert.False(retry.IsCompleted, "the retry should wait for the cleanup's row lock");
        DriverFixture.ReleaseBarrier(barrier);
        await WithTimeout(batch);
        var racedReply = await WithTimeout(retry);
        Assert.Equal((StoreStatus.HistoryUnavailable, "payload_unavailable"), (racedReply.Status, racedReply.Error?.Code));

        await PassAsync(c.Store, c.Application, RecordClasses.JobDetailFailed);
        var afterReply = await RetryAsync(f, c, after);
        Assert.Equal((StoreStatus.HistoryUnavailable, "payload_unavailable"), (afterReply.Status, afterReply.Error?.Code));

        Assert.Equal((1, 0L), (await Payloads(f, before), await Expired(f, before)));
        Assert.Equal((0, 1L), (await Payloads(f, after), await Expired(f, after)));
        Assert.Equal("failed", (await PositionAsync(f, after)).State);

        // Error codes stay; error text goes.
        Assert.Equal(["boom|true"], await TextAsync(f, "SELECT (error ->> 'code') || '|' || (error ->> 'expired') FROM runwick.job_attempts WHERE job_id = @j",
            ("j", after)));
        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-021")]
    [Trait("Case", "RET-01")]
    public async Task Ret01d_a_receipt_still_inside_retain_until_survives_and_replays()
    {
        await using var f = await DriverFixture.StartAsync(Ns("ret01d"));
        var c = await OpenAsync(f);
        var a = await TerminalAsync(c, new PingJob("a"));
        var b = await TerminalAsync(c, new PingJob("b"));
        var gone = Guid.CreateVersion7();
        Assert.Equal(StoreStatus.Applied, (await RetryAsync(f, c, a, gone)).Status);
        var p = await PositionAsync(f, b);
        var keptCommand = c.Command(new RetryRequest(b, p.Generation, p.Revision, Actor));
        var kept = keptCommand.CommandId;
        Assert.Equal(StoreStatus.Applied, (await c.Store.ManualRetryAsync(keptCommand, CancellationToken.None)).Status);
        // The gone receipt ends 1 ms ago (its deadline moved with it, as the retain check requires); the kept one is untouched.
        await f.ExecAsync("""
            UPDATE runwick.storage_commands SET not_after = not_after - d.shift, retain_until = retain_until - d.shift
            FROM (SELECT retain_until - (runwick.ms_floor(clock_timestamp()) - interval '1 ms') AS shift
                  FROM runwick.storage_commands WHERE command_id = @gone) AS d
            WHERE command_id = @gone
            """, ("gone", gone));

        await PassAsync(c.Store, c.Application, RecordClasses.ReceiptsDurable);

        Assert.Equal((0, 1), (await Receipts(f, gone), await Receipts(f, kept)));
        var replay = await c.Store.ManualRetryAsync(keptCommand, CancellationToken.None);
        Assert.Equal((StoreStatus.Applied, true), (replay.Status, replay.Replayed));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-021")]
    [Trait("Case", "RET-01")]
    public async Task Ret01e_expired_window_keys_race_dispatch_without_a_duplicate_job()
    {
        await using var f = await DriverFixture.StartAsync(Ns("ret01e"));
        var c = await OpenAsync(f);
        for (var round = 0; round < 20; round++)
        {
            var first = (await c.Dispatcher.DispatchAsync(new WindowJob("a" + round, "k" + round))).JobId.Value;
            await f.ExecAsync("""
                UPDATE runwick.dispatch_keys SET expires_at = runwick.ms_floor(clock_timestamp()) - interval '1 ms'
                WHERE key_kind = 'window' AND key = @k
                """, ("k", "k" + round));
            var dispatch = c.Dispatcher.DispatchAsync(new WindowJob("b" + round, "k" + round));
            var cleanup = BatchAsync(c.Store, c.Application, RecordClasses.DispatchKeys);
            var second = (await WithTimeout(dispatch)).JobId.Value;
            await WithTimeout(cleanup);
            Assert.NotEqual(first, second);

            // The new dispatch either re-bound the row or made a fresh one after cleanup: one key row, bound to it.
            Assert.Equal([second.ToString()], await TextAsync(f, "SELECT bound_job_id::text FROM runwick.dispatch_keys WHERE key_kind = 'window' AND key = @k", ("k", "k" + round)));
            var again = await c.Dispatcher.DispatchAsync(new WindowJob("b" + round, "k" + round));
            Assert.Equal(second, again.JobId.Value);
        }
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-021")]
    [Trait("Case", "RET-01")]
    public async Task Ret01f_a_terminal_job_with_a_live_window_binding_or_a_permit_is_never_purged()
    {
        await using var f = await DriverFixture.StartAsync(Ns("ret01f"));
        var c = await OpenAsync(f);
        var windowed = await TerminalAsync(c, new WindowJob("w", "k"), AttemptOutcome.Succeeded);
        var permitted = await TerminalAsync(c, new ResourceJob("r", "res"), AttemptOutcome.Succeeded);
        await f.ExecAsync("""
            INSERT INTO runwick.concurrency_permits (application, scope_id, attempt_id, job_id, ownership_token, lease_expires_at)
            SELECT q.application, q.scope_id, a.attempt_id, q.job_id, gen_random_uuid(), runwick.ms_floor(clock_timestamp())
            FROM runwick.job_concurrency_requirements AS q JOIN runwick.job_attempts AS a USING (application, job_id)
            WHERE q.job_id = @j
            """, ("j", permitted));
        await AgeAsync(f, TimeSpan.FromDays(200));

        await FullPassAsync(c);
        await FullPassAsync(c);
        Assert.Equal((1, 1L), (await Exists(f, windowed), await Expired(f, windowed)));
        Assert.Equal((1, 1L), (await Exists(f, permitted), await Expired(f, permitted)));

        // Binding expired and permit gone: both purge.
        await f.ExecAsync("UPDATE runwick.dispatch_keys SET expires_at = runwick.ms_floor(clock_timestamp()) - interval '1 ms'");
        await f.ExecAsync("DELETE FROM runwick.concurrency_permits");
        await FullPassAsync(c);
        Assert.Equal((0, 0), (await Exists(f, windowed), await Exists(f, permitted)));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.dispatch_keys"));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-021")]
    [Trait("Case", "RET-01")]
    public async Task Ret01g_fences_and_scope_rows_are_unchanged_after_a_full_pass_of_every_class()
    {
        await using var f = await DriverFixture.StartAsync(Ns("ret01g"));
        var c = await OpenAsync(f);
        await TerminalAsync(c, new PingJob("p"));
        await TerminalAsync(c, new ResourceJob("r", "res"), AttemptOutcome.Succeeded);
        await TerminalAsync(c, new KeyedJob("k", "key"), AttemptOutcome.Succeeded);
        await c.Dispatcher.DispatchAsync(new PingJob("pending"));
        await AgeAsync(f, TimeSpan.FromDays(400));
        await f.ExecAsync("UPDATE runwick.storage_commands SET not_after = not_after - interval '400 days', retain_until = retain_until - interval '400 days'");
        var before = await f.ReadCommittedSnapshot();

        await FullPassAsync(c);
        await FullPassAsync(c);

        var changed = before.DiffTables(await f.ReadCommittedSnapshot());
        Assert.DoesNotContain(changed, t => !Deletable.Contains(t));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.jobs"));
        Assert.Empty(await f.HistoryViolations());
    }

    // ------------------------------------------------------------------ OPS-02

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-021")]
    [Trait("Case", "OPS-02")]
    public async Task Ops02_retries_racing_cleanup_never_retry_a_job_without_its_payload()
    {
        const int N = 200;
        await using var f = await DriverFixture.StartAsync(Ns("ops02"));
        var c = await OpenAsync(f);
        var jobs = new List<Guid>();
        for (var i = 0; i < N; i++)
        {
            jobs.Add(await TerminalAsync(c, new PingJob("j" + i)));
        }

        // At the 30 d edge: some just past it, some just inside.
        await AgeAsync(f, TimeSpan.FromDays(30) - TimeSpan.FromSeconds(1));
        await f.ExecAsync("UPDATE runwick.jobs SET terminal_at = terminal_at - interval '2 seconds' WHERE (get_byte(uuid_send(job_id), 15) % 2) = 0");

        var queue = new System.Collections.Concurrent.ConcurrentQueue<Guid>(jobs);
        var replies = new System.Collections.Concurrent.ConcurrentDictionary<Guid, StoreStatus>();
        var cleanup = Task.Run(async () =>
        {
            while (!queue.IsEmpty)
            {
                await BatchAsync(c.Store, c.Application, RecordClasses.JobDetailFailed, new CleanupLimits(10, 1 << 20, TimeSpan.FromSeconds(5)));
            }
        });
        var retries = Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
        {
            while (queue.TryDequeue(out var job))
            {
                var r = await RetryAsync(f, c, job);
                Assert.Contains(r.Status, new[] { StoreStatus.Applied, StoreStatus.HistoryUnavailable });
                replies[job] = r.Status;
            }
        })).ToList();
        await WithTimeout(Task.WhenAll([.. retries, cleanup]));
        await PassAsync(c.Store, c.Application, RecordClasses.JobDetailFailed);

        foreach (var job in jobs)
        {
            var state = (await PositionAsync(f, job)).State;
            if (replies[job] == StoreStatus.Applied)
            {
                Assert.NotEqual("failed", state);
                Assert.Equal((1, 0L), (await Payloads(f, job), await Expired(f, job)));
            }
            else
            {
                Assert.Equal(("failed", 1L), (state, await Expired(f, job)));
            }
        }

        output.WriteLine($"retried {replies.Values.Count(s => s == StoreStatus.Applied)}, expired first {replies.Values.Count(s => s != StoreStatus.Applied)}");
        Assert.Empty(await f.HistoryViolations());
    }

    // ------------------------------------------------------------------ OPS-04 / OBS-10

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-021")]
    [Trait("Case", "OPS-04")]
    [Trait("Case", "OBS-10")]
    public async Task Ops04_the_minimum_policy_never_deletes_fixed_records_still_in_force()
    {
        await using var f = await DriverFixture.StartAsync(Ns("ops04"));
        var c = await OpenAsync(f);
        Assert.Equal(StoreStatus.Applied, (await c.Store.ActivateCapabilitiesAsync(
            c.Command(new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["rate_admission"] = 1 }, Actor)), CancellationToken.None)).Status);

        var minimum = new RetentionPolicy(TimeSpan.FromHours(1), TimeSpan.FromHours(1), TimeSpan.FromHours(1), TimeSpan.FromDays(7), TimeSpan.FromHours(1),
            TimeSpan.FromHours(1), TimeSpan.FromHours(1));
        var refused = await c.Store.EditRetentionPolicyAsync(c.Command(new RetentionPolicyRequest(
            minimum with { JobHistory = TimeSpan.FromMinutes(30) }, 0, Actor)), CancellationToken.None);
        Assert.Equal((StoreStatus.InvalidRequest, "invalid_retention_policy"), (refused.Status, refused.Error?.Code));
        Assert.Equal(StoreStatus.Applied, (await c.Store.EditRetentionPolicyAsync(c.Command(new RetentionPolicyRequest(minimum, 0, Actor)),
            CancellationToken.None)).Status);

        var plain = await TerminalAsync(c, new PingJob("plain"));
        var windowed = await TerminalAsync(c, new WindowJob("w", "k"), AttemptOutcome.Succeeded);
        var rated = await TerminalAsync(c, new RatedJob("r"), AttemptOutcome.Succeeded);
        var active = (await c.Dispatcher.DispatchAsync(new KeyedJob("active", "key"))).JobId.Value;
        await AgeAsync(f, TimeSpan.FromHours(2));
        var receipts = await f.CountAsync("SELECT count(*) FROM runwick.storage_commands WHERE retain_until > clock_timestamp() + interval '1 minute'");
        var before = await f.ReadCommittedSnapshot();

        // OBS-10: many passes over the same fixture.
        for (var pass = 0; pass < 5; pass++)
        {
            await FullPassAsync(c);
        }

        Assert.Equal(0, await Exists(f, plain));
        Assert.Equal(0, await Exists(f, rated));
        Assert.Equal((1, 1L), (await Exists(f, windowed), await Expired(f, windowed)));
        Assert.Equal(1, await Exists(f, active));
        Assert.Equal(receipts, await f.CountAsync("SELECT count(*) FROM runwick.storage_commands WHERE retain_until > clock_timestamp()"));
        Assert.Equal(2, await f.CountAsync("SELECT count(*) FROM runwick.dispatch_keys"));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.rate_grants"));
        Assert.DoesNotContain(before.DiffTables(await f.ReadCommittedSnapshot()), t => !Deletable.Contains(t));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE operation = 'EditRetentionPolicy'"));
    }

    // ------------------------------------------------------------------ OPS-05 (scaled)

    [Fact]
    [Trait("Category", "Load")]
    [Trait("Item", "SP-021")]
    [Trait("Case", "OPS-05")]
    public async Task Ops05_a_large_backlog_drains_in_bounded_batches_and_resumes_from_its_cursor()
    {
        const int Seeded = 5_000;
        await using var f = await DriverFixture.StartAsync(Ns("ops05"));
        var c = await OpenAsync(f);
        var template = await TerminalAsync(c, new PingJob(new string('x', 4000)));
        await f.ExecAsync("""
            CREATE TEMP TABLE ids AS SELECT gen_random_uuid() AS job_id FROM generate_series(2, @n);
            INSERT INTO runwick.jobs SELECT (jsonb_populate_record(NULL::runwick.jobs, to_jsonb(j) || jsonb_build_object('job_id', i.job_id))).*
            FROM runwick.jobs AS j, ids AS i WHERE j.job_id = @t;
            INSERT INTO runwick.job_payloads SELECT (jsonb_populate_record(NULL::runwick.job_payloads, to_jsonb(p) || jsonb_build_object('job_id', i.job_id))).*
            FROM runwick.job_payloads AS p, ids AS i WHERE p.job_id = @t;
            INSERT INTO runwick.job_generations SELECT (jsonb_populate_record(NULL::runwick.job_generations, to_jsonb(g) || jsonb_build_object('job_id', i.job_id))).*
            FROM runwick.job_generations AS g, ids AS i WHERE g.job_id = @t;
            INSERT INTO runwick.job_attempts SELECT (jsonb_populate_record(NULL::runwick.job_attempts,
                to_jsonb(a) || jsonb_build_object('job_id', i.job_id, 'attempt_id', gen_random_uuid()))).*
            FROM runwick.job_attempts AS a, ids AS i WHERE a.job_id = @t;
            """, ("n", Seeded), ("t", template));
        await AgeAsync(f, TimeSpan.FromDays(200));
        var bytes = await f.CountAsync("SELECT octet_length(payload_utf8) FROM runwick.job_payloads WHERE job_id = @t", ("t", template));

        // Live work during cleanup.
        using var stop = new CancellationTokenSource();
        var finishes = new List<double>();
        var load = Task.Run(async () =>
        {
            for (var i = 0; !stop.IsCancellationRequested; i++)
            {
                var sw = Stopwatch.StartNew();
                await TerminalAsync(c, new PingJob("live" + i), AttemptOutcome.Succeeded);
                finishes.Add(sw.Elapsed.TotalMilliseconds);
                await Task.Delay(20);
            }
        });

        // Three "restarts": each segment uses a new store instance, so only the stored cursor carries over.
        var limits = new CleanupLimits(100, 64 * 1024, TimeSpan.FromSeconds(5));
        var times = new List<double>();
        var results = new List<(string Class, CleanupBatchResult Result)>();
        IRetentionStore store = c.Store;
        foreach (var recordClass in new[] { RecordClasses.JobDetailFailed, RecordClasses.JobHistory })
        {
            for (var segment = 0; ; segment++)
            {
                var sw = Stopwatch.StartNew();
                var r = await BatchAsync(store, c.Application, recordClass, limits);
                times.Add(sw.Elapsed.TotalMilliseconds);
                results.Add((recordClass, r));
                if (r.PassComplete)
                {
                    break;
                }

                if (segment % 70 == 69)
                {
                    store = f.NewStore("restart" + segment);
                }
            }
        }

        await stop.CancelAsync();
        await load;
        Assert.All(results, r => Assert.InRange(r.Result.Rows, 0, 100));
        Assert.All(results.Where(r => r.Class == RecordClasses.JobDetailFailed), r => Assert.InRange(r.Result.Rows, 0, Math.Max(1, (int)(64 * 1024 / bytes))));
        Assert.Equal(0, await f.CountAsync("""
            SELECT count(*) FROM runwick.jobs WHERE state IN ('succeeded', 'failed', 'cancelled') AND terminal_at < clock_timestamp() - interval '90 days'
            """));
        times.Sort();
        finishes.Sort();
        output.WriteLine($"batches {times.Count}: p50 {times[times.Count / 2]:F1} ms, p95 {times[times.Count * 95 / 100]:F1} ms, max {times[^1]:F1} ms; "
            + $"live dispatch-run-finish p99 {finishes[finishes.Count * 99 / 100]:F1} ms over {finishes.Count}");
    }

    // ------------------------------------------------------------------ CLN

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-021")]
    [Trait("Case", "CLN-01")]
    public async Task Cln01_two_processes_share_a_class_one_batch_at_a_time()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cln01"));
        var c = await OpenAsync(f);
        for (var i = 0; i < 60; i++)
        {
            await TerminalAsync(c, new PingJob("j" + i));
        }

        await AgeAsync(f, TimeSpan.FromDays(31));
        var other = f.NewStore("other");

        var barrier = f.ArmFault(FaultPoint.BeforeFirstMutation, FaultSpec.Barrier, FaultTarget.Op(Operations.CleanupBatch));
        var held = BatchAsync(c.Store, c.Application, RecordClasses.JobDetailFailed, new CleanupLimits(10, 1 << 20, TimeSpan.FromSeconds(5)));
        await DriverFixture.WaitAtBarrier(barrier);
        Assert.Equal("skipped_locked", (await BatchAsync(other, c.Application, RecordClasses.JobDetailFailed)).Status);
        DriverFixture.ReleaseBarrier(barrier);
        Assert.Equal(10, (await WithTimeout(held)).Rows);

        var small = new CleanupLimits(7, 1 << 20, TimeSpan.FromSeconds(5));
        var counted = await Task.WhenAll(new IRetentionStore[] { c.Store, other }.Select(s => Task.Run(async () =>
        {
            var (rows, skipped) = (0, 0);
            for (var i = 0; i < 40; i++)
            {
                var r = await BatchAsync(s, c.Application, RecordClasses.JobDetailFailed, small);
                rows += r.Rows;
                skipped += r.Status == "skipped_locked" ? 1 : 0;
            }

            return (rows, skipped);
        })));

        Assert.Equal(50, counted.Sum(x => x.rows));
        Assert.Equal(60, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE detail_expired_at IS NOT NULL"));
        Assert.Equal(60, await f.CountAsync("SELECT total_rows FROM runwick.cleanup_progress WHERE record_class = 'job_detail_failed'"));
        output.WriteLine($"skipped_locked {counted.Sum(x => x.skipped)}");
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-021")]
    [Trait("Case", "CLN-02")]
    public async Task Cln02_a_closed_day_writes_one_summary_per_class()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cln02"));
        var c = await OpenAsync(f);
        await BatchAsync(c.Store, c.Application, RecordClasses.ScalingEvents);
        await BatchAsync(c.Store, c.Application, RecordClasses.WorkerPresence);
        await f.ExecAsync("""
            UPDATE runwick.cleanup_progress SET summary_day = (clock_timestamp() AT TIME ZONE 'UTC')::date - 1, day_rows = 42, day_batches = 3, day_failures = 1
            """);

        await BatchAsync(c.Store, c.Application, RecordClasses.ScalingEvents);
        await BatchAsync(c.Store, c.Application, RecordClasses.ScalingEvents);
        await BatchAsync(c.Store, c.Application, RecordClasses.WorkerPresence);

        var rows = await TextAsync(f, """
            SELECT target_id || '|' || (details ->> 'rows') || '|' || (details ->> 'batches') || '|' || (details ->> 'failures') || '|' || actor_subject
            FROM runwick.management_audit WHERE operation = 'RetentionSummary' ORDER BY target_id
            """);
        Assert.Equal(["scaling_events|42|3|1|runwick:retention", "worker_presence|42|3|1|runwick:retention"], rows);
        Assert.Equal(2, await f.CountAsync("SELECT day_batches FROM runwick.cleanup_progress WHERE record_class = 'scaling_events'"));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-021")]
    [Trait("Case", "CLN-03")]
    public async Task Cln03_a_failed_batch_rolls_back_records_its_error_and_the_next_resumes_from_the_cursor()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cln03"));
        var c = await OpenAsync(f);
        for (var i = 0; i < 6; i++)
        {
            await TerminalAsync(c, new PingJob("j" + i));
        }

        await AgeAsync(f, TimeSpan.FromDays(31));
        var three = new CleanupLimits(3, 1 << 20, TimeSpan.FromSeconds(5));
        Assert.Equal(3, (await BatchAsync(c.Store, c.Application, RecordClasses.JobDetailFailed, three)).Rows);
        var cursor = await TextAsync(f, "SELECT cursor_key FROM runwick.cleanup_progress WHERE record_class = 'job_detail_failed'");

        // A lock the batch cannot get within its 100 ms lock_timeout fails it.
        await using (var blocker = new NpgsqlConnection(f.AdminConnectionString))
        {
            await blocker.OpenAsync();
            await using var tx = await blocker.BeginTransactionAsync();
            await ExecAsync(blocker, tx, "LOCK TABLE runwick.job_payloads IN ACCESS EXCLUSIVE MODE");
            var failed = await c.Store.RunCleanupBatchAsync(c.Application, RecordClasses.JobDetailFailed, three, CancellationToken.None);
            Assert.Equal(("failed", "lock_timeout"), (failed.Status, failed.ErrorCode));
            await tx.RollbackAsync();
        }

        Assert.Equal(["lock_timeout|1"], await TextAsync(f,
            "SELECT last_error_code || '|' || day_failures FROM runwick.cleanup_progress WHERE record_class = 'job_detail_failed'"));
        Assert.Equal(cursor, await TextAsync(f, "SELECT cursor_key FROM runwick.cleanup_progress WHERE record_class = 'job_detail_failed'"));
        Assert.Equal(3, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE detail_expired_at IS NOT NULL"));

        var next = await BatchAsync(c.Store, c.Application, RecordClasses.JobDetailFailed, three);
        Assert.Equal(3, next.Rows);
        Assert.Equal(6, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE detail_expired_at IS NOT NULL"));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-021")]
    [Trait("Case", "CLN-04")]
    public async Task Cln04_no_batch_starts_once_the_worker_drains()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cln04"));
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults);
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => Interlocked.Read(ref w.Engine.CleanupBatches) >= RecordClasses.All.Count, Wait, "one batch per class");
        await w.StopAsync();
        var after = Interlocked.Read(ref w.Engine.CleanupBatches);
        await Task.Delay(1500);
        Assert.Equal(after, Interlocked.Read(ref w.Engine.CleanupBatches));
        Assert.Equal(0, Interlocked.Read(ref w.Engine.CleanupFailures));
        Assert.Equal(RecordClasses.All.Count, await f.CountAsync("SELECT count(*) FROM runwick.cleanup_progress WHERE last_batch_at IS NOT NULL"));
    }
}
