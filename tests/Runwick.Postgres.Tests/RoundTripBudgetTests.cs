using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Dispatching;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Postgres.Store;
using Runwick.Protocol;
using Runwick.Registration;
using Runwick.Scheduling;
using Runwick.Scheduling.Evaluation;
using Runwick.Serialization;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>One job-type rate scope.</summary>
public sealed record PerfRatedJob(string Id) : IQueueJob;

/// <summary>Eight rate scopes: the job type and seven per-resource scopes.</summary>
public sealed record PerfRated8Job(string Id) : IQueueJob;

/// <summary>
/// PERF-01 (performance plan PF-01): the SQL commands each store operation sends, counted through Npgsql's
/// <c>Npgsql</c> ActivitySource, asserted exactly. One Npgsql command is one round trip (a multi-statement command is
/// one batch on the wire). Transaction control is not traced: COMMIT adds one round trip to every driver-owned
/// operation, and the staged paths' <c>SAVEPOINT</c> and <c>RELEASE</c> add two. A change that adds a round trip fails
/// here, not in a gate run; each PF item lowers the numbers.
/// </summary>
public sealed class RoundTripBudgetTests
{
    static readonly ControlActor Actor = new("user:perf", null, "host", "perf-01");

    static readonly TimeSpan Window = TimeSpan.FromMinutes(5);

    /// <summary>Expected Npgsql commands per operation (COMMIT excluded). Baseline: docs/reports/pf-01.md.</summary>
    static readonly Dictionary<string, int> Expected = new()
    {
        ["Dispatch"] = 4,
        ["Reserve"] = 5,
        ["Begin"] = 4,
        ["Begin rated 1"] = 5,
        ["Begin rated 8"] = 5,
        ["Renew"] = 3,
        ["RenewMany"] = 3,
        ["Finish"] = 4,
        ["Recover"] = 8,
        ["ReleaseReservation"] = 8,
        ["ReadinessProbe"] = 1,
        ["PublishPresence"] = 5,
        ["StageAsync"] = 5,
        ["StageFinishAsync"] = 5,
        ["Cancel"] = 8,
        ["SetQueueState"] = 8,
        ["EditRatePolicy"] = 9,
        ["MaterializeOccurrences"] = 14,
        ["ReceiveOutbox"] = 13,
    };

    static readonly AsyncLocal<List<string>?> Current = new();

    static void Register(RunwickBuilder r)
    {
        TestJobs.Register(r);
        r.AddJob<TickJob>().Named("test.tick").ToQueue("default");
        r.AddJob<PerfRatedJob>().Named("perf.rated").ToQueue("rated").RollingWindow(1_000, Window);
        var eight = r.AddJob<PerfRated8Job>().Named("perf.rated8").ToQueue("rated").RollingWindow(1_000, Window);
        foreach (var resource in new[] { "a", "b", "c", "d", "e", "f", "g" })
        {
            eight = eight.RollingWindowPerResource(j => j.Id + "-" + resource, 1_000, Window);
        }
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "PF-01")]
    [Trait("Case", "PERF-01")]
    public async Task Each_operation_sends_its_budgeted_number_of_commands()
    {
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == "Npgsql",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = a =>
            {
                if (Current.Value is { } list)
                {
                    lock (list)
                    {
                        list.Add(a.GetTagItem("db.query.text") as string ?? a.GetTagItem("db.statement") as string ?? a.DisplayName);
                    }
                }
            },
        };
        ActivitySource.AddActivityListener(listener);

        await using var f = await DriverFixture.StartAsync("perf01_" + Guid.NewGuid().ToString("N")[..8]);
        var c = await f.OpenClient(configure: Register);
        var ct = CancellationToken.None;
        Assert.Equal(StoreStatus.Applied, (await c.Store.ActivateCapabilitiesAsync(c.Command(new ActivateCapabilitiesRequest(
            new Dictionary<string, int> { ["rate_admission"] = 1, ["scheduler"] = 1 }, Actor)), ct)).Status);

        var actual = new Dictionary<string, int>();
        var detail = new Dictionary<string, List<string>>();
        async Task<T> Count<T>(string name, Func<Task<T>> call)
        {
            var list = new List<string>();
            Current.Value = list;
            try
            {
                return await call();
            }
            finally
            {
                Current.Value = null;
                actual[name] = list.Count;
                detail[name] = list;
            }
        }

        // Warm-up: one full cycle so first-use work (pool opens, registry reads) is not counted.
        await c.Dispatcher.DispatchAsync(new PingJob("warm"), cancellationToken: ct);
        var warm = (await c.ReserveAsync()).Value!.Lease.Handle;
        await c.BeginAsync(warm);
        await c.RenewAsync(warm);
        await c.FinishAsync(warm, AttemptOutcome.Succeeded);

        // M2 job path.
        await Count("Dispatch", () => c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: ct));
        var reserved = await Count("Reserve", () => c.ReserveAsync());
        Assert.Equal(StoreStatus.Applied, reserved.Status);
        var lease = reserved.Value!.Lease.Handle;
        Assert.Equal(StoreStatus.Applied, (await Count("Begin", () => c.BeginAsync(lease))).Reply.Status);
        Assert.Equal(StoreStatus.Applied, (await Count("Renew", () => c.RenewAsync(lease))).Status);

        // RenewMany (PF-06): the same three commands for any number of leases.
        await c.Dispatcher.DispatchAsync(new PingJob("b"), cancellationToken: ct);
        var second = (await c.ReserveAsync()).Value!.Lease.Handle;
        var many = await Count("RenewMany", () => c.RenewManyAsync([lease, second]));
        Assert.Equal(StoreStatus.Applied, many.Status);
        Assert.All(many.Value!.Outcomes, o => Assert.Equal(StoreStatus.Applied, o.Status));
        Assert.Equal(StoreStatus.Applied, (await c.ReleaseAsync(second)).Status);
        Assert.Equal(StoreStatus.Applied, (await Count("Finish", () => c.FinishAsync(lease, AttemptOutcome.Succeeded))).Status);

        // Rated Begin: one scope and eight scopes.
        foreach (var (name, dispatch) in new (string, Func<Task>)[]
        {
            ("Begin rated 1", () => c.Dispatcher.DispatchAsync(new PerfRatedJob("r1"), cancellationToken: ct)),
            ("Begin rated 8", () => c.Dispatcher.DispatchAsync(new PerfRated8Job("r8"), cancellationToken: ct)),
        })
        {
            await dispatch();
            var rated = (await c.ReserveAsync(queue: "rated")).Value!.Lease.Handle;
            Assert.Equal(StoreStatus.Applied, (await Count(name, () => c.BeginAsync(rated))).Reply.Status);
            Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(rated, AttemptOutcome.Succeeded)).Status);
        }

        // ReleaseReservation, then Recover of an expired lease.
        await c.Dispatcher.DispatchAsync(new PingJob("release"), cancellationToken: ct);
        var toRelease = (await c.ReserveAsync()).Value!.Lease.Handle;
        Assert.Equal(StoreStatus.Applied, (await Count("ReleaseReservation", () => c.ReleaseAsync(toRelease))).Status);
        var toExpire = (await c.ReserveAsync(lease: TimeSpan.FromSeconds(1))).Value!.Lease.Handle;
        await c.BeginAsync(toExpire);
        ExpiredLease? expired = null;
        for (var i = 0; i < 100 && expired is null; i++)
        {
            await Task.Delay(100, ct);
            expired = (await c.Store.ListExpiredLeasesAsync(c.Application, 10, ct)).SingleOrDefault();
        }

        Assert.Equal(StoreStatus.Applied, (await Count("Recover", () => c.RecoverAsync(expired!))).Status);

        // Readiness and presence.
        await Count("ReadinessProbe", () => c.Store.ProbeAsync(c.Application, [new LaneKey("default", "medium")], c.Contracts, ct));
        Assert.True((await Count("PublishPresence", () => c.Store.PublishPresenceAsync(c.Application, new PresenceRecord(c.WorkerInstanceId, 1, 1, "ready",
            Environment.ProcessId, "host", "build", ["default"], new Dictionary<string, int>(), c.Contracts, TimeSpan.FromSeconds(30)), ct))).Accepted);

        // Staged paths on a caller's transaction.
        await using (var conn = await OpenBusinessAsync(f))
        {
            await using (var tx = await conn.BeginTransactionAsync(ct))
            {
                var staged = await Count("StageAsync", () => Staging(c).StageAsync(new PingJob("staged"), tx, cancellationToken: ct));
                Assert.Equal(DispatchDisposition.Created, staged.Disposition);
                await tx.CommitAsync(ct);
            }

            var stagedLease = (await c.ReserveAsync()).Value!.Lease.Handle;
            await c.BeginAsync(stagedLease);
            await using (var tx = await conn.BeginTransactionAsync(ct))
            {
                var finish = c.Command(new FinishRequest(stagedLease, AttemptOutcome.Succeeded, TimeSpan.FromMilliseconds(5), null, null));
                Assert.Equal(StoreStatus.Applied, (await Count("StageFinishAsync", () => c.Store.StageFinishAsync(tx, finish, ct))).Status);
                await tx.CommitAsync(ct);
            }
        }

        // Controls and rate policy.
        var pending = (await c.Dispatcher.DispatchAsync(new PingJob("cancel"), cancellationToken: ct)).JobId.Value;
        var position = (await TextAsync(f, "SELECT generation || '/' || revision FROM runwick.jobs WHERE job_id = @j", ("j", pending))).Single().Split('/');
        Assert.Equal(StoreStatus.Applied, (await Count("Cancel", () => c.Store.CancelAsync(c.Command(new CancelRequest(pending,
            int.Parse(position[0], System.Globalization.CultureInfo.InvariantCulture), long.Parse(position[1], System.Globalization.CultureInfo.InvariantCulture), Actor)), ct))).Status);
        var queueRevision = await f.CountAsync("SELECT revision FROM runwick.queue_controls WHERE queue = 'default'");
        Assert.Equal(StoreStatus.Applied, (await Count("SetQueueState", () => c.Store.SetQueueStateAsync(
            c.Command(new QueueStateRequest("default", true, queueRevision, Actor)), ct))).Status);
        var policyRevision = await f.CountAsync("SELECT policy_revision FROM runwick.rate_scopes WHERE scope_kind = 'job-type' AND scope_key = 'perf.rated'");
        Assert.Equal(StoreStatus.Applied, (await Count("EditRatePolicy", () => c.Store.EditRatePolicyAsync(
            c.Command(new RatePolicyRequest("job-type", "perf.rated", 2_000, (long)Window.TotalMilliseconds, policyRevision, Actor)), ct))).Status);

        // Schedule materialization of one missed minute.
        var preparer = c.Services.GetRequiredService<DispatchPreparer>();
        var definition = ScheduleCompiler.Compile(new ScheduleDeclaration("tick", new TickJob("tick")) { Cron = "* * * * *" }, preparer, TzdbArtifact.Pinned);
        var cutoff = await c.Store.StoreNowAsync(ct);
        var first = ScheduleRecurrence.For(definition, TzdbArtifact.Pinned, TimeProvider.System)(cutoff)?.At;
        Assert.Equal(StoreStatus.Applied, (await c.Store.RegisterScheduleAsync(c.Command(new RegisterScheduleRequest("tick", definition, cutoff, first)), ct)).Status);
        await f.ExecAsync("""
            UPDATE runwick.schedules
            SET evaluated_through = date_trunc('minute', clock_timestamp()) - interval '1 minute',
                next_due_at = date_trunc('minute', clock_timestamp()),
                scan_after_at = clock_timestamp() - interval '1 second'
            WHERE schedule_key = 'tick'
            """);
        var schedule = (await c.Store.ReadScheduleAsync(c.Application, "tick", ct))!;
        var now = await c.Store.StoreNowAsync(ct);
        var plan = SchedulePlanner.Plan(schedule, now, ScheduleRecurrence.For(schedule.Definition, TzdbArtifact.Pinned, TimeProvider.System), Guid.CreateVersion7)!;
        Assert.Equal(StoreStatus.Applied, (await Count("MaterializeOccurrences", () => c.Store.MaterializeOccurrencesAsync(c.Command(plan), ct))).Status);

        // Outbox delivery.
        var prepared = preparer.Prepare(c.Runtime.StoreId, new PingJob("outbox"), null);
        var acceptUntil = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.AddDays(1).ToUnixTimeMilliseconds());
        var identity = new OutboxSourceIdentity(Guid.NewGuid(), Guid.CreateVersion7());
        var delivery = OutboxFingerprints.Delivery(prepared.StoreId, prepared.Application, identity, acceptUntil.ToUnixTimeMilliseconds(), prepared.Intent, prepared.Digests);
        var receive = new StoreCommand<ReceiveOutboxRequest>(c.Application, Guid.CreateVersion7(),
            DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.AddSeconds(20).ToUnixTimeMilliseconds()),
            new ReceiveOutboxRequest(identity, new DispatchItem(prepared.Intent, prepared.Scopes, prepared.Digests), acceptUntil, delivery, prepared.StoreId));
        Assert.Equal(StoreStatus.Applied, (await Count("ReceiveOutbox", () => c.Store.ReceiveOutboxAsync(receive, ct))).Status);

        var output = TestContext.Current.TestOutputHelper;
        foreach (var (name, n) in actual)
        {
            output?.WriteLine($"{name}: {n}");
            foreach (var sql in detail[name])
            {
                output?.WriteLine("    " + string.Join(' ', sql.Split((char[])['\n', '\r', ' '], StringSplitOptions.RemoveEmptyEntries)));
            }
        }

        var table = string.Join(", ", actual.Select(kv => $"{kv.Key}={kv.Value}"));
        foreach (var (name, n) in Expected)
        {
            Assert.True(actual.TryGetValue(name, out var got) && got == n, $"{name}: expected {n}, sent {got}. All: {table}");
        }

        Assert.Empty(await f.HistoryViolations());
    }
}
