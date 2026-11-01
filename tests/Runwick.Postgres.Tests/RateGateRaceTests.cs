using Npgsql;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>
/// PGX-01 at schema revision 7 (04 §7.15, postgres-schema-and-operations §7): application-gate changes race every
/// command family, including the rate-aware Begin. Each in-flight transaction either completes under the old gate
/// before the change commits, or observes the new gate before any mutation.
/// </summary>
public sealed class RateGateRaceTests
{
    /// <summary>An uncommitted 04 §7.15 gate change on the admin connection (FOR UPDATE on the application row, then UPDATE).</summary>
    sealed class GateChange(NpgsqlConnection conn, NpgsqlTransaction tx) : IAsyncDisposable
    {
        public static async Task<GateChange> StartAsync(DriverFixture f, string set)
        {
            var conn = new NpgsqlConnection(f.AdminConnectionString);
            await conn.OpenAsync();
            var tx = await conn.BeginTransactionAsync();
            foreach (var sql in new[]
            {
                "SELECT revision FROM runwick.store_metadata WHERE singleton_id = 1 FOR SHARE",
                "SELECT revision FROM runwick.application_controls WHERE application = @a FOR UPDATE",
                $"UPDATE runwick.application_controls SET {set}, revision = revision + 1, updated_at = runwick.ms_floor(clock_timestamp()) WHERE application = @a",
            })
            {
                using var cmd = new NpgsqlCommand(sql, conn, tx);
                cmd.Parameters.AddWithValue("a", f.Application);
                await cmd.ExecuteNonQueryAsync();
            }

            return new GateChange(conn, tx);
        }

        public Task CommitAsync() => tx.CommitAsync();

        public async ValueTask DisposeAsync()
        {
            await tx.DisposeAsync();
            await conn.DisposeAsync();
        }
    }

    const string Drain = "admission_mode = 'drain'";
    const string Maintenance = "admission_mode = 'maintenance'";
    const string Capability = "required_capabilities = required_capabilities || '{\"teleport\": 1}'::jsonb";
    const string Reset = "admission_mode = 'open', required_capabilities = required_capabilities - 'teleport'";

    static readonly string[] Settlement = ["Renew", "Finish", "Recover", "ReleaseReservation", "PublishPresence"];

    /// <summary>Rows any family could write; a command that observed the new gate must leave this unchanged.</summary>
    static Task<long> Footprint(DriverFixture f) => f.CountAsync("""
        SELECT (SELECT count(*) FROM runwick.jobs) + (SELECT coalesce(sum(revision), 0) FROM runwick.jobs)
             + (SELECT count(*) FROM runwick.job_attempts) + (SELECT count(*) FROM runwick.storage_commands)
             + (SELECT count(*) FROM runwick.worker_instances) + (SELECT coalesce(sum(presence_sequence), 0) FROM runwick.worker_instances)
             + (SELECT count(*) FROM runwick.queue_controls) + (SELECT count(*) FROM runwick.rate_grants)
        """);

    /// <summary>One family's command, prepared under the open gate. <c>Run</c> returns true when it applied.</summary>
    sealed record Family(string Operation, Func<Task<bool>> Run);

    static async Task<Family> PrepareAsync(DriverFixture f, TestClient c, string family, int n)
    {
        static bool Ok<T>(StoreReply<T> r) where T : class => r.Status == StoreStatus.Applied;
        async Task<bool> NotRejected(Func<Task> call)
        {
            try
            {
                await call();
                return true;
            }
            catch (RunwickException)
            {
                return false;
            }
        }

        switch (family)
        {
            case "Dispatch":
                return new(Operations.Dispatch, () => NotRejected(() => c.Dispatcher.DispatchAsync(new PingJob($"d{n}"))));
            case "DispatchMany":
                return new(Operations.DispatchMany, () => NotRejected(() => c.Dispatcher.DispatchManyAsync(
                    [new BatchDispatchItem(new PingJob($"m{n}a")), new BatchDispatchItem(new PingJob($"m{n}b"))])));
            case "Reserve":
                await c.Dispatcher.DispatchAsync(new PingJob($"r{n}"));
                return new(Operations.Reserve, async () => Ok(await c.ReserveAsync()));
            case "Begin":
            {
                // The rate-aware Begin (10 §4) of a rated job.
                await c.Dispatcher.DispatchAsync(new RatedReplicaJob($"b{n}", $"res{n}"));
                var lease = await RateSupport.ReserveAsync(c);
                return new(Operations.Begin, async () => Ok((await c.BeginAsync(lease)).Reply));
            }

            case "Renew":
            {
                await c.Dispatcher.DispatchAsync(new PingJob($"n{n}"));
                var lease = await RunAsync(c);
                return new(Operations.Renew, async () => Ok(await c.RenewAsync(lease)));
            }

            case "Finish":
            {
                await c.Dispatcher.DispatchAsync(new PingJob($"f{n}"));
                var lease = await RunAsync(c);
                return new(Operations.Finish, async () => Ok(await c.FinishAsync(lease, AttemptOutcome.Succeeded)));
            }

            case "Recover":
            {
                await c.Dispatcher.DispatchAsync(new PingJob($"v{n}"));
                var job = (await RateSupport.ReserveAsync(c, TimeSpan.FromSeconds(1))).JobId;
                ExpiredLease? expired = null;
                var deadline = DateTime.UtcNow + Wait;
                while (expired is null)
                {
                    Assert.True(DateTime.UtcNow < deadline, "lease never expired");
                    await Task.Delay(100);
                    expired = (await c.Store.ListExpiredLeasesAsync(c.Application, 10, CancellationToken.None)).SingleOrDefault(e => e.JobId == job);
                }

                return new(Operations.Recover, async () => Ok(await c.RecoverAsync(expired)));
            }

            case "ReleaseReservation":
            {
                await c.Dispatcher.DispatchAsync(new PingJob($"l{n}"));
                var lease = await RateSupport.ReserveAsync(c);
                return new(Operations.ReleaseReservation, async () => Ok(await c.ReleaseAsync(lease)));
            }

            case "PublishPresence":
                return new("PublishPresence", async () =>
                {
                    try
                    {
                        var ack = await c.Store.PublishPresenceAsync(c.Application, new PresenceRecord(c.WorkerInstanceId, n + 1, 1, "ready",
                            Environment.ProcessId, "host", "build", ["default"], new Dictionary<string, int>(), c.Contracts, TimeSpan.FromSeconds(30)),
                            CancellationToken.None);
                        return ack.Accepted;
                    }
                    catch (RunwickException)
                    {
                        return false;
                    }
                });
            case "RegisterQueue":
                return new(Operations.RegisterQueue, async () =>
                    Ok(await c.Store.RegisterQueuesAsync(c.Command(new RegisterQueuesRequest([$"q{n}"])), CancellationToken.None)));
            default:
                throw new ArgumentOutOfRangeException(nameof(family));
        }
    }

    /// <summary>Stage (caller-owned): the business transaction stages and is held open; it commits when <paramref name="commit"/> completes.</summary>
    static async Task<bool> StageAsync(DriverFixture f, TestClient c, int n, Task commit, TaskCompletionSource staged)
    {
        await using var conn = await OpenBusinessAsync(f);
        await using var tx = await conn.BeginTransactionAsync();
        try
        {
            await Staging(c).StageAsync(new PingJob($"s{n}"), tx);
        }
        catch (RunwickException)
        {
            staged.TrySetResult();
            await tx.RollbackAsync();
            return false;
        }

        staged.TrySetResult();
        await commit;
        await tx.CommitAsync();
        return true;
    }

    [Theory]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-013")]
    [Trait("Case", "PGX-01")]
    [InlineData("Dispatch")]
    [InlineData("DispatchMany")]
    [InlineData("Stage")]
    [InlineData("Reserve")]
    [InlineData("Begin")]
    [InlineData("Renew")]
    [InlineData("Finish")]
    [InlineData("Recover")]
    [InlineData("ReleaseReservation")]
    [InlineData("PublishPresence")]
    [InlineData("RegisterQueue")]
    public async Task Gate_changes_race_every_command_family_at_the_current_revision(string family)
    {
        await using var f = await DriverFixture.StartAsync(RateSupport.Ns("pgx01"));
        var c = await RateSupport.OpenAsync(f);
        Assert.Equal(Migrations.PgMigrator.CurrentRevision, await f.CountAsync("SELECT schema_revision FROM runwick.store_metadata"));
        var settlement = Settlement.Contains(family);
        var changes = new List<(string Name, string? From, string To)> { ("drain", null, Drain), ("capability", null, Capability) };
        if (settlement)
        {
            changes.Add(("maintenance", Drain, Maintenance));
        }

        var n = 0;
        foreach (var (name, from, to) in changes)
        {
            foreach (var oldFirst in new[] { true, false })
            {
                n++;
                var label = $"{family}/{name}/{(oldFirst ? "old-first" : "new-first")}";
                var op = family == "Stage" ? null : await PrepareAsync(f, c, family, n);
                if (from is not null)
                {
                    await using var pre = await GateChange.StartAsync(f, from);
                    await pre.CommitAsync();
                }

                var footprint = await Footprint(f);
                bool applied;
                if (oldFirst)
                {
                    // The command holds the gate FOR SHARE (at BeforeCommit, or in its open business transaction);
                    // the change waits for it, and the command commits under the old gate.
                    var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    var staged = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    FaultHandle? h = null;
                    Task<bool> run;
                    if (op is null)
                    {
                        run = StageAsync(f, c, n, release.Task, staged);
                        await WithTimeout(staged.Task);
                    }
                    else
                    {
                        h = f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Op(op.Operation));
                        run = op.Run();
                        await DriverFixture.WaitAtBarrier(h, Wait);
                    }

                    var change = GateChange.StartAsync(f, to);
                    await RateSupport.WaitForLockWaiter(f, "rw:harness:admin");
                    Assert.False(change.IsCompleted, $"{label}: the gate change did not wait for the in-flight command");
                    if (h is not null)
                    {
                        DriverFixture.ReleaseBarrier(h);
                    }

                    release.TrySetResult();
                    applied = await WithTimeout(run);
                    await using var gc = await WithTimeout(change);
                    await gc.CommitAsync();
                    Assert.True(applied, $"{label}: the in-flight command did not complete under the old gate");
                }
                else
                {
                    // The change holds the application row; the command waits at the gate and then observes the new gate.
                    await using var gc = await GateChange.StartAsync(f, to);
                    var run = op is null ? StageAsync(f, c, n, Task.CompletedTask, new TaskCompletionSource()) : op.Run();
                    await RateSupport.WaitForLockWaiter(f);
                    Assert.False(run.IsCompleted, $"{label}: the command did not wait for the gate change");
                    await gc.CommitAsync();
                    applied = await WithTimeout(run);
                    var expected = settlement && name != "maintenance";
                    Assert.True(applied == expected, $"{label}: applied={applied}, expected {expected}");
                    if (!applied)
                    {
                        Assert.Equal(footprint, await Footprint(f));
                    }
                }

                f.Faults.DisarmAll();
                await using (var reset = await GateChange.StartAsync(f, Reset))
                {
                    await reset.CommitAsync();
                }
            }
        }

        await RateSupport.NoInvariantViolations(f);
    }
}
