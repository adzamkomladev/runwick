using Npgsql;
using Runwick.Harness.Faults;
using Runwick.Harness.Store;
using Runwick.Postgres.Tests.Support;
using Runwick.Protocol;
using Runwick.ReferenceModel;

namespace Runwick.Postgres.Tests;

/// <summary>PGX-05: injected rollbacks leave the pre-state; lost commit replies leave a valid committed state.</summary>
[Trait("Category", "Fault")]
[Trait("Item", "SP-005")]
[Trait("Case", "PGX-05")]
public sealed class ConstraintFaultTests
{
    public static TheoryData<string> Operations() =>
        ["Dispatch", "DispatchMany", "Reserve", "Begin", "Finish", "Recover", "ReleaseReservation"];

    /// <summary>Arranges the pre-state for an operation and returns the call under test (same command ID on every invocation).</summary>
    static async Task<Func<Task>> ArrangeAsync(string operation, DriverFixture f, TestClient c)
    {
        var commandId = Guid.CreateVersion7();
        var notAfter = DateTimeOffset.FromUnixTimeMilliseconds((DateTimeOffset.UtcNow + TimeSpan.FromSeconds(50)).ToUnixTimeMilliseconds());
        switch (operation)
        {
            case "Dispatch":
                var single = c.Prepared.Prepare(new PingJob("x"));
                return () => c.Prepared.DispatchAsync(single);
            case "DispatchMany":
                var batch = c.Prepared.PrepareMany(
                    [new BatchDispatchItem(new KeyedJob("a", "k1")), new BatchDispatchItem(new PingJob("b")), new BatchDispatchItem(new ResourceJob("c", "r1"))]);
                return () => c.Prepared.DispatchAsync(batch);
        }

        await c.Dispatcher.DispatchAsync(new KeyedJob("x", "k1"));
        var reserved = (await c.ReserveAsync(lease: operation == "Recover" ? TimeSpan.FromSeconds(1) : null)).Value!;
        var lease = reserved.Lease.Handle;
        switch (operation)
        {
            case "Reserve":
                await c.ReleaseAsync(lease);
                return async () => Check(await c.ReserveAsync(commandId: commandId, notAfter: notAfter));
            case "Begin":
                return async () => Check((await c.BeginAsync(lease, commandId, notAfter)).Reply);
            case "ReleaseReservation":
                return async () => Check(await c.ReleaseAsync(lease, commandId, notAfter));
            case "Finish":
                await c.BeginAsync(lease);
                return async () => Check(await c.FinishAsync(lease, AttemptOutcome.RetryableFailure, commandId: commandId, notAfter: notAfter));
            case "Recover":
                await Task.Delay(TimeSpan.FromMilliseconds(1100));
                var expired = (await c.Store.ListExpiredLeasesAsync(c.Application, 10, CancellationToken.None)).Single();
                return async () => Check(await c.RecoverAsync(expired, commandId, notAfter));
            default:
                throw new ArgumentOutOfRangeException(nameof(operation));
        }
    }

    static void Check<T>(StoreReply<T> reply) where T : class
    {
        if (reply.Effect != StoreEffect.Applied)
        {
            throw new InvalidOperationException($"Expected an applied reply, got {reply}");
        }
    }

    static string StoreOperation(string op) => op;

    [Theory]
    [MemberData(nameof(Operations))]
    public async Task Rolled_back_faults_leave_the_pre_state_and_lost_replies_leave_a_valid_commit(string operation)
    {
        await using var f = await DriverFixture.StartAsync("pgx05_" + operation.ToLowerInvariant()[..Math.Min(10, operation.Length)] + "_" + Guid.NewGuid().ToString("N")[..6]);
        await using var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var call = await ArrangeAsync(operation, f, c);
        var pre = await f.ReadCommittedSnapshot();

        // Throw(Transient) after every mutation step, then before commit.
        var steps = 0;
        for (var step = 1; step < 64; step++)
        {
            var h = f.ArmFault(FaultPoint.AfterMutation, FaultSpec.Throw(), new FaultTarget { Operation = StoreOperation(operation), Step = step });
            var ex = await Record.ExceptionAsync(call);
            f.Faults.DisarmAll();
            if (h.Fired == 0)
            {
                Assert.Fail($"{operation} committed while probing step {step} (fault not exercised); exception: {ex}");
            }

            Assert.NotNull(ex);
            var after = await f.ReadCommittedSnapshot();
            Assert.Empty(pre.DiffTables(after));
            steps = step;

            // Probe whether a further mutation step exists by checking the next one on the following iteration;
            // stop when the operation has no step `step + 1` (detected with a non-firing probe below).
            var probe = f.ArmFault(FaultPoint.AfterMutation, FaultSpec.Throw(), new FaultTarget { Operation = operation, Step = step + 1 });
            var beforeCommit = f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Throw(), FaultTarget.Op(operation));
            var probeEx = await Record.ExceptionAsync(call);
            f.Faults.DisarmAll();
            Assert.NotNull(probeEx);
            Assert.Empty(pre.DiffTables(await f.ReadCommittedSnapshot()));
            if (probe.Fired == 0)
            {
                Assert.Equal(1, beforeCommit.Fired);
                break;
            }
        }

        Assert.True(steps >= 1, "no mutation step was exercised");

        // DropReply after COMMIT: outcome unknown, the committed state is valid, and the same command replays.
        var drop = f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Op(operation));
        var lost = await Record.ExceptionAsync(call);
        f.Faults.DisarmAll();
        Assert.Equal(1, drop.Fired);
        Assert.NotNull(lost);
        var committed = await f.ReadCommittedSnapshot();
        Assert.NotEmpty(pre.DiffTables(committed));
        Assert.Empty(SnapshotInvariants.Violations(committed.Model));

        await call();   // replay of the same command ID (or dispatch idempotency) succeeds without a second effect
        var replayed = await f.ReadCommittedSnapshot();
        Assert.Equal(committed.Model.Jobs.Count, replayed.Model.Jobs.Count);
        Assert.Equal(committed.Model.Attempts.Count, replayed.Model.Attempts.Count);
        Assert.Empty(SnapshotInvariants.Violations(replayed.Model));
    }

    [Fact]
    public async Task A_committed_receipt_placeholder_fails_the_deferred_trigger()
    {
        await using var f = await DriverFixture.StartAsync("pgx05_placeholder_" + Guid.NewGuid().ToString("N")[..6]);
        var ex = await Assert.ThrowsAsync<PostgresException>(() => f.ExecAsync("""
            INSERT INTO runwick.storage_commands (
                application, retention_class, command_id, operation, operation_version, protocol_epoch,
                fingerprint, fingerprint_version, not_after, receipt_codec_version, retain_until)
            VALUES ('app', 'durable', gen_random_uuid(), 'Dispatch', 1, 1, sha256('x'), 1,
                    runwick.ms_floor(now()), 1, now() + interval '2 days')
            """));
        Assert.Equal("RWR01", ex.SqlState);
        var snapshot = await f.ReadCommittedSnapshot();
        Assert.Empty(snapshot["storage_commands"]);
    }
}
