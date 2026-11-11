using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Postgres.Migrations;
using Runwick.Protocol;

namespace Runwick.Contract.Tests;

/// <summary>
/// The operator capability call (10 §6.1, 23 §3.6) on every driver: it raises the capability in the store's own format, a repeat
/// changes nothing, and non-activatable or unknown names and unknown applications are refused. Each raise writes the store operation's
/// <c>ActivateCapabilities</c> audit row and <c>capabilities_activated</c> event, with the operator as actor; a repeat writes neither.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-028")]
[Collection("EventCursor")] // An earlier PostgreSQL transaction can hold back the event cursor across databases.
public sealed class CapabilityActivationContractTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static Task<IReadOnlyList<CapabilityChange>> Activate(DriverFixture f, string? app, string name)
    {
        var cs = f.Kind == StoreKind.Redis ? RedisServer.ConfigurationOf(f.AdminConnectionString) : f.AdminConnectionString;
        return f.Kind switch
        {
            StoreKind.Postgres => RunwickPostgresMigrations.ActivateCapabilityAsync(cs, app, name, "ops", Ct),
            StoreKind.Sqlite => RunwickSqliteMigrations.ActivateCapabilityAsync(cs, app, name, "ops", Ct),
            _ => RunwickRedisMigrations.ActivateCapabilityAsync(cs, app, name, "ops", Ct),
        };
    }

    static string CodeOf(Exception e) => e switch
    {
        PostgresMigrationException p => p.Code,
        SqliteMigrationException s => s.Code,
        RedisMigrationException r => r.Code,
        _ => throw new InvalidOperationException("not a migration exception: " + e),
    };

    /// <summary>The application's audit rows and capabilities_activated events, oldest first.</summary>
    static async Task<(List<AuditEntry> Audit, List<JobEvent> Events)> Records(TestClient c)
    {
        var audit = (await ControlContractTests.Reads(c).ListAuditAsync(c.Application, new AuditFilter("application", c.Application, null, null), null, 100, Ct)).Value
            .OrderBy(a => a.AuditId).ToList();
        var events = (await ControlContractTests.Controls(c).EventsSinceAsync(c.Application, null, 1000, Ct)).Events
            .Where(e => e.Kind == "capabilities_activated").ToList();
        return (audit, events);
    }

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "CAP-ACT")]
    public async Task Operator_activation_raises_the_capability_once_and_is_seen_by_the_store(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync("capact_" + Guid.NewGuid().ToString("N")[..8], kind);
        await using var c = await f.OpenClient(configure: r => TestJobs.Register(r));

        var first = (await Activate(f, f.Application, "scheduler")).Single();
        Assert.Equal((f.Application, "scheduler", 1, true), (first.Application, first.Capability, first.Version, first.Changed));
        var again = (await Activate(f, f.Application, "scheduler")).Single();
        Assert.False(again.Changed);
        Assert.Equal(first.Revision, again.Revision);

        // One audit row and one event, as the store operation writes them; the repeat added neither.
        var (audit, events) = await Records(c);
        var row = Assert.Single(audit);
        Assert.Equal(("ActivateCapabilities", "application", f.Application, "ops", (string?)null, "operator", (string?)null),
            (row.Operation, row.TargetKind, row.TargetId, row.ActorSubject, row.ActorDisplay, row.ActorScheme, row.Reason));
        Assert.Equal(((long?)first.Revision - 1, (long?)first.Revision), (row.ExpectedRevision, row.ResultingRevision));
        Assert.Equal(1, row.Details["requiredCapabilities"]!["scheduler"]!.GetValue<int>());
        var ev = Assert.Single(events);
        Assert.Equal(("ops", row.CommandId, first.Revision), (ev.ActorId, ev.CommandId, ev.Details["revision"]!.GetValue<long>()));
        Assert.Equal(1, ev.Details["requiredCapabilities"]!["scheduler"]!.GetValue<int>());
        Assert.Null(ev.JobId);

        // The store's own operation sees the capability already there, so it is stored in the driver's format.
        var rates = (IRateControlStore)c.Driver;
        var repeat = await rates.ActivateCapabilitiesAsync(
            c.Command(new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["scheduler"] = 1 }, new ControlActor("user:ops", null, "host", "t"))), Ct);
        Assert.Equal((StoreStatus.InvalidState, "no_change"), (repeat.Status, repeat.Error!.Code));

        // A second capability merges with the first; every application when none is named. Redis tests share one server, where
        // "every application" also raises the capability of other tests' applications running alongside, so Redis names its own.
        var rate = (await Activate(f, f.Kind == StoreKind.Redis ? f.Application : null, "rate_admission")).Single();
        Assert.True(rate.Changed);
        Assert.Equal(first.Revision + 1, rate.Revision);
        (audit, events) = await Records(c);
        Assert.Equal(2, audit.Count);
        Assert.Equal(2, events.Count);
        Assert.Equal((long?)rate.Revision, audit[1].ResultingRevision);
        Assert.Equal(1, events[1].Details["requiredCapabilities"]!["rate_admission"]!.GetValue<int>());
        Assert.NotEqual(audit[0].CommandId, audit[1].CommandId);

        Assert.Equal("invalid_capability", CodeOf(await Assert.ThrowsAnyAsync<Exception>(() => Activate(f, f.Application, "execution"))));
        Assert.Equal("invalid_capability", CodeOf(await Assert.ThrowsAnyAsync<Exception>(() => Activate(f, f.Application, "teleport"))));
        Assert.Equal("application_not_registered", CodeOf(await Assert.ThrowsAnyAsync<Exception>(() => Activate(f, "nobody", "scheduler"))));
    }
}
