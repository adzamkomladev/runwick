using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Runwick;
using Runwick.Postgres.Migrations;
using Runwick.Samples.Contracts;

// Usage: Runwick.Samples.Host [--conn <connection string>] [--migrate] [--dispatch <n>]
//   --migrate      apply the Runwick schema before starting
//   --dispatch n   dispatch n welcome emails, run them on the in-process worker, print "<n> succeeded"
var conn = ArgValue(args, "--conn") ?? Environment.GetEnvironmentVariable("RUNWICK_CONN") ?? "Host=localhost;Username=postgres;Password=pg;Database=rw";
var dispatch = int.TryParse(ArgValue(args, "--dispatch"), out var n) ? n : 0;

if (args.Contains("--migrate"))
{
    var migrated = await RunwickPostgresMigrations.MigrateAsync(conn).ConfigureAwait(false);
    Console.WriteLine($"migrated: schema revision {migrated.SchemaRevision}, phases [{string.Join(", ", migrated.AppliedPhases)}]");
}

var builder = Host.CreateApplicationBuilder(args);
builder.Logging.SetMinimumLevel(LogLevel.Warning);
builder.Services.AddRunwick(runwick =>
{
    runwick.Application("samples")
        .UsePostgres(conn)
        .AddJobsFromAssemblyContaining<SendWelcomeEmail>()
        .Queue("emails", q => q.MaxConcurrent(8));

    // Attribute-declared contract with a historical version.
    runwick.ConfigureJob<SendWelcomeEmail>()
        .Upgrade<SendWelcomeEmailV1, SendWelcomeEmail>(1, 2, v1 => new SendWelcomeEmail(v1.UserId, "en"));

    // Fluent-only contract.
    runwick.AddJob<BuildReport>()
        .Named("reports.build")
        .ToQueue("reports")
        .WithPriority("low")
        .WithMaxExecutions(5)
        .WithExecutionTimeout(TimeSpan.FromMinutes(2))
        .UniqueWithinWindow(r => r.ReportId, TimeSpan.FromMinutes(10));

    runwick.AddHandler<SendWelcomeEmail, SendWelcomeEmailHandler>();
    runwick.AddHandler<BuildReport, BuildReportHandler>();
    runwick.AddWorker(worker => worker.Pool("main", pool => pool.Queues("emails", "reports").FixedConcurrency(8)));
});

using var host = builder.Build();
await host.StartAsync().ConfigureAwait(false);
var exitCode = 0;
if (dispatch > 0)
{
    var dispatcher = host.Services.GetRequiredService<IJobDispatcher>();
    var ids = new List<Guid>();
    for (var i = 0; i < dispatch; i++)
    {
        ids.Add((await dispatcher.DispatchAsync(new SendWelcomeEmail(Guid.NewGuid(), "en")).ConfigureAwait(false)).JobId.Value);
    }

    var succeeded = await WaitForSuccessAsync(conn, ids, TimeSpan.FromMinutes(2)).ConfigureAwait(false);
    Console.WriteLine($"{succeeded} succeeded");
    exitCode = succeeded == dispatch ? 0 : 1;
}

await host.StopAsync().ConfigureAwait(false);
return exitCode;

static async Task<long> WaitForSuccessAsync(string conn, List<Guid> ids, TimeSpan timeout)
{
    await using var source = NpgsqlDataSource.Create(conn);
    var deadline = DateTime.UtcNow + timeout;
    long succeeded;
    do
    {
        await using var cmd = source.CreateCommand("SELECT count(*) FROM runwick.jobs WHERE job_id = ANY(@ids) AND state = 'succeeded'");
        cmd.Parameters.AddWithValue("ids", ids.ToArray());
        succeeded = (long)(await cmd.ExecuteScalarAsync().ConfigureAwait(false))!;
        if (succeeded < ids.Count)
        {
            await Task.Delay(200).ConfigureAwait(false);
        }
    }
    while (succeeded < ids.Count && DateTime.UtcNow < deadline);
    return succeeded;
}

static string? ArgValue(string[] args, string name)
{
    var i = Array.IndexOf(args, name);
    return i >= 0 && i + 1 < args.Length ? args[i + 1] : null;
}

sealed class SendWelcomeEmailHandler(ILogger<SendWelcomeEmailHandler> logger, IJobContext context) : IJobHandler<SendWelcomeEmail>
{
    public Task HandleAsync(SendWelcomeEmail job, CancellationToken cancellationToken)
    {
        logger.LogInformation("Welcome email to {UserId} ({Locale}), execution {Execution}.", job.UserId, job.Locale, context.ExecutionNumber);
        return Task.CompletedTask;
    }
}

sealed class BuildReportHandler(ILogger<BuildReportHandler> logger) : IJobHandler<BuildReport>
{
    public async Task HandleAsync(BuildReport job, CancellationToken cancellationToken)
    {
        await Task.Delay(TimeSpan.FromMilliseconds(Math.Min(job.Rows, 1000)), cancellationToken).ConfigureAwait(false);
        logger.LogInformation("Report {ReportId} built.", job.ReportId);
    }
}
