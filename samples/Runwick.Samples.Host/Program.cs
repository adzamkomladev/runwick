using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Runwick;
using Runwick.Samples.Contracts;

var conn = ArgValue(args, "--conn") ?? Environment.GetEnvironmentVariable("RUNWICK_CONN") ?? "Host=localhost;Username=postgres;Password=pg;Database=rw";

var builder = Host.CreateApplicationBuilder(args);
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
});

using var host = builder.Build();
await host.StartAsync().ConfigureAwait(false);
await host.StopAsync().ConfigureAwait(false);
return 0;

static string? ArgValue(string[] args, string name)
{
    var i = Array.IndexOf(args, name);
    return i >= 0 && i + 1 < args.Length ? args[i + 1] : null;
}
