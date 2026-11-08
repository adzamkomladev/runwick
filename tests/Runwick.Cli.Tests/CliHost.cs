using System.Collections.Concurrent;
using System.Security.Claims;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Nodes;
using Json.Schema;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Runwick.AspNetCore;
using Runwick.Dispatching;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Protocol;
using Runwick.Scheduling;
using Runwick.Scheduling.Evaluation;

namespace Runwick.Cli.Tests;

public sealed record OtherJob(string Id) : IQueueJob;

/// <summary><c>Authorization: Bearer tok-&lt;user&gt;</c>: a machine-client scheme (neither cookie nor Basic) for the bearer group.</summary>
internal sealed class BearerTestHandler(IOptionsMonitor<AuthenticationSchemeOptions> o, ILoggerFactory l, UrlEncoder e)
    : AuthenticationHandler<AuthenticationSchemeOptions>(o, l, e)
{
    public const string Name = "TestBearer";

    protected override Task<AuthenticateResult> HandleAuthenticateAsync() =>
        Task.FromResult(Request.Headers.Authorization.ToString() is { } h && h.StartsWith("Bearer tok-", StringComparison.Ordinal)
            ? AuthenticateResult.Success(new AuthenticationTicket(CliHost.Principal(h["Bearer tok-".Length..], Scheme.Name), Scheme.Name))
            : AuthenticateResult.NoResult());
}

internal sealed record CliRun(int Exit, string Out, string Error)
{
    /// <summary>The single agent envelope (non-streaming), or the last line of a stream.</summary>
    public JsonNode Envelope => JsonNode.Parse(Out.TrimEnd().Split('\n')[^1])!;

    public JsonNode? Result => Envelope["result"];

    public string? Code => Envelope["error"]?["code"]?.GetValue<string>();

    public IReadOnlyList<JsonNode> Lines => [.. Out.Split('\n', StringSplitOptions.RemoveEmptyEntries).Select(l => JsonNode.Parse(l)!)];
}

/// <summary>
/// A standalone management host (16 MGMT-12 setup) on the real PostgreSQL store, mapped either with Basic +
/// antiforgery or with a bearer scheme on a <c>ForMachineClients</c> group, and the CLI wired to it in-process.
/// </summary>
internal sealed class CliHost : IAsyncDisposable
{
    public const string Url = "https://localhost/runwick/api";
    public const string Admin = "admin", Viewer = "viewer", Scoped = "scoped";

    readonly string _dir = Directory.CreateTempSubdirectory("runwick-cli-").FullName;

    CliHost(WebApplication app, DriverFixture f, bool bearer)
    {
        App = app;
        Fixture = f;
        Bearer = bearer;
        Grants[Admin] = [.. ManagementPermissions.All.Select(p => new ManagementGrant(p))];
        Grants[Viewer] = ManagementRoles.Viewer;
        Grants[Scoped] = [.. ManagementPermissions.QueueScoped.Select(p => new ManagementGrant(p, new HashSet<string>(StringComparer.Ordinal) { "default" }))];
        ConfigPath = Path.Combine(_dir, "config.json");
    }

    public WebApplication App { get; }

    public DriverFixture Fixture { get; }

    public bool Bearer { get; }

    public string ConfigPath { get; }

    public ConcurrentDictionary<string, IReadOnlyList<ManagementGrant>> Grants { get; } = new(StringComparer.Ordinal);

    /// <summary>When set, every run goes through a new <see cref="FaultHandler"/> configured by it; the last one is <see cref="LastFault"/>.</summary>
    public Action<FaultHandler>? Fault { get; set; }

    public FaultHandler? LastFault { get; private set; }

    public static ClaimsPrincipal Principal(string user, string scheme) =>
        new(new ClaimsIdentity([new Claim(ClaimTypes.NameIdentifier, user), new Claim(ClaimTypes.Name, user)], scheme));

    public static string Secret(string user, bool bearer) => bearer ? "tok-" + user : "pw-" + user;

    public static async Task<CliHost> StartAsync(DriverFixture f, bool bearer = false)
    {
        CliHost? host = null;
        var b = WebApplication.CreateSlimBuilder();
        b.WebHost.UseTestServer();
        b.Logging.SetMinimumLevel(LogLevel.Warning);
        b.Services.AddSingleton<IFaultInjector>(f.Faults);
        b.Services.AddRunwick(r =>
        {
            r.Application(f.Application).UsePostgres(f.ClientConnectionString("mgmt"));
            Jobs(r);
        });
        b.Services.AddAuthorization();
        b.Services.AddAuthentication()
            .AddRunwickBasic(o => o.Verifier = (user, password, _) => ValueTask.FromResult(password == "pw-" + user))
            .AddScheme<AuthenticationSchemeOptions, BearerTestHandler>(BearerTestHandler.Name, null);
        b.Services.AddRunwickManagement(m =>
        {
            m.AuthenticationSchemes(bearer ? BearerTestHandler.Name : RunwickBasicDefaults.Scheme);
            m.MapGrants(p => host!.Grants.TryGetValue(p.Identity!.Name!, out var g) ? g : []);
            m.Configure(o => (o.ReadsPerMinute, o.MutationsPerMinute, o.RevealsPerMinute) = (100_000, 100_000, 100_000));
        });
        var app = b.Build();
        var group = app.MapRunwickManagement("/runwick/api");
        if (bearer)
        {
            group.ForMachineClients();
        }

        await app.StartAsync();
        host = new CliHost(app, f, bearer);
        await host.SaveContextAsync(Admin);
        return host;
    }

    public static void Jobs(RunwickBuilder r)
    {
        TestJobs.Register(r);
        r.AddJob<OtherJob>().Named("test.other").ToQueue("other");
    }

    public IJobDispatcher Dispatcher => App.Services.GetRequiredService<IJobDispatcher>();

    public async Task<Guid> DispatchAsync<T>(T job) where T : class, IQueueJob =>
        (await Dispatcher.DispatchAsync(job, cancellationToken: TestContext.Current.CancellationToken)).JobId.Value;

    /// <summary>One context per user, named after it; the current one is <paramref name="user"/>.</summary>
    public async Task SaveContextAsync(string user)
    {
        var config = File.Exists(ConfigPath) ? JsonNode.Parse(await File.ReadAllTextAsync(ConfigPath))!.AsObject() : new JsonObject { ["version"] = 1, ["contexts"] = new JsonObject() };
        config["current"] = user;
        config["contexts"]![user] = new JsonObject
        {
            ["url"] = Url, ["auth"] = Bearer ? "bearer" : "basic", ["user"] = Bearer ? null : user, ["secretEnv"] = "RW_SECRET_" + user.ToUpperInvariant(),
        };
        await File.WriteAllTextAsync(ConfigPath, config.ToJsonString());
    }

    /// <summary>Every user's secret is in its context's variable.</summary>
    public Dictionary<string, string> Variables() =>
        Grants.Keys.Union(["nobody", "preset"]).ToDictionary(u => "RW_SECRET_" + u.ToUpperInvariant(), u => Secret(u, Bearer));

    /// <summary>
    /// Runs the CLI in-process. Every agent-mode line is validated against the committed envelope schema (CLI-03).
    /// </summary>
    public async Task<CliRun> RunAsync(string[] args, string? stdin = null, Dictionary<string, string>? variables = null, bool stdinIsTerminal = false,
        CancellationToken interrupt = default)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        HttpMessageHandler handler = App.GetTestServer().CreateHandler();
        if (Fault is not null)
        {
            Fault(LastFault = new FaultHandler(handler));
            handler = LastFault;
        }

        var env = new CliEnvironment
        {
            Out = output,
            Error = error,
            In = new StringReader(stdin ?? ""),
            Variables = variables ?? Variables(),
            ConfigPath = ConfigPath,
            Handler = handler,
            Interrupt = interrupt,
            StdinIsTerminal = stdinIsTerminal,
            PromptSecret = () => throw new InvalidOperationException("The CLI prompted."),
            ResendDelays = [TimeSpan.FromMilliseconds(20), TimeSpan.FromMilliseconds(20), TimeSpan.FromMilliseconds(20)],
        };
        var exit = await RunwickCli.RunAsync(args, env);
        var run = new CliRun(exit, output.ToString(), error.ToString());
        if (args.Contains("--agent"))
        {
            Assert.Empty(run.Error);
            foreach (var line in run.Out.Split('\n', StringSplitOptions.RemoveEmptyEntries))
            {
                Schemas.AssertValidEnvelope(line);
            }
        }

        return run;
    }

    public Task<CliRun> AgentAsync(params string[] args) => RunAsync(["--agent", .. args]);

    public async ValueTask DisposeAsync()
    {
        await App.StopAsync();
        await App.DisposeAsync();
        Directory.Delete(_dir, recursive: true);
    }
}

/// <summary>Store-side setup that is not under test (as in the AspNetCore suite): schedules and finished attempts.</summary>
internal static class StoreSupport
{
    static readonly ControlActor Ops = new("user:ops", null, "host", "setup");

    sealed class FrozenBudgetClock : TimeProvider
    {
        public static readonly FrozenBudgetClock Instance = new();

        public override long GetTimestamp() => 0;
    }

    public static async Task RegisterScheduleAsync(this DriverFixture f, string key)
    {
        var c = await f.OpenClient("local", CliHost.Jobs);
        var ct = CancellationToken.None;
        Assert.Equal(StoreStatus.Applied, (await c.Store.ActivateCapabilitiesAsync(
            c.Command(new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["scheduler"] = 1 }, Ops)), ct)).Status);
        var definition = ScheduleCompiler.Compile(new ScheduleDeclaration(key, new PingJob(key)) { Cron = "0 3 * * *" },
            c.Services.GetRequiredService<DispatchPreparer>(), TzdbArtifact.Pinned);
        var cutoff = await c.Store.StoreNowAsync(ct);
        var first = ScheduleRecurrence.For(definition, TzdbArtifact.Pinned, FrozenBudgetClock.Instance)(cutoff)?.At;
        Assert.Equal(StoreStatus.Applied, (await c.Store.RegisterScheduleAsync(c.Command(new RegisterScheduleRequest(key, definition, cutoff, first)), ct)).Status);
    }

    /// <summary>Runs the next job of queue <c>default</c> on a store-level client and finishes it; returns the attempt ID.</summary>
    public static async Task<Guid> FinishNextAsync(this DriverFixture f, AttemptOutcome outcome, string? message = null)
    {
        var c = await f.OpenClient("local", CliHost.Jobs);
        var reserved = await c.ReserveAsync();
        Assert.Equal(StoreStatus.Applied, reserved.Status);
        var lease = reserved.Value!.Lease.Handle;
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(lease)).Reply.Status);
        var finished = await c.FinishAsync(lease, outcome, error: message is null ? null : new AttemptError("boom", "System.Exception", message, null));
        Assert.Equal(StoreStatus.Applied, finished.Status);
        return lease.AttemptId;
    }
}

/// <summary>Client-side faults: the first <c>Lose</c> POSTs reach the server but their replies are lost; <c>Refuse</c> fails before sending.</summary>
internal sealed class FaultHandler(HttpMessageHandler inner) : DelegatingHandler(inner)
{
    public int Lose { get; set; }

    public bool Refuse { get; set; }

    public Func<HttpRequestMessage, HttpResponseMessage?>? Answer { get; set; }

    public int Requests { get; private set; }

    public List<string> Paths { get; } = [];

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        Requests++;
        Paths.Add(request.RequestUri!.PathAndQuery);
        if (Refuse)
        {
            throw new HttpRequestException(HttpRequestError.ConnectionError, "refused");
        }

        if (Answer?.Invoke(request) is { } canned)
        {
            return canned;
        }

        var response = await base.SendAsync(request, cancellationToken);
        if (request.Method == HttpMethod.Post && Lose > 0)
        {
            Lose--;
            response.Dispose();
            throw new HttpRequestException(HttpRequestError.ResponseEnded, "reply lost");
        }

        return response;
    }
}

internal static class Schemas
{
    public static string Root
    {
        get
        {
            for (var d = new DirectoryInfo(AppContext.BaseDirectory); d is not null; d = d.Parent)
            {
                if (File.Exists(Path.Combine(d.FullName, "Runwick.slnx")))
                {
                    return Path.Combine(d.FullName, "schemas");
                }
            }

            throw new InvalidOperationException("Repository root not found.");
        }
    }

    public static string EnvelopePath => Path.Combine(Root, "runwick-cli-agent-envelope-v1.json");

    public static string DescribePath => Path.Combine(Root, "runwick-cli-describe-v1.json");

    static readonly Lazy<JsonSchema> Envelope = new(() => JsonSchema.FromText(File.ReadAllText(EnvelopePath)));

    public static void AssertValidEnvelope(string line)
    {
        using var doc = JsonDocument.Parse(line);
        var result = Envelope.Value.Evaluate(doc.RootElement, new EvaluationOptions { OutputFormat = OutputFormat.List });
        Assert.True(result.IsValid, "Envelope does not match the committed schema: " + line);
    }
}
