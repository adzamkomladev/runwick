using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Runwick.Redis.Prototype;
using StackExchange.Redis;

[assembly: CollectionBehavior(CollectionBehavior.CollectionPerAssembly)]

namespace Runwick.Redis.Prototype.Tests;

/// <summary>One Redis 8.2 server in a Testcontainers container, with crash/pause controls for failure injection.</summary>
public sealed class RedisNode : IAsyncDisposable
{
    // Pinned by digest (Redis 8.2.11), recorded in docs/reports/sp-018.md.
    public const string Image = "redis:8.2-alpine@sha256:fe9daae35cd4fd6ec22416aa4fe0cf6b3e36741e06ff45b9986253fe2a81c041";

    /// <summary>redis-aof-always-single-primary-v1 (RDB snapshots off so AOF is the only persistence).</summary>
    public static readonly string[] Profile =
    [
        "--appendonly", "yes", "--appendfsync", "always", "--no-appendfsync-on-rewrite", "no",
        "--maxmemory-policy", "noeviction", "--aof-load-truncated", "no", "--save", "",
    ];

    private readonly IContainer container;

    private RedisNode(IContainer container, int port)
    {
        this.container = container;
        Port = port;
    }

    public int Port { get; }

    public string Id => container.Id;

    public ConnectionMultiplexer Mux { get; private set; } = null!;

    public IDatabase Db => Mux.GetDatabase();

    public static async Task<RedisNode> StartAsync(string[]? args = null, string? dataDir = null)
    {
        var port = FreePort();
        var builder = new ContainerBuilder(Image)
            .WithPortBinding(port, 6379)
            .WithCommand(["redis-server", .. args ?? Profile])
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Ready to accept connections"));
        if (dataDir is not null) builder = builder.WithBindMount(dataDir, "/data");
        var node = new RedisNode(builder.Build(), port);
        await node.container.StartAsync();
        node.Mux = await ConnectionMultiplexer.ConnectAsync(Options(port));
        return node;
    }

    /// <summary>Client lane settings from the design: no offline backlog, finite timeouts.</summary>
    public static ConfigurationOptions Options(int port) => new()
    {
        EndPoints = { new IPEndPoint(IPAddress.Loopback, port) },
        AbortOnConnectFail = false,
        BacklogPolicy = BacklogPolicy.FailFast,
        ConnectTimeout = 2000,
        SyncTimeout = 2000,
        AsyncTimeout = 2000,
        ReconnectRetryPolicy = new LinearRetry(200),
        AllowAdmin = true,
    };

    public RedisPrototypeStore Store(string app, string storeId = "store-1") => new(Db, storeId, app);

    public Task KillAsync() => Docker("kill", "-s", "KILL", Id);

    public async Task StartAgainAsync()
    {
        await Docker("start", Id);
        await WaitReadyAsync();
    }

    public async Task CrashAndRestartAsync()
    {
        await KillAsync();
        await StartAgainAsync();
    }

    public Task PauseAsync() => Docker("pause", Id);

    public Task UnpauseAsync() => Docker("unpause", Id);

    public async Task<bool> IsRunningAsync() => (await Docker("inspect", "-f", "{{.State.Running}}", Id)).Trim() == "true";

    public Task<string> LogsAsync() => Docker("logs", Id);

    /// <summary>Runs a shell command in a throwaway container that mounts this (stopped) node's /data.</summary>
    public Task<string> WithDataAsync(string shell) =>
        Docker("run", "--rm", "--volumes-from", Id, "--entrypoint", "sh", Image, "-c", shell);

    public async Task WaitReadyAsync()
    {
        var sw = Stopwatch.StartNew();
        while (true)
        {
            try
            {
                await Db.PingAsync();
                return;
            }
            catch (Exception) when (sw.Elapsed < TimeSpan.FromSeconds(30))
            {
                await Task.Delay(100);
            }
        }
    }

    /// <summary>Every key in one application's namespace, typed and sorted (raw, not an authoritative read).</summary>
    public async Task<SortedDictionary<string, string>> SnapshotAsync(string app)
    {
        var prefix = $"rw:{{{app}}}:";
        var server = Mux.GetServer(Mux.GetEndPoints()[0]);
        var snap = new SortedDictionary<string, string>(StringComparer.Ordinal);
        await foreach (var key in server.KeysAsync(pattern: prefix + "*"))
        {
            var name = ((string)key!)[prefix.Length..];
            snap[name] = (await Db.KeyTypeAsync(key)) switch
            {
                RedisType.Hash => string.Join(",", (await Db.HashGetAllAsync(key)).Where(e => e.Value != "")
                    .Select(e => $"{e.Name}={e.Value}").Order(StringComparer.Ordinal)),
                RedisType.SortedSet => string.Join(",", (await Db.SortedSetRangeByRankWithScoresAsync(key))
                    .Select(e => $"{e.Element}@{e.Score}")),
                var t => $"<{t}>:" + await Db.StringGetAsync(key),
            };
        }

        return snap;
    }

    public static async Task<string> Docker(params string[] args)
    {
        var psi = new ProcessStartInfo("docker") { RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var a in args) psi.ArgumentList.Add(a);
        using var p = Process.Start(psi)!;
        var stdout = p.StandardOutput.ReadToEndAsync();
        var stderr = p.StandardError.ReadToEndAsync();
        await p.WaitForExitAsync();
        return await stdout + await stderr;
    }

    public async ValueTask DisposeAsync()
    {
        await Mux.DisposeAsync();
        await container.DisposeAsync();
    }

    private static int FreePort()
    {
        using var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        return ((IPEndPoint)l.LocalEndpoint).Port;
    }
}
