using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

namespace Runwick.Redis.Tests;

/// <summary>
/// A throwaway Redis container this test run owns (name <c>runwick-g4c-*</c>), with crash, pause and data-directory controls for the
/// fault cases of 22 §12. Only containers created here are ever touched. Image and profile are the ones of 22 §1 / §2.3.
/// </summary>
internal sealed class RedisNode : IAsyncDisposable
{
    public const string Image = "redis:8.2-alpine";

    /// <summary>redis-aof-always-single-primary-v1, RDB snapshots off so the AOF is the only persistence.</summary>
    public static readonly string[] Profile =
    [
        "--appendonly", "yes", "--appendfsync", "always", "--no-appendfsync-on-rewrite", "no",
        "--maxmemory-policy", "noeviction", "--aof-load-truncated", "no", "--save", "",
    ];

    RedisNode(string name, int port)
    {
        Name = name;
        Port = port;
    }

    public string Name { get; }

    public int Port { get; }

    public string Configuration => "127.0.0.1:" + Port;

    /// <summary>Starts a container; <paramref name="args"/> replaces the profile (pass [] for the image defaults).</summary>
    public static async Task<RedisNode> StartAsync(string[]? args = null, string? dataDir = null)
    {
        var port = FreePort();
        var node = new RedisNode("runwick-g4c-" + Guid.NewGuid().ToString("N")[..8], port);
        List<string> run = ["run", "-d", "--name", node.Name, "-p", $"127.0.0.1:{port}:6379"];
        if (dataDir is not null)
        {
            run.AddRange(["-v", dataDir + ":/data"]);
        }

        run.Add(Image);
        run.Add("redis-server");
        run.AddRange(args ?? Profile);
        var started = await Docker([.. run]);
        if (started.Code != 0)
        {
            throw new InvalidOperationException("docker run failed: " + started.Output);
        }

        await node.WaitReadyAsync();
        return node;
    }

    public async Task KillAsync() => Ok(await Docker("kill", "-s", "KILL", Name));

    public async Task StartAgainAsync()
    {
        Ok(await Docker("start", Name));
        await WaitReadyAsync();
    }

    public async Task CrashAndRestartAsync()
    {
        await KillAsync();
        await StartAgainAsync();
    }

    public async Task PauseAsync() => Ok(await Docker("pause", Name));

    public async Task UnpauseAsync() => Ok(await Docker("unpause", Name));

    public async Task<bool> IsRunningAsync() => (await Docker("inspect", "-f", "{{.State.Running}}", Name)).Output.Trim() == "true";

    public async Task<string> LogsAsync() => (await Docker("logs", Name)).Output;

    /// <summary>One redis-cli command inside the container; returns stdout (use it for reads that must survive restarts).</summary>
    public async Task<string> CliAsync(params string[] command) =>
        (await Docker(["exec", Name, "redis-cli", .. command])).Output.Trim();

    /// <summary>A shell command in a throwaway container that mounts this (stopped or running) node's /data.</summary>
    public async Task<string> WithDataAsync(string shell) =>
        (await Docker("run", "--rm", "--volumes-from", Name, "--entrypoint", "sh", Image, "-c", shell)).Output;

    public async Task WaitReadyAsync(int seconds = 30)
    {
        var sw = Stopwatch.StartNew();
        while (true)
        {
            var r = await Docker("exec", Name, "redis-cli", "ping");
            if (r.Code == 0 && r.Output.Contains("PONG", StringComparison.Ordinal))
            {
                return;
            }

            if (sw.Elapsed > TimeSpan.FromSeconds(seconds))
            {
                throw new TimeoutException("Redis did not become ready: " + r.Output);
            }

            await Task.Delay(100);
        }
    }

    public static async Task<(int Code, string Output)> Docker(params string[] args) => await Run("docker", args);

    public static async Task<(int Code, string Output)> Run(string file, params string[] args)
    {
        var psi = new ProcessStartInfo(file) { RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var a in args)
        {
            psi.ArgumentList.Add(a);
        }

        using var p = Process.Start(psi)!;
        var stdout = p.StandardOutput.ReadToEndAsync();
        var stderr = p.StandardError.ReadToEndAsync();
        await p.WaitForExitAsync();
        return (p.ExitCode, await stdout + await stderr);
    }

    static void Ok((int Code, string Output) r)
    {
        if (r.Code != 0)
        {
            throw new InvalidOperationException("docker failed: " + r.Output);
        }
    }

    public async ValueTask DisposeAsync() => await Docker("rm", "-f", "-v", Name);

    static int FreePort()
    {
        using var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        return ((IPEndPoint)l.LocalEndpoint).Port;
    }
}
