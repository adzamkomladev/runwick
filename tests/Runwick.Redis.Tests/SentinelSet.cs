using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;

namespace Runwick.Redis.Tests;

/// <summary>
/// One primary, two replicas and three Sentinels as throwaway containers on the host network (names <c>runwick-fork-r-*</c>), so the
/// addresses the Sentinels report are reachable from the test process. Only containers created here are touched.
/// </summary>
internal sealed class SentinelSet : IAsyncDisposable
{
    public const string Service = "mymaster";

    readonly List<string> _containers = [];

    SentinelSet()
    {
    }

    public int[] NodePorts { get; private set; } = [];

    public int[] SentinelPorts { get; private set; } = [];

    string[] _nodeNames = [];
    string[] _sentinelNames = [];

    /// <summary>The Sentinel seeds and service name as a StackExchange.Redis configuration string.</summary>
    public string Configuration => string.Join(',', SentinelPorts.Select(p => "127.0.0.1:" + p)) + ",serviceName=" + Service;

    public static async Task<SentinelSet> StartAsync()
    {
        var set = new SentinelSet();
        var tag = Guid.NewGuid().ToString("N")[..6];
        set.NodePorts = [Free(), Free(), Free()];
        set.SentinelPorts = [Free(), Free(), Free()];
        set._nodeNames = [.. set.NodePorts.Select((_, i) => $"runwick-fork-r-{tag}-n{i}")];
        set._sentinelNames = [.. set.SentinelPorts.Select((_, i) => $"runwick-fork-r-{tag}-s{i}")];
        try
        {
            for (var i = 0; i < 3; i++)
            {
                List<string> args = ["run", "-d", "--name", set._nodeNames[i], "--network", "host", RedisNode.Image, "redis-server",
                    "--port", set.NodePorts[i].ToString(CultureInfo.InvariantCulture), "--bind", "127.0.0.1", .. RedisNode.Profile];
                if (i > 0)
                {
                    args.AddRange(["--replicaof", "127.0.0.1", set.NodePorts[0].ToString(CultureInfo.InvariantCulture)]);
                }

                Ok(await RedisNode.Docker([.. args]));
                set._containers.Add(set._nodeNames[i]);
            }

            for (var i = 0; i < 3; i++)
            {
                var conf = $"port {set.SentinelPorts[i]}\\nbind 127.0.0.1\\nsentinel monitor {Service} 127.0.0.1 {set.NodePorts[0]} 2\\n"
                    + $"sentinel down-after-milliseconds {Service} 3000\\nsentinel failover-timeout {Service} 8000\\nsentinel parallel-syncs {Service} 1\\n";
                Ok(await RedisNode.Docker("run", "-d", "--name", set._sentinelNames[i], "--network", "host", RedisNode.Image, "sh", "-c",
                    $"printf '{conf}' > /tmp/s.conf && exec redis-sentinel /tmp/s.conf"));
                set._containers.Add(set._sentinelNames[i]);
            }

            await set.WaitUntilAsync("replicas and Sentinels discovered", async () =>
            {
                var master = await set.SentinelCliAsync(0, "sentinel", "master", Service);
                return master.Contains("num-slaves\n2", StringComparison.Ordinal) && master.Contains("num-other-sentinels\n2", StringComparison.Ordinal);
            }, 60);
            await set.WaitReplicasSyncedAsync();
            return set;
        }
        catch
        {
            await set.DisposeAsync();
            throw;
        }
    }

    public string Primary => "127.0.0.1:" + NodePorts[0];

    public string NodeName(int port) => _nodeNames[Array.IndexOf(NodePorts, port)];

    /// <summary>The port the Sentinels currently name as primary.</summary>
    public async Task<int> PrimaryPortAsync()
    {
        for (var i = 0; i < 3; i++)
        {
            var r = await SentinelCliAsync(i, "sentinel", "get-master-addr-by-name", Service);
            var lines = r.Split('\n', StringSplitOptions.RemoveEmptyEntries);
            if (lines.Length == 2 && int.TryParse(lines[1], CultureInfo.InvariantCulture, out var port))
            {
                return port;
            }
        }

        throw new InvalidOperationException("No Sentinel named a primary.");
    }

    public async Task<string> NodeCliAsync(int port, params string[] command) =>
        (await RedisNode.Docker(["exec", NodeName(port), "redis-cli", "-p", port.ToString(CultureInfo.InvariantCulture), .. command])).Output.Trim().Replace("\r", "", StringComparison.Ordinal);

    async Task<string> SentinelCliAsync(int index, params string[] command) =>
        (await RedisNode.Docker(["exec", _sentinelNames[index], "redis-cli", "-p", SentinelPorts[index].ToString(CultureInfo.InvariantCulture), .. command]))
            .Output.Trim().Replace("\r", "", StringComparison.Ordinal);

    public Task KillNodeAsync(int port) => RedisNode.Docker("kill", "-s", "KILL", NodeName(port));

    public Task StartNodeAsync(int port) => RedisNode.Docker("start", NodeName(port));

    public Task PauseNodeAsync(int port) => RedisNode.Docker("pause", NodeName(port));

    public Task UnpauseNodeAsync(int port) => RedisNode.Docker("unpause", NodeName(port));

    public async Task PauseSentinelsAsync()
    {
        foreach (var n in _sentinelNames)
        {
            await RedisNode.Docker("pause", n);
        }
    }

    public async Task UnpauseSentinelsAsync()
    {
        foreach (var n in _sentinelNames)
        {
            await RedisNode.Docker("unpause", n);
        }
    }

    /// <summary>Waits until every replica has applied all of the primary's writes (so a promotion loses nothing).</summary>
    public async Task WaitReplicasSyncedAsync()
    {
        var primary = await PrimaryPortAsync();
        await WaitUntilAsync("replicas caught up", async () =>
        {
            var info = await NodeCliAsync(primary, "info", "replication");
            var offsets = info.Split('\n').Where(l => l.StartsWith("slave", StringComparison.Ordinal) && l.Contains("offset=", StringComparison.Ordinal))
                .Select(l => long.Parse(l[(l.IndexOf("offset=", StringComparison.Ordinal) + 7)..].Split(',')[0], CultureInfo.InvariantCulture)).ToList();
            var master = long.Parse(info.Split('\n').First(l => l.StartsWith("master_repl_offset:", StringComparison.Ordinal))[19..].Trim(), CultureInfo.InvariantCulture);
            return offsets.Count == 2 && offsets.All(o => o >= master);
        }, 30);
    }

    public async Task WaitUntilAsync(string what, Func<Task<bool>> condition, int seconds)
    {
        var sw = Stopwatch.StartNew();
        while (true)
        {
            try
            {
                if (await condition())
                {
                    return;
                }
            }
            catch (Exception ex) when (ex is FormatException or InvalidOperationException or InvalidCastException)
            {
                // Not ready yet.
            }

            if (sw.Elapsed > TimeSpan.FromSeconds(seconds))
            {
                throw new TimeoutException("Timed out waiting for: " + what);
            }

            await Task.Delay(250);
        }
    }

    static void Ok((int Code, string Output) r)
    {
        if (r.Code != 0)
        {
            throw new InvalidOperationException("docker failed: " + r.Output);
        }
    }

    static int Free()
    {
        using var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        return ((IPEndPoint)l.LocalEndpoint).Port;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var name in _containers)
        {
            await RedisNode.Docker("rm", "-f", "-v", name);
        }
    }
}
