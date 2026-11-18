using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;

namespace Runwick.Redis.Tests;

/// <summary>
/// Three primaries and one replica (of the first primary) of a Redis Cluster as throwaway containers on the host network (names
/// <c>runwick-fork-cl-*</c>), so the addresses the nodes announce are reachable from the test process. Slots are split evenly:
/// node 0 owns 0–5460, node 1 5461–10922, node 2 10923–16383. <c>cluster-require-full-coverage</c> is <c>no</c>, so a failing owner does not
/// stop the slots the other primaries own. Only containers created here are touched.
/// </summary>
internal sealed class ClusterSet : IAsyncDisposable
{
    readonly List<string> _containers = [];
    string[] _names = [];

    ClusterSet()
    {
    }

    /// <summary>Three primary ports followed by the replica's port.</summary>
    public int[] Ports { get; private set; } = [];

    public int ReplicaPort => Ports[3];

    /// <summary>The three primaries as a configuration string (the replica joins the node list by discovery).</summary>
    public string Configuration => string.Join(',', Ports.Take(3).Select(p => "127.0.0.1:" + p));

    public static async Task<ClusterSet> StartAsync()
    {
        var set = new ClusterSet();
        var tag = Guid.NewGuid().ToString("N")[..6];
        set.Ports = [Free(), Free(), Free(), Free()];
        set._names = [.. set.Ports.Select((_, i) => $"runwick-fork-cl-{tag}-n{i}")];
        try
        {
            for (var i = 0; i < 4; i++)
            {
                var port = set.Ports[i].ToString(CultureInfo.InvariantCulture);
                Ok(await RedisNode.Docker(["run", "-d", "--name", set._names[i], "--network", "host", RedisNode.Image, "redis-server",
                    "--port", port, "--bind", "127.0.0.1", "--cluster-enabled", "yes", "--cluster-config-file", "nodes.conf",
                    "--cluster-node-timeout", "3000", "--cluster-announce-ip", "127.0.0.1", "--cluster-require-full-coverage", "no", .. RedisNode.Profile]));
                set._containers.Add(set._names[i]);
            }

            foreach (var port in set.Ports)
            {
                await set.WaitUntilAsync("node answers", async () => (await set.CliAsync(port, "ping")) == "PONG", 30);
            }

            Ok(await RedisNode.Docker(["exec", set._names[0], "redis-cli", "--cluster", "create", .. set.Ports.Take(3).Select(p => "127.0.0.1:" + p), "--cluster-yes"]));
            var id = await set.CliAsync(set.Ports[0], "cluster", "myid");
            Ok(await RedisNode.Docker("exec", set._names[0], "redis-cli", "--cluster", "add-node", "127.0.0.1:" + set.Ports[3], "127.0.0.1:" + set.Ports[0],
                "--cluster-slave", "--cluster-master-id", id));
            foreach (var port in set.Ports)
            {
                await set.WaitUntilAsync("cluster state ok", async () => (await set.CliAsync(port, "cluster", "info")).Contains("cluster_state:ok", StringComparison.Ordinal), 60);
            }

            await set.WaitUntilAsync("replica linked", async () => (await set.CliAsync(set.ReplicaPort, "info", "replication")).Contains("master_link_status:up", StringComparison.Ordinal), 60);
            return set;
        }
        catch
        {
            await set.DisposeAsync();
            throw;
        }
    }

    /// <summary>The first application name starting with <paramref name="prefix"/> whose slot is owned by primary <paramref name="index"/> (0–2).</summary>
    public static string ApplicationOn(int index, string prefix)
    {
        for (var i = 0; ; i++)
        {
            var name = prefix + i.ToString(CultureInfo.InvariantCulture);
            if (OwnerIndexOfSlot(Runwick.Redis.Store.RxClient.SlotOf(name)) == index)
            {
                return name;
            }
        }
    }

    public static int OwnerIndexOfSlot(int slot) => slot <= 5460 ? 0 : slot <= 10922 ? 1 : 2;

    public string NodeName(int port) => _names[Array.IndexOf(Ports, port)];

    public async Task<string> CliAsync(int port, params string[] command) =>
        (await RedisNode.Docker(["exec", NodeName(port), "redis-cli", "-p", port.ToString(CultureInfo.InvariantCulture), .. command])).Output.Trim().Replace("\r", "", StringComparison.Ordinal);

    public Task<string> NodeIdAsync(int port) => CliAsync(port, "cluster", "myid");

    /// <summary>The port whose node currently holds <paramref name="slot"/> as a primary, read from every live node's own view until one answers.</summary>
    public async Task<int> OwnerPortAsync(int slot)
    {
        foreach (var asking in Ports)
        {
            try
            {
                var text = await CliAsync(asking, "cluster", "nodes");
                foreach (var line in text.Split('\n', StringSplitOptions.RemoveEmptyEntries))
                {
                    var f = line.Split(' ');
                    if (f.Length < 9 || !f[2].Contains("master", StringComparison.Ordinal) || f[2].Contains("fail", StringComparison.Ordinal))
                    {
                        continue;
                    }

                    foreach (var token in f.Skip(8).Where(t => !t.StartsWith('[')))
                    {
                        var r = token.Split('-');
                        if (int.Parse(r[0], CultureInfo.InvariantCulture) <= slot && slot <= int.Parse(r[^1], CultureInfo.InvariantCulture))
                        {
                            return int.Parse(f[1].Split('@')[0].Split(':')[1], CultureInfo.InvariantCulture);
                        }
                    }
                }
            }
            catch (Exception ex) when (ex is FormatException or InvalidOperationException)
            {
                // That node is down or still joining: ask the next.
            }
        }

        throw new InvalidOperationException("No node owns slot " + slot);
    }

    public Task KillNodeAsync(int port) => RedisNode.Docker("kill", "-s", "KILL", NodeName(port));

    public Task PauseNodeAsync(int port) => RedisNode.Docker("pause", NodeName(port));

    public Task UnpauseNodeAsync(int port) => RedisNode.Docker("unpause", NodeName(port));

    /// <summary>Waits until the replica has applied everything its primary wrote.</summary>
    public async Task WaitReplicaSyncedAsync()
    {
        await WaitUntilAsync("replica caught up", async () =>
        {
            long Offset(string info, string field) =>
                long.Parse(info.Split('\n').First(l => l.StartsWith(field + ":", StringComparison.Ordinal))[(field.Length + 1)..].Trim(), CultureInfo.InvariantCulture);
            var primary = Offset(await CliAsync(Ports[0], "info", "replication"), "master_repl_offset");
            return Offset(await CliAsync(ReplicaPort, "info", "replication"), "slave_repl_offset") >= primary;
        }, 30);
    }

    public async Task<long> DbSizeAsync(int port) => long.Parse(await CliAsync(port, "dbsize"), CultureInfo.InvariantCulture);

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
            catch (Exception ex) when (ex is FormatException or InvalidOperationException or InvalidCastException or InvalidDataException)
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

    /// <summary>A port whose cluster-bus port (port + 10000) is also free; kept below the ephemeral range so the bus port stays a valid port.</summary>
    static int Free()
    {
        while (true)
        {
            var port = Random.Shared.Next(20000, 45000);
            try
            {
                using var a = new TcpListener(IPAddress.Loopback, port);
                using var b = new TcpListener(IPAddress.Loopback, port + 10000);
                a.Start();
                b.Start();
                return port;
            }
            catch (SocketException)
            {
                // In use: pick another.
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var name in _containers)
        {
            await RedisNode.Docker("rm", "-f", "-v", name);
        }
    }
}
