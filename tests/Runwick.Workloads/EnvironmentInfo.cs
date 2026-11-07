using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text.Json.Nodes;
using Npgsql;
using Runwick.Harness.Store;

namespace Runwick.Workloads;

/// <summary>07 §14.2 environment fields, captured at the start of every run.</summary>
internal static class EnvironmentInfo
{
    public static async Task<JsonObject> CaptureAsync(TestDatabase db, string adminConnectionString, Args a)
    {
        var settings = new JsonObject();
        string version;
        await using (var conn = new NpgsqlConnection(adminConnectionString))
        {
            await conn.OpenAsync().ConfigureAwait(false);
            await using (var cmd = new NpgsqlCommand("SELECT version()", conn))
            {
                version = (string)(await cmd.ExecuteScalarAsync().ConfigureAwait(false))!;
            }

            foreach (var name in new[] { "fsync", "synchronous_commit", "full_page_writes", "shared_buffers", "max_connections",
                         "wal_level", "wal_buffers", "wal_sync_method", "max_wal_size", "checkpoint_timeout", "work_mem", "effective_io_concurrency" })
            {
                await using var cmd = new NpgsqlCommand($"SHOW {name}", conn);
                settings[name] = (string?)await cmd.ExecuteScalarAsync().ConfigureAwait(false);
            }
        }

        var rtt = await MeasureRttAsync(adminConnectionString).ConfigureAwait(false);
        return new JsonObject
        {
            ["capturedAt"] = DateTimeOffset.UtcNow.ToString("O"),
            ["os"] = RuntimeInformation.OSDescription,
            ["kernel"] = Read("/proc/sys/kernel/osrelease"),
            ["cpuModel"] = File.ReadLines("/proc/cpuinfo").FirstOrDefault(l => l.StartsWith("model name", StringComparison.Ordinal))?.Split(':', 2)[1].Trim(),
            ["cpuCount"] = Environment.ProcessorCount,
            ["memTotal"] = File.ReadLines("/proc/meminfo").First().Split(':', 2)[1].Trim(),
            ["loadAverageAtStart"] = Read("/proc/loadavg"),
            ["cgroupCpuMax"] = Read("/sys/fs/cgroup/cpu.max"),
            ["cgroupMemoryMax"] = Read("/sys/fs/cgroup/memory.max"),
            ["postgresContainerLimits"] = db.CanRestart ? "none (Testcontainers default)" : "external server",
            ["disk"] = Shell("lsblk -d -o NAME,ROTA,TYPE,SIZE,MODEL 2>/dev/null; df -T /var/lib/docker 2>/dev/null | tail -1"),
            ["postgresVersion"] = version,
            ["postgresImage"] = db.CanRestart ? TestDatabase.Image : "external",
            ["postgresImageDigest"] = db.CanRestart ? Shell($"docker image inspect --format '{{{{index .RepoDigests 0}}}}' {TestDatabase.Image} 2>/dev/null") : "n/a",
            ["postgresSettings"] = settings,
            ["npgsql"] = typeof(NpgsqlConnection).Assembly.GetName().Version?.ToString(),
            ["dotnet"] = RuntimeInformation.FrameworkDescription,
            ["globalJson"] = Shell("cat global.json 2>/dev/null | tr -d '\\n '"),
            ["commit"] = Shell("git rev-parse HEAD 2>/dev/null"),
            ["uncommittedFiles"] = Shell("git status --porcelain 2>/dev/null | wc -l"),
            ["seed"] = a.Int("seed", 20261030),
            ["arguments"] = string.Join(' ', Environment.GetCommandLineArgs().Skip(1)),
            ["topology"] = "direct primary, PostgreSQL in a local Docker container on the same host as the worker and producer",
            ["rttMs"] = rtt,
            ["connectionBudgets"] = "control 8, work 8, listener 0 (no notifications in M2)",
            ["referenceEnvironment"] = false,
        };
    }

    static async Task<JsonObject> MeasureRttAsync(string cs)
    {
        await using var conn = new NpgsqlConnection(cs);
        await conn.OpenAsync().ConfigureAwait(false);
        var samples = new List<double>();
        for (var i = 0; i < 200; i++)
        {
            var t = Stopwatch.GetTimestamp();
            await using var cmd = new NpgsqlCommand("SELECT 1", conn);
            await cmd.ExecuteScalarAsync().ConfigureAwait(false);
            samples.Add(Stopwatch.GetElapsedTime(t).TotalMilliseconds);
        }

        samples.Sort();
        return new JsonObject
        {
            ["selectOneP50"] = Math.Round(samples[99], 3),
            ["selectOneP99"] = Math.Round(samples[197], 3),
            ["selectOneMax"] = Math.Round(samples[^1], 3),
        };
    }

    public static string Read(string path)
    {
        try
        {
            return File.ReadAllText(path).Trim();
        }
        catch (IOException)
        {
            return "n/a";
        }
        catch (UnauthorizedAccessException)
        {
            return "n/a";
        }
    }

    public static string Shell(string command)
    {
        using var p = Process.Start(new ProcessStartInfo("sh", ["-c", command]) { RedirectStandardOutput = true, UseShellExecute = false })!;
        var output = p.StandardOutput.ReadToEnd();
        p.WaitForExit();
        return output.Trim();
    }
}
