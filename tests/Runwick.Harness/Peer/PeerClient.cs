using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Channels;

namespace Runwick.Harness.Peer;

/// <summary>A peer event (<c>atBarrier</c>, <c>handlerInvoked</c>, <c>log</c>) emitted on stdout.</summary>
internal sealed record PeerEvent(string Type, JsonObject Body);

/// <summary>Fixture side of the JSON Lines control channel to a <c>Runwick.Harness.Peer</c> process (07 §2.4).</summary>
internal sealed class PeerClient : IAsyncDisposable
{
    readonly Process _process;
    readonly ConcurrentDictionary<long, TaskCompletionSource<JsonObject>> _pending = new();
    readonly Channel<PeerEvent> _events = Channel.CreateUnbounded<PeerEvent>();
    readonly ConcurrentQueue<string> _stderr = new();
    readonly Task _reader;
    long _nextId;

    PeerClient(string identity, Process process)
    {
        Identity = identity;
        _process = process;
        _reader = ReadLoopAsync();
        process.ErrorDataReceived += (_, e) =>
        {
            if (e.Data is not null)
            {
                _stderr.Enqueue(e.Data);
            }
        };
        process.BeginErrorReadLine();
    }

    public string Identity { get; }

    public int ProcessId => _process.Id;

    public bool HasExited => _process.HasExited;

    public IReadOnlyCollection<string> StandardError => _stderr;

    public ChannelReader<PeerEvent> Events => _events.Reader;

    static string PeerAssembly()
    {
        var harnessDir = Path.GetDirectoryName(typeof(PeerClient).Assembly.Location)!;
        // tests/<Project>/bin/<Config>/<tfm>/ → tests/Runwick.Harness.Peer/bin/<Config>/<tfm>/Runwick.Harness.Peer.dll
        var tfm = new DirectoryInfo(harnessDir);
        var config = tfm.Parent!;
        var testsDir = config.Parent!.Parent!.Parent!;
        var candidate = Path.Combine(testsDir.FullName, "Runwick.Harness.Peer", "bin", config.Name, tfm.Name, "Runwick.Harness.Peer.dll");
        return File.Exists(candidate)
            ? candidate
            : throw new FileNotFoundException("Build tests/Runwick.Harness.Peer before running peer tests.", candidate);
    }

    public static Task<PeerClient> StartAsync(string identity)
    {
        var psi = new ProcessStartInfo("dotnet")
        {
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        psi.ArgumentList.Add(PeerAssembly());
        psi.ArgumentList.Add(identity);
        var process = Process.Start(psi) ?? throw new InvalidOperationException("Could not start the peer process.");
        return Task.FromResult(new PeerClient(identity, process));
    }

    async Task ReadLoopAsync()
    {
        try
        {
            while (await _process.StandardOutput.ReadLineAsync().ConfigureAwait(false) is { } line)
            {
                if (JsonNode.Parse(line) is not JsonObject msg)
                {
                    continue;
                }

                var type = msg["type"]?.GetValue<string>();
                if (type == "reply" && msg["id"]?.GetValue<long>() is { } id && _pending.TryRemove(id, out var tcs))
                {
                    tcs.TrySetResult(msg);
                }
                else if (type is not null)
                {
                    await _events.Writer.WriteAsync(new PeerEvent(type, msg)).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            foreach (var tcs in _pending.Values)
            {
                tcs.TrySetException(new IOException($"Peer {Identity} exited. stderr: {string.Join(" | ", _stderr.TakeLast(5))}"));
            }

            _events.Writer.TryComplete();
        }
    }

    /// <summary>Sends a command and awaits its reply; a failed reply throws <see cref="PeerCommandException"/>.</summary>
    public async Task<JsonObject> SendAsync(string cmd, JsonObject? args = null, TimeSpan? timeout = null)
    {
        var id = Interlocked.Increment(ref _nextId);
        var msg = args?.DeepClone().AsObject() ?? [];
        msg["id"] = id;
        msg["cmd"] = cmd;
        var tcs = new TaskCompletionSource<JsonObject>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pending[id] = tcs;
        await _process.StandardInput.WriteLineAsync(msg.ToJsonString()).ConfigureAwait(false);
        await _process.StandardInput.FlushAsync().ConfigureAwait(false);
        var reply = await tcs.Task.WaitAsync(timeout ?? TimeSpan.FromSeconds(60)).ConfigureAwait(false);
        return reply["ok"]?.GetValue<bool>() == true
            ? reply["result"] as JsonObject ?? []
            : throw new PeerCommandException(cmd, reply["error"]?.GetValue<string>() ?? "unknown error");
    }

    /// <summary>Sends without waiting for the reply (used when the command will block at a barrier or be killed).</summary>
    public Task<JsonObject> SendInBackground(string cmd, JsonObject? args = null) => SendAsync(cmd, args, TimeSpan.FromMinutes(5));

    public Task OpenAsync(string connectionString, string application, int maxExecutions) =>
        SendAsync("open", new JsonObject { ["cs"] = connectionString, ["app"] = application, ["maxExecutions"] = maxExecutions });

    /// <summary>Waits for the next event of a type (default 30 s, then a test failure).</summary>
    public async Task<PeerEvent> WaitForEventAsync(string type, Func<PeerEvent, bool>? match = null, TimeSpan? timeout = null)
    {
        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(30));
        try
        {
            while (true)
            {
                var e = await _events.Reader.ReadAsync(cts.Token).ConfigureAwait(false);
                if (e.Type == type && (match?.Invoke(e) ?? true))
                {
                    return e;
                }
            }
        }
        catch (OperationCanceledException)
        {
            throw new TimeoutException($"Peer {Identity} did not emit '{type}' in time.");
        }
    }

    /// <summary>SIGKILL with the whole process tree; no finally/dispose runs in the peer.</summary>
    public void Kill()
    {
        if (!_process.HasExited)
        {
            _process.Kill(entireProcessTree: true);
            _process.WaitForExit(10_000);
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (!_process.HasExited)
            {
                _process.StandardInput.Close();
                if (!_process.WaitForExit(5_000))
                {
                    Kill();
                }
            }

            await _reader.ConfigureAwait(false);
        }
        catch (IOException)
        {
        }
        finally
        {
            _process.Dispose();
        }
    }
}

internal sealed class PeerCommandException(string command, string error) : Exception($"Peer command '{command}' failed: {error}")
{
    public string Error => error;
}

/// <summary>Lease handle and value conversions shared by both sides of the channel.</summary>
internal static class PeerJson
{
    public static JsonObject Lease(Protocol.LeaseHandle h) => new()
    {
        ["jobId"] = h.JobId.ToString("D"),
        ["attemptId"] = h.AttemptId.ToString("D"),
        ["workerInstanceId"] = h.WorkerInstanceId.ToString("D"),
        ["ownershipToken"] = h.OwnershipToken.ToString("D"),
        ["generation"] = h.Generation,
    };

    public static Protocol.LeaseHandle Lease(JsonNode n) => new(
        Guid.Parse(n["jobId"]!.GetValue<string>()), Guid.Parse(n["attemptId"]!.GetValue<string>()),
        Guid.Parse(n["workerInstanceId"]!.GetValue<string>()), Guid.Parse(n["ownershipToken"]!.GetValue<string>()),
        n["generation"]!.GetValue<int>());

    public static JsonObject Reply<T>(Protocol.StoreReply<T> r) where T : class => new()
    {
        ["status"] = r.Status.ToString(),
        ["effect"] = r.Effect.ToString(),
        ["replayed"] = r.Replayed,
        ["code"] = r.Error?.Code,
        ["decidedAt"] = r.DecidedAt.ToUnixTimeMilliseconds(),
    };

    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);
}
