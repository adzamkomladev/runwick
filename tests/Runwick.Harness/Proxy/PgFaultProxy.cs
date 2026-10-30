using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Runwick.Harness.Proxy;

public enum LatencyDirection { ClientToServer, ServerToClient, Both }

/// <summary>
/// A TCP relay that speaks enough of the PostgreSQL v3 wire protocol to find message boundaries.
/// It can forward a client's COMMIT to the server, wait for the server to finish it, and then
/// close the client socket before the reply arrives: a real "committed, reply lost" (DEP-04).
/// It can also add latency or refuse all traffic to simulate an outage.
/// </summary>
public sealed class PgFaultProxy : IAsyncDisposable
{
    const int SslRequestCode = 80877103;
    const int GssEncRequestCode = 80877104;
    const int CancelRequestCode = 80877102;

    readonly TcpListener _listener;
    string _targetHost;
    int _targetPort;
    Func<string?, bool> _latencyFilter = _ => true;
    LatencyDirection _latencyDirection = LatencyDirection.Both;
    readonly CancellationTokenSource _stop = new();
    readonly ConcurrentDictionary<int, Relay> _relays = new();
    readonly Task _acceptLoop;
    int _nextId;
    int _commitDropsArmed;
    Func<string?, bool> _commitDropFilter = _ => true;
    TaskCompletionSource _commitDropped = NewSignal();

    public PgFaultProxy(string targetHost, int targetPort)
    {
        _targetHost = targetHost;
        _targetPort = targetPort;
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        _acceptLoop = AcceptLoopAsync();
    }

    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

    /// <summary>Per-message latency (all connections, both directions unless set with <see cref="SetLatency"/>).</summary>
    public TimeSpan Latency { get; set; }

    /// <summary>Latency(delay, direction, roleFilter) mode of 07 §2.5; a null filter matches every connection.</summary>
    public void SetLatency(TimeSpan delay, LatencyDirection direction = LatencyDirection.Both, Func<string?, bool>? applicationNameFilter = null)
    {
        _latencyFilter = applicationNameFilter ?? (_ => true);
        _latencyDirection = direction;
        Latency = delay;
    }

    /// <summary>
    /// CommitReplyLatency(delay, roleFilter): holds the server's reply to each COMMIT on matching connections for
    /// <paramref name="delay"/>, leaving every other message on time (one late reply per store command; TIME-02).
    /// </summary>
    public void SetCommitReplyLatency(TimeSpan delay, Func<string?, bool>? applicationNameFilter = null)
    {
        _commitDelayFilter = applicationNameFilter ?? (_ => true);
        CommitReplyLatency = delay;
    }

    public TimeSpan CommitReplyLatency { get; private set; }

    Func<string?, bool> _commitDelayFilter = _ => true;

    bool CommitDelayApplies(string? applicationName) => CommitReplyLatency > TimeSpan.Zero && _commitDelayFilter(applicationName);

    /// <summary>Filter matching the <c>rw:&lt;identity&gt;:&lt;role&gt;</c> application-name convention.</summary>
    public static Func<string?, bool> Role(string role, string? identity = null) =>
        name => name is not null && name.EndsWith(":" + role, StringComparison.Ordinal)
            && (identity is null || name.StartsWith("rw:" + identity + ":", StringComparison.Ordinal));

    /// <summary>CommandComplete tags of the COMMITs whose replies were dropped (<c>COMMIT</c> or <c>ROLLBACK</c>).</summary>
    public IReadOnlyList<string> DroppedCommitTags => [.. _droppedTags];

    readonly ConcurrentQueue<string> _droppedTags = new();

    /// <summary>Sever(roleFilter): immediately closes matching client and server sockets.</summary>
    public int Sever(Func<string?, bool>? applicationNameFilter = null)
    {
        var count = 0;
        foreach (var relay in _relays.Values.Where(r => (applicationNameFilter ?? (_ => true))(r.ApplicationName)))
        {
            relay.Abort();
            count++;
        }

        return count;
    }

    /// <summary>Re-points new connections at a restarted server; live relays are cut.</summary>
    public void Retarget(string host, int port)
    {
        _targetHost = host;
        _targetPort = port;
        Sever();
    }

    bool DelayApplies(string? applicationName, bool clientToServer) =>
        Latency > TimeSpan.Zero
        && (_latencyDirection == LatencyDirection.Both || (_latencyDirection == LatencyDirection.ClientToServer) == clientToServer)
        && _latencyFilter(applicationName);

    /// <summary>When true, new connections are closed at once and live ones are cut.</summary>
    public bool DropEverything
    {
        get => _dropEverything;
        set
        {
            _dropEverything = value;
            if (value)
            {
                foreach (var relay in _relays.Values)
                {
                    relay.Abort();
                }
            }
        }
    }

    bool _dropEverything;

    /// <summary>Number of COMMIT replies dropped so far.</summary>
    public int CommitRepliesDropped => _commitRepliesDropped;

    int _commitRepliesDropped;

    /// <summary>
    /// Arms the next <paramref name="count"/> COMMITs (optionally only on connections whose
    /// application_name satisfies <paramref name="applicationNameFilter"/>) to lose their reply.
    /// </summary>
    public Task ArmCommitReplyDrop(int count = 1, Func<string?, bool>? applicationNameFilter = null)
    {
        _commitDropFilter = applicationNameFilter ?? (_ => true);
        _commitDropped = NewSignal();
        Volatile.Write(ref _commitDropsArmed, count);
        return _commitDropped.Task;
    }

    public void Disarm() => Volatile.Write(ref _commitDropsArmed, 0);

    public string Rewrite(string connectionString)
    {
        var parts = connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries)
            .Where(p => !p.TrimStart().StartsWith("Host=", StringComparison.OrdinalIgnoreCase)
                     && !p.TrimStart().StartsWith("Port=", StringComparison.OrdinalIgnoreCase)
                     && !p.TrimStart().StartsWith("Server=", StringComparison.OrdinalIgnoreCase))
            .ToList();
        parts.Add("Host=127.0.0.1");
        parts.Add($"Port={Port}");
        parts.Add("SSL Mode=Disable");
        parts.Add("GSS Encryption Mode=Disable");
        return string.Join(';', parts);
    }

    bool TryClaimCommitDrop(string? applicationName)
    {
        if (!_commitDropFilter(applicationName))
        {
            return false;
        }

        while (true)
        {
            var armed = Volatile.Read(ref _commitDropsArmed);
            if (armed <= 0)
            {
                return false;
            }

            if (Interlocked.CompareExchange(ref _commitDropsArmed, armed - 1, armed) == armed)
            {
                return true;
            }
        }
    }

    async Task AcceptLoopAsync()
    {
        while (!_stop.IsCancellationRequested)
        {
            TcpClient client;
            try
            {
                client = await _listener.AcceptTcpClientAsync(_stop.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (ObjectDisposedException)
            {
                return;
            }

            if (_dropEverything)
            {
                client.Dispose();
                continue;
            }

            var id = Interlocked.Increment(ref _nextId);
            var relay = new Relay(this, id, client);
            _relays[id] = relay;
            _ = relay.RunAsync().ContinueWith(_ => _relays.TryRemove(id, out var _), TaskScheduler.Default);
        }
    }

    static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync().ConfigureAwait(false);
        _listener.Stop();
        foreach (var relay in _relays.Values)
        {
            relay.Abort();
        }

        try
        {
            await _acceptLoop.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }

        _stop.Dispose();
    }

    sealed class Relay(PgFaultProxy owner, int id, TcpClient client)
    {
        readonly TcpClient _server = new() { NoDelay = true };
        readonly CancellationTokenSource _abort = new();
        volatile bool _commitInFlightToDrop;
        string? _applicationName;

        public int Id => id;

        public string? ApplicationName => _applicationName;

        public void Abort()
        {
            try
            {
                _abort.Cancel();
            }
            catch (ObjectDisposedException)
            {
            }

            client.Dispose();
            _server.Dispose();
        }

        public async Task RunAsync()
        {
            try
            {
                client.NoDelay = true;
                await _server.ConnectAsync(owner._targetHost, owner._targetPort, _abort.Token).ConfigureAwait(false);
                var c = client.GetStream();
                var s = _server.GetStream();
                var up = PumpClientToServerAsync(c, s);
                var down = PumpServerToClientAsync(s, c);
                await Task.WhenAny(up, down).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Relays end by closing sockets; errors are the expected way out.
            }
            finally
            {
                Abort();
                _abort.Dispose();
            }
        }

        async Task PumpClientToServerAsync(NetworkStream from, NetworkStream to)
        {
            // Untyped start-up phase: SSL/GSS negotiation requests and the StartupMessage.
            while (true)
            {
                var lengthBytes = await ReadExactAsync(from, 4).ConfigureAwait(false);
                var length = BinaryPrimitives.ReadInt32BigEndian(lengthBytes);
                var body = await ReadExactAsync(from, length - 4).ConfigureAwait(false);
                var code = BinaryPrimitives.ReadInt32BigEndian(body);
                await to.WriteAsync(lengthBytes, _abort.Token).ConfigureAwait(false);
                await to.WriteAsync(body, _abort.Token).ConfigureAwait(false);
                if (code is SslRequestCode or GssEncRequestCode)
                {
                    // The server answers with one untyped byte; the down pump handles it.
                    _awaitingNegotiationByte = true;
                    continue;
                }

                if (code == CancelRequestCode)
                {
                    return;
                }

                _applicationName = ParseApplicationName(body.AsSpan(4));
                break;
            }

            while (true)
            {
                var header = await ReadExactAsync(from, 5).ConfigureAwait(false);
                var length = BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(1));
                var body = await ReadExactAsync(from, length - 4).ConfigureAwait(false);
                if (owner.DelayApplies(_applicationName, clientToServer: true))
                {
                    await Task.Delay(owner.Latency, _abort.Token).ConfigureAwait(false);
                }

                if (IsCommit((char)header[0], body) && owner.TryClaimCommitDrop(_applicationName))
                {
                    _commitInFlightToDrop = true;
                }
                else if (IsCommit((char)header[0], body) && owner.CommitDelayApplies(_applicationName))
                {
                    _commitReplyToDelay = true;
                }

                await to.WriteAsync(header, _abort.Token).ConfigureAwait(false);
                await to.WriteAsync(body, _abort.Token).ConfigureAwait(false);
            }
        }

        volatile bool _awaitingNegotiationByte;
        volatile bool _commitReplyToDelay;

        async Task PumpServerToClientAsync(NetworkStream from, NetworkStream to)
        {
            var buffered = new List<byte[]>();
            while (true)
            {
                if (_awaitingNegotiationByte)
                {
                    var b = await ReadExactAsync(from, 1).ConfigureAwait(false);
                    _awaitingNegotiationByte = false;
                    await to.WriteAsync(b, _abort.Token).ConfigureAwait(false);
                    continue;
                }

                var header = await ReadExactAsync(from, 5).ConfigureAwait(false);
                var length = BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(1));
                var body = await ReadExactAsync(from, length - 4).ConfigureAwait(false);
                if (_commitInFlightToDrop)
                {
                    // Swallow everything up to ReadyForQuery: the server has finished the COMMIT.
                    if (header[0] == (byte)'C')
                    {
                        owner._droppedTags.Enqueue(Encoding.ASCII.GetString(body, 0, Math.Max(0, body.Length - 1)));
                    }

                    if (header[0] == (byte)'Z')
                    {
                        Interlocked.Increment(ref owner._commitRepliesDropped);
                        owner._commitDropped.TrySetResult();
                        Abort();
                        return;
                    }

                    continue;
                }

                if (owner.DelayApplies(_applicationName, clientToServer: false))
                {
                    await Task.Delay(owner.Latency, _abort.Token).ConfigureAwait(false);
                }

                if (_commitReplyToDelay && header[0] == (byte)'C')
                {
                    _commitReplyToDelay = false;
                    await Task.Delay(owner.CommitReplyLatency, _abort.Token).ConfigureAwait(false);
                }

                await to.WriteAsync(header, _abort.Token).ConfigureAwait(false);
                await to.WriteAsync(body, _abort.Token).ConfigureAwait(false);
            }
        }

        async Task<byte[]> ReadExactAsync(NetworkStream stream, int count)
        {
            var buffer = new byte[count];
            await stream.ReadExactlyAsync(buffer, _abort.Token).ConfigureAwait(false);
            return buffer;
        }

        static bool IsCommit(char type, byte[] body)
        {
            string? sql = type switch
            {
                // Simple query: null-terminated text.
                'Q' => Encoding.UTF8.GetString(body, 0, Math.Max(0, body.Length - 1)),
                // Parse: statement name\0 query\0 ...
                'P' => ReadParseQuery(body),
                _ => null,
            };
            if (sql is null)
            {
                return false;
            }

            var trimmed = sql.Trim().TrimEnd(';').Trim();
            return trimmed.Equals("COMMIT", StringComparison.OrdinalIgnoreCase)
                || trimmed.Equals("END", StringComparison.OrdinalIgnoreCase)
                || trimmed.StartsWith("COMMIT ", StringComparison.OrdinalIgnoreCase);
        }

        static string ReadParseQuery(byte[] body)
        {
            var nameEnd = Array.IndexOf(body, (byte)0);
            var queryEnd = Array.IndexOf(body, (byte)0, nameEnd + 1);
            return Encoding.UTF8.GetString(body, nameEnd + 1, queryEnd - nameEnd - 1);
        }

        static string? ParseApplicationName(ReadOnlySpan<byte> parameters)
        {
            var text = Encoding.UTF8.GetString(parameters);
            var items = text.Split('\0');
            for (var i = 0; i + 1 < items.Length; i += 2)
            {
                if (items[i] == "application_name")
                {
                    return items[i + 1];
                }
            }

            return null;
        }
    }
}
