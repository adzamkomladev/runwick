using System.Collections.Concurrent;
using System.Diagnostics;

namespace Runwick.Workloads;

/// <summary>A worker or producer child: this executable in another mode, driven over stdin/stdout.</summary>
internal sealed class ChildProcess : IAsyncDisposable
{
    readonly Process _process;
    readonly BlockingCollection<string> _lines = [];
    readonly StreamWriter _log;

    ChildProcess(string name, Process process, string logPath)
    {
        Name = name;
        _process = process;
        _log = new StreamWriter(logPath, append: false) { AutoFlush = true };
        process.OutputDataReceived += (_, e) => Line(e.Data, isError: false);
        process.ErrorDataReceived += (_, e) => Line(e.Data, isError: true);
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();
    }

    public string Name { get; }

    public int Id => _process.Id;

    public bool HasExited => _process.HasExited;

    public static ChildProcess Start(string name, string logPath, params string[] args)
    {
        var self = Environment.ProcessPath!;
        var info = new ProcessStartInfo
        {
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        if (Path.GetFileNameWithoutExtension(self) == "dotnet")
        {
            info.FileName = self;
            info.ArgumentList.Add(typeof(ChildProcess).Assembly.Location);
        }
        else
        {
            info.FileName = self;
        }

        foreach (var a in args)
        {
            info.ArgumentList.Add(a);
        }

        info.Environment["DOTNET_gcServer"] = "0";
        return new ChildProcess(name, Process.Start(info)!, logPath);
    }

    void Line(string? text, bool isError)
    {
        if (text is null)
        {
            return;
        }

        lock (_log)
        {
            _log.WriteLine((isError ? "! " : "  ") + text);
        }

        if (!isError)
        {
            _lines.Add(text);
        }
    }

    public async Task SendAsync(string line)
    {
        await _process.StandardInput.WriteLineAsync(line).ConfigureAwait(false);
        await _process.StandardInput.FlushAsync().ConfigureAwait(false);
    }

    public string WaitFor(string prefix, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (_lines.TryTake(out var line, TimeSpan.FromMilliseconds(200)) && line.StartsWith(prefix, StringComparison.Ordinal))
            {
                return line;
            }

            if (_process.HasExited && _lines.Count == 0)
            {
                throw new InvalidOperationException($"{Name} exited with {_process.ExitCode} before '{prefix}'");
            }
        }

        throw new TimeoutException($"{Name}: no '{prefix}' within {timeout}");
    }

    public async Task<int> StopAsync(TimeSpan timeout)
    {
        if (!_process.HasExited)
        {
            await SendAsync("stop").ConfigureAwait(false);
            using var cts = new CancellationTokenSource(timeout);
            try
            {
                await _process.WaitForExitAsync(cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                _process.Kill(entireProcessTree: true);
                await _process.WaitForExitAsync().ConfigureAwait(false);
                return -1;
            }
        }

        return _process.ExitCode;
    }

    public async ValueTask DisposeAsync()
    {
        if (!_process.HasExited)
        {
            _process.Kill(entireProcessTree: true);
            await _process.WaitForExitAsync().ConfigureAwait(false);
        }

        _process.Dispose();
        _lines.Dispose();
        await _log.DisposeAsync().ConfigureAwait(false);
    }
}
