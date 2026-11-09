#!/usr/bin/env bash
# SP-030 smoke test: install the packed Runwick and Runwick.Sqlite packages from artifacts/packages into a new console
# project (outside the repo, with an empty NuGet cache so nothing comes from an earlier build), then dispatch 5 jobs to a
# temporary SQLite file and run them on an in-process worker. The job and wiring are the ones in docs/guide/getting-started.md.
# Usage: eng/smoke-package-install.sh [linux-x64|win-x64]   (default linux-x64; win-x64 runs from Git Bash on Windows)
# Needs the packages first: eng/check-release.sh (or `dotnet pack <each src project> -c Release -o artifacts/packages`).
set -euo pipefail
cd "$(dirname "$0")/.."
rid=${1:-linux-x64}
feed="$PWD/artifacts/packages"
version=$(sed -n 's:.*<Version>\(.*\)</Version>.*:\1:p' Directory.Build.props | head -1)
ls "$feed"/Runwick.Sqlite."$version".nupkg >/dev/null 2>&1 || { echo "no Runwick.Sqlite $version package in $feed; run eng/check-release.sh first"; exit 1; }
sdk=$(sed -n 's|.*"version": *"\([^"]*\)".*|\1|p' global.json | head -1)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
if command -v cygpath >/dev/null; then feed=$(cygpath -m "$feed"); work_native=$(cygpath -m "$work"); else work_native=$work; fi

cat > "$work/global.json" <<EOF
{ "sdk": { "version": "$sdk", "rollForward": "disable" } }
EOF
cat > "$work/nuget.config" <<EOF
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <clear />
    <add key="local" value="$feed" />
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
  </packageSources>
  <packageSourceMapping>
    <packageSource key="local"><package pattern="Runwick*" /></packageSource>
    <packageSource key="nuget.org"><package pattern="*" /></packageSource>
  </packageSourceMapping>
</configuration>
EOF
cat > "$work/FreshInstall.csproj" <<EOF
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net10.0</TargetFramework>
    <RuntimeIdentifier>$rid</RuntimeIdentifier>
    <SelfContained>false</SelfContained>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
    <TreatWarningsAsErrors>true</TreatWarningsAsErrors>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Runwick" Version="$version" />
    <PackageReference Include="Runwick.Sqlite" Version="$version" />
    <PackageReference Include="Microsoft.Extensions.Hosting" Version="10.0.0" />
  </ItemGroup>
</Project>
EOF
cat > "$work/Program.cs" <<'EOF'
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Runwick;

var dbPath = Path.Combine(Path.GetTempPath(), $"fresh-{Guid.NewGuid():N}.db");
var connectionString = $"Data Source={dbPath}";
await RunwickSqliteMigrations.MigrateAsync(connectionString);

var builder = Host.CreateApplicationBuilder(args);
builder.Logging.SetMinimumLevel(LogLevel.Warning);
builder.Services.AddRunwick(runwick =>
{
    runwick.Application("my-app")
        .UseSqlite(connectionString)
        .AddJobsFromAssemblyContaining<SendWelcomeEmail>();
    runwick.AddHandler<SendWelcomeEmail, SendWelcomeEmailHandler>();
    runwick.AddWorker(w => w.Pool("main", p => p.Queues("default").FixedConcurrency(4)));
});

using var host = builder.Build();
await host.StartAsync();
var dispatcher = host.Services.GetRequiredService<IJobDispatcher>();
for (var i = 0; i < SendWelcomeEmailHandler.Expected; i++)
{
    await dispatcher.DispatchAsync(new SendWelcomeEmail(Guid.NewGuid()));
}

var done = await Task.WhenAny(SendWelcomeEmailHandler.AllDone.Task, Task.Delay(TimeSpan.FromSeconds(60)));
await host.StopAsync();
foreach (var f in Directory.GetFiles(Path.GetDirectoryName(dbPath)!, Path.GetFileName(dbPath) + "*")) File.Delete(f);
var ok = done == SendWelcomeEmailHandler.AllDone.Task;
Console.WriteLine(ok ? $"{SendWelcomeEmailHandler.Expected} handled" : $"timed out: {SendWelcomeEmailHandler.Handled} handled");
return ok ? 0 : 1;

[JobName("email.welcome")]
public sealed record SendWelcomeEmail(Guid UserId) : IQueueJob;

public sealed class SendWelcomeEmailHandler : IJobHandler<SendWelcomeEmail>
{
    public const int Expected = 5;
    public static int Handled;
    public static readonly TaskCompletionSource AllDone = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task HandleAsync(SendWelcomeEmail job, CancellationToken cancellationToken)
    {
        if (Interlocked.Increment(ref Handled) == Expected) AllDone.TrySetResult();
        return Task.CompletedTask;
    }
}
EOF

export NUGET_PACKAGES="$work_native/.nuget"
(cd "$work" && dotnet run --nologo -v quiet) | tee "$work/out.txt"
grep -q '^5 handled$' "$work/out.txt"
echo "$rid package install smoke OK (Runwick $version from $feed)"
