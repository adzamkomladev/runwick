using System.Diagnostics;
using System.Reflection;
using System.Text.Json;
using System.Text.RegularExpressions;
using Runwick.Core.Tests.Support;

namespace Runwick.Core.Tests;

/// <summary>
/// DEP-01 rerun for SP-015 (provider-dependency-plan §7, 15 §3, §8): the package graph including Cronos and NodaTime
/// restores to the exact pins and keeps its boundaries. Cronos and NodaTime are internal dependencies of
/// <c>Runwick</c> only; no type from either is reachable from <c>Runwick.Contracts</c> or any public API (12 §1).
/// </summary>
[Trait("Category", "Pure")]
[Trait("Item", "SP-015")]
[Trait("Case", "DEP-01")]
public sealed class PackageGraphTests
{
    static readonly string[] EvaluatorPackages = ["Cronos", "NodaTime"];

    static JsonElement Graph() => JsonDocument.Parse(File.ReadAllBytes(Path.Combine(Repo.Root, "eng", "package-graph.json"))).RootElement;

    [Fact]
    public void The_graph_check_passes_with_the_evaluator_packages_allowed_for_runwick_only()
    {
        ProcessStartInfo start = new("bash", [Path.Combine(Repo.Root, "eng", "check-package-graph.sh")])
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            WorkingDirectory = Repo.Root,
        };
        Process? process;
        try
        {
            process = Process.Start(start);
        }
        catch (System.ComponentModel.Win32Exception)
        {
            Assert.Skip("bash is not installed; CI runs eng/check-package-graph.sh separately.");
            return;
        }

        using (process)
        {
            Assert.NotNull(process);
            var output = process.StandardOutput.ReadToEnd() + process.StandardError.ReadToEnd();
            process.WaitForExit();
            Assert.True(process.ExitCode == 0, output);
            Assert.Contains("package graph OK", output, StringComparison.Ordinal);
        }

        foreach (var package in Graph().GetProperty("packages").EnumerateObject())
        {
            var allowed = package.Value.GetProperty("allowedPackages").EnumerateArray().Select(p => p.GetString()).ToList();
            foreach (var evaluator in EvaluatorPackages)
            {
                Assert.True(allowed.Contains(evaluator) == (package.Name == "Runwick"), $"{package.Name} allows {evaluator}: {allowed.Contains(evaluator)}");
            }
        }
    }

    [Fact]
    public void Central_pins_lock_files_and_the_graph_agree_exactly()
    {
        var pins = Graph().GetProperty("pins");
        var props = File.ReadAllText(Path.Combine(Repo.Root, "Directory.Packages.props"));
        foreach (var package in EvaluatorPackages)
        {
            var pinned = pins.GetProperty(package).GetString();
            Assert.Equal(pinned, Regex.Match(props, $"""<PackageVersion Include="{package}" Version="([^"]+)" />""").Groups[1].Value);

            // Direct and exact in Runwick, transitive (never direct) wherever else it flows.
            foreach (var lockFile in Directory.GetFiles(Path.Combine(Repo.Root, "src"), "packages.lock.json", SearchOption.AllDirectories))
            {
                var project = Path.GetFileName(Path.GetDirectoryName(lockFile)!);
                using var doc = JsonDocument.Parse(File.ReadAllBytes(lockFile));
                if (project != "Runwick" && !doc.RootElement.GetProperty("dependencies").EnumerateObject().Any(f => f.Value.TryGetProperty("runwick", out _)))
                {
                    continue; // does not build on Runwick (the dashboard spike), so the evaluators never flow into it
                }

                foreach (var framework in doc.RootElement.GetProperty("dependencies").EnumerateObject())
                {
                    if (!framework.Value.TryGetProperty(package, out var entry))
                    {
                        Assert.True(project == "Runwick.Contracts", $"{project} does not resolve {package}");
                        continue;
                    }

                    Assert.NotEqual("Runwick.Contracts", project);
                    Assert.Equal(pinned, entry.GetProperty("resolved").GetString());
                    var type = entry.GetProperty("type").GetString();
                    Assert.True(project == "Runwick" ? type == "Direct" : type is "Transitive" or "CentralTransitive", $"{project}: {package} is {type}");
                }
            }
        }

        foreach (var csproj in Directory.GetFiles(Path.Combine(Repo.Root, "src"), "*.csproj", SearchOption.AllDirectories))
        {
            var text = File.ReadAllText(csproj);
            foreach (var package in EvaluatorPackages)
            {
                Assert.True(text.Contains($"""<PackageReference Include="{package}" """.TrimEnd(), StringComparison.Ordinal)
                    == (Path.GetFileNameWithoutExtension(csproj) == "Runwick"), $"{csproj} references {package}");
            }
        }
    }

    [Fact]
    public void No_public_api_exposes_a_cronos_or_nodatime_type()
    {
        var runwick = typeof(Runwick.Scheduling.Evaluation.CronGrammar).Assembly;
        var contracts = typeof(IQueueJob).Assembly;

        Assert.Contains(runwick.GetReferencedAssemblies(), a => a.Name == "Cronos");
        Assert.Contains(runwick.GetReferencedAssemblies(), a => a.Name == "NodaTime");
        Assert.DoesNotContain(contracts.GetReferencedAssemblies(), a => a.Name is "Cronos" or "NodaTime");

        var leaks = new List<string>();
        foreach (var type in runwick.GetExportedTypes().Concat(contracts.GetExportedTypes()))
        {
            foreach (var (where, used) in Surface(type))
            {
                if (Leaks(used))
                {
                    leaks.Add($"{type.FullName}.{where}: {used.FullName}");
                }
            }
        }

        Assert.Empty(leaks);
    }

    static bool Leaks(Type type)
    {
        if (type.HasElementType)
        {
            return Leaks(type.GetElementType()!);
        }

        if (type.IsGenericType && type.GetGenericArguments().Any(Leaks))
        {
            return true;
        }

        var assembly = type.Assembly.GetName().Name;
        return assembly is "Cronos" or "NodaTime";
    }

    static IEnumerable<(string Where, Type Used)> Surface(Type type)
    {
        const BindingFlags Visible = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly;
        if (type.BaseType is { } baseType)
        {
            yield return ("<base>", baseType);
        }

        foreach (var i in type.GetInterfaces())
        {
            yield return ("<interface>", i);
        }

        foreach (var member in type.GetMembers(Visible))
        {
            switch (member)
            {
                case MethodBase m when m.IsPublic || m.IsFamily || m.IsFamilyOrAssembly:
                    if (m is MethodInfo mi)
                    {
                        yield return (m.Name, mi.ReturnType);
                    }

                    foreach (var p in m.GetParameters())
                    {
                        yield return (m.Name + "(" + p.Name + ")", p.ParameterType);
                    }

                    break;
                case FieldInfo fi when fi.IsPublic || fi.IsFamily || fi.IsFamilyOrAssembly:
                    yield return (fi.Name, fi.FieldType);
                    break;
                case PropertyInfo pi when (pi.GetMethod ?? pi.SetMethod) is { } accessor && (accessor.IsPublic || accessor.IsFamily || accessor.IsFamilyOrAssembly):
                    yield return (pi.Name, pi.PropertyType);
                    break;
                case EventInfo ei when ei.AddMethod is { } add && (add.IsPublic || add.IsFamily):
                    yield return (ei.Name, ei.EventHandlerType!);
                    break;
            }
        }
    }
}
