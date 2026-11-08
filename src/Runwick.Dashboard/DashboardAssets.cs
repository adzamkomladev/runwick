using System.Security.Cryptography;
using System.Text.Json;

namespace Runwick.Dashboard;

/// <summary>
/// One immutable build of the dashboard client: the hashed files, the Vite manifest's entry and its stylesheets, and the asset
/// version every page reports. The version is a hash of every file, so any rebuild that changes a byte changes it.
/// </summary>
internal sealed class DashboardAssets
{
    private const string ResourcePrefix = "runwick-dashboard/";
    private readonly Dictionary<string, byte[]> _files;

    /// <summary>Creates a build from files keyed by their path in the build output (<c>manifest.json</c>, <c>assets/app-x.js</c>).</summary>
    public DashboardAssets(IReadOnlyDictionary<string, byte[]> files)
    {
        ArgumentNullException.ThrowIfNull(files);
        _files = new(files, StringComparer.Ordinal);
        if (!_files.TryGetValue("manifest.json", out var manifest))
        {
            throw new InvalidOperationException("The dashboard build has no manifest.json.");
        }

        using var document = JsonDocument.Parse(manifest);
        foreach (var chunk in document.RootElement.EnumerateObject())
        {
            if (chunk.Value.TryGetProperty("isEntry", out var isEntry) && isEntry.GetBoolean())
            {
                Entry = chunk.Value.GetProperty("file").GetString()!;
                Css = chunk.Value.TryGetProperty("css", out var css) ? [.. css.EnumerateArray().Select(c => c.GetString()!)] : [];
            }
        }

        if (Entry is null || !_files.ContainsKey(Entry) || Css.Any(c => !_files.ContainsKey(c)))
        {
            throw new InvalidOperationException("The dashboard manifest names an entry or stylesheet the build does not contain.");
        }

        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        foreach (var (path, bytes) in _files.OrderBy(f => f.Key, StringComparer.Ordinal))
        {
            hash.AppendData(System.Text.Encoding.UTF8.GetBytes(path));
            hash.AppendData(bytes);
        }

        Version = Convert.ToHexStringLower(hash.GetHashAndReset())[..16];
    }

    /// <summary>The build embedded in this assembly.</summary>
    public static DashboardAssets Embedded { get; } = LoadEmbedded();

    /// <summary>The asset version pages report and Inertia requests must match.</summary>
    public string Version { get; }

    /// <summary>The entry module, relative to the build root.</summary>
    public string Entry { get; } = null!;

    /// <summary>The entry's stylesheets, relative to the build root.</summary>
    public IReadOnlyList<string> Css { get; } = [];

    /// <summary>Every file of the build, keyed by path.</summary>
    public IReadOnlyDictionary<string, byte[]> Files => _files;

    internal bool TryGetAsset(string path, out byte[] bytes) =>
        _files.TryGetValue(path, out bytes!) && path.StartsWith("assets/", StringComparison.Ordinal);

    private static DashboardAssets LoadEmbedded()
    {
        var assembly = typeof(DashboardAssets).Assembly;
        var files = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        foreach (var name in assembly.GetManifestResourceNames().Where(n => n.StartsWith(ResourcePrefix, StringComparison.Ordinal)))
        {
            using var stream = assembly.GetManifestResourceStream(name)!;
            using var buffer = new MemoryStream();
            stream.CopyTo(buffer);
            files[name[ResourcePrefix.Length..].Replace('\\', '/')] = buffer.ToArray();
        }

        return new DashboardAssets(files);
    }
}
