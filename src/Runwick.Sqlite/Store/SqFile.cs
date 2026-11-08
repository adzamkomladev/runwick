using System.Globalization;
using Microsoft.Data.Sqlite;

namespace Runwick.Sqlite.Store;

/// <summary>A startup or profile check failed (20 §2): the code is the ConfigurationError or UnsupportedVersion code.</summary>
internal sealed class SqProfileException(string code, string message) : Exception(message)
{
    public string Code { get; } = code;
}

/// <summary>Connection-string rules, per-connection settings, topology and engine checks (20 §2, §1).</summary>
internal static class SqFile
{
    /// <summary>Minimum engine: STRICT (3.37), RETURNING (3.35), built-in JSON (3.38) (20 §1).</summary>
    public static readonly Version MinimumEngine = new(3, 38, 0);

    /// <summary>Test seam (SQL-08): replaces <c>sqlite_version()</c> in the engine check.</summary>
    internal static Func<string, string>? VersionOverride { get; set; }

    static readonly string[] NetworkFileSystems =
        ["nfs", "nfs4", "cifs", "smb3", "smbfs", "9p", "fuse.sshfs", "afs", "ceph", "glusterfs", "lustre", "gpfs", "fuse.s3fs"];

    static readonly string[] LocalFileSystems = ["ext4", "xfs", "btrfs", "zfs", "tmpfs", "overlay", "ntfs", "refs"];

    /// <summary>Test seam (SQL-06): the mountinfo text read instead of <c>/proc/self/mountinfo</c>.</summary>
    internal static Func<string?>? MountInfoOverride { get; set; }

    /// <summary>
    /// Validates the configured string and returns the runtime form: <c>Pooling=False</c>, the given mode, the full path.
    /// Rejects the keywords that change semantics with <c>invalid_policy</c> (20 §2.1).
    /// </summary>
    public static string Normalize(string connectionString, SqliteOpenMode mode)
    {
        SqliteConnectionStringBuilder csb;
        try
        {
            csb = new SqliteConnectionStringBuilder(connectionString);
        }
        catch (ArgumentException ex)
        {
            throw new SqProfileException("invalid_policy", "The SQLite connection string is invalid: " + ex.Message);
        }

        if (string.IsNullOrWhiteSpace(csb.DataSource) || csb.DataSource.Trim() == ":memory:")
        {
            throw new SqProfileException("invalid_policy", "The SQLite store needs a database file (Data Source=<path>), not an in-memory database.");
        }

        if (csb.Mode is SqliteOpenMode.Memory or SqliteOpenMode.ReadOnly)
        {
            throw new SqProfileException("invalid_policy", $"Mode={csb.Mode} is not supported by the SQLite store.");
        }

        if (csb.Cache == SqliteCacheMode.Shared)
        {
            throw new SqProfileException("invalid_policy", "Cache=Shared is not supported by the SQLite store.");
        }

        if (csb.ForeignKeys == false)
        {
            throw new SqProfileException("invalid_policy", "Foreign Keys=False is not supported by the SQLite store.");
        }

        csb.DataSource = Path.GetFullPath(csb.DataSource);
        csb.Mode = mode;
        csb.Pooling = false;
        csb.ForeignKeys = true;

        // Microsoft.Data.Sqlite retries a busy statement until the command timeout (0 = forever); the wait is busy_timeout's.
        csb.DefaultTimeout = 1;
        return csb.ConnectionString;
    }

    public static string FilePath(string connectionString) => new SqliteConnectionStringBuilder(connectionString).DataSource;

    /// <summary>Opens a driver connection and applies 20 §2.2; <paramref name="checkWal"/> is false only for the migration.</summary>
    public static SqliteConnection Open(string connectionString, int busyTimeoutMs, bool checkWal = true)
    {
        var conn = new SqliteConnection(connectionString);
        try
        {
            conn.Open();
            Exec(conn, $"""
                PRAGMA foreign_keys = ON;
                PRAGMA synchronous = FULL;
                PRAGMA busy_timeout = {busyTimeoutMs.ToString(CultureInfo.InvariantCulture)};
                PRAGMA trusted_schema = OFF;
                PRAGMA journal_size_limit = 67108864;
                """);
            CheckProfile(conn, checkWal);
            return conn;
        }
        catch
        {
            conn.Dispose();
            throw;
        }
    }

    /// <summary>Reads back journal_mode (wal), synchronous (2) and foreign_keys (1); a mismatch is store_profile_mismatch.</summary>
    public static void CheckProfile(SqliteConnection conn, bool checkWal = true)
    {
        var journal = Scalar(conn, "PRAGMA journal_mode");
        var sync = Scalar(conn, "PRAGMA synchronous");
        var fk = Scalar(conn, "PRAGMA foreign_keys");
        if ((checkWal && !string.Equals(journal, "wal", StringComparison.OrdinalIgnoreCase)) || sync != "2" || fk != "1")
        {
            throw new SqProfileException("store_profile_mismatch",
                $"The SQLite connection profile is journal_mode={journal}, synchronous={sync}, foreign_keys={fk}; the store needs wal, 2 (FULL), 1.");
        }
    }

    public static void SetBusyTimeout(SqliteConnection conn, long ms) =>
        Exec(conn, "PRAGMA busy_timeout = " + Math.Max(0, ms).ToString(CultureInfo.InvariantCulture));

    /// <summary><c>sqlite_version()</c> ≥ 3.38.0 or <c>engine_version_unsupported</c> (20 §1, DEP-06).</summary>
    public static (string Version, string SourceId) CheckEngine(SqliteConnection conn)
    {
        var version = Scalar(conn, "SELECT sqlite_version()")!;
        var sourceId = Scalar(conn, "SELECT sqlite_source_id()")!;
        var effective = VersionOverride?.Invoke(version) ?? version;
        if (!Version.TryParse(effective, out var v) || v < MinimumEngine)
        {
            throw new SqProfileException("engine_version_unsupported",
                $"SQLite engine {effective} is older than the required {MinimumEngine}.");
        }

        return (effective, sourceId);
    }

    /// <summary>
    /// Topology check (20 §2.3): a network filesystem is <c>topology_unsupported</c>; returns <c>local</c> for a known local
    /// filesystem and <c>unverified</c> otherwise.
    /// </summary>
    public static string CheckTopology(string path)
    {
        var full = Path.GetFullPath(path);
        full = ResolveLinks(full);
        if (OperatingSystem.IsWindows())
        {
            if (full.StartsWith(@"\\", StringComparison.Ordinal) || new DriveInfo(Path.GetPathRoot(full)!).DriveType == DriveType.Network)
            {
                throw new SqProfileException("topology_unsupported", "The SQLite database is on a network share (UNC or network drive).");
            }

            return "local";
        }

        if (!OperatingSystem.IsLinux())
        {
            return "unverified";
        }

        var info = MountInfoOverride is { } o ? o() : TryRead("/proc/self/mountinfo");
        if (info is null)
        {
            return "unverified";
        }

        var fsType = FileSystemOf(info, full);
        if (fsType is null)
        {
            return "unverified";
        }

        if (NetworkFileSystems.Contains(fsType, StringComparer.Ordinal))
        {
            throw new SqProfileException("topology_unsupported", $"The SQLite database is on a '{fsType}' filesystem; only a local filesystem is supported.");
        }

        return LocalFileSystems.Contains(fsType, StringComparer.Ordinal) ? "local" : "unverified";
    }

    /// <summary>The filesystem type of the longest mount point containing <paramref name="path"/> in mountinfo text.</summary>
    internal static string? FileSystemOf(string mountInfo, string path)
    {
        string? best = null;
        var bestLength = -1;
        foreach (var line in mountInfo.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            // id parent major:minor root mountpoint options [optional...] - fstype source superoptions
            var parts = line.Split(' ');
            var dash = Array.IndexOf(parts, "-");
            if (parts.Length < 5 || dash < 0 || dash + 1 >= parts.Length)
            {
                continue;
            }

            var mount = Unescape(parts[4]);
            var inside = mount == "/" || path == mount || path.StartsWith(mount.TrimEnd('/') + "/", StringComparison.Ordinal);
            if (inside && mount.Length > bestLength)
            {
                best = parts[dash + 1];
                bestLength = mount.Length;
            }
        }

        return best;
    }

    static string Unescape(string s) => s.Replace("\\040", " ", StringComparison.Ordinal).Replace("\\011", "\t", StringComparison.Ordinal)
        .Replace("\\012", "\n", StringComparison.Ordinal).Replace("\\134", "\\", StringComparison.Ordinal);

    static string ResolveLinks(string full)
    {
        try
        {
            var dir = new DirectoryInfo(Path.GetDirectoryName(full)!);
            var target = dir.ResolveLinkTarget(returnFinalTarget: true);
            var resolvedDir = target?.FullName ?? dir.FullName;
            var file = new FileInfo(Path.Combine(resolvedDir, Path.GetFileName(full)));
            return file.Exists ? file.ResolveLinkTarget(returnFinalTarget: true)?.FullName ?? file.FullName : file.FullName;
        }
        catch (IOException)
        {
            return full;
        }
    }

    static string? TryRead(string path)
    {
        try
        {
            return File.ReadAllText(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return null;
        }
    }

    public static void Exec(SqliteConnection conn, string sql, SqliteTransaction? tx = null)
    {
        using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    public static string? Scalar(SqliteConnection conn, string sql, SqliteTransaction? tx = null)
    {
        using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = sql;
        return Convert.ToString(cmd.ExecuteScalar(), CultureInfo.InvariantCulture);
    }
}
