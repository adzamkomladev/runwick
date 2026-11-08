using System.Buffers.Text;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;

namespace Runwick.Management;

/// <summary>Seals and opens list cursors (16 §2.1). A host may replace the default HMAC protector (Data Protection).</summary>
internal interface IManagementCursorProtector
{
    byte[] Protect(byte[] payload);

    /// <summary>The payload, or null when the tag does not verify.</summary>
    byte[]? Unprotect(byte[] sealedCursor);
}

/// <summary><c>payload ‖ HMAC-SHA256(key, payload)</c>.</summary>
internal sealed class HmacCursorProtector : IManagementCursorProtector
{
    readonly byte[] _key;

    public HmacCursorProtector(byte[]? key)
    {
        if (key is not null && key.Length < 32)
        {
            throw new ArgumentException("ManagementOptions.CursorKey must be at least 32 bytes.", nameof(key));
        }

        // shortcut: without a configured key, cursors are valid in this process only; hosts share one through CursorKey or Data Protection.
        _key = key ?? RandomNumberGenerator.GetBytes(32);
    }

    public byte[] Protect(byte[] payload) => [.. payload, .. HMACSHA256.HashData(_key, payload)];

    public byte[]? Unprotect(byte[] sealedCursor)
    {
        if (sealedCursor.Length <= 32)
        {
            return null;
        }

        var payload = sealedCursor[..^32];
        return CryptographicOperations.FixedTimeEquals(HMACSHA256.HashData(_key, payload), sealedCursor.AsSpan(^32)) ? payload : null;
    }
}

/// <summary>
/// Cursor payload: format version 1, read name, application, normalized filters, the caller's allowed-queue hash,
/// expiry (15 min) and the last row's sort key. Anything that does not match is <c>invalid_cursor</c>.
/// </summary>
internal sealed class ManagementCursors(IManagementCursorProtector protector, TimeProvider time)
{
    public const int MaxLength = 2048;
    static readonly TimeSpan Lifetime = TimeSpan.FromMinutes(15);

    public string Seal(string read, string application, string filters, string queueHash, IReadOnlyList<string> key)
    {
        var json = new JsonObject
        {
            ["v"] = 1, ["r"] = read, ["a"] = application, ["f"] = filters, ["q"] = queueHash,
            ["e"] = (time.GetUtcNow() + Lifetime).ToUnixTimeMilliseconds(),
            ["k"] = new JsonArray([.. key.Select(k => (JsonNode?)k)]),
        };
        return Base64Url.EncodeToString(protector.Protect(Encoding.UTF8.GetBytes(json.ToJsonString())));
    }

    public IReadOnlyList<string> Open(string cursor, string read, string application, string filters, string queueHash)
    {
        try
        {
            if (cursor.Length <= MaxLength && protector.Unprotect(Base64Url.DecodeFromChars(cursor)) is { } payload
                && JsonNode.Parse(payload) is JsonObject o
                && o["v"]?.GetValue<int>() == 1 && o["r"]?.GetValue<string>() == read && o["a"]?.GetValue<string>() == application
                && o["f"]?.GetValue<string>() == filters && o["q"]?.GetValue<string>() == queueHash
                && o["e"]?.GetValue<long>() > time.GetUtcNow().ToUnixTimeMilliseconds()
                && o["k"] is JsonArray k)
            {
                return [.. k.Select(x => x!.GetValue<string>())];
            }
        }
        catch (Exception ex) when (ex is FormatException or System.Text.Json.JsonException or InvalidOperationException or ArgumentException
            or CryptographicException)
        {
            // fall through: malformed is the same as tampered
        }

        throw new ManagementValidationException("invalid_cursor", "The cursor is invalid or expired.");
    }

    /// <summary>A stable digest of the allowed-queue set (<c>*</c> for every queue).</summary>
    public static string QueueHash(IReadOnlyList<string>? queues) => queues is null
        ? "*"
        : Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(string.Join('\n', queues.Order(StringComparer.Ordinal)))));
}
