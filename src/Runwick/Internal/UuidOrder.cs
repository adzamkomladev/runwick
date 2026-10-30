namespace Runwick.Internal;

/// <summary>
/// Orders GUIDs the way PostgreSQL orders <c>uuid</c>: bytewise over the RFC 9562 big-endian
/// layout. <see cref="Guid.CompareTo(Guid)"/> compares the little-endian CLR fields and differs.
/// </summary>
internal sealed class UuidOrder : IComparer<Guid>
{
    public static readonly UuidOrder Instance = new();

    public int Compare(Guid x, Guid y)
    {
        Span<byte> a = stackalloc byte[16];
        Span<byte> b = stackalloc byte[16];
        x.TryWriteBytes(a, bigEndian: true, out _);
        y.TryWriteBytes(b, bigEndian: true, out _);
        return a.SequenceCompareTo(b);
    }
}

/// <summary>Bytewise ordinal comparison of UTF-8 encodings, matching PostgreSQL <c>COLLATE "C"</c>.</summary>
internal sealed class Utf8Order : IComparer<string>
{
    public static readonly Utf8Order Instance = new();

    // UTF-16 ordinal order differs from UTF-8 byte order only for surrogate pairs vs U+E000–U+FFFF.
    public int Compare(string? x, string? y)
    {
        if (ReferenceEquals(x, y))
        {
            return 0;
        }

        if (x is null)
        {
            return -1;
        }

        if (y is null)
        {
            return 1;
        }

        return System.Text.Encoding.UTF8.GetBytes(x).AsSpan().SequenceCompareTo(System.Text.Encoding.UTF8.GetBytes(y));
    }
}
