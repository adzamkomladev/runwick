using System.Buffers;
using System.Buffers.Binary;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Runwick.Serialization;

/// <summary>
/// The typed value model of the canonical hashing encoding (tags 0–6). It is distinct from the
/// JSON storage format and is used only to compute fingerprints.
/// </summary>
internal abstract record CanonicalValue
{
    public static readonly CanonicalValue Null = new CanonicalNull();
    public static readonly CanonicalValue True = new CanonicalBool(true);
    public static readonly CanonicalValue False = new CanonicalBool(false);

    public static CanonicalValue Str(string value) => new CanonicalString(value);

    public static CanonicalValue Bool(bool value) => value ? True : False;

    /// <summary>Schema-generated integer: invariant base-10, no leading zeroes or plus sign.</summary>
    public static CanonicalValue Int(long value) => new CanonicalNumber(value.ToString(CultureInfo.InvariantCulture));

    public static CanonicalValue Number(string lexeme) => new CanonicalNumber(lexeme);

    public static CanonicalValue Array(params IEnumerable<CanonicalValue> items) => new CanonicalArray([.. items]);

    public static CanonicalObjectBuilder Object() => new();

    public static CanonicalValue OrNull(string? value) => value is null ? Null : Str(value);
}

internal sealed record CanonicalNull : CanonicalValue;

internal sealed record CanonicalBool(bool Value) : CanonicalValue;

internal sealed record CanonicalString(string Value) : CanonicalValue;

internal sealed record CanonicalNumber(string Lexeme) : CanonicalValue;

internal sealed record CanonicalArray(IReadOnlyList<CanonicalValue> Items) : CanonicalValue;

internal sealed record CanonicalObject(IReadOnlyList<KeyValuePair<string, CanonicalValue>> Properties) : CanonicalValue;

/// <summary>A value already encoded by <see cref="CanonicalEncoder.Encode"/>, written verbatim (a value hashed alone and
/// inside a larger envelope is encoded once).</summary>
internal sealed record CanonicalEncoded(ReadOnlyMemory<byte> Bytes) : CanonicalValue;

internal sealed class CanonicalObjectBuilder
{
    readonly List<KeyValuePair<string, CanonicalValue>> _properties = [];

    public CanonicalObjectBuilder Add(string name, CanonicalValue value)
    {
        _properties.Add(new(name, value));
        return this;
    }

    public CanonicalObjectBuilder Add(string name, string? value) => Add(name, CanonicalValue.OrNull(value));

    public CanonicalObjectBuilder Add(string name, long value) => Add(name, CanonicalValue.Int(value));

    public CanonicalObjectBuilder Add(string name, bool value) => Add(name, CanonicalValue.Bool(value));

    public CanonicalValue Build() => new CanonicalObject([.. _properties]);
}

internal sealed class CanonicalEncodingException(string code, string message) : Exception(message)
{
    public string Code { get; } = code;
}

/// <summary>
/// Encodes <see cref="CanonicalValue"/> trees and hashes them as
/// <c>SHA-256(UTF8(domain) || 0x00 || Encode(value))</c>.
/// </summary>
internal static class CanonicalEncoder
{
    public const string DispatchRequestDomain = "runwick.dispatch-request.v1";
    public const string CommandDomain = "runwick.command.v1";

    const byte TagNull = 0, TagFalse = 1, TagTrue = 2, TagString = 3, TagNumber = 4, TagArray = 5, TagObject = 6;

    static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    public static byte[] Encode(CanonicalValue value)
    {
        var buffer = new MemoryStream();
        Write(buffer, value);
        return buffer.ToArray();
    }

    // The buffer stays private to one fingerprint calculation. The view excludes unused capacity.
    public static CanonicalEncoded EncodeForDigest(CanonicalValue value)
    {
        using var buffer = new MemoryStream();
        Write(buffer, value);
        return new CanonicalEncoded(buffer.GetBuffer().AsMemory(0, checked((int)buffer.Length)));
    }

    public static byte[] Digest(string domain, CanonicalValue value) =>
        DigestEncoded(domain, value is CanonicalEncoded encoded ? encoded.Bytes.Span : Encode(value));

    // Dispatch command bodies may contain megabytes of already encoded values.
    public static byte[] DigestStreaming(string domain, CanonicalValue value)
    {
        using var hash = SHA256.Create();
        using (var stream = new BufferedStream(new CryptoStream(Stream.Null, hash, CryptoStreamMode.Write), 1024))
        {
            stream.Write(Encoding.ASCII.GetBytes(domain));
            stream.WriteByte(0);
            Write(stream, value);
        }

        return hash.Hash!;
    }

    /// <summary>The digest of a value already encoded by <see cref="Encode"/>.</summary>
    static byte[] DigestEncoded(string domain, ReadOnlySpan<byte> encoded)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        hash.AppendData(Encoding.ASCII.GetBytes(domain));
        hash.AppendData([0]);
        hash.AppendData(encoded);
        return hash.GetHashAndReset();
    }

    static void Write(Stream output, CanonicalValue value)
    {
        switch (value)
        {
            case CanonicalEncoded e:
                output.Write(e.Bytes.Span);
                break;
            case CanonicalNull:
                output.WriteByte(TagNull);
                break;
            case CanonicalBool b:
                output.WriteByte(b.Value ? TagTrue : TagFalse);
                break;
            case CanonicalString s:
                output.WriteByte(TagString);
                WriteString(output, s.Value);
                break;
            case CanonicalNumber n:
                ValidateNumber(n.Lexeme);
                output.WriteByte(TagNumber);
                WriteBytes(output, Encoding.ASCII.GetBytes(n.Lexeme));
                break;
            case CanonicalArray a:
                output.WriteByte(TagArray);
                WriteUInt32(output, (uint)a.Items.Count);
                foreach (var item in a.Items)
                {
                    Write(output, item);
                }

                break;
            case CanonicalObject o:
                output.WriteByte(TagObject);
                var sorted = o.Properties
                    .Select(p => (Name: Utf8(p.Key), p.Value))
                    .OrderBy(p => p.Name, ByteOrder.Instance)
                    .ToList();
                for (var i = 1; i < sorted.Count; i++)
                {
                    if (sorted[i - 1].Name.AsSpan().SequenceEqual(sorted[i].Name))
                    {
                        throw new CanonicalEncodingException(RunwickErrorCodes.PayloadDuplicateProperty, "Duplicate decoded property name.");
                    }
                }

                WriteUInt32(output, (uint)sorted.Count);
                foreach (var (name, item) in sorted)
                {
                    output.WriteByte(TagString);
                    WriteBytes(output, name);
                    Write(output, item);
                }

                break;
            default:
                throw new InvalidOperationException($"Unknown canonical value {value.GetType().Name}.");
        }
    }

    static byte[] Utf8(string value)
    {
        try
        {
            return StrictUtf8.GetBytes(value);
        }
        catch (EncoderFallbackException)
        {
            throw new CanonicalEncodingException(RunwickErrorCodes.PayloadInvalidUtf8, "String contains an unpaired surrogate.");
        }
    }

    static void WriteString(Stream output, string value)
    {
        if (value.Length < 1024)
        {
            WriteBytes(output, Utf8(value));
            return;
        }

        try
        {
            var count = StrictUtf8.GetByteCount(value);
            var buffer = ArrayPool<byte>.Shared.Rent(count);
            try
            {
                var written = StrictUtf8.GetBytes(value, buffer);
                WriteBytes(output, buffer.AsSpan(0, written));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
            }
        }
        catch (EncoderFallbackException)
        {
            throw new CanonicalEncodingException(RunwickErrorCodes.PayloadInvalidUtf8, "String contains an unpaired surrogate.");
        }
    }

    static void WriteBytes(Stream output, ReadOnlySpan<byte> bytes)
    {
        WriteUInt32(output, (uint)bytes.Length);
        output.Write(bytes);
    }

    static void WriteUInt32(Stream output, uint value)
    {
        Span<byte> span = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(span, value);
        output.Write(span);
    }

    /// <summary>Validates an RFC 8259 number lexeme (no NaN/Infinity, no leading plus or zeroes).</summary>
    public static void ValidateNumber(string lexeme)
    {
        if (lexeme.Length == 0)
        {
            Invalid();
        }

        var i = 0;
        if (lexeme[i] == '-')
        {
            i++;
        }

        if (i >= lexeme.Length)
        {
            Invalid();
        }

        if (lexeme[i] == '0')
        {
            i++;
        }
        else if (lexeme[i] is >= '1' and <= '9')
        {
            while (i < lexeme.Length && char.IsAsciiDigit(lexeme[i]))
            {
                i++;
            }
        }
        else
        {
            Invalid();
        }

        if (i < lexeme.Length && lexeme[i] == '.')
        {
            i++;
            var start = i;
            while (i < lexeme.Length && char.IsAsciiDigit(lexeme[i]))
            {
                i++;
            }

            if (i == start)
            {
                Invalid();
            }
        }

        if (i < lexeme.Length && lexeme[i] is 'e' or 'E')
        {
            i++;
            if (i < lexeme.Length && lexeme[i] is '+' or '-')
            {
                i++;
            }

            var start = i;
            while (i < lexeme.Length && char.IsAsciiDigit(lexeme[i]))
            {
                i++;
            }

            if (i == start)
            {
                Invalid();
            }
        }

        if (i != lexeme.Length)
        {
            Invalid();
        }

        // Grammar first, then length — the same order as the scanner and the Node encoder.
        if (lexeme.Length > PayloadBounds.MaxNumberLexemeBytes)
        {
            throw new CanonicalEncodingException(RunwickErrorCodes.PayloadNumberTooLong, "Number lexeme is too long.");
        }

        static void Invalid() => throw new CanonicalEncodingException(RunwickErrorCodes.PayloadInvalidNumber, "Not a JSON number lexeme.");
    }

    sealed class ByteOrder : IComparer<byte[]>
    {
        public static readonly ByteOrder Instance = new();

        public int Compare(byte[]? x, byte[]? y) => x.AsSpan().SequenceCompareTo(y);
    }
}
