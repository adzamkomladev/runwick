using System.Globalization;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using System.Text.RegularExpressions;

namespace Runwick.Serialization;

/// <summary>
/// The <c>runwick.json</c> v1 serializer profile (spec 02 §3). Every option is set explicitly and
/// the options are read-only, so neither SDK defaults nor host configuration can change stored bytes.
/// </summary>
internal sealed partial class RunwickJsonProfile
{
    public const string ProfileId = "runwick.json";
    public const int ProfileVersion = 1;

    public static readonly RunwickJsonProfile V1 = new();

    RunwickJsonProfile()
    {
        var options = new JsonSerializerOptions
        {
            AllowDuplicateProperties = false,
            AllowOutOfOrderMetadataProperties = false,
            AllowTrailingCommas = false,
            ReadCommentHandling = JsonCommentHandling.Disallow,
            NumberHandling = JsonNumberHandling.Strict,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DictionaryKeyPolicy = null,
            PropertyNameCaseInsensitive = false,
            UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
            RespectNullableAnnotations = true,
            RespectRequiredConstructorParameters = true,
            DefaultIgnoreCondition = JsonIgnoreCondition.Never,
            IgnoreReadOnlyProperties = false,
            IgnoreReadOnlyFields = true,
            IncludeFields = false,
            ReferenceHandler = null,
            MaxDepth = PayloadBounds.MaxDepth,
            WriteIndented = false,
            IndentCharacter = ' ',
            IndentSize = 2,
            NewLine = "\n",
            Encoder = JavaScriptEncoder.Default,
            UnknownTypeHandling = JsonUnknownTypeHandling.JsonElement,
            PreferredObjectCreationHandling = JsonObjectCreationHandling.Replace,
            DefaultBufferSize = 16_384,
            TypeInfoResolver = new DefaultJsonTypeInfoResolver(),
        };
        options.Converters.Add(new JsonStringEnumConverter(namingPolicy: null, allowIntegerValues: false));
        options.Converters.Add(new UtcDateTimeConverter());
        options.MakeReadOnly(populateMissingResolver: false);
        Options = options;
    }

    public JsonSerializerOptions Options { get; }

    public string Id => ProfileId;

    public int Version => ProfileVersion;

    /// <summary>Serializes into a bounded buffer and validates the result with the scanner.</summary>
    public SerializedPayload Serialize(object payload, Type type)
    {
        byte[] bytes;
        try
        {
            using var buffer = new BoundedBufferWriter(PayloadBounds.MaxPayloadBytes);
            using (var writer = new Utf8JsonWriter(buffer, new JsonWriterOptions
            {
                Encoder = Options.Encoder,
                Indented = false,
                MaxDepth = PayloadBounds.MaxDepth,
                SkipValidation = false,
            }))
            {
                JsonSerializer.Serialize(writer, payload, type, Options);
            }

            bytes = buffer.ToArray();
        }
        catch (BoundedBufferWriter.LimitExceededException)
        {
            throw new PayloadValidationException("payload_too_large", $"Payload exceeds {PayloadBounds.MaxPayloadBytes} bytes.");
        }
        catch (Exception ex) when (ex is JsonException or ArgumentException or InvalidOperationException)
        {
            // The writer enforces MaxDepth 32 (InvalidOperationException) and cycles fail via depth (JsonException).
            throw new PayloadValidationException(MentionsDepth(ex) ? "payload_depth_exceeded" : "payload_invalid_value", ex.Message);
        }

        // JavaScriptEncoder silently replaces an unpaired surrogate with U+FFFD; malformed text must be
        // rejected, never replaced (spec 02 §8.1). Only output that contains an escaped U+FFFD can hide a
        // replacement, so only then is the graph re-walked with strict string validation.
        if (bytes.AsSpan().IndexOf("\\uFFFD"u8) >= 0 || bytes.AsSpan().IndexOf("\\ufffd"u8) >= 0)
        {
            StrictUnicode.Validate(payload, type, Options);
        }

        var canonical = JsonPayloadScanner.ScanPayload(bytes);
        return new SerializedPayload(bytes, canonical);
    }

    /// <summary>Decodes stored bytes into <paramref name="type"/> after re-validating them.</summary>
    public object Deserialize(ReadOnlySpan<byte> utf8, Type type)
    {
        JsonPayloadScanner.ScanPayload(utf8);
        try
        {
            return JsonSerializer.Deserialize(utf8, type, Options)
                ?? throw new PayloadValidationException("payload_root_not_object", "Payload decoded to null.");
        }
        catch (JsonException ex)
        {
            throw new PayloadValidationException(ClassifyDecode(ex), ex.Message);
        }
        catch (InvalidOperationException ex)
        {
            throw new PayloadValidationException("payload_invalid_value", ex.Message);
        }
    }

    /// <summary>STJ wraps the writer's depth failure ("…maximum allowed depth of 32…") in a JsonException.</summary>
    static bool MentionsDepth(Exception? ex)
    {
        for (; ex is not null; ex = ex.InnerException)
        {
            if (ex.Message.Contains("depth", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    static string ClassifyDecode(JsonException ex)
    {
        var message = ex.Message;
        if (message.Contains("could not be mapped", StringComparison.Ordinal))
        {
            return "payload_unmapped_member";
        }

        // A null for a non-nullable member is an invalid value, not a missing member.
        if (message.Contains("missing required", StringComparison.OrdinalIgnoreCase)
            || message.Contains("required properties", StringComparison.OrdinalIgnoreCase))
        {
            return "payload_missing_required";
        }

        return "payload_invalid_value";
    }

    [GeneratedRegex(@"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,7})?Z$", RegexOptions.CultureInvariant)]
    private static partial Regex UtcPattern();

    /// <summary>UTC-only <see cref="DateTime"/> representation (spec 02 §3.3).</summary>
    sealed class UtcDateTimeConverter : JsonConverter<DateTime>
    {
        public override DateTime Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType != JsonTokenType.String)
            {
                throw new JsonException("DateTime must be a UTC ISO-8601 string.");
            }

            var text = reader.GetString()!;
            if (!UtcPattern().IsMatch(text))
            {
                throw new JsonException("DateTime must match yyyy-MM-ddTHH:mm:ss[.fffffff]Z.");
            }

            return DateTime.ParseExact(text, "yyyy-MM-ddTHH:mm:ss.FFFFFFFZ", CultureInfo.InvariantCulture,
                DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        }

        public override void Write(Utf8JsonWriter writer, DateTime value, JsonSerializerOptions options)
        {
            if (value.Kind != DateTimeKind.Utc)
            {
                throw new JsonException("Only DateTimeKind.Utc values can be serialized under runwick.json v1.");
            }

            writer.WriteStringValue(value.ToString("yyyy-MM-ddTHH:mm:ss.FFFFFFFZ", CultureInfo.InvariantCulture));
        }
    }
}

/// <summary>
/// Validation-only serialization pass that rejects unpaired surrogates in strings, dictionary keys and
/// chars. Its output is discarded; the profile's own options and converters are unchanged.
/// </summary>
internal static class StrictUnicode
{
    static readonly System.Runtime.CompilerServices.ConditionalWeakTable<JsonSerializerOptions, JsonSerializerOptions> Strict = new();

    public static void Validate(object payload, Type type, JsonSerializerOptions profile)
    {
        var options = Strict.GetValue(profile, static p =>
        {
            var strict = new JsonSerializerOptions(p);
            strict.Converters.Insert(0, new StrictStringConverter());
            strict.Converters.Insert(0, new StrictCharConverter());
            strict.MakeReadOnly(populateMissingResolver: false);
            return strict;
        });
        try
        {
            JsonSerializer.Serialize(Stream.Null, payload, type, options);
        }
        catch (MalformedTextException)
        {
            throw new PayloadValidationException("payload_invalid_utf8", "A string in the payload contains an unpaired surrogate.");
        }
    }

    static void Check(ReadOnlySpan<char> text)
    {
        for (var i = 0; i < text.Length; i++)
        {
            if (char.IsHighSurrogate(text[i]) && i + 1 < text.Length && char.IsLowSurrogate(text[i + 1]))
            {
                i++;
            }
            else if (char.IsSurrogate(text[i]))
            {
                throw new MalformedTextException();
            }
        }
    }

    sealed class MalformedTextException : Exception;

    sealed class StrictStringConverter : JsonConverter<string>
    {
        public override string? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) => reader.GetString();

        public override void Write(Utf8JsonWriter writer, string value, JsonSerializerOptions options)
        {
            Check(value);
            writer.WriteStringValue(value);
        }

        public override string ReadAsPropertyName(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) => reader.GetString()!;

        public override void WriteAsPropertyName(Utf8JsonWriter writer, string value, JsonSerializerOptions options)
        {
            Check(value);
            writer.WritePropertyName(value);
        }
    }

    sealed class StrictCharConverter : JsonConverter<char>
    {
        public override char Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) => reader.GetString()![0];

        public override void Write(Utf8JsonWriter writer, char value, JsonSerializerOptions options)
        {
            if (char.IsSurrogate(value))
            {
                throw new MalformedTextException();
            }

            writer.WriteStringValue(value.ToString());
        }
    }
}

/// <summary>Original payload bytes and their canonical value.</summary>
internal sealed record SerializedPayload(byte[] Utf8, CanonicalValue Canonical);

/// <summary>
/// An <see cref="System.Buffers.IBufferWriter{T}"/> that rejects output as soon as more than
/// <c>limit</c> bytes have been written (spec 07 I-13: never more than limit + 64 KiB buffered).
/// </summary>
/// <remarks>
/// <see cref="Utf8JsonWriter"/> asks for worst-case spans (up to 3 bytes per UTF-16 char plus a few
/// structural bytes) and advances by what it actually wrote, so a request larger than the remaining
/// budget is not by itself proof of an over-limit payload. A request is refused only when even the
/// smallest output it can stand for (one byte per requested 3, minus the constant) would cross
/// limit + slack; the written length is enforced exactly on every <see cref="Advance"/>.
/// </remarks>
internal sealed class BoundedBufferWriter(int limit) : System.Buffers.IBufferWriter<byte>, IDisposable
{
    const int Slack = 65_536;
    const int MaxExpansion = 3;
    const int RequestOverhead = 64;

    byte[] _buffer = new byte[Math.Min(limit, 4096)];
    int _written;

    public int Written => _written;

    public void Advance(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        if (count > _buffer.Length - _written)
        {
            throw new InvalidOperationException("Advanced past the end of the buffer.");
        }

        _written += count;
        if (_written > limit)
        {
            throw new LimitExceededException();
        }
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        Ensure(sizeHint);
        return _buffer.AsMemory(_written);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        Ensure(sizeHint);
        return _buffer.AsSpan(_written);
    }

    void Ensure(int sizeHint)
    {
        var hint = (long)Math.Max(sizeHint, 1);
        var needed = _written + hint;
        if (needed <= _buffer.Length)
        {
            return;
        }

        var minimumOutput = Math.Max(0, hint - RequestOverhead) / MaxExpansion;
        if (_written + minimumOutput > (long)limit + Slack)
        {
            throw new LimitExceededException();
        }

        var size = Math.Max(needed, Math.Min((long)_buffer.Length * 2, (long)limit + Slack));
        Array.Resize(ref _buffer, checked((int)size));
    }

    public byte[] ToArray() => _written > limit ? throw new LimitExceededException() : _buffer.AsSpan(0, _written).ToArray();

    public void Dispose()
    {
    }

    public sealed class LimitExceededException : Exception;
}
