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
        catch (JsonException ex)
        {
            throw new PayloadValidationException(ex.Message.Contains("depth", StringComparison.OrdinalIgnoreCase) ? "payload_depth_exceeded" : "payload_invalid_value", ex.Message);
        }
        catch (ArgumentException ex)
        {
            throw new PayloadValidationException("payload_invalid_value", ex.Message);
        }
        catch (InvalidOperationException ex)
        {
            throw new PayloadValidationException("payload_invalid_value", ex.Message);
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

    static string ClassifyDecode(JsonException ex)
    {
        var message = ex.Message;
        if (message.Contains("could not be mapped", StringComparison.Ordinal))
        {
            return "payload_unmapped_member";
        }

        if (message.Contains("missing required", StringComparison.OrdinalIgnoreCase)
            || message.Contains("required properties", StringComparison.OrdinalIgnoreCase)
            || message.Contains("constructor parameter", StringComparison.OrdinalIgnoreCase))
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

/// <summary>Original payload bytes and their canonical value.</summary>
internal sealed record SerializedPayload(byte[] Utf8, CanonicalValue Canonical);

/// <summary>An <see cref="System.Buffers.IBufferWriter{T}"/> that refuses to grow past a limit.</summary>
internal sealed class BoundedBufferWriter(int limit) : System.Buffers.IBufferWriter<byte>, IDisposable
{
    byte[] _buffer = new byte[Math.Min(limit, 4096)];
    int _written;

    public void Advance(int count) => _written += count;

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
        var needed = _written + Math.Max(sizeHint, 1);
        if (needed <= _buffer.Length)
        {
            return;
        }

        // Writers ask for chunks larger than they use; allow a fixed slack past the limit and check
        // the written length at the end. Memory stays bounded by limit + slack.
        const int slack = 65_536;
        if (_written > limit || needed > limit + slack)
        {
            throw new LimitExceededException();
        }

        var size = Math.Min(limit + slack, Math.Max(needed, _buffer.Length * 2));
        Array.Resize(ref _buffer, size);
    }

    public byte[] ToArray() => _written > limit ? throw new LimitExceededException() : _buffer.AsSpan(0, _written).ToArray();

    public void Dispose()
    {
    }

    public sealed class LimitExceededException : Exception;
}
