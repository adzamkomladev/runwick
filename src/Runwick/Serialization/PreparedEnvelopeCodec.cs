using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Runwick.Registration;

namespace Runwick.Serialization;

/// <summary>Writes and strictly reads the prepared export envelopes (spec 02 §9).</summary>
internal static partial class PreparedEnvelopeCodec
{
    public const string SingleFormat = "runwick.prepared-dispatch";
    public const string BatchFormat = "runwick.prepared-batch";
    public const int FormatVersion = 1;
    public const int PayloadEncodingVersion = 1;

    static readonly string[] ItemMembers =
    [
        "proposedJobId", "jobName", "payloadVersion", "serializerProfileId", "serializerProfileVersion",
        "payloadEncodingVersion", "payloadBase64", "queue", "priority", "eligibility", "concurrencyScopes",
        "rateScopes", "uniqueness", "businessMetadata", "diagnosticMetadata", "policy", "businessDigest", "policyDigest",
    ];

    static readonly string[] SingleMembers =
    [
        "format", "formatVersion", "fingerprintVersion", "storeId", "application", "operation", "operationVersion",
        "operationId", "notAfter", .. ItemMembers, "commandDigest",
    ];

    static readonly string[] BatchMembers =
    [
        "format", "formatVersion", "fingerprintVersion", "storeId", "application", "operation", "operationVersion",
        "operationId", "notAfter", "continueOnConflict", "items", "commandDigest",
    ];

    // ---------------------------------------------------------------- write

    public static byte[] Write(PreparedDispatch prepared)
    {
        var w = new EnvelopeWriter();
        w.Begin();
        WriteHeader(w, SingleFormat, prepared.StoreId, prepared.Application, "Dispatch", prepared.OperationId.Value, prepared.NotAfterMs);
        WriteItem(w, prepared.Intent, prepared.Digests);
        w.Member("commandDigest").String(DispatchDigests.Hex(prepared.Digests.Command));
        w.End();
        return w.ToArray();
    }

    public static byte[] Write(PreparedBatch prepared)
    {
        var w = new EnvelopeWriter();
        w.Begin();
        WriteHeader(w, BatchFormat, prepared.StoreId, prepared.Application, "DispatchMany", prepared.OperationId.Value, prepared.NotAfterMs);
        w.Member("continueOnConflict").Bool(prepared.ContinueOnConflict);
        w.Member("items").BeginArray();
        for (var i = 0; i < prepared.Items.Count; i++)
        {
            w.Begin();
            w.Member("ordinal").Int(i);
            WriteItem(w, prepared.Items[i], prepared.ItemDigests[i]);
            w.End();
        }

        w.EndArray();
        w.Member("commandDigest").String(DispatchDigests.Hex(prepared.CommandDigest));
        w.End();
        return w.ToArray();
    }

    // ---------------------------------------------------------------- size bound
    // An upper bound on the UTF-8 length of Write(...), from lengths alone, so Prepare can skip building the envelope
    // when it surely fits. Quote writes at most 6 bytes per UTF-16 unit ("\u00XX"; a plain unit is at most 3 UTF-8
    // bytes); base64 is exactly 4·ceil(n/3) ASCII bytes. Fixed text (member names, punctuation, numbers ≤ 20 digits,
    // UUIDs, hex digests, enum words) is covered by generous constants: header 1,024 and item 4,096 bytes.

    const long HeaderBound = 1024;
    const long ItemBound = 4096;
    const long EntryBound = 64;

    static long Text(string? value) => value is null ? 4 : 6L * value.Length + 2;

    static long ItemUpperBound(DispatchIntent intent)
    {
        var n = ItemBound + 4L * ((intent.PayloadUtf8.Length + 2) / 3)
            + Text(intent.JobName) + Text(intent.SerializerProfileId) + Text(intent.Queue) + Text(intent.Priority)
            + Text(intent.ActiveKey) + Text(intent.WindowKey)
            + Text(intent.Policy.Retry.Backoff) + Text(intent.Policy.Retry.JitterKind)
            + Text(intent.Policy.UnstartedRetry.Backoff) + Text(intent.Policy.UnstartedRetry.JitterKind);
        foreach (var s in intent.ConcurrencyScopes.Concat(intent.RateScopes))
        {
            n += EntryBound + Text(s.Kind) + Text(s.Key);
        }

        foreach (var (k, v) in intent.BusinessMetadata.Concat(intent.DiagnosticMetadata))
        {
            n += EntryBound + Text(k) + Text(v);
        }

        return n;
    }

    /// <summary>At least the byte length of <see cref="Write(PreparedDispatch)"/>.</summary>
    public static long UpperBound(PreparedDispatch prepared) =>
        UpperBound(prepared.Application, prepared.Intent);

    internal static long UpperBound(string application, DispatchIntent intent) =>
        HeaderBound + Text(application) + ItemUpperBound(intent);

    /// <summary>At least the byte length of <see cref="Write(PreparedBatch)"/>.</summary>
    public static long UpperBound(PreparedBatch prepared) =>
        HeaderBound + Text(prepared.Application) + prepared.Items.Sum(ItemUpperBound);

    static void WriteHeader(EnvelopeWriter w, string format, Guid storeId, string application, string operation, Guid operationId, long notAfterMs)
    {
        w.Member("format").String(format);
        w.Member("formatVersion").Int(FormatVersion);
        w.Member("fingerprintVersion").Int(Fingerprints.Version);
        w.Member("storeId").String(Uuid(storeId));
        w.Member("application").String(application);
        w.Member("operation").String(operation);
        w.Member("operationVersion").Int(1);
        w.Member("operationId").String(Uuid(operationId));
        w.Member("notAfter").Int(notAfterMs);
    }

    static void WriteItem(EnvelopeWriter w, DispatchIntent intent, DispatchDigests digests)
    {
        w.Member("proposedJobId").String(Uuid(intent.ProposedJobId));
        w.Member("jobName").String(intent.JobName);
        w.Member("payloadVersion").Int(intent.PayloadVersion);
        w.Member("serializerProfileId").String(intent.SerializerProfileId);
        w.Member("serializerProfileVersion").Int(intent.SerializerProfileVersion);
        w.Member("payloadEncodingVersion").Int(PayloadEncodingVersion);
        w.Member("payloadBase64").String(Convert.ToBase64String(intent.PayloadUtf8));
        w.Member("queue").String(intent.Queue);
        w.Member("priority").String(intent.Priority);
        w.Member("eligibility").Begin();
        if (intent.NotBeforeMs is { } nb)
        {
            w.Member("mode").String("instant");
            w.Member("utcEpochMilliseconds").Int(nb);
        }
        else
        {
            w.Member("mode").String("immediate");
        }

        w.End();
        WriteScopes(w.Member("concurrencyScopes"), intent.ConcurrencyScopes);
        WriteScopes(w.Member("rateScopes"), intent.RateScopes);
        w.Member("uniqueness").Begin();
        w.Member("activeKey").StringOrNull(intent.ActiveKey);
        w.Member("windowKey").StringOrNull(intent.WindowKey);
        w.Member("window").IntOrNull(intent.WindowMs);
        w.End();
        WriteMap(w.Member("businessMetadata"), intent.BusinessMetadata);
        WriteMap(w.Member("diagnosticMetadata"), intent.DiagnosticMetadata);
        var p = intent.Policy;
        w.Member("policy").Begin();
        w.Member("maxExecutions").Int(p.MaxExecutions);
        w.Member("executionTimeout").IntOrNull(p.ExecutionTimeoutMs);
        WriteRetry(w.Member("retry"), p.Retry);
        w.Member("maxConsecutiveUnstartedAbandonments").Int(p.MaxConsecutiveUnstartedAbandonments);
        WriteRetry(w.Member("unstartedRetry"), p.UnstartedRetry);
        w.End();
        w.Member("businessDigest").String(DispatchDigests.Hex(digests.Business));
        w.Member("policyDigest").String(DispatchDigests.Hex(digests.Policy));
    }

    static void WriteScopes(EnvelopeWriter w, IReadOnlyList<ScopeRef> scopes)
    {
        w.BeginArray();
        foreach (var s in scopes)
        {
            w.Begin();
            w.Member("kind").String(s.Kind);
            w.Member("key").String(s.Key);
            w.End();
        }

        w.EndArray();
    }

    static void WriteMap(EnvelopeWriter w, IReadOnlyDictionary<string, string> map)
    {
        w.Begin();
        foreach (var key in map.Keys.OrderBy(k => k, Internal.Utf8Order.Instance))
        {
            w.Member(key).String(map[key]);
        }

        w.End();
    }

    static void WriteRetry(EnvelopeWriter w, RetryPolicy r)
    {
        w.Begin();
        w.Member("backoff").String(r.Backoff);
        w.Member("evaluatorVersion").Int(r.EvaluatorVersion);
        w.Member("jitterKind").String(r.JitterKind);
        w.Member("initialDelay").Int(r.InitialDelayMs);
        w.Member("maximumDelay").Int(r.MaximumDelayMs);
        w.End();
    }

    static string Uuid(Guid g) => g.ToString("D", CultureInfo.InvariantCulture);

    /// <summary>Compact JSON writer with ECMAScript <c>JSON.stringify</c> string escaping (spec 02 §9.1).</summary>
    sealed class EnvelopeWriter
    {
        readonly StringBuilder _sb = new();
        readonly Stack<bool> _first = new();

        public void Begin()
        {
            ValueSeparator();
            _sb.Append('{');
            _first.Push(true);
        }

        public void End()
        {
            _sb.Append('}');
            _first.Pop();
        }

        public void BeginArray()
        {
            ValueSeparator();
            _sb.Append('[');
            _first.Push(true);
        }

        public void EndArray()
        {
            _sb.Append(']');
            _first.Pop();
        }

        public EnvelopeWriter Member(string name)
        {
            Separator();
            Quote(name);
            _sb.Append(':');
            return this;
        }

        void Separator()
        {
            if (_first.Count == 0)
            {
                return;
            }

            if (!_first.Pop())
            {
                _sb.Append(',');
            }

            _first.Push(false);
        }

        // A value not preceded by a member name is an array element and needs its own separator.
        bool InArrayValue => _sb.Length > 0 && _sb[^1] != ':';

        void ValueSeparator()
        {
            if (InArrayValue)
            {
                Separator();
            }
        }

        public void String(string value)
        {
            ValueSeparator();
            Quote(value);
        }

        public void StringOrNull(string? value)
        {
            if (value is null)
            {
                ValueSeparator();
                _sb.Append("null");
            }
            else
            {
                String(value);
            }
        }

        public void Int(long value)
        {
            ValueSeparator();
            _sb.Append(value.ToString(CultureInfo.InvariantCulture));
        }

        public void IntOrNull(long? value)
        {
            ValueSeparator();
            _sb.Append(value is { } v ? v.ToString(CultureInfo.InvariantCulture) : "null");
        }

        public void Bool(bool value)
        {
            ValueSeparator();
            _sb.Append(value ? "true" : "false");
        }

        void Quote(string value)
        {
            _sb.Append('"');
            foreach (var c in value)
            {
                switch (c)
                {
                    case '"': _sb.Append("\\\""); break;
                    case '\\': _sb.Append("\\\\"); break;
                    case '\b': _sb.Append("\\b"); break;
                    case '\f': _sb.Append("\\f"); break;
                    case '\n': _sb.Append("\\n"); break;
                    case '\r': _sb.Append("\\r"); break;
                    case '\t': _sb.Append("\\t"); break;
                    case < ' ': _sb.Append("\\u00").Append(((int)c).ToString("x2", CultureInfo.InvariantCulture)); break;
                    default: _sb.Append(c); break;
                }
            }

            _sb.Append('"');
        }

        public byte[] ToArray() => Encoding.UTF8.GetBytes(_sb.ToString());
    }

    // ---------------------------------------------------------------- read

    public static PreparedDispatch ReadSingle(ReadOnlySpan<byte> envelope, Guid storeId, string application)
    {
        // Spec 02 §9.3, in order: size, strict parse, versions, exact schema, payload, store/app, digests.
        using var doc = Parse(envelope);
        var root = doc.RootElement;
        CheckVersions(root, SingleFormat, "Dispatch");
        ExactMembers(root, SingleMembers);
        var (store, app, opId, notAfter) = ReadHeader(root);
        var intent = ReadItem(root);
        CheckIdentity(store, app, storeId, application);
        var digests = Fingerprints.ForDispatch(store, app, opId, notAfter, intent);
        CheckDigest(root, "businessDigest", digests.Business);
        CheckDigest(root, "policyDigest", digests.Policy);
        CheckDigest(root, "commandDigest", digests.Command);
        return new PreparedDispatch(store, app, opId, notAfter, intent, digests);
    }

    public static PreparedBatch ReadBatch(ReadOnlySpan<byte> envelope, Guid storeId, string application)
    {
        using var doc = Parse(envelope);
        var root = doc.RootElement;
        CheckVersions(root, BatchFormat, "DispatchMany");
        ExactMembers(root, BatchMembers);
        var (store, app, opId, notAfter) = ReadHeader(root);
        var continueOnConflict = root.GetProperty("continueOnConflict").ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => throw Invalid("continueOnConflict must be a boolean."),
        };
        var items = root.GetProperty("items");
        if (items.ValueKind != JsonValueKind.Array || items.GetArrayLength() is < 1 or > PayloadBounds.MaxBatchItems)
        {
            throw Invalid($"items must be an array of 1–{PayloadBounds.MaxBatchItems} entries.");
        }

        var intents = new List<DispatchIntent>();
        var ordinal = 0;
        foreach (var item in items.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.Object)
            {
                throw Invalid("Batch items must be objects.");
            }

            if (item.TryGetProperty("payloadEncodingVersion", out var pev) && pev.ValueKind == JsonValueKind.Number
                && pev.GetRawText() != PayloadEncodingVersion.ToString(CultureInfo.InvariantCulture))
            {
                throw Unsupported("payloadEncodingVersion");
            }

            ExactMembers(item, ["ordinal", .. ItemMembers]);
            if (Int(item, "ordinal") != ordinal++)
            {
                throw Invalid("Batch ordinals must be 0..n-1 in order.");
            }

            intents.Add(ReadItem(item));
        }

        CheckIdentity(store, app, storeId, application);
        var (digests, command) = Fingerprints.ForBatch(store, app, opId, notAfter, continueOnConflict, intents);
        var i = 0;
        foreach (var item in items.EnumerateArray())
        {
            CheckDigest(item, "businessDigest", digests[i].Business);
            CheckDigest(item, "policyDigest", digests[i].Policy);
            i++;
        }

        CheckDigest(root, "commandDigest", command);
        return new PreparedBatch(store, app, opId, notAfter, continueOnConflict, intents, digests, command);
    }

    static JsonDocument Parse(ReadOnlySpan<byte> envelope)
    {
        if (envelope.Length > PayloadBounds.MaxEnvelopeBytes)
        {
            throw new DispatchValidationException(RunwickErrorCodes.PreparedEnvelopeTooLarge,
                $"Prepared envelope is {envelope.Length} bytes; the limit is {PayloadBounds.MaxEnvelopeBytes}.");
        }

        // Strict §4.2 parse first: UTF-8, grammar, BOM, escapes, lone surrogates, decoded duplicates.
        try
        {
            JsonPayloadScanner.Scan(envelope, ScannerLimits.Envelope);
        }
        catch (PayloadValidationException ex)
        {
            throw Invalid($"Prepared envelope is not strict JSON ({ex.Code}).");
        }

        JsonDocument doc;
        try
        {
            doc = JsonDocument.Parse(envelope.ToArray(), new JsonDocumentOptions { AllowDuplicateProperties = false, MaxDepth = 16 });
        }
        catch (JsonException ex)
        {
            throw Invalid("Prepared envelope is not strict JSON: " + ex.Message);
        }

        if (doc.RootElement.ValueKind != JsonValueKind.Object)
        {
            doc.Dispose();
            throw Invalid("Prepared envelope root must be an object.");
        }

        return doc;
    }

    static void CheckVersions(JsonElement root, string format, string operation)
    {
        // Versions are checked before the exact schema so that a future format reports "unsupported".
        if (!root.TryGetProperty("format", out var f) || f.ValueKind != JsonValueKind.String)
        {
            throw Invalid("format is missing.");
        }

        if (f.GetString() != format)
        {
            throw Unsupported("format");
        }

        foreach (var (name, expected) in new[] { ("formatVersion", FormatVersion), ("fingerprintVersion", Fingerprints.Version), ("operationVersion", 1) })
        {
            if (!root.TryGetProperty(name, out var v) || v.ValueKind != JsonValueKind.Number)
            {
                throw Invalid($"{name} is missing.");
            }

            if (v.GetRawText() != expected.ToString(CultureInfo.InvariantCulture))
            {
                throw Unsupported(name);
            }
        }

        if (!root.TryGetProperty("operation", out var op) || op.ValueKind != JsonValueKind.String)
        {
            throw Invalid("operation is missing.");
        }

        if (op.GetString() != operation)
        {
            throw Unsupported("operation");
        }

        if (root.TryGetProperty("payloadEncodingVersion", out var pev) && pev.ValueKind == JsonValueKind.Number
            && pev.GetRawText() != PayloadEncodingVersion.ToString(CultureInfo.InvariantCulture))
        {
            throw Unsupported("payloadEncodingVersion");
        }
    }

    static (Guid Store, string Application, Guid OperationId, long NotAfter) ReadHeader(JsonElement root)
    {
        var store = Guid(root, "storeId");
        var app = Str(root, "application");
        if (!Identifiers.IsName(app))
        {
            throw Invalid("application is not a valid name.");
        }

        var opId = Guid(root, "operationId");
        var notAfter = Int(root, "notAfter");
        return (store, app, opId, notAfter);
    }

    static void CheckIdentity(Guid store, string app, Guid storeId, string application)
    {
        if (store != storeId)
        {
            throw new DispatchValidationException(RunwickErrorCodes.PreparedEnvelopeStoreMismatch,
                "The prepared envelope was made for a different store.");
        }

        if (!string.Equals(app, application, StringComparison.Ordinal))
        {
            throw new DispatchValidationException(RunwickErrorCodes.PreparedEnvelopeApplicationMismatch,
                "The prepared envelope was made for a different application.");
        }
    }

    static DispatchIntent ReadItem(JsonElement e)
    {
        var jobName = Str(e, "jobName");
        if (!Identifiers.IsJobName(jobName))
        {
            throw Invalid("jobName is not a valid job name.");
        }

        var payloadVersion = Int32(e, "payloadVersion");
        var profileId = Str(e, "serializerProfileId");
        var profileVersion = Int32(e, "serializerProfileVersion");
        if (payloadVersion < 1 || profileVersion < 1 || !Identifiers.IsName(profileId))
        {
            throw Invalid("Payload version and serializer profile must be valid.");
        }

        var base64 = Str(e, "payloadBase64");
        byte[] payload;
        try
        {
            payload = Convert.FromBase64String(base64);
        }
        catch (FormatException)
        {
            throw Invalid("payloadBase64 is not base64.");
        }

        if (!string.Equals(Convert.ToBase64String(payload), base64, StringComparison.Ordinal))
        {
            throw Invalid("payloadBase64 is not canonical base64.");
        }

        try
        {
            JsonPayloadScanner.ScanPayload(payload);
        }
        catch (PayloadValidationException ex)
        {
            throw new DispatchValidationException(ex.Code, ex.Message);
        }

        var queue = Str(e, "queue");
        var priority = Str(e, "priority");
        if (!Identifiers.IsName(queue) || !Identifiers.IsPriority(priority))
        {
            throw Invalid("queue/priority invalid.");
        }

        var eligibility = Obj(e, "eligibility");
        if (!eligibility.TryGetProperty("mode", out _))
        {
            throw Invalid("eligibility.mode is missing.");
        }

        long? notBefore = null;
        switch (Str(eligibility, "mode"))
        {
            case "immediate":
                ExactMembers(eligibility, ["mode"]);
                break;
            case "instant":
                ExactMembers(eligibility, ["mode", "utcEpochMilliseconds"]);
                notBefore = Int(eligibility, "utcEpochMilliseconds");
                break;
            default:
                throw Invalid("eligibility.mode must be immediate or instant.");
        }

        var concurrency = Scopes(e, "concurrencyScopes");
        var rate = Scopes(e, "rateScopes");
        if (rate.Count > 0)
        {
            throw Invalid("rateScopes must be empty in this release.");
        }

        var uniqueness = Obj(e, "uniqueness");
        ExactMembers(uniqueness, ["activeKey", "windowKey", "window"]);
        var activeKey = StrOrNull(uniqueness, "activeKey");
        var windowKey = StrOrNull(uniqueness, "windowKey");
        var window = IntOrNull(uniqueness, "window");
        if ((windowKey is null) != (window is null) || window is <= 0 or > PolicyBounds.MaxWindowMs
            || activeKey is not null && !Identifiers.IsKey(activeKey) || windowKey is not null && !Identifiers.IsKey(windowKey))
        {
            throw Invalid("uniqueness is inconsistent.");
        }

        var business = Map(e, "businessMetadata");
        var diagnostic = Map(e, "diagnosticMetadata");
        if (diagnostic.Keys.Any(k => k is not ("traceparent" or "tracestate")))
        {
            throw Invalid("diagnosticMetadata holds only traceparent/tracestate.");
        }

        CheckMetadata(business, diagnostic);

        // Historical policy is checked against current mandatory constraints, never re-defaulted (§9.3 step 8).
        var p = Obj(e, "policy");
        ExactMembers(p, ["maxExecutions", "executionTimeout", "retry", "maxConsecutiveUnstartedAbandonments", "unstartedRetry"]);
        var policy = new ExecutionPolicy(
            Int32(p, "maxExecutions"),
            IntOrNull(p, "executionTimeout"),
            Retry(p, "retry"),
            Int32(p, "maxConsecutiveUnstartedAbandonments"),
            Retry(p, "unstartedRetry"));
        if (PolicyBounds.Violation(policy) is { } violation)
        {
            throw Invalid("policy: " + violation);
        }

        var proposed = Guid(e, "proposedJobId");
        return new DispatchIntent(jobName, payloadVersion, profileId, profileVersion, payload, queue, priority, notBefore,
            concurrency, rate, activeKey, windowKey, window, business, diagnostic, policy, proposed);
    }

    static RetryPolicy Retry(JsonElement parent, string name)
    {
        var r = Obj(parent, name);
        ExactMembers(r, ["backoff", "evaluatorVersion", "jitterKind", "initialDelay", "maximumDelay"]);
        return new RetryPolicy(Str(r, "backoff"), Int32(r, "evaluatorVersion"), Str(r, "jitterKind"),
            Int(r, "initialDelay"), Int(r, "maximumDelay"));
    }

    static List<ScopeRef> Scopes(JsonElement parent, string name)
    {
        var a = parent.GetProperty(name);
        if (a.ValueKind != JsonValueKind.Array)
        {
            throw Invalid($"{name} must be an array.");
        }

        var list = new List<ScopeRef>();
        foreach (var s in a.EnumerateArray())
        {
            if (s.ValueKind != JsonValueKind.Object)
            {
                throw Invalid($"{name} entries must be objects.");
            }

            ExactMembers(s, ["kind", "key"]);
            var kind = Str(s, "kind");
            if (kind is not (ScopeRef.QueueKind or ScopeRef.JobTypeKind or ScopeRef.ResourceKind) || !Identifiers.IsKey(Str(s, "key")))
            {
                throw Invalid($"{name} entry is invalid.");
            }

            list.Add(new ScopeRef(kind, Str(s, "key")));
        }

        if (!list.SequenceEqual(ScopeRef.Normalize(list)))
        {
            throw Invalid($"{name} must be deduplicated and sorted.");
        }

        return list;
    }

    static Dictionary<string, string> Map(JsonElement parent, string name)
    {
        var o = Obj(parent, name);
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var p in o.EnumerateObject())
        {
            if (p.Value.ValueKind != JsonValueKind.String)
            {
                throw Invalid($"{name} values must be strings.");
            }

            map[p.Name] = p.Value.GetString()!;
        }

        return map;
    }

    /// <summary>The same metadata rules and codes as Prepare (spec 01 §10.3, spec 02 §10, IC-08).</summary>
    static void CheckMetadata(Dictionary<string, string> business, Dictionary<string, string> diagnostic)
    {
        foreach (var (key, value) in business)
        {
            if (!Identifiers.IsName(key) || !Identifiers.IsMetadataValue(value) || key is "traceparent" or "tracestate")
            {
                throw new DispatchValidationException(RunwickErrorCodes.InvalidMetadata, "A business metadata entry is invalid.");
            }
        }

        if ((diagnostic.TryGetValue("traceparent", out var traceparent) && !Identifiers.IsTraceParent(traceparent))
            || (diagnostic.TryGetValue("tracestate", out var tracestate) && !Identifiers.IsTraceState(tracestate)))
        {
            throw new DispatchValidationException(RunwickErrorCodes.InvalidMetadata, "Diagnostic metadata is not valid W3C trace context.");
        }

        var entries = business.Count + diagnostic.Count;
        var bytes = business.Concat(diagnostic).Sum(kv => Encoding.UTF8.GetByteCount(kv.Key) + Encoding.UTF8.GetByteCount(kv.Value));
        if (entries > PayloadBounds.MaxMetadataEntries || bytes > PayloadBounds.MaxMetadataBytes)
        {
            throw new DispatchValidationException(RunwickErrorCodes.MetadataTooLarge,
                $"Metadata holds {entries} entries / {bytes} bytes; limits are {PayloadBounds.MaxMetadataEntries} / {PayloadBounds.MaxMetadataBytes}.");
        }
    }

    static void ExactMembers(JsonElement o, IReadOnlyList<string> members)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var p in o.EnumerateObject())
        {
            if (!members.Contains(p.Name))
            {
                throw Invalid($"Unknown member '{p.Name}'.");
            }

            names.Add(p.Name);
        }

        if (names.Count != members.Count)
        {
            throw Invalid($"Missing member(s): {string.Join(", ", members.Where(m => !names.Contains(m)))}.");
        }
    }

    static void CheckDigest(JsonElement o, string name, byte[] computed)
    {
        var hex = Str(o, name);
        if (!HexRegex().IsMatch(hex))
        {
            throw Invalid($"{name} must be 64 lowercase hex characters.");
        }

        if (!string.Equals(hex, DispatchDigests.Hex(computed), StringComparison.Ordinal))
        {
            throw new DispatchValidationException(RunwickErrorCodes.PreparedEnvelopeDigestMismatch,
                $"{name} does not match the recomputed digest.");
        }
    }

    static JsonElement Obj(JsonElement o, string name)
    {
        var v = o.GetProperty(name);
        return v.ValueKind == JsonValueKind.Object ? v : throw Invalid($"{name} must be an object.");
    }

    static string Str(JsonElement o, string name)
    {
        var v = o.GetProperty(name);
        return v.ValueKind == JsonValueKind.String ? v.GetString()! : throw Invalid($"{name} must be a string.");
    }

    static string? StrOrNull(JsonElement o, string name) =>
        o.GetProperty(name).ValueKind == JsonValueKind.Null ? null : Str(o, name);

    static long Int(JsonElement o, string name)
    {
        var v = o.GetProperty(name);
        if (v.ValueKind != JsonValueKind.Number || !IntegerRegex().IsMatch(v.GetRawText()) || !v.TryGetInt64(out var value))
        {
            throw Invalid($"{name} must be an integer.");
        }

        return value;
    }

    static int Int32(JsonElement o, string name)
    {
        var value = Int(o, name);
        return value is >= int.MinValue and <= int.MaxValue ? (int)value : throw Invalid($"{name} is out of range.");
    }

    static long? IntOrNull(JsonElement o, string name) =>
        o.GetProperty(name).ValueKind == JsonValueKind.Null ? null : Int(o, name);

    static Guid Guid(JsonElement o, string name)
    {
        var s = Str(o, name);
        if (!System.Guid.TryParseExact(s, "D", out var g) || !string.Equals(g.ToString("D"), s, StringComparison.Ordinal))
        {
            throw Invalid($"{name} must be a lowercase UUID.");
        }

        return g;
    }

    static DispatchValidationException Invalid(string message) =>
        new(RunwickErrorCodes.PreparedEnvelopeInvalid, message);

    static DispatchValidationException Unsupported(string member) =>
        new(RunwickErrorCodes.PreparedEnvelopeUnsupportedVersion, $"Unsupported {member}.");

    [GeneratedRegex(@"\A-?(0|[1-9][0-9]*)\z")]
    private static partial Regex IntegerRegex();

    [GeneratedRegex(@"\A[0-9a-f]{64}\z")]
    private static partial Regex HexRegex();
}
