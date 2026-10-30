using System.Diagnostics;
using System.Text;
using Runwick.Protocol;
using Runwick.Registration;
using Runwick.Serialization;

namespace Runwick.Dispatching;

/// <summary>
/// Validates and freezes dispatch requests without I/O (spec 01 §13 Prepare, spec 02 §6).
/// </summary>
internal sealed class DispatchPreparer(JobRegistry registry, ProducerOptions options, TimeProvider time)
{
    public PreparedDispatch Prepare(Guid storeId, IQueueJob job, DispatchOptions? dispatchOptions)
    {
        ArgumentNullException.ThrowIfNull(job);
        var (intent, scopes) = PrepareItem(job, dispatchOptions);
        var operationId = Guid.CreateVersion7();
        var notAfter = NotAfterMs();
        var digests = Fingerprints.ForDispatch(storeId, registry.Application, operationId, notAfter, intent);
        var prepared = new PreparedDispatch(storeId, registry.Application, operationId, notAfter, intent, digests) { Scopes = scopes };
        if (prepared.Export().Length > PayloadBounds.MaxEnvelopeBytes)
        {
            throw new DispatchValidationException(RunwickErrorCodes.EnvelopeTooLarge,
                $"The prepared envelope exceeds {PayloadBounds.MaxEnvelopeBytes} bytes.");
        }

        return prepared;
    }

    public PreparedBatch PrepareMany(Guid storeId, IReadOnlyList<BatchDispatchItem> items, BatchDispatchOptions? batchOptions)
    {
        ArgumentNullException.ThrowIfNull(items);
        if (items.Count == 0)
        {
            throw new DispatchValidationException(RunwickErrorCodes.BatchEmpty, "A batch needs at least one item.");
        }

        if (items.Count > PayloadBounds.MaxBatchItems)
        {
            throw new DispatchValidationException(RunwickErrorCodes.BatchTooLarge, $"A batch holds at most {PayloadBounds.MaxBatchItems} items.");
        }

        var intents = new List<DispatchIntent>(items.Count);
        var scopes = new List<IReadOnlyList<ScopeRequirement>>(items.Count);
        var failures = new List<BatchItemFailure>();
        for (var ordinal = 0; ordinal < items.Count; ordinal++)
        {
            try
            {
                var item = items[ordinal] ?? throw new DispatchValidationException(RunwickErrorCodes.InvalidPolicy, "Batch item is null.");
                var (intent, s) = PrepareItem(item.Job, item.Options);
                intents.Add(intent);
                scopes.Add(s);
            }
            catch (DispatchValidationException ex)
            {
                failures.Add(new BatchItemFailure(ordinal, ex.Code, ex.Message, null));
            }
        }

        if (failures.Count > 0)
        {
            throw new DispatchValidationException(failures[0].Code,
                $"{failures.Count} batch item(s) are invalid; nothing was dispatched.", failures);
        }

        var operationId = Guid.CreateVersion7();
        var notAfter = NotAfterMs();
        var continueOnConflict = batchOptions?.ContinueOnConflict ?? false;
        var (digests, command) = Fingerprints.ForBatch(storeId, registry.Application, operationId, notAfter, continueOnConflict, intents);
        var prepared = new PreparedBatch(storeId, registry.Application, operationId, notAfter, continueOnConflict, intents, digests, command)
        {
            Scopes = scopes,
        };
        if (prepared.Export().Length > PayloadBounds.MaxEnvelopeBytes)
        {
            throw new DispatchValidationException(RunwickErrorCodes.BatchEnvelopeTooLarge,
                $"The batch envelope exceeds {PayloadBounds.MaxEnvelopeBytes} bytes; split the batch.");
        }

        return prepared;
    }

    long NotAfterMs()
    {
        var lifetime = options.CommandLifetime;
        if (lifetime <= TimeSpan.Zero || lifetime > TimeSpan.FromMinutes(5))
        {
            throw new InvalidOperationException("ProducerOptions.CommandLifetime must be within (0, 5 min].");
        }

        return Fingerprints.FloorEpochMs(time.GetUtcNow() + lifetime);
    }

    /// <summary>Scope requirements for an imported item: membership from the envelope, seed capacity from this registry.</summary>
    public IReadOnlyList<ScopeRequirement> ScopesForImport(DispatchIntent intent)
    {
        var job = registry.ByName(intent.JobName);
        return [.. intent.ConcurrencyScopes.Select(s => new ScopeRequirement(s.Kind, s.Key, s.Kind switch
        {
            ScopeRef.QueueKind => registry.QueueCapacities.GetValueOrDefault(s.Key) ?? 1,
            ScopeRef.JobTypeKind => job?.JobTypeCapacity ?? 1,
            _ => job?.Resources.Count > 0 ? job.Resources.Min(r => r.Capacity) : 1,
        }))];
    }

    (DispatchIntent Intent, IReadOnlyList<ScopeRequirement> Scopes) PrepareItem(IQueueJob job, DispatchOptions? o)
    {
        ArgumentNullException.ThrowIfNull(job);
        var compiled = registry.ByType(job.GetType())
            ?? throw new DispatchValidationException(RunwickErrorCodes.JobNotRegistered,
                $"{job.GetType().Name} is not a registered job contract.");

        // Route.
        var queue = compiled.Queue;
        if (o?.Queue is { } q)
        {
            if (!Identifiers.IsName(q))
            {
                throw new DispatchValidationException(RunwickErrorCodes.InvalidQueueName, $"'{q}' is not a valid queue name.");
            }

            if (!registry.QueueCapacities.ContainsKey(q))
            {
                throw new DispatchValidationException(RunwickErrorCodes.UnknownQueue, $"Queue '{q}' is not registered.");
            }

            queue = q;
        }

        var priority = o?.Priority ?? compiled.Priority;
        if (!Identifiers.IsPriority(priority))
        {
            throw new DispatchValidationException(RunwickErrorCodes.InvalidPriority, "Priority must be low, medium or high.");
        }

        long? notBefore = o?.NotBefore is { } nb ? Fingerprints.CeilEpochMs(nb) : null;

        // Payload.
        SerializedPayload payload;
        try
        {
            payload = RunwickJsonProfile.V1.Serialize(job, compiled.PayloadType);
        }
        catch (PayloadValidationException ex)
        {
            throw new DispatchValidationException(ex.Code, ex.Message);
        }
        catch (Exception ex) when (ex is NotSupportedException or System.Text.Json.JsonException)
        {
            throw new DispatchValidationException(RunwickErrorCodes.PayloadSerializationFailed, ex.Message);
        }

        // Uniqueness.
        string? activeKey = o?.Uniqueness?.ActiveKey;
        string? windowKey = o?.Uniqueness?.WindowKey;
        var window = o?.Uniqueness?.Window;
        if (window is not null && windowKey is null)
        {
            throw new DispatchValidationException(RunwickErrorCodes.InvalidUniqueness, "A Window requires a WindowKey.");
        }

        if (compiled.ActiveSelector is { } activeSelector)
        {
            var derived = RunSelector(activeSelector, job, required: true);
            if (activeKey is not null && !string.Equals(activeKey, derived, StringComparison.Ordinal))
            {
                throw new DispatchValidationException(RunwickErrorCodes.UniquenessKeyMismatch, "Explicit ActiveKey differs from the derived key.");
            }

            activeKey = derived;
        }

        if (compiled.WindowSelector is { } windowSelector)
        {
            var derived = RunSelector(windowSelector.Select, job, required: true);
            if (windowKey is not null && (!string.Equals(windowKey, derived, StringComparison.Ordinal) || window != windowSelector.Window))
            {
                throw new DispatchValidationException(RunwickErrorCodes.UniquenessKeyMismatch, "Explicit WindowKey/Window differ from the derived ones.");
            }

            windowKey = derived;
            window = windowSelector.Window;
        }

        foreach (var key in new[] { activeKey, windowKey })
        {
            if (key is not null && !Identifiers.IsKey(key))
            {
                throw new DispatchValidationException(RunwickErrorCodes.InvalidKey, "Uniqueness keys are 1–512 UTF-8 bytes of clean text.");
            }
        }

        long? windowMs = null;
        if (windowKey is not null)
        {
            if (window is not { } w || w <= TimeSpan.Zero || w.Ticks % TimeSpan.TicksPerMillisecond != 0 || w > TimeSpan.FromDays(3650))
            {
                throw new DispatchValidationException(RunwickErrorCodes.InvalidUniqueness,
                    "WindowKey requires a positive whole-millisecond Window of at most 3,650 days.");
            }

            windowMs = w.Ticks / TimeSpan.TicksPerMillisecond;
        }

        // Scopes.
        var requirements = new Dictionary<(string Kind, string Key), int>();
        if (registry.QueueCapacities.GetValueOrDefault(queue) is { } queueCapacity)
        {
            requirements[(ScopeRef.QueueKind, queue)] = queueCapacity;
        }

        if (compiled.JobTypeCapacity is { } jobCapacity)
        {
            requirements[(ScopeRef.JobTypeKind, compiled.Name)] = jobCapacity;
        }

        foreach (var resource in compiled.Resources)
        {
            var key = RunSelector(resource.Select, job, required: true)!;
            if (!Identifiers.IsKey(key))
            {
                throw new DispatchValidationException(RunwickErrorCodes.InvalidKey, "Resource keys are 1–512 UTF-8 bytes of clean text.");
            }

            if (requirements.TryGetValue((ScopeRef.ResourceKind, key), out var existing) && existing != resource.Capacity)
            {
                throw new DispatchValidationException(RunwickErrorCodes.ScopeCapacityConflict,
                    "Two resource selectors produced the same key with different capacities.");
            }

            requirements[(ScopeRef.ResourceKind, key)] = resource.Capacity;
        }

        var scopeRefs = ScopeRef.Normalize(requirements.Keys.Select(k => new ScopeRef(k.Kind, k.Key)));
        var scopes = scopeRefs.Select(s => new ScopeRequirement(s.Kind, s.Key, requirements[(s.Kind, s.Key)])).ToList();

        // Metadata.
        var business = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (key, value) in o?.Metadata ?? new Dictionary<string, string>())
        {
            // traceparent/tracestate are diagnostic-only keys and never business metadata (spec 02 §10).
            if (!Identifiers.IsName(key) || !Identifiers.IsMetadataValue(value) || key is "traceparent" or "tracestate")
            {
                throw new DispatchValidationException(RunwickErrorCodes.InvalidMetadata, $"Metadata entry '{key}' is invalid.");
            }

            business[key] = value;
        }

        var diagnostic = new Dictionary<string, string>(StringComparer.Ordinal);
        if (Activity.Current is { } activity && activity.IdFormat == ActivityIdFormat.W3C)
        {
            var traceparent = activity.Id;
            if (Identifiers.IsTraceParent(traceparent))
            {
                diagnostic["traceparent"] = traceparent!;
                if (Identifiers.IsTraceState(activity.TraceStateString) && !string.IsNullOrEmpty(activity.TraceStateString))
                {
                    diagnostic["tracestate"] = activity.TraceStateString!;
                }
            }
        }

        var entries = business.Count + diagnostic.Count;
        var bytes = business.Concat(diagnostic).Sum(kv => Encoding.UTF8.GetByteCount(kv.Key) + Encoding.UTF8.GetByteCount(kv.Value));
        if (entries > PayloadBounds.MaxMetadataEntries || bytes > PayloadBounds.MaxMetadataBytes)
        {
            throw new DispatchValidationException(RunwickErrorCodes.MetadataTooLarge,
                $"Metadata holds {entries} entries / {bytes} bytes; limits are {PayloadBounds.MaxMetadataEntries} / {PayloadBounds.MaxMetadataBytes}.");
        }

        var intent = new DispatchIntent(
            compiled.Name, compiled.CurrentVersion, RunwickJsonProfile.ProfileId, RunwickJsonProfile.ProfileVersion,
            payload.Utf8, queue, priority, notBefore, scopeRefs, [], activeKey, windowKey, windowMs,
            business, diagnostic, compiled.Policy, Guid.CreateVersion7());
        return (intent, scopes);
    }

    static string? RunSelector(Func<object, string> selector, object job, bool required)
    {
        string? value;
        try
        {
            value = selector(job);
        }
        catch (Exception ex)
        {
            throw new DispatchValidationException(RunwickErrorCodes.KeySelectorFailed, $"A key selector threw {ex.GetType().Name}.");
        }

        if (value is null && required)
        {
            throw new DispatchValidationException(RunwickErrorCodes.UniquenessRequired, "A required key selector returned null.");
        }

        return value;
    }
}
