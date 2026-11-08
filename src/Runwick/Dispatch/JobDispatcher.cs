using System.Diagnostics;
using System.Text.Json;
using Runwick.Hosting;
using Runwick.Protocol;
using Runwick.Serialization;
using Runwick.Telemetry;
using T = Runwick.Telemetry.Telemetry;

namespace Runwick.Dispatching;

/// <summary>The convenience and prepared dispatchers (spec 01 §10, §12, §13).</summary>
internal sealed class JobDispatcher(RunwickRuntime runtime, DispatchPreparer preparer, LocalAdmission admission)
    : IJobDispatcher, IPreparedJobDispatcher
{
    static readonly TimeSpan DurableFloor = TimeSpan.FromHours(24);

    public async Task<DispatchReceipt> DispatchAsync<TJob>(TJob job, DispatchOptions? options = null, CancellationToken cancellationToken = default)
        where TJob : class, IQueueJob
    {
        cancellationToken.ThrowIfCancellationRequested();

        // The send span is current while preparing, so the stored traceparent is its context (18 §2.1).
        var send = new SendScope("single");
        try
        {
            await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
            using var lease = await admission.TryEnterAsync(cancellationToken).ConfigureAwait(false) ?? throw new LocalCapacityExceededException();
            var prepared = preparer.Prepare(runtime.StoreId, job, options);
            send.Queue(prepared.Intent.Queue);
            return send.Done(await SendAsync(prepared, cancellationToken).ConfigureAwait(false));
        }
        catch (Exception ex)
        {
            send.Failed(ex, runtime);
            throw;
        }
        finally
        {
            send.End();
        }
    }

    public async Task<BatchDispatchReceipt> DispatchManyAsync(
        IReadOnlyList<BatchDispatchItem> items, BatchDispatchOptions? options = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var send = new SendScope("batch");
        try
        {
            await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
            using var lease = await admission.TryEnterAsync(cancellationToken).ConfigureAwait(false) ?? throw new LocalCapacityExceededException();
            var prepared = preparer.PrepareMany(runtime.StoreId, items, options);
            send.Batch(prepared.Items);
            return send.Done(await SendAsync(prepared, cancellationToken).ConfigureAwait(false));
        }
        catch (Exception ex)
        {
            send.Failed(ex, runtime);
            throw;
        }
        finally
        {
            send.End();
        }
    }

    public PreparedDispatch Prepare<TJob>(TJob job, DispatchOptions? options = null) where TJob : class, IQueueJob =>
        preparer.Prepare(runtime.StoreId, job, options);

    public PreparedBatch PrepareMany(IReadOnlyList<BatchDispatchItem> items, BatchDispatchOptions? options = null) =>
        preparer.PrepareMany(runtime.StoreId, items, options);

    public async Task<DispatchReceipt> DispatchAsync(PreparedDispatch prepared, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(prepared);
        cancellationToken.ThrowIfCancellationRequested();
        var send = new SendScope("prepared");
        send.Queue(prepared.Intent.Queue);
        try
        {
            await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
            using var lease = await admission.TryEnterAsync(cancellationToken).ConfigureAwait(false) ?? throw new LocalCapacityExceededException(prepared);
            return send.Done(await SendAsync(prepared, cancellationToken).ConfigureAwait(false));
        }
        catch (Exception ex)
        {
            send.Failed(ex, runtime);
            throw;
        }
        finally
        {
            send.End();
        }
    }

    public async Task<BatchDispatchReceipt> DispatchAsync(PreparedBatch prepared, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(prepared);
        cancellationToken.ThrowIfCancellationRequested();
        var send = new SendScope("prepared");
        send.Batch(prepared.Items);
        try
        {
            await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
            using var lease = await admission.TryEnterAsync(cancellationToken).ConfigureAwait(false) ?? throw new LocalCapacityExceededException(preparedBatch: prepared);
            return send.Done(await SendAsync(prepared, cancellationToken).ConfigureAwait(false));
        }
        catch (Exception ex)
        {
            send.Failed(ex, runtime);
            throw;
        }
        finally
        {
            send.End();
        }
    }

    public async Task<DispatchResolution> ResolveAsync(DispatchOperationId operationId, CancellationToken cancellationToken = default)
    {
        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        using var lease = await admission.TryEnterAsync(cancellationToken).ConfigureAwait(false) ?? throw new LocalCapacityExceededException();

        // Without the prepared request the NotAfter is unknown; operation IDs are UUIDv7, and no dispatch
        // NotAfter can lie beyond creation + the 10-minute deadline cap.
        var created = UuidV7Time(operationId.Value);
        var bound = created + Operations.DeadlineCap(Operations.Dispatch);
        var resolution = await runtime.Store.ResolveOperationAsync(runtime.Registry.Application, operationId.Value, bound, cancellationToken).ConfigureAwait(false);
        return resolution switch
        {
            ReceiptResolution.Recorded r when r.Operation == Operations.Dispatch =>
                new(operationId, DispatchResolutionStatus.Confirmed, ReceiptFrom(operationId, r.Result), null),
            ReceiptResolution.Recorded r when r.Operation == Operations.DispatchMany =>
                new(operationId, DispatchResolutionStatus.Confirmed, null, BatchReceiptFrom(operationId, r.Result)),
            ReceiptResolution.Recorded => throw new DispatchValidationException(RunwickErrorCodes.NotADispatchOperation,
                $"Operation {operationId} is not a dispatch."),
            ReceiptResolution.NotObserved => new(operationId, DispatchResolutionStatus.NotObserved, null, null),
            _ => new(operationId, DispatchResolutionStatus.ExpiredUnknown, null, null),
        };
    }

    public PreparedDispatch ImportPrepared(ReadOnlySpan<byte> envelope)
    {
        var prepared = PreparedEnvelopeCodec.ReadSingle(envelope, runtime.StoreId, runtime.Registry.Application);
        return new PreparedDispatch(prepared.StoreId, prepared.Application, prepared.OperationId.Value, prepared.NotAfterMs, prepared.Intent, prepared.Digests)
        {
            Scopes = preparer.ScopesForImport(prepared.Intent),
        };
    }

    public PreparedBatch ImportPreparedBatch(ReadOnlySpan<byte> envelope)
    {
        var p = PreparedEnvelopeCodec.ReadBatch(envelope, runtime.StoreId, runtime.Registry.Application);
        return new PreparedBatch(p.StoreId, p.Application, p.OperationId.Value, p.NotAfterMs, p.ContinueOnConflict, p.Items, p.ItemDigests, p.CommandDigest)
        {
            Scopes = [.. p.Items.Select(preparer.ScopesForImport)],
        };
    }

    async Task<DispatchReceipt> SendAsync(PreparedDispatch prepared, CancellationToken ct)
    {
        var item = new DispatchItem(prepared.Intent, prepared.Scopes, prepared.Digests) { Rates = preparer.RatesFor(prepared.Intent) };
        var command = new StoreCommand<DispatchRequest>(prepared.Application, prepared.OperationId.Value, prepared.NotAfter,
            new DispatchRequest(item, prepared.StoreId, prepared.Digests.Command));
        StoreReply<DispatchResult> reply;
        try
        {
            reply = await runtime.Store.DispatchAsync(command, ct).ConfigureAwait(false);
            runtime.Producer.Ok();
        }
        catch (Exception ex) when (ex is StoreNotAppliedException or StoreOutcomeUnknownException)
        {
            runtime.Producer.Failed(ex);
            throw Translate(ex, prepared.OperationId, prepared, null, ct);
        }

        if (reply.Status == StoreStatus.Applied && reply.Value is { } r)
        {
            return Receipt(prepared.OperationId, r);
        }

        throw Rejection(reply.Status, reply.Error, reply.DecidedAt, prepared.OperationId, prepared.NotAfter, prepared, null);
    }

    async Task<BatchDispatchReceipt> SendAsync(PreparedBatch prepared, CancellationToken ct)
    {
        var items = prepared.Items.Select((intent, i) => new DispatchItem(intent, prepared.Scopes[i], prepared.ItemDigests[i])
        {
            Rates = preparer.RatesFor(intent),
        }).ToList();
        var command = new StoreCommand<DispatchBatchRequest>(prepared.Application, prepared.OperationId.Value, prepared.NotAfter,
            new DispatchBatchRequest(items, prepared.ContinueOnConflict, prepared.StoreId, prepared.CommandDigest));
        StoreReply<DispatchBatchResult> reply;
        try
        {
            reply = await runtime.Store.DispatchManyAsync(command, ct).ConfigureAwait(false);
            runtime.Producer.Ok();
        }
        catch (Exception ex) when (ex is StoreNotAppliedException or StoreOutcomeUnknownException)
        {
            runtime.Producer.Failed(ex);
            throw Translate(ex, prepared.OperationId, null, prepared, ct);
        }

        if (reply.Status == StoreStatus.Applied && reply.Value is { } r)
        {
            return new BatchDispatchReceipt(prepared.OperationId, [.. r.Items.Select(i => new BatchItemResult(
                i.Ordinal,
                i.Result is { } ok ? Receipt(prepared.OperationId, ok) : null,
                i.Failure is { } f ? new BatchItemFailure(i.Ordinal, f.Code, f.Detail ?? f.Code, f.ConflictingJobId is { } c ? new JobId(c) : null) : null))],
                runtime.Store.AcknowledgementProfile);
        }

        throw Rejection(reply.Status, reply.Error, reply.DecidedAt, prepared.OperationId, prepared.NotAfter, null, prepared);
    }

    DispatchReceipt Receipt(DispatchOperationId operationId, DispatchResult r) =>
        new(new JobId(r.JobId), operationId, r.Disposition, r.Match, r.PolicyDrift, runtime.Store.AcknowledgementProfile);

    static Exception Translate(Exception ex, DispatchOperationId operationId, PreparedDispatch? prepared, PreparedBatch? batch, CancellationToken ct) =>
        ex switch
        {
            StoreOutcomeUnknownException when ct.IsCancellationRequested =>
                new DispatchCanceledAfterSendException(operationId, prepared, batch, ct),
            StoreOutcomeUnknownException u =>
                new DispatchOutcomeUnknownException(RunwickErrorCodes.OutcomeUnknown, operationId, prepared, batch, u),
            StoreNotAppliedException n when n.Code == RunwickErrorCodes.LocalCapacityExceeded =>
                new LocalCapacityExceededException(prepared, batch),
            StoreNotAppliedException n when ct.IsCancellationRequested =>
                new OperationCanceledException("Cancelled before the command committed; nothing was applied.", n, ct),
            StoreNotAppliedException n =>
                new StoreUnavailableException($"The store did not apply the command ({n.Code}); it is safe to retry.", n),
            _ => ex,
        };

    internal static Exception Rejection(StoreStatus status, StoreError? error, DateTimeOffset decidedAt, DispatchOperationId operationId,
        DateTimeOffset notAfter, PreparedDispatch? prepared, PreparedBatch? batch)
    {
        var code = error?.Code ?? status.ToString();
        var detail = error?.Detail is { } d ? $" ({d})" : "";
        return status switch
        {
            StoreStatus.UniquenessConflict => new UniquenessConflictException(
                code == "window_fingerprint_mismatch" ? RunwickErrorCodes.UniquenessConflict : code,
                $"A live uniqueness binding conflicts with this request{detail}.",
                error?.ConflictingJobId is { } c ? new JobId(c) : null,
                BatchConflicts(error)),
            StoreStatus.CommandConflict => new CommandConflictException(operationId),
            StoreStatus.CommandExpired when decidedAt >= notAfter + DurableFloor =>
                new DispatchOutcomeUnknownException(RunwickErrorCodes.ExpiredUnknown, operationId, prepared, batch),
            StoreStatus.CommandExpired => new CommandExpiredException(operationId, prepared, batch),
            StoreStatus.AdmissionClosed => new AdmissionClosedException($"Application admission is closed: {code}{detail}."),
            StoreStatus.UnsupportedVersion => new StoreIncompatibleException($"The store rejected the command as incompatible: {code}{detail}."),
            StoreStatus.InvalidRequest => new DispatchValidationException(code, $"The store rejected the request: {code}{detail}."),
            _ => new StoreIncompatibleException($"Unexpected store reply {status} {code}{detail}."),
        };
    }

    static List<BatchItemFailure>? BatchConflicts(StoreError? error)
    {
        if (error?.Detail is not { Length: > 0 } detail || !detail.StartsWith('['))
        {
            return null;
        }

        // DispatchMany conflicts carry a JSON array of {ordinal, code, conflictingJobId} in Detail.
        try
        {
            using var doc = JsonDocument.Parse(detail);
            return [.. doc.RootElement.EnumerateArray().Select(e => new BatchItemFailure(
                e.GetProperty("ordinal").GetInt32(),
                e.GetProperty("code").GetString()!,
                "Uniqueness conflict.",
                e.TryGetProperty("conflictingJobId", out var c) && c.ValueKind == JsonValueKind.String ? new JobId(c.GetGuid()) : null))];
        }
        catch (JsonException)
        {
            return null;
        }
    }

    DispatchReceipt ReceiptFrom(DispatchOperationId operationId, JsonElement result) =>
        new(new JobId(result.GetProperty("jobId").GetGuid()), operationId,
            Enum.Parse<DispatchDisposition>(result.GetProperty("disposition").GetString()!),
            Enum.Parse<DuplicateMatch>(result.GetProperty("match").GetString()!),
            result.GetProperty("policyDrift").GetBoolean(),
            runtime.Store.AcknowledgementProfile);

    BatchDispatchReceipt BatchReceiptFrom(DispatchOperationId operationId, JsonElement result) =>
        new(operationId, [.. result.GetProperty("items").EnumerateArray().Select(i =>
        {
            var ordinal = i.GetProperty("ordinal").GetInt32();
            if (i.TryGetProperty("failureCode", out var fc) && fc.ValueKind == JsonValueKind.String)
            {
                JobId? conflicting = i.TryGetProperty("conflictingJobId", out var c) && c.ValueKind == JsonValueKind.String ? new JobId(c.GetGuid()) : null;
                return new BatchItemResult(ordinal, null, new BatchItemFailure(ordinal, fc.GetString()!, "Uniqueness conflict.", conflicting));
            }

            return new BatchItemResult(ordinal, ReceiptFrom(operationId, i), null);
        })], runtime.Store.AcknowledgementProfile);

    /// <summary>Unix-ms timestamp in the first 48 bits of a UUIDv7.</summary>
    internal static DateTimeOffset UuidV7Time(Guid id)
    {
        Span<byte> bytes = stackalloc byte[16];
        id.TryWriteBytes(bytes, bigEndian: true, out _);
        long ms = 0;
        for (var i = 0; i < 6; i++)
        {
            ms = (ms << 8) | bytes[i];
        }

        return DateTimeOffset.FromUnixTimeMilliseconds(ms);
    }

    /// <summary>
    /// One client-observed dispatch (18 §2.1, §3.1): the <c>send</c> span and the outcome counters, recorded once the reply
    /// or the failure is known. Batch items count one operation each.
    /// </summary>
    sealed class SendScope
    {
        readonly Activity? _span;
        readonly long _start = Stopwatch.GetTimestamp();
        readonly string _kind;
        List<string> _queues = [];
        List<string>? _results;

        public SendScope(string kind)
        {
            _kind = kind;
            _span = T.StartActivity("send", ActivityKind.Producer);
            _span?.SetTag("messaging.system", "runwick").SetTag("messaging.operation.type", "send").SetTag("messaging.operation.name", "dispatch")
                .SetTag("runwick.dispatch.kind", kind);
        }

        public void Queue(string queue)
        {
            _queues = [queue];
            if (_span is not null)
            {
                _span.DisplayName = "send " + queue;
                _span.SetTag("messaging.destination.name", queue);
            }
        }

        public void Batch(IReadOnlyList<DispatchIntent> items)
        {
            _queues = [.. items.Select(i => i.Queue)];
            if (_span is not null && _queues.Count > 0)
            {
                _span.DisplayName = "send " + _queues[0];
                _span.SetTag("messaging.destination.name", _queues[0]).SetTag("messaging.batch.message_count", _queues.Count);
            }
        }

        public DispatchReceipt Done(DispatchReceipt receipt)
        {
            _results = [Disposition(receipt.Disposition)];
            _span?.SetTag("runwick.job.id", receipt.JobId.Value.ToString("D")).SetTag("messaging.message.id", receipt.JobId.Value.ToString("D"));
            return receipt;
        }

        public BatchDispatchReceipt Done(BatchDispatchReceipt receipt)
        {
            _results = [.. receipt.Items.OrderBy(i => i.Ordinal).Select(i => i.Receipt is { } r ? Disposition(r.Disposition) : "conflict")];
            return receipt;
        }

        static string Disposition(DispatchDisposition d) => d == DispatchDisposition.Created ? "created" : "reused";

        public void Failed(Exception ex, RunwickRuntime runtime)
        {
            var result = ex switch
            {
                LocalCapacityExceededException => "local_capacity_exceeded",
                DispatchOutcomeUnknownException or DispatchCanceledAfterSendException => "unknown",
                UniquenessConflictException => "conflict",
                _ => "rejected",
            };
            _results = [result];
            _span?.SetTag("error.type", ex is RunwickException r ? r.Code : T.ErrorType(ex.GetType().FullName ?? ex.GetType().Name))
                .SetStatus(ActivityStatusCode.Error);
            if (ex is DispatchOutcomeUnknownException u)
            {
                RunwickLog.DispatchOutcomeUnknown(runtime.Logger, u.OperationId.Value, T.Queue(_queues.FirstOrDefault()));
            }
        }

        public void End()
        {
            var results = _results ?? ["rejected"];
            _span?.SetTag("runwick.dispatch.result", results[0]);
            T.DispatchDuration.Record(Stopwatch.GetElapsedTime(_start).TotalSeconds, results[0]);
            var count = Math.Max(1, Math.Max(results.Count, _queues.Count));
            for (var i = 0; i < count; i++)
            {
                // A whole batch rejected or unknown: every item had that outcome.
                var result = results[Math.Min(i, results.Count - 1)];
                var queue = _queues.Count == 0 ? null : _queues[Math.Min(i, _queues.Count - 1)];
                T.DispatchOperations.Add(1, result, _kind, T.Queue(queue));
            }

            T.Stop(_span);
        }
    }
}
