using Npgsql;
using NpgsqlTypes;
using Runwick.Protocol;

namespace Runwick.Postgres.Store;

internal sealed partial class PgStore : IGroupedDispatchStore
{
    long _dispatchGroups;
    long _groupedDispatches;
    long _groupFallbacks;

    /// <summary>Groups committed by dispatch_group_v1 (diagnostics, tests and the bench).</summary>
    internal long DispatchGroups => Interlocked.Read(ref _dispatchGroups);

    /// <summary>Commands carried by those groups.</summary>
    internal long GroupedDispatches => Interlocked.Read(ref _groupedDispatches);

    /// <summary>Commands of groups that committed nothing and were sent alone.</summary>
    internal long GroupFallbacks => Interlocked.Read(ref _groupFallbacks);

    public bool GroupsDispatch => options?.GroupProducerAdmission == true;

    public bool CanGroup(StoreCommand<DispatchRequest> command)
    {
        var item = command.Body.Item;
        var intent = item.Intent;
        return command.Body.CommandFingerprint is { Length: > 0 } && item.Scopes.Count == 0 && item.Rates.Count == 0
            && item.Schedule is null && intent.ActiveKey is null && intent.WindowKey is null
            && intent.ConcurrencyScopes.Count == 0 && intent.RateScopes.Count == 0;
    }

    /// <summary>
    /// dispatch_group_v1 for 2 to 8 plain commands (0028_dispatch_group). An applied group answers each command from its own
    /// row. A group that committed nothing (a refusal, or a server error such as a late deadline or a job ID collision) sends
    /// each command alone under its own identity. A transport failure before COMMIT leaves every command not applied; one
    /// after COMMIT was sent is outcome unknown for every command under its own ID.
    /// </summary>
    public async Task<GroupedDispatchReply[]> DispatchGroupAsync(IReadOnlyList<StoreCommand<DispatchRequest>> commands, CancellationToken ct)
    {
        var replies = new GroupedDispatchReply[commands.Count];
        if (commands.Count > 1 && commands.Count <= IGroupedDispatchStore.MaxGroupItems && commands.All(CanGroup))
        {
            var first = commands[0];

            // The call travels under the first command's ID (fault targeting, logs). It has no fingerprint, so it has no receipt
            // of its own; every command's receipt is written inside the function.
            var op = Op(first.Application, first.CommandId, Operations.Dispatch, commands.Max(c => c.NotAfter), null, PgLane.Producer,
                first.Body.StoreId);
            try
            {
                var reply = await runner.CallAsync(op, DispatchGroupCall(commands),
                    row => row.HasValue ? row.Applied(row.More) : row.Reject<List<PgRow>>(), ct).ConfigureAwait(false);
                if (reply is { Status: StoreStatus.Applied, Value: { } rows } && rows.Count == commands.Count)
                {
                    Interlocked.Increment(ref _dispatchGroups);
                    Interlocked.Add(ref _groupedDispatches, commands.Count);
                    HashSet<string> queues = new(StringComparer.Ordinal);
                    for (var i = 0; i < commands.Count; i++)
                    {
                        var r = MapDispatch(rows[i]);
                        if (r.Status == StoreStatus.Applied && !r.Replayed)
                        {
                            Wake(commands[i].Application, commands[i].Body.Item, r.Value, PgTime.Ms(r.DecidedAt), queues);
                        }

                        replies[i] = new(r, null);
                    }

                    return replies;
                }
            }
            catch (StoreOutcomeUnknownException ex)
            {
                for (var i = 0; i < commands.Count; i++)
                {
                    replies[i] = new(null, new StoreOutcomeUnknownException(ex.Code, commands[i].CommandId, Operations.Dispatch, ex.InnerException));
                }

                return replies;
            }
            catch (StoreNotAppliedException ex) when (ex.InnerException is not PostgresException)
            {
                Array.Fill(replies, new GroupedDispatchReply(null, ex));
                return replies;
            }
            catch (StoreNotAppliedException)
            {
                // The server refused the group and rolled it back: each command goes alone below.
            }

            Interlocked.Add(ref _groupFallbacks, commands.Count);
        }

        // One at a time: the group holds one producer slot, and a fallback is rare.
        for (var i = 0; i < commands.Count; i++)
        {
            try
            {
                replies[i] = new(await DispatchAsync(commands[i], ct).ConfigureAwait(false), null);
            }
            catch (Exception ex)
            {
                replies[i] = new(null, ex);
            }
        }

        return replies;
    }

    static readonly HashSet<string> GroupArrays = new(StringComparer.Ordinal)
    {
        "p_job_ids", "p_job_names", "p_payload_versions", "p_profile_ids", "p_profile_versions", "p_queues", "p_priorities",
        "p_not_before_ms", "p_payloads", "p_headers", "p_max_executions", "p_max_unstarted", "p_timeouts", "p_retry", "p_unstarted",
    };

    /// <summary>The parallel arrays of a single dispatch, plus one command ID, deadline, fingerprint and bounds verdict per command.</summary>
    static PgCall DispatchGroupCall(IReadOnlyList<StoreCommand<DispatchRequest>> commands)
    {
        var items = commands.Select(c => c.Body.Item).ToArray();
        var call = new PgCall("dispatch_group_v1")
            .Arr("p_command_ids", commands.Select(c => c.CommandId).ToArray(), NpgsqlDbType.Uuid)
            .Arr("p_not_afters", commands.Select(c => PgTime.Floor(c.NotAfter).UtcDateTime).ToArray(), NpgsqlDbType.TimestampTz)
            .Arr("p_fingerprints", commands.Select(c => c.Body.CommandFingerprint).ToArray(), NpgsqlDbType.Bytea)
            .Arr("p_invalid_items", items.Select(i => InvalidDispatch([i])).ToArray(), NpgsqlDbType.Text);
        foreach (var (name, value, type) in DispatchArgs(items))
        {
            if (GroupArrays.Contains(name))
            {
                call.P(name, value, type);
            }
        }

        return call;
    }
}
