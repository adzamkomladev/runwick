using Microsoft.Data.Sqlite;
using Npgsql;
using Runwick.ReferenceModel;

namespace Runwick.Harness.Store;

/// <summary>
/// The committed history of every job, rebuilt from the store's own durable records and replayed through the
/// ReferenceModel (07 §2.2, 15 §12.2 rule 4). It needs no client-side recorder, so it covers engine-driven runs
/// (workers, staged completion, peers) as well as direct store calls.
/// </summary>
/// <remarks>
/// <para>Sources, per job and generation: <c>job_attempts</c> (Reserve at <c>reserved_at</c>, Begin at
/// <c>authorized_at</c>, the closing command at <c>finished_at</c>, identified by the receipt operation of
/// <c>closed_by_command_id</c> or, when that receipt is pruned, by the outcome), and the <c>job_cancelled</c> /
/// <c>job_cancel_requested</c> events (08 §7), which name their generation. A generation created by ManualRetry or
/// RerunSucceeded starts again from <c>pending</c> with a fresh budget and the lifetime counter carried over.</para>
/// <para>The model decides every state itself and the result is compared with the committed job: state, ownership
/// (attempt and token), started count, consecutive unstarted count, lifetime executions and the cancel flag; and each
/// earlier generation must have ended terminal.</para>
/// <para>Limit: renewals are not durable, so a Reserve carries a synthetic lease that ends at the closing instant
/// when the attempt was recovered and after it otherwise. Lease liveness is therefore not rechecked by the model;
/// the ordering, the state machine, budgets and cancellation precedence are. A rate deferral
/// (<c>admission_deferred</c>, T20) is the model's <see cref="ModelOp.BeginDeferred"/>.</para>
/// </remarks>
internal static class CommittedHistory
{
    sealed record Gen(int Generation, DateTimeOffset CreatedAt, int MaxExecutions, int MaxUnstarted, int StartedCount, int ConsecutiveUnstarted);

    sealed record Job(string Application, Guid JobId, int Generation, string State, Guid? AttemptId, Guid? Token, bool CancelRequested,
        long ExecutionCount);

    sealed record Attempt(string Application, Guid AttemptId, Guid JobId, int Generation, long ReservationNumber, Guid Token,
        DateTimeOffset ReservedAt, DateTimeOffset? AuthorizedAt, DateTimeOffset? FinishedAt, string? Outcome, string? ClosedBy);

    sealed record Cancel(string Application, Guid JobId, int Generation, DateTimeOffset At);

    /// <summary>Every rule the committed histories break; empty when the ReferenceModel accepts all of them.</summary>
    public static async Task<IReadOnlyList<string>> ViolationsAsync(string adminConnectionString)
    {
        var conn = new NpgsqlConnection(adminConnectionString);
        await using (conn.ConfigureAwait(false))
        {
            await conn.OpenAsync().ConfigureAwait(false);
            var tx = await conn.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead).ConfigureAwait(false);
            await using (tx.ConfigureAwait(false))
            {
                var jobs = await QueryAsync(conn, tx, """
                    SELECT j.application, j.job_id, j.generation, j.state, j.current_attempt_id, j.ownership_token,
                           j.cancel_requested_at IS NOT NULL, j.execution_count
                    FROM runwick.jobs AS j ORDER BY j.application, j.job_id
                    """, r => new Job(r.GetString(0), r.GetGuid(1), r.GetInt32(2), r.GetString(3), Gn(r, 4), Gn(r, 5), r.GetBoolean(6), r.GetInt64(7)))
                    .ConfigureAwait(false);
                var gens = (await QueryAsync(conn, tx, """
                    SELECT application, job_id, generation, created_at, max_executions, max_unstarted_abandonments, started_count,
                           consecutive_unstarted_count
                    FROM runwick.job_generations ORDER BY application, job_id, generation
                    """, r => (Key: (r.GetString(0), r.GetGuid(1)), Gen: new Gen(r.GetInt32(2), T(r, 3), r.GetInt32(4), r.GetInt32(5), r.GetInt32(6), r.GetInt32(7))))
                    .ConfigureAwait(false)).ToLookup(x => x.Key, x => x.Gen);
                var attempts = (await QueryAsync(conn, tx, """
                    SELECT a.application, a.attempt_id, a.job_id, a.generation, a.reservation_number, a.ownership_token,
                           a.reserved_at, a.authorized_at, a.finished_at, a.outcome, c.operation
                    FROM runwick.job_attempts AS a
                    LEFT JOIN runwick.storage_commands AS c
                      ON c.application = a.application AND c.command_id = a.closed_by_command_id
                    ORDER BY a.application, a.job_id, a.reservation_number
                    """, r => new Attempt(r.GetString(0), r.GetGuid(1), r.GetGuid(2), r.GetInt32(3), r.GetInt64(4), r.GetGuid(5), T(r, 6), Tn(r, 7), Tn(r, 8),
                        Sn(r, 9), Sn(r, 10))).ConfigureAwait(false)).ToLookup(a => (a.Application, a.JobId));
                var cancels = (await QueryAsync(conn, tx, """
                    SELECT application, job_id, (details ->> 'generation')::integer, occurred_at
                    FROM runwick.queue_events
                    WHERE event_kind IN ('job_cancelled', 'job_cancel_requested') AND job_id IS NOT NULL
                    ORDER BY event_id
                    """, r => new Cancel(r.GetString(0), r.GetGuid(1), r.GetInt32(2), T(r, 3))).ConfigureAwait(false)).ToLookup(c => (c.Application, c.JobId));

                return Evaluate(jobs, gens, attempts, cancels);
            }
        }
    }

    /// <summary>The same check over a SQLite file (20 §12): UUID blobs, epoch-millisecond instants, <c>runwick_</c> tables.</summary>
    public static IReadOnlyList<string> SqliteViolations(string path)
    {
        using var conn = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = path, Mode = SqliteOpenMode.ReadWrite, Pooling = false }.ConnectionString);
        conn.Open();
        using var tx = conn.BeginTransaction(deferred: true);
        List<TRow> Query<TRow>(string sql, Func<SqliteDataReader, TRow> map)
        {
            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = sql;
            using var r = cmd.ExecuteReader();
            var list = new List<TRow>();
            while (r.Read())
            {
                list.Add(map(r));
            }

            return list;
        }

        static Guid G(SqliteDataReader r, int i) => new((byte[])r.GetValue(i), bigEndian: true);
        static Guid? Gq(SqliteDataReader r, int i) => r.IsDBNull(i) ? null : G(r, i);
        static DateTimeOffset Tq(SqliteDataReader r, int i) => DateTimeOffset.FromUnixTimeMilliseconds(r.GetInt64(i));
        static DateTimeOffset? Tqn(SqliteDataReader r, int i) => r.IsDBNull(i) ? null : Tq(r, i);
        static string? Sq(SqliteDataReader r, int i) => r.IsDBNull(i) ? null : r.GetString(i);

        var jobs = Query("""
            SELECT application, job_id, generation, state, current_attempt_id, ownership_token, cancel_requested_at IS NOT NULL, execution_count
            FROM runwick_jobs ORDER BY application, job_id
            """, r => new Job(r.GetString(0), G(r, 1), r.GetInt32(2), r.GetString(3), Gq(r, 4), Gq(r, 5), r.GetInt64(6) != 0, r.GetInt64(7)));
        var gens = Query("""
            SELECT application, job_id, generation, created_at, max_executions, max_unstarted_abandonments, started_count, consecutive_unstarted_count
            FROM runwick_job_generations ORDER BY application, job_id, generation
            """, r => (Key: (r.GetString(0), G(r, 1)), Gen: new Gen(r.GetInt32(2), Tq(r, 3), r.GetInt32(4), r.GetInt32(5), r.GetInt32(6), r.GetInt32(7))))
            .ToLookup(x => x.Key, x => x.Gen);
        var attempts = Query("""
            SELECT a.application, a.attempt_id, a.job_id, a.generation, a.reservation_number, a.ownership_token,
                   a.reserved_at, a.authorized_at, a.finished_at, a.outcome, c.operation
            FROM runwick_job_attempts AS a
            LEFT JOIN runwick_storage_commands AS c ON c.application = a.application AND c.command_id = a.closed_by_command_id
            ORDER BY a.application, a.job_id, a.reservation_number
            """, r => new Attempt(r.GetString(0), G(r, 1), G(r, 2), r.GetInt32(3), r.GetInt64(4), G(r, 5), Tq(r, 6), Tqn(r, 7), Tqn(r, 8), Sq(r, 9), Sq(r, 10)))
            .ToLookup(a => (a.Application, a.JobId));
        var cancels = Query("""
            SELECT application, job_id, CAST(json_extract(details, '$.generation') AS INTEGER), occurred_at
            FROM runwick_queue_events
            WHERE event_kind IN ('job_cancelled', 'job_cancel_requested') AND job_id IS NOT NULL
            ORDER BY event_id
            """, r => new Cancel(r.GetString(0), G(r, 1), r.GetInt32(2), Tq(r, 3))).ToLookup(c => (c.Application, c.JobId));
        return Evaluate(jobs, gens, attempts, cancels);
    }

    static List<string> Evaluate(List<Job> jobs, ILookup<(string, Guid), Gen> gens, ILookup<(string, Guid), Attempt> attempts,
        ILookup<(string, Guid), Cancel> cancels)
    {
        var errors = new List<string>();
        foreach (var job in jobs)
        {
            Check(job, [.. gens[(job.Application, job.JobId)]], attempts[(job.Application, job.JobId)], cancels[(job.Application, job.JobId)], errors);
        }

        return errors;
    }

    static void Check(Job job, List<Gen> generations, IEnumerable<Attempt> attempts, IEnumerable<Cancel> cancels, List<string> errors)
    {
        var name = $"{job.Application}/{job.JobId:N}";
        long lifetime = 0;
        foreach (var gen in generations.OrderBy(g => g.Generation))
        {
            var history = new List<ModelCommand>();
            foreach (var a in attempts.Where(a => a.Generation == gen.Generation))
            {
                var close = a.FinishedAt is null ? null : Close(a, errors, name);
                var leaseEnd = close is { Op: ModelOp.Recover } ? a.FinishedAt!.Value : (a.FinishedAt ?? a.ReservedAt) + TimeSpan.FromDays(1);
                history.Add(new ModelCommand(ModelOp.Reserve, job.JobId, a.ReservedAt, a.AttemptId, a.Token, Lease: leaseEnd - a.ReservedAt));
                if (a.AuthorizedAt is { } begun)
                {
                    history.Add(new ModelCommand(ModelOp.Begin, job.JobId, begun, a.AttemptId, a.Token));
                }

                if (close is not null)
                {
                    history.Add(close);
                }
            }

            history.AddRange(cancels.Where(c => c.Generation == gen.Generation).Select(c => new ModelCommand(ModelOp.RequestCancel, job.JobId, c.At)));
            var initial = JobModel.Dispatched(job.JobId, gen.MaxExecutions, gen.MaxUnstarted) with { LifetimeExecutions = lifetime };
            var verdict = HistoryChecker.Check(initial, history);
            if (!verdict.Legal || verdict.Final is null)
            {
                errors.Add($"{name} generation {gen.Generation}: history rejected: {verdict.Explanation}");
                return;
            }

            var final = verdict.Final;
            lifetime = final.LifetimeExecutions;
            if (gen.Generation != job.Generation)
            {
                if (!States.Terminal.Contains(final.State))
                {
                    errors.Add($"{name} generation {gen.Generation}: superseded while {final.State}");
                }

                continue;
            }

            void Expect<T>(string what, T model, T store)
            {
                if (!EqualityComparer<T>.Default.Equals(model, store))
                {
                    errors.Add($"{name}: {what} model={model} store={store} ({verdict.Explanation})");
                }
            }

            Expect("state", final.State, job.State);
            Expect("attempt", final.AttemptId, job.AttemptId);
            Expect("token", final.Token, job.Token);
            Expect("started_count", final.StartedCount, gen.StartedCount);
            Expect("consecutive_unstarted", final.ConsecutiveUnstarted, gen.ConsecutiveUnstarted);
            Expect("execution_count", final.LifetimeExecutions, job.ExecutionCount);
            if (!States.Terminal.Contains(job.State))
            {
                Expect("cancel_requested", final.CancelRequested, job.CancelRequested);
            }
        }
    }

    /// <summary>The command that closed an attempt: its receipt operation, or the outcome when the receipt is gone.</summary>
    static ModelCommand? Close(Attempt a, List<string> errors, string name)
    {
        ModelOutcome? Executed() => a.Outcome switch
        {
            "succeeded" => ModelOutcome.Succeeded,
            "retryable_failure" => ModelOutcome.RetryableFailure,
            "permanent_failure" => ModelOutcome.PermanentFailure,
            "user_cancelled" => ModelOutcome.UserCancelled,
            "interrupted" => ModelOutcome.Interrupted,
            "timed_out" => ModelOutcome.TimedOut,
            _ => null,
        };

        var at = a.FinishedAt!.Value;
        var op = a.ClosedBy switch
        {
            "Finish" => ModelOp.Finish,
            "Recover" => ModelOp.Recover,
            "ReleaseReservation" => ModelOp.Release,
            "Begin" when a.Outcome == "admission_deferred" => ModelOp.BeginDeferred,
            "Begin" => ModelOp.Begin,

            // ReserveAndBegin/1 (PF-06 batched claim) closes an attempt only through its Begin step.
            "ReserveAndBegin" when a.Outcome == "admission_deferred" => ModelOp.BeginDeferred,
            "ReserveAndBegin" => ModelOp.Begin,
            _ => a.Outcome switch
            {
                "released" or "cancelled_before_start" => ModelOp.Release,
                "admission_deferred" => ModelOp.BeginDeferred,
                "abandoned_before_start" or "abandoned_running" => ModelOp.Recover,
                _ when Executed() is not null => ModelOp.Finish,
                _ => (ModelOp?)null,
            },
        };
        if (op is null)
        {
            errors.Add($"{name} attempt {a.AttemptId:N}: outcome {a.Outcome} closed by {a.ClosedBy ?? "<pruned>"} is not in the ReferenceModel");
            return null;
        }

        return new ModelCommand(op.Value, a.JobId, at, a.AttemptId, a.Token, op == ModelOp.Finish ? Executed() : null);
    }

    static DateTimeOffset T(NpgsqlDataReader r, int i) => new(DateTime.SpecifyKind(r.GetDateTime(i), DateTimeKind.Utc));

    static DateTimeOffset? Tn(NpgsqlDataReader r, int i) => r.IsDBNull(i) ? null : T(r, i);

    static Guid? Gn(NpgsqlDataReader r, int i) => r.IsDBNull(i) ? null : r.GetGuid(i);

    static string? Sn(NpgsqlDataReader r, int i) => r.IsDBNull(i) ? null : r.GetString(i);

    static async Task<List<T>> QueryAsync<T>(NpgsqlConnection conn, NpgsqlTransaction tx, string sql, Func<NpgsqlDataReader, T> map)
    {
        using var cmd = new NpgsqlCommand(sql, conn, tx);
        var list = new List<T>();
        var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
        await using (reader.ConfigureAwait(false))
        {
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                list.Add(map(reader));
            }
        }

        return list;
    }
}
