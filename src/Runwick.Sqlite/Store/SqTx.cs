using Microsoft.Data.Sqlite;
using Runwick.Protocol;

namespace Runwick.Sqlite.Store;

/// <summary>
/// An operation's open write transaction (20 §4.1): the connection, fault hooks and the decision time read after
/// <c>BEGIN IMMEDIATE</c>. Everything runs synchronously on the thread that owns the connection.
/// </summary>
internal sealed class SqTx(SqliteConnection connection, SqliteTransaction? transaction, SqOp op, IFaultInjector faults, CancellationToken ct)
{
    int _mutations;

    public SqliteConnection Connection => connection;

    public SqliteTransaction? Transaction => transaction;

    public SqOp Op => op;

    public string Application => op.Application;

    public CancellationToken Ct => ct;

    /// <summary>Decision time: ms floor of the clock read after the write lock (20 §4.1 step 3).</summary>
    public DateTimeOffset Now { get; set; }

    /// <summary>RegisterApplication's gate inserted the application row in this transaction.</summary>
    public bool ApplicationSeeded { get; set; }

    /// <summary>The unfloored clock read (for the NotAfter comparison).</summary>
    public DateTimeOffset Clock { get; set; }

    /// <summary>A command on this transaction with <c>@application</c> bound.</summary>
    public SqliteCommand Sql(string sql)
    {
        var cmd = connection.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = sql;
        cmd.Parameters.AddWithValue("@application", op.Application);
        return cmd;
    }

    /// <summary>A fault point (07 §2.3); a planned Barrier or Delay holds this thread, as a server function would hold its backend.</summary>
    public void Fault(FaultPoint point, int step = 0)
    {
        var pending = faults.OnAsync(new FaultContext(point, op.Operation, op.CommandId, op.Application, step), ct);
        if (!pending.IsCompletedSuccessfully)
        {
            pending.AsTask().GetAwaiter().GetResult();
        }
    }

    /// <summary>The decision time is read once after the lock; this only reports whether the command is still in time.</summary>
    public bool Decide() => Clock < op.NotAfter;

    public int Exec(SqliteCommand cmd, bool mutation = true)
    {
        using (cmd)
        {
            BeforeMutation(mutation);
            ct.ThrowIfCancellationRequested();
            var n = cmd.ExecuteNonQuery();
            AfterMutation(mutation);
            return n;
        }
    }

    public List<T> Query<T>(SqliteCommand cmd, Func<SqliteDataReader, T> map, bool mutation = false)
    {
        using (cmd)
        {
            BeforeMutation(mutation);
            ct.ThrowIfCancellationRequested();
            var list = new List<T>();
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    list.Add(map(reader));
                }
            }

            AfterMutation(mutation);
            return list;
        }
    }

    /// <summary>The first row mapped, or default when there is none.</summary>
    public T? One<T>(SqliteCommand cmd, Func<SqliteDataReader, T> map, bool mutation = false)
    {
        var rows = Query(cmd, map, mutation);
        return rows.Count == 0 ? default : rows[0];
    }

    public long Count(SqliteCommand cmd)
    {
        using (cmd)
        {
            return Convert.ToInt64(cmd.ExecuteScalar(), System.Globalization.CultureInfo.InvariantCulture);
        }
    }

    /// <summary>Runs statements without fault points or results (savepoints).</summary>
    public void Raw(string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    /// <summary>Fires <c>BeforeFirstMutation</c> once, before the first mutation (as PgTx).</summary>
    public void BeforeMutation(bool mutation = true)
    {
        if (mutation && _mutations == 0)
        {
            Fault(FaultPoint.BeforeFirstMutation);
        }
    }

    public void AfterMutation(bool mutation = true)
    {
        if (mutation)
        {
            Fault(FaultPoint.AfterMutation, ++_mutations);
        }
    }

    public StoreReply<T> Reject<T>(StoreStatus status, string code, Guid? conflictingJobId = null, Guid? blockedScope = null,
        CandidateCursor? resumeAfter = null, string? detail = null) where T : class =>
        StoreReply<T>.NoEffect(status, new StoreError(code, null, conflictingJobId, blockedScope, resumeAfter, detail), Now);
}
