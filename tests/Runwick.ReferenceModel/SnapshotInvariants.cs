namespace Runwick.ReferenceModel;

/// <summary>Invariants every committed snapshot must satisfy, whatever crashed or raced.</summary>
public static class SnapshotInvariants
{
    public static IReadOnlyList<string> Violations(StoreSnapshot snapshot)
    {
        var errors = new List<string>();
        var permitsByJob = snapshot.Permits.ToLookup(p => p.JobId);
        var attemptsByJob = snapshot.Attempts.ToLookup(a => a.JobId);
        foreach (var job in snapshot.Jobs)
        {
            if (!States.All.Contains(job.State))
            {
                errors.Add($"{job.JobId}: unknown state {job.State}");
            }

            var ownership = new object?[] { job.CurrentAttemptId, job.OwnershipToken, job.WorkerInstanceId, job.LeaseExpiresAt };
            var present = ownership.Count(o => o is not null);
            if (present is not 0 and not 4)
            {
                errors.Add($"{job.JobId}: ownership columns partially set ({present}/4)");
            }

            var owned = States.OwnedStates.Contains(job.State);
            if (owned != (present == 4))
            {
                errors.Add($"{job.JobId}: state {job.State} but ownership present={present == 4}");
            }

            var permits = permitsByJob[job.JobId].ToList();
            if (!owned && permits.Count > 0)
            {
                errors.Add($"{job.JobId}: {permits.Count} permit(s) held while {job.State}");
            }

            if (permits.Any(p => p.AttemptId != job.CurrentAttemptId))
            {
                errors.Add($"{job.JobId}: permit held by a non-current attempt");
            }

            if (job.StartedCount > job.MaxExecutions)
            {
                errors.Add($"{job.JobId}: started {job.StartedCount} > budget {job.MaxExecutions}");
            }

            var open = attemptsByJob[job.JobId].Where(a => a.ClosedAt is null).ToList();
            if (open.Count > 1)
            {
                errors.Add($"{job.JobId}: {open.Count} open attempts");
            }

            if (owned && (open.Count != 1 || open[0].AttemptId != job.CurrentAttemptId))
            {
                errors.Add($"{job.JobId}: owned but the open attempt is not the current attempt");
            }

            if (!owned && open.Count != 0)
            {
                errors.Add($"{job.JobId}: open attempt while {job.State}");
            }

            if (job.State == States.Running && open.SingleOrDefault() is { ExecutionNumber: null })
            {
                errors.Add($"{job.JobId}: running attempt without an execution number");
            }
        }

        foreach (var group in snapshot.DispatchKeys.Where(k => k.Kind == "active" && k.Active).GroupBy(k => k.Key))
        {
            if (group.Select(k => k.JobId).Distinct().Count() > 1)
            {
                errors.Add($"active key {group.Key} held by {group.Count()} jobs");
            }
        }

        return errors;
    }

    /// <summary>Counters never decrease between two snapshots of the same job generation.</summary>
    public static IReadOnlyList<string> MonotonicViolations(StoreSnapshot before, StoreSnapshot after)
    {
        var errors = new List<string>();
        foreach (var b in before.Jobs)
        {
            var a = after.Jobs.SingleOrDefault(j => j.JobId == b.JobId);
            if (a is null)
            {
                continue;
            }

            if (a.Generation < b.Generation)
            {
                errors.Add($"{b.JobId}: generation decreased");
            }

            if (a.LifetimeExecutions < b.LifetimeExecutions)
            {
                errors.Add($"{b.JobId}: lifetime executions decreased");
            }

            if (a.Generation == b.Generation && a.StartedCount < b.StartedCount)
            {
                errors.Add($"{b.JobId}: started count decreased within a generation");
            }

            if (a.Revision < b.Revision)
            {
                errors.Add($"{b.JobId}: revision decreased");
            }

            if (States.Terminal.Contains(b.State) && a.Generation == b.Generation && a.State != b.State)
            {
                errors.Add($"{b.JobId}: left terminal state {b.State} within a generation");
            }
        }

        return errors;
    }
}
