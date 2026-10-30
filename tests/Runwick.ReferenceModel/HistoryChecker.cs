namespace Runwick.ReferenceModel;

public sealed record HistoryVerdict(bool Legal, string Explanation, JobModel? Final);

/// <summary>
/// Replays an observed per-job history through <see cref="TransitionRules"/>. Commands that
/// share a decided time may have committed in either order, so every permutation of such a
/// group is tried; the history is legal if some ordering applies every command.
/// </summary>
public static class HistoryChecker
{
    public static HistoryVerdict Check(JobModel initial, IEnumerable<ModelCommand> observed)
    {
        var groups = observed.OrderBy(c => c.DecidedAt).GroupBy(c => c.DecidedAt).Select(g => g.ToList()).ToList();
        return Walk(initial, groups, 0, []);
    }

    static HistoryVerdict Walk(JobModel job, List<List<ModelCommand>> groups, int index, List<string> trace)
    {
        if (index == groups.Count)
        {
            return new HistoryVerdict(true, string.Join(" → ", trace), job);
        }

        HistoryVerdict? lastFailure = null;
        foreach (var order in Permutations(groups[index]))
        {
            var current = job;
            var steps = new List<string>(trace);
            var failed = false;
            foreach (var cmd in order)
            {
                var result = TransitionRules.Apply(current, cmd);
                if (!result.Applied)
                {
                    lastFailure = new HistoryVerdict(false, $"{cmd} rejected ({result.Code}) after {string.Join(" → ", steps)}", null);
                    failed = true;
                    break;
                }

                if (result.Job.State != current.State && !TransitionRules.IsLegalEdge(current.State, result.Job.State))
                {
                    lastFailure = new HistoryVerdict(false, $"{cmd} produced illegal edge {current.State}→{result.Job.State}", null);
                    failed = true;
                    break;
                }

                steps.Add($"{cmd.Op}:{result.Job.State}");
                current = result.Job;
            }

            if (failed)
            {
                continue;
            }

            var verdict = Walk(current, groups, index + 1, steps);
            if (verdict.Legal)
            {
                return verdict;
            }

            lastFailure = verdict;
        }

        return lastFailure ?? new HistoryVerdict(false, "empty group", null);
    }

    static IEnumerable<IReadOnlyList<ModelCommand>> Permutations(List<ModelCommand> items)
    {
        if (items.Count <= 1)
        {
            yield return items;
            yield break;
        }

        for (var i = 0; i < items.Count; i++)
        {
            var rest = items.Where((_, j) => j != i).ToList();
            foreach (var tail in Permutations(rest))
            {
                yield return [items[i], .. tail];
            }
        }
    }
}
