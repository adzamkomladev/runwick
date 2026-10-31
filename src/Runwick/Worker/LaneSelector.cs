using Runwick.Protocol;

namespace Runwick.Worker;

/// <summary>One selection: the chosen lane plus the trial scores to commit on an Applied reservation.</summary>
internal sealed record LaneChoice(
    string Queue,
    string Priority,
    bool Aging,
    IReadOnlyList<string> EligibleQueues,
    IReadOnlyList<string> EligibleLanes);

/// <summary>
/// Pure per-pool admission-opportunity state (05 §8.2–§8.5): queue and lane smooth weighted round robin, aging,
/// cooldowns and tie rotation. Weights divide admission opportunities, never execution time.
/// </summary>
internal sealed class LaneSelector
{
    public static readonly string[] Priorities = ["high", "medium", "low"];

    readonly IReadOnlyList<(string Queue, int Weight)> _queues;
    readonly Dictionary<string, int> _laneWeight;
    readonly LaneSelectionMode _mode;
    readonly Dictionary<string, long> _queueScore = new(StringComparer.Ordinal);
    readonly Dictionary<(string, string), long> _laneScore = [];
    readonly Dictionary<string, int> _agingCount = new(StringComparer.Ordinal);
    readonly Dictionary<string, int> _agingPointer = new(StringComparer.Ordinal);
    readonly Dictionary<string, (TimeSpan Until, int Step)> _queueCooldown = new(StringComparer.Ordinal);
    readonly Dictionary<(string, string), (TimeSpan Until, int Step)> _laneCooldown = [];
    HashSet<string> _lastQueues = new(StringComparer.Ordinal);
    readonly Dictionary<string, HashSet<string>> _lastLanes = new(StringComparer.Ordinal);
    int _tieBreak;

    public LaneSelector(PoolSettings pool)
    {
        _queues = pool.Queues;
        _mode = pool.Mode;
        _laneWeight = new(StringComparer.Ordinal) { ["high"] = pool.LaneWeights.High, ["medium"] = pool.LaneWeights.Medium, ["low"] = pool.LaneWeights.Low };
    }

    public IEnumerable<LaneKey> Lanes => _queues.SelectMany(q => Priorities.Select(p => new LaneKey(q.Queue, p)));

    public long QueueScore(string q) => _queueScore.GetValueOrDefault(q);

    public long LaneScore(string q, string p) => _laneScore.GetValueOrDefault((q, p));

    public int AgingCount(string q) => _agingCount.GetValueOrDefault(q);

    /// <summary>
    /// Picks (q, p) among lanes in <paramref name="ready"/> (lane → age) that are not cooling down and not excluded.
    /// Returns null when nothing is eligible.
    /// </summary>
    public LaneChoice? Choose(IReadOnlyDictionary<LaneKey, TimeSpan> ready, TimeSpan now)
    {
        var eligibleQueues = new List<(string Queue, int Weight)>();
        var lanesByQueue = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        foreach (var (q, w) in _queues)
        {
            if (_queueCooldown.TryGetValue(q, out var qc) && now < qc.Until)
            {
                continue;
            }

            var lanes = Priorities.Where(p => ready.ContainsKey(new LaneKey(q, p))
                && !(_laneCooldown.TryGetValue((q, p), out var lc) && now < lc.Until)).ToList();
            if (lanes.Count > 0)
            {
                eligibleQueues.Add((q, w));
                lanesByQueue[q] = lanes;
            }
        }

        if (eligibleQueues.Count == 0)
        {
            return null;
        }

        // Queue membership change: entering members start at 0; carried scores clamp to ± the new total.
        var queueSet = eligibleQueues.Select(e => e.Queue).ToHashSet(StringComparer.Ordinal);
        if (!queueSet.SetEquals(_lastQueues))
        {
            var total = eligibleQueues.Sum(e => (long)e.Weight);
            foreach (var q in queueSet.Where(q => !_lastQueues.Contains(q)))
            {
                _queueScore[q] = 0;
            }

            foreach (var q in queueSet)
            {
                _queueScore[q] = Math.Clamp(_queueScore.GetValueOrDefault(q), -total, total);
            }

            _lastQueues = queueSet;
        }

        var start = _tieBreak++ % eligibleQueues.Count;
        var best = -1;
        for (var k = 0; k < eligibleQueues.Count; k++)
        {
            var i = (start + k) % eligibleQueues.Count;
            if (best < 0 || Trial(eligibleQueues[i]) > Trial(eligibleQueues[best]))
            {
                best = i;
            }
        }

        var queue = eligibleQueues[best].Queue;
        var (priority, aging) = ChooseLane(queue, lanesByQueue[queue], ready, now);
        return new LaneChoice(queue, priority, aging, [.. queueSet], lanesByQueue[queue]);

        long Trial((string Queue, int Weight) e) => checked(_queueScore.GetValueOrDefault(e.Queue) + e.Weight);
    }

    (string Priority, bool Aging) ChooseLane(string q, List<string> lanes, IReadOnlyDictionary<LaneKey, TimeSpan> ready, TimeSpan now)
    {
        var laneSet = lanes.ToHashSet(StringComparer.Ordinal);
        if (!_lastLanes.TryGetValue(q, out var last) || !laneSet.SetEquals(last))
        {
            var total = lanes.Sum(p => (long)_laneWeight[p]);
            foreach (var p in lanes.Where(p => last is null || !last.Contains(p)))
            {
                _laneScore[(q, p)] = 0;
            }

            foreach (var p in lanes)
            {
                _laneScore[(q, p)] = Math.Clamp(_laneScore.GetValueOrDefault((q, p)), -total, total);
            }

            _lastLanes[q] = laneSet;
        }

        // Aging (05 §8.5): after AgingEvery non-aging admissions, an aged lower lane gets the next opportunity.
        if (_mode != LaneSelectionMode.Strict && _agingCount.GetValueOrDefault(q) >= WorkerSettings.AgingEvery)
        {
            var top = lanes.Max(p => _laneWeight[p]);
            var aged = Priorities
                .Where(p => laneSet.Contains(p) && _laneWeight[p] < top && ready[new LaneKey(q, p)] >= WorkerSettings.AgingThreshold)
                .ToList();
            if (aged.Count > 0)
            {
                var pointer = _agingPointer.GetValueOrDefault(q);
                for (var k = 0; k < Priorities.Length; k++)
                {
                    var p = Priorities[(pointer + k) % Priorities.Length];
                    if (aged.Contains(p))
                    {
                        return (p, true);
                    }
                }
            }
        }

        var start = _tieBreak % lanes.Count;
        var best = -1;
        for (var k = 0; k < lanes.Count; k++)
        {
            var i = (start + k) % lanes.Count;
            var better = best < 0 || (_mode == LaneSelectionMode.Weighted
                ? Trial(lanes[i]) > Trial(lanes[best])
                : _laneWeight[lanes[i]] > _laneWeight[lanes[best]]);
            if (better)
            {
                best = i;
            }
        }

        return (lanes[best], false);

        long Trial(string p) => checked(_laneScore.GetValueOrDefault((q, p)) + _laneWeight[p]);
    }

    /// <summary>Commits the trial scores of an Applied reservation (05 §8.4 step 2, §8.5).</summary>
    public void OnApplied(LaneChoice c)
    {
        var queueWeights = _queues.Where(e => c.EligibleQueues.Contains(e.Queue)).ToList();
        foreach (var (q, w) in queueWeights)
        {
            _queueScore[q] = checked(_queueScore.GetValueOrDefault(q) + w);
        }

        _queueScore[c.Queue] = checked(_queueScore[c.Queue] - queueWeights.Sum(e => (long)e.Weight));

        if (c.Aging)
        {
            _agingCount[c.Queue] = 0;
            foreach (var p in Priorities)
            {
                _laneScore[(c.Queue, p)] = 0;
            }

            return;
        }

        if (_mode == LaneSelectionMode.Weighted)
        {
            foreach (var p in c.EligibleLanes)
            {
                _laneScore[(c.Queue, p)] = checked(_laneScore.GetValueOrDefault((c.Queue, p)) + _laneWeight[p]);
            }

            _laneScore[(c.Queue, c.Priority)] = checked(_laneScore[(c.Queue, c.Priority)] - c.EligibleLanes.Sum(p => (long)_laneWeight[p]));
        }

        _agingCount[c.Queue] = Math.Min(WorkerSettings.AgingEvery, _agingCount.GetValueOrDefault(c.Queue) + 1);
        ResetCooldown(c.Queue, c.Priority);
    }

    /// <summary>Any non-Applied result discards the trial; a failed aging attempt advances the aging pointer.</summary>
    public void OnNotApplied(LaneChoice c)
    {
        if (c.Aging)
        {
            var index = Array.IndexOf(Priorities, c.Priority);
            _agingPointer[c.Queue] = (index + 1) % Priorities.Length;
        }
    }

    public void CoolQueue(string q, TimeSpan now) => _queueCooldown[q] = Next(_queueCooldown.GetValueOrDefault(q), now);

    public void CoolLane(string q, string p, TimeSpan now) => _laneCooldown[(q, p)] = Next(_laneCooldown.GetValueOrDefault((q, p)), now);

    /// <summary>Clears the queue cooldown and every lane cooldown of <paramref name="q"/> (relevant control change).</summary>
    public void ResetQueue(string q)
    {
        _queueCooldown.Remove(q);
        foreach (var p in Priorities)
        {
            _laneCooldown.Remove((q, p));
        }
    }

    /// <summary>A local attempt of <paramref name="q"/> retired: the queue-scope cooldown ends; lane cooldowns stay.</summary>
    public void ReleaseQueue(string q) => _queueCooldown.Remove(q);

    public void ResetCooldown(string q, string p)
    {
        _queueCooldown.Remove(q);
        _laneCooldown.Remove((q, p));
    }

    /// <summary>250 ms doubling per consecutive cooldown to 2 s, ±20 % (05 §8.6).</summary>
    static (TimeSpan Until, int Step) Next((TimeSpan Until, int Step) current, TimeSpan now)
    {
        var step = Math.Min(current.Step + 1, 8);
        var nominal = TimeSpan.FromTicks(Math.Min(WorkerSettings.CooldownCap.Ticks, WorkerSettings.CooldownBase.Ticks << (step - 1)));
        return (now + EngineClock.Jitter(nominal), step);
    }
}
