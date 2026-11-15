import json, statistics, sys

w = sys.argv[1]
arms = sys.argv[2].split(',') if len(sys.argv) > 2 else ['off', 'on', 'bullmq']
delayed = w.startswith('delayed')


def extract(arm, d):
    r = arm != 'bullmq'
    if w.startswith('drain'):
        return {'jobsPerSec': d['completionsPerSec'], 'finished': d['finished'] if not r else d['finished']}
    out = {
        'acceptedPerSec': d['inserted'] / d['seconds'], 'completedPerSec': d['completed'] / d['seconds'],
        'inserted': d['inserted'], 'completed': d['completed'],
        'refused': d.get('failedDispatch') if r else d.get('refused'),
    }
    enq = d['enqueueLatencyMs'] if 'enqueueLatencyMs' in d else None
    if enq:
        out['enqP95'], out['enqP99'] = enq['p95'], enq['p99']
    if r:
        due = d.get('trace', {}).get('stage:due->handlerInvoked')
        if due:
            out['pickupP95'], out['pickupP99'] = due['p95Ms'], due['p99Ms']
        out['offerToFinishP99'] = d['latencyMs']['p99']
    else:
        if delayed:
            out['pickupP95'], out['pickupP99'] = d['latencyMs']['p95'], d['latencyMs']['p99']
        else:
            out['offerToFinishP99'] = d['latencyMs']['p99']
    return out


rows = {}
for r in range(1, 20):
    try:
        rows[r] = {a: extract(a, json.load(open(f'results/{w}/{r}-{a}.json'))) for a in arms}
    except FileNotFoundError:
        break
metrics = sorted({m for r in rows.values() for a in r.values() for m in a})
for m in metrics:
    line = f'{m:18s}'
    for a in arms:
        v = [rows[r][a].get(m) for r in rows if rows[r][a].get(m) is not None]
        if v:
            line += f' {a}={statistics.median(v):9.1f}'
    print(line)
for num, den in [(x, 'off') for x in arms if x not in ('off', 'bullmq')] + [(x, 'bullmq') for x in arms if x != 'bullmq'] if 'bullmq' in arms else []:
    for m in ('acceptedPerSec', 'completedPerSec', 'jobsPerSec', 'enqP99', 'pickupP99'):
        t = [rows[r][num][m] / rows[r][den][m] for r in rows if rows[r][num].get(m) and rows[r][den].get(m)]
        if t:
            print(f'{num}/{den} {m}: median {statistics.median(t):.2f} pairs {[round(x, 2) for x in t]}')
