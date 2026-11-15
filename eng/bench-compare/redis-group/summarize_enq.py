import json, statistics, sys
w = sys.argv[1]
arms = sys.argv[2].split(',') if len(sys.argv) > 2 else ['off', 'on', 'bullmq']
rows = {}
for r in range(1, 20):
    try:
        rows[r] = {a: json.load(open(f'results/{w}/{r}-{a}.json')) for a in arms}
    except FileNotFoundError:
        break
for a in arms:
    v = [(rows[r][a]['insertsPerSec'], rows[r][a]['insertLatencyMs']['p95'], rows[r][a]['insertLatencyMs']['p99']) for r in rows]
    print(a, 'jobs/s', [x[0] for x in v], 'median', statistics.median(x[0] for x in v),
          'p95', statistics.median(x[1] for x in v), 'p99', statistics.median(x[2] for x in v))
pairs_ = [(x, 'off') for x in arms if x not in ('off', 'bullmq')] + ([('off', 'bullmq')] + [(x, 'bullmq') for x in arms if x not in ('off', 'bullmq')] if 'bullmq' in arms else [])
for num, den in pairs_:
    t = [rows[r][num]['insertsPerSec'] / rows[r][den]['insertsPerSec'] for r in rows]
    p95 = [rows[r][num]['insertLatencyMs']['p95'] / rows[r][den]['insertLatencyMs']['p95'] for r in rows]
    p99 = [rows[r][num]['insertLatencyMs']['p99'] / rows[r][den]['insertLatencyMs']['p99'] for r in rows]
    print(f'{num}/{den} throughput', [round(x, 2) for x in t], 'median', round(statistics.median(t), 2),
          '| p95 median', round(statistics.median(p95), 2), '| p99 median', round(statistics.median(p99), 2))
for a in arms:
    if a.startswith('on'):
        print('mean group size', a, [rows[r][a].get('redisGroup', {}).get('meanGroupSize') for r in rows])
