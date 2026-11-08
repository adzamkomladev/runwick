#!/usr/bin/env python3
"""Driver conformance report (SP-028, gate G-4): joins the case registry with TRX test results, one report per driver.

  python3 eng/conformance.py            read artifacts/conformance/**/*.trx, write docs/conformance/{postgres,sqlite,redis}.md
  python3 eng/conformance.py --check    same, but write nothing (use in CI)

Exit code 0 only if every mandatory applicable case of every driver has a passing result and nothing is hidden:
  - a mandatory applicable case with no passing test is "missing" (G-4 NOT PASSED);
  - a skipped test, or a theory row that returns early for a store kind, that is not listed in docs/conformance/exemptions.csv
    is a hidden skip;
  - a registry case whose test selector matches no test in the source (and is not declared GAP) is an unmapped case.
No third-party packages. See docs/conformance/README.md for the registry format.
"""
import csv, glob, os, re, sys, collections, datetime
import xml.etree.ElementTree as ET

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
CONF = os.path.join(ROOT, 'docs', 'conformance')
ART = os.environ.get('CONFORMANCE_OUT', os.path.join(ROOT, 'artifacts', 'conformance'))
DRIVERS = ['postgres', 'sqlite', 'redis']
KIND = {'Postgres': 'postgres', 'Sqlite': 'sqlite', 'Redis': 'redis'}
# Projects whose tests are driver independent (they run the engine on fakes); they never count as driver evidence.
ENGINE_PROJECTS = {'Runwick.Core.Tests'}
# Projects bound to one driver.
PROJECT_DRIVER = {'Runwick.Postgres.Tests': 'postgres', 'Runwick.Outbox.Tests': 'postgres', 'Runwick.Dashboard.Tests': 'postgres',
                  'Runwick.AspNetCore.Tests': 'postgres', 'Runwick.Sqlite.Tests': 'sqlite', 'Runwick.Redis.Tests': 'redis'}
# Host projects whose store-kind theories count for the named kind; their other tests stay PostgreSQL evidence.
HOST_PROJECTS = {'Runwick.AspNetCore.Tests', 'Runwick.Dashboard.Tests'}
EXCLUDED_PROJECTS ={'Runwick.Redis.Prototype.Tests'}  # SP-018 prototype, replaced by Runwick.Redis.Tests (22 §12)

# ---------------------------------------------------------------- source scan: which test method carries which traits

ATTR = re.compile(r'^\s*\[(.*)\]\s*$')
TRAIT = re.compile(r'Trait\(\s*"(\w+)"\s*,\s*"([^"]*)"\s*\)')
CLASS = re.compile(r'^\s*(?:public\s+|internal\s+)?(?:sealed\s+|static\s+|abstract\s+|partial\s+)*class\s+(\w+)')
METHOD = re.compile(r'^\s*(?:public|internal)\s+(?:static\s+)?(?:async\s+)?[\w<>\[\],\.? ]+?\s+(\w+)\s*\(')
NS = re.compile(r'^\s*namespace\s+([\w\.]+)')
EARLY = re.compile(r'if\s*\(\s*(?:kind|f\.Kind)\s*(==|!=|is)\s*((?:StoreKind\.\w+(?:\s+or\s+)?)+)\s*\)\s*\{\s*return;')


class Method:
    def __init__(self, project, key, traits, vacuous):
        self.project, self.key, self.traits, self.vacuous = project, key, traits, vacuous
        self.partial = set()  # kinds for which only the first part of the body runs (return in the middle)

    def cls(self):
        return self.key.rsplit('.', 1)[0].rsplit('.', 1)[-1]


def scan_source():
    methods = {}
    for path in glob.glob(os.path.join(ROOT, 'tests', '*', '**', '*.cs'), recursive=True):
        rel = os.path.relpath(path, os.path.join(ROOT, 'tests')).split(os.sep)
        if rel[0] in ('Runwick.Harness', 'Runwick.Harness.Peer', 'Runwick.ReferenceModel', 'Runwick.Workloads') or 'obj' in rel or 'bin' in rel:
            continue
        if rel[0] in EXCLUDED_PROJECTS:
            continue
        lines = open(path, encoding='utf-8').read().split('\n')
        ns, pending, is_test = '', {}, False
        stack, depth = [], 0  # classes: [name, depth at declaration, traits, brace seen]
        starts = []  # (line index, key)
        for i, ln in enumerate(lines):
            m = NS.match(ln)
            if m:
                ns = m.group(1)
            a = ATTR.match(ln)
            plain = re.sub(r'"(?:[^"\\]|\\.)*"', '""', ln)
            opens, closes = plain.count('{'), plain.count('}')
            if a:
                body = a.group(1)
                for k, v in TRAIT.findall(body):
                    pending.setdefault(k, set()).add(v)
                if re.search(r'\b(Fact|Theory)\b', body):
                    is_test = True
                continue
            if not ln.strip() or ln.strip().startswith('//'):
                continue
            c = CLASS.match(ln)
            if c:
                stack.append([c.group(1), depth, {k: set(v) for k, v in pending.items()}, False])
                pending, is_test = {}, False
            elif is_test:
                mm = METHOD.match(ln)
                if mm and stack:
                    traits = {k: set(v) for k, v in stack[-1][2].items()}
                    for k, v in pending.items():
                        traits.setdefault(k, set()).update(v)
                    key = f'{ns}.{stack[-1][0]}.{mm.group(1)}'
                    methods[key] = Method(rel[0], key, traits, set())
                    starts.append((i, key))
                pending, is_test = {}, False
            else:
                pending = {}
            depth += opens - closes
            if stack and opens and not stack[-1][3] and depth > stack[-1][1]:
                stack[-1][3] = True
            while stack and ((stack[-1][3] and depth <= stack[-1][1]) or (not stack[-1][3] and ln.rstrip().endswith(';'))):
                stack.pop()
        for n, (i, key) in enumerate(starts):
            end = starts[n + 1][0] if n + 1 < len(starts) else len(lines)
            body = '\n'.join(lines[i:end])
            for em in EARLY.finditer(body):
                kinds = {KIND[k] for k in re.findall(r'StoreKind\.(\w+)', em.group(2)) if k in KIND}
                skipped = (set(DRIVERS) - kinds) if em.group(1) == '!=' else kinds
                if body[body.index('{') + 1:em.start()].strip():
                    methods[key].partial |= skipped
                else:
                    methods[key].vacuous |= skipped
    return methods


NOROWS = {d: set() for d in DRIVERS}

# ---------------------------------------------------------------- TRX

def parse_trx(path):
    text = re.sub(r'\sxmlns="[^"]+"', '', open(path, encoding='utf-8').read(), count=1)
    root = ET.fromstring(text)
    tests = {}
    for ut in root.iter('UnitTest'):
        tm = ut.find('TestMethod')
        if tm is not None:
            tests[ut.get('id')] = (tm.get('className').replace('+', '.'), tm.get('name'))
    out = []
    for r in root.iter('UnitTestResult'):
        cls, name = tests.get(r.get('testId'), (None, None))
        if cls is None:
            continue
        msg = r.find('Output/ErrorInfo/Message')
        out.append(dict(key=f'{cls}.{name.split("(")[0]}', display=r.get('testName'), outcome=r.get('outcome'),
                        message=(msg.text or '').strip().split('\n')[0][:200] if msg is not None else ''))
    return out


def load_results(methods):
    """Returns {driver: {display: result}}; a result is the worst outcome of the first runs, replaced by a later 'rerun' run."""
    results = {d: {} for d in DRIVERS}
    runs = []
    for d in DRIVERS + ['common']:
        for trx in sorted(glob.glob(os.path.join(ART, d, '**', '*.trx'), recursive=True), key=os.path.getmtime):
            runs.append((d, trx))
    rank = {'Passed': 0, 'NotExecuted': 1, 'Failed': 2}
    for run_dir, trx in runs:
        rel = os.path.relpath(trx, ART).split(os.sep)
        project = next((p for p in rel if p.startswith('Runwick.') or p.startswith('rerun-Runwick.')), '').replace('rerun-', '')
        rerun = any(p.startswith('rerun-') for p in rel)
        for r in parse_trx(trx):
            m = methods.get(r['key'])
            if m is None or project in EXCLUDED_PROJECTS:
                continue
            if r['outcome'] == 'Failed' and r['message'].startswith('No data found for'):
                # An xunit theory whose data excludes this store kind: the filtered run has no rows for it, which is not a result.
                if run_dir in DRIVERS:
                    NOROWS[run_dir].add(r['key'].split('Tests.', 1)[-1])
                continue
            drivers = set()
            if project in ENGINE_PROJECTS:
                drivers = set(DRIVERS)
            elif project in HOST_PROJECTS and (host_kinds := {KIND[k] for k in re.findall(r'\b(Postgres|Sqlite|Redis)\b', r['display'].split('(', 1)[1] if '(' in r['display'] else '')}):
                drivers = host_kinds  # a host test that is a theory over the store kind runs on each (SP-028)
            elif project == 'Runwick.Outbox.Tests' and (dest := re.findall(r'\bstore: (Postgres|Sqlite|Redis)\b', r['display'])):
                drivers = {KIND[k] for k in dest}  # a relay theory over the destination store kind (its "kind:" argument is the source)
            elif project in PROJECT_DRIVER:
                drivers = {PROJECT_DRIVER[project]}
            else:  # Contract.Tests and Cli.Tests: the store kind named in the theory arguments, else the run's own driver
                kinds = {KIND[k] for k in re.findall(r'\b(Postgres|Sqlite|Redis)\b', r['display'].split('(', 1)[1] if '(' in r['display'] else '')}
                if kinds:
                    drivers = kinds
                elif project == 'Runwick.Cli.Tests':
                    drivers = {'sqlite'} if 'Sqlite' in r['key'] else ({'redis'} if 'Redis' in r['key'] else {'postgres'})
                elif run_dir in DRIVERS:
                    drivers = {run_dir}
                else:
                    drivers = set(DRIVERS)
            res = dict(r, project=project, engine=project in ENGINE_PROJECTS, rerun=rerun, first_failure='')
            for d in drivers:
                old = results[d].get(r['display'])
                if old is not None:
                    if rerun and old['outcome'] in ('Failed', 'NotExecuted'):
                        res = dict(res, first_failure=old['message'] or old['outcome'])
                    elif rank.get(res['outcome'], 1) < rank.get(old['outcome'], 1):
                        res = old  # worst outcome of repeated plain runs wins
                results[d][r['display']] = res
    return results, [t for _, t in runs]


# ---------------------------------------------------------------- registry

def read_csv(path):
    with open(path, newline='', encoding='utf-8') as f:
        return [r for r in csv.DictReader(f)]


def resolve(selector_text, methods):
    out, gap = set(), None
    for sel in [s.strip() for s in selector_text.split(';') if s.strip()]:
        if sel.startswith('GAP:'):
            gap = sel[4:].strip()
            continue
        projects = None
        if '@' in sel:
            sel, p = sel.split('@', 1)
            projects = set(p.split('|'))
        kind, _, val = sel.partition('=')
        for k, m in methods.items():
            if projects and m.project not in projects:
                continue
            if (kind == 'Case' and val in m.traits.get('Case', ())) or (kind == 'Class' and m.cls() == val) or (kind == 'Method' and k.endswith('.' + val)):
                out.add(k)
    return out, gap


def cell(text):
    kind, _, reason = text.partition(':')
    return kind.strip(), reason.strip()


def short(display):
    name = display.split('Tests.', 1)[-1]
    return name if len(name) < 110 else name[:107] + '...'


def evaluate(case, drv, keys, results, methods, exempt):
    kind, reason = cell(case[drv])
    mandatory = case['mandatory'].strip().lower() == 'yes'
    if kind == 'n/a':
        return dict(status='N/A', note=reason, evidence=[], mandatory=mandatory, kind=kind)
    layer = case['layer']
    mine = [r for r in results[drv].values() if r['key'] in keys and (layer != 'driver' or not r['engine'])]
    if kind == 'unsupported':
        mine = [r for r in mine if drv in methods[r['key']].traits.get('Unsupported', ())]
    hidden = []
    usable = []
    for r in mine:
        m = methods[r['key']]
        if r['outcome'] == 'NotExecuted':
            if not exempt_for(exempt, r['key'], drv):
                hidden.append(f'skipped without exemption: {short(r["display"])}')
            continue
        e = exempt_for(exempt, r['key'], drv)
        if drv in m.vacuous and not e:
            hidden.append(f'returns early for {drv}, no exemption: {short(r["display"])}')
            continue
        if drv in m.vacuous or (e and e['kind'] == 'vacuous'):
            continue  # exempt early return: documented, never counted as a pass
        if drv in m.partial and not e:
            hidden.append(f'part of the body is skipped for {drv}, no exemption: {short(r["display"])}')
        usable.append(r)
    fails = [r for r in usable if r['outcome'] == 'Failed']
    passes = [r for r in usable if r['outcome'] == 'Passed']
    if kind in ('gap', 'partial'):
        status, note = ('not-run' if kind == 'gap' else 'partial'), reason
    elif fails:
        status, note = 'fail', fails[0]['message']
    elif passes:
        status, note = 'pass', reason if kind == 'unsupported' else ''
    else:
        status, note = 'not-run', 'no test result for this driver in the recorded run' if not hidden else '; '.join(hidden)
    ev = sorted({short(r['display']) for r in (passes if status == 'pass' else usable)})
    rr = [r for r in passes if r['rerun'] and r['first_failure']]
    if rr:
        note = (note + ' ' if note else '') + f'(passed on a rerun alone; first run failed: {rr[0]["first_failure"][:90]})'
    return dict(status=status, note=note, evidence=ev, mandatory=mandatory, kind=kind, hidden=hidden, n=len(usable))


def exempt_for(exempt, key, drv):
    for e in exempt:
        if e['driver'] in (drv, '*') and key.endswith('.' + e['test'].strip()):
            return e
    return None


# ---------------------------------------------------------------- report

def md(s):
    return str(s).replace('|', '\\|').replace('\n', ' ')


def write_report(drv, rows, meta, exempt, problems, mdir):
    count = collections.Counter(r['res']['status'] for r in rows)
    mand_missing = [r for r in rows if r['res']['mandatory'] and r['res']['status'] not in ('pass', 'N/A')]
    verdict = 'PASS' if not mand_missing and not problems else 'NOT PASSED'
    title = {'postgres': 'PostgreSQL', 'sqlite': 'SQLite', 'redis': 'Redis'}[drv]
    L = [f'# Conformance report: {title}', '',
         '> Generated by `eng/conformance.py` from `docs/conformance/cases.csv` and the TRX files of the recorded run. Do not edit by hand.', '',
         f'**G-4 verdict for {title}: {verdict}.**', '',
         'What this is: every test case the specs require of this driver, with what happened when the whole test suite was run against a real '
         f'{title} store. Each row is one case. "Applicability" says whether the case applies to this driver and why not if it does not. '
         '"Result" is one of: **pass** (every test for the case passed), **fail**, **not-run** (no test exists or it could not run here), '
         '**partial** (only part of the case is built or run), **N/A** (the specs say the case does not apply to this driver), or '
         '**unsupported: refused (pass)** (the driver declares it cannot do this, and a test shows it refuses with an explicit error). '
         'A case never passes by being skipped: skipped tests and early-return theory rows are listed below and do not count.', '',
         '## Run', '']
    for k in ('commit', 'date', 'host', 'dotnet', 'server', 'started', 'finished', 'seconds'):
        if meta.get(drv, {}).get(k):
            L.append(f'- {k}: {meta[drv][k]}')
    for k, v in sorted(meta.get('common', {}).items()):
        if k in ('seconds', 'started', 'finished'):
            L.append(f'- shared engine/host projects, {k}: {v}')
    L += ['', '## Counts', '', f'- cases in the registry: {len(rows)}',
          f'- pass: {count["pass"]}, fail: {count["fail"]}, partial: {count["partial"]}, not-run: {count["not-run"]}, N/A: {count["N/A"]}',
          f'- mandatory cases that apply to this driver: {sum(1 for r in rows if r["res"]["mandatory"] and r["res"]["status"] != "N/A")}',
          f'- mandatory applicable cases without a passing result: {len(mand_missing)}', '']
    if mand_missing:
        L += ['## Missing for G-4', '', 'These mandatory applicable cases have no passing result:', '']
        L += [f'- **{r["case"]["case"]}** ({r["res"]["status"]}): {md(r["res"]["note"] or "no result")}' for r in mand_missing]
        L.append('')
    if problems:
        L += ['## Hidden-skip and registry problems (each blocks G-4)', ''] + [f'- {md(p)}' for p in problems] + ['']
    ex = [e for e in exempt if e['driver'] in (drv, '*')]
    L += ['## Documented exemptions', '',
          'A test listed here returns early or is skipped for this driver. It is shown, never counted as a pass.', '']
    L += [f'- `{e["test"]}` ({e["kind"]}): {md(e["reason"])}' for e in ex] or ['- none']
    if NOROWS[drv]:
        L += ['', 'Theories that have no rows for this store (their data lists exclude it, so the filtered run reports "No data found"). They are not results and do not count:', ''] + [f'- `{x}`' for x in sorted(NOROWS[drv])]
    L += ['', '## Cases', '', '| Case | Source | Mandatory | Applicability | Result | Evidence |', '|---|---|---|---|---|---|']
    for r in rows:
        res, c = r['res'], r['case']
        kind, reason = cell(c[drv])
        app = kind + (': ' + reason if reason else '')
        result = res['status'] + (' ' + 'refused (pass)' if kind == 'unsupported' and res['status'] == 'pass' else '')
        if kind == 'unsupported' and res['status'] == 'pass':
            result = 'unsupported: refused (pass)'
        ev = '; '.join(res['evidence'][:2]) + (f' (+{len(res["evidence"]) - 2} more)' if len(res['evidence']) > 2 else '')
        if res['note'] and kind not in ('gap', 'partial') and (res['status'] in ('not-run', 'fail', 'partial') or 'rerun' in res['note']):
            ev = (ev + ' ' if ev else '') + res['note']
        stamp = f' [{meta.get(drv, {}).get("date", "")} {meta.get(drv, {}).get("commit", "")}]' if res['evidence'] else ''
        L.append(f'| {c["case"]} | {md(c["source"])} | {c["mandatory"]} | {md(app)} | {result} | {md(ev + stamp)} |')
    open(os.path.join(mdir, f'{drv}.md'), 'w', encoding='utf-8').write('\n'.join(L) + '\n')
    return verdict, count, mand_missing


def read_meta():
    meta = {}
    for d in DRIVERS + ['common']:
        p = os.path.join(ART, d, 'meta.txt')
        if os.path.exists(p):
            meta[d] = dict(l.rstrip('\n').split('=', 1) for l in open(p) if '=' in l)
    return meta


def main():
    check_only = '--check' in sys.argv
    methods = scan_source()
    cases = read_csv(os.path.join(CONF, 'cases.csv'))
    exempt = read_csv(os.path.join(CONF, 'exemptions.csv')) if os.path.exists(os.path.join(CONF, 'exemptions.csv')) else []
    results, _ = load_results(methods)
    meta = read_meta()
    # Registry integrity: every case must map to tests or declare a GAP.
    unmapped = []
    resolved = {}
    for c in cases:
        keys, gap = resolve(c['tests'], methods)
        resolved[c['case']] = keys
        applies = any(cell(c[d])[0] != 'n/a' for d in DRIVERS)
        if applies and not keys and not gap:
            unmapped.append(f'{c["case"]}: selector "{c["tests"]}" matches no test and no GAP is declared')
        if gap and any(cell(c[d])[0] == 'unsupported' for d in DRIVERS):
            unmapped.append(f'{c["case"]}: an unsupported capability needs a test, GAP is not allowed')
    summary, ok = [], not unmapped
    os.makedirs(CONF, exist_ok=True)
    for drv in DRIVERS:
        rows, problems = [], list(unmapped)
        for c in cases:
            res = evaluate(c, drv, resolved[c['case']], results, methods, exempt)
            for h in res.get('hidden', []):
                problems.append(f'{c["case"]}: {h}')
            gapkeys = resolve(c['tests'], methods)[1]
            if gapkeys and res['status'] == 'not-run':
                res['note'] = 'gap: ' + gapkeys
            rows.append(dict(case=c, res=res))
        # a skipped test that maps to no case is still a hidden skip
        for r in results[drv].values():
            if r['outcome'] == 'NotExecuted' and not exempt_for(exempt, r['key'], drv):
                msg = f'skipped test without exemption: {short(r["display"])}'
                if msg not in problems:
                    problems.append(msg)
        if not check_only:
            verdict, count, miss = write_report(drv, [dict(case=r['case'], res=r['res']) for r in rows], meta, exempt, sorted(set(problems)), CONF)
        else:
            count = collections.Counter(r['res']['status'] for r in rows)
            miss = [r for r in rows if r['res']['mandatory'] and r['res']['status'] not in ('pass', 'N/A')]
            verdict = 'PASS' if not miss and not problems else 'NOT PASSED'
        ok = ok and verdict == 'PASS'
        if '--verbose' in sys.argv:
            for x in sorted(set(problems)):
                print('   ', drv, x)
        summary.append((drv, verdict, count, len(miss), len(set(problems))))
    for drv, verdict, count, miss, probs in summary:
        print(f'{drv:9} G-4 {verdict:10} pass={count["pass"]} fail={count["fail"]} partial={count["partial"]} not-run={count["not-run"]} '
              f'N/A={count["N/A"]} missing-mandatory={miss} problems={probs}')
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
