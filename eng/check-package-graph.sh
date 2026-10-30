#!/usr/bin/env bash
# Enforces eng/package-graph.json: allowed project/package edges, no cycles, producer budget.
set -euo pipefail
cd "$(dirname "$0")/.."
python3 - <<'PY'
import json, re, sys, pathlib
graph = json.load(open("eng/package-graph.json"))
pkgs = graph["packages"]
errors = []
def refs(csproj):
    text = pathlib.Path(csproj).read_text()
    projects = [pathlib.PureWindowsPath(m).stem for m in re.findall(r'<ProjectReference Include="([^"]+)"', text)]
    packages = re.findall(r'<PackageReference Include="([^"]+)"', text)
    return projects, packages
edges = {}
for name, spec in pkgs.items():
    path = pathlib.Path(f"src/{name}/{name}.csproj")
    if spec["exists"] != path.exists():
        errors.append(f"{name}: exists={spec['exists']} but {path} {'present' if path.exists() else 'missing'}")
        continue
    if not path.exists():
        continue
    projects, packages = refs(path)
    edges[name] = projects
    for p in projects:
        if p not in spec["allowedProjectRefs"]:
            errors.append(f"{name} -> {p}: project edge not allowed")
    for p in packages:
        if p not in spec["allowedPackages"]:
            errors.append(f"{name} -> {p}: package not allowed")
for src in pathlib.Path("src").glob("*/*.csproj"):
    if src.stem not in pkgs:
        errors.append(f"{src.stem}: not declared in eng/package-graph.json")
# acyclic
state = {}
def visit(n, stack):
    if state.get(n) == 1:
        errors.append("cycle: " + " -> ".join(stack + [n])); return
    if state.get(n) == 2: return
    state[n] = 1
    for m in edges.get(n, []): visit(m, stack + [n])
    state[n] = 2
for n in edges: visit(n, [])
# producer budget: transitive closure of a contracts-only sample has no packages
for name, budget in graph["producerBudget"].items():
    path = next(pathlib.Path(".").glob(f"samples/{name}/{name}.csproj"))
    projects, packages = refs(path)
    seen, todo, external = set(), list(projects), list(packages)
    while todo:
        p = todo.pop()
        if p in seen: continue
        seen.add(p)
        pp = pathlib.Path(f"src/{p}/{p}.csproj")
        if pp.exists():
            sub_projects, sub_packages = refs(pp)
            todo += sub_projects; external += sub_packages
    if len(external) > budget["maxNonRunwickPackages"]:
        errors.append(f"{name}: producer budget exceeded by {external}")
if errors:
    print("package graph check FAILED"); [print("  " + e) for e in errors]; sys.exit(1)
print(f"package graph OK ({len(edges)} packages built, {len(pkgs) - len(edges)} reserved)")
PY
