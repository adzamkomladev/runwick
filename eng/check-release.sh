#!/usr/bin/env bash
# Packs every src project into artifacts/packages (0 warnings required), then inspects each .nupkg against
# eng/package-graph.json: metadata, dependency list, net10.0-only, README, no unexpected files, symbols package.
# Set SKIP_PACK=1 to inspect existing packages only. Nothing is published.
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/.dotnet:$PATH" DOTNET_NOLOGO=1
out=artifacts/packages
if [ "${SKIP_PACK:-0}" != "1" ]; then
  rm -rf "$out"; mkdir -p "$out"
  for proj in src/*/*.csproj; do
    log=$(dotnet pack "$proj" -c Release -o "$out" --nologo 2>&1) || { echo "$log"; echo "pack failed: $proj"; exit 1; }
    if echo "$log" | grep -qE "warning "; then echo "$log" | grep -E "warning "; echo "pack warnings: $proj"; exit 1; fi
  done
fi
python3 - <<'PY'
import json, re, sys, zipfile, pathlib
import xml.etree.ElementTree as ET
graph = json.load(open("eng/package-graph.json"))["packages"]
errors = []
pinned = set(re.findall(r'PackageVersion Include="([^"]+)"', open("Directory.Packages.props").read()))
def closure(n):  # packages and frameworks that flow in through allowed project edges
    spec = graph[n]; got = set(spec["allowedPackages"])
    for r in spec["allowedProjectRefs"]: got |= closure(r)
    return got
for name, spec in graph.items():
    ids = list(pathlib.Path("artifacts/packages").glob(f"{name}.*.nupkg"))
    ids = [p for p in ids if re.fullmatch(re.escape(name) + r"\.\d.*\.nupkg", p.name)]
    if len(ids) != 1:
        errors.append(f"{name}: expected one .nupkg, found {len(ids)}"); continue
    pkg = ids[0]
    if not pkg.with_suffix(".snupkg").exists():
        errors.append(f"{name}: symbols package (.snupkg) missing")
    z = zipfile.ZipFile(pkg)
    names = z.namelist()
    nuspec = next(n for n in names if n.endswith(".nuspec"))
    root = ET.fromstring(z.read(nuspec))
    ns = {"n": root.tag.split("}")[0].strip("{")}
    meta = root.find("n:metadata", ns)
    def get(tag):
        e = meta.find(f"n:{tag}", ns); return (e.text or "").strip() if e is not None else ""
    if get("id") != name: errors.append(f"{name}: id is '{get('id')}'")
    for tag in ("version", "description", "authors", "license", "readme", "projectUrl", "copyright", "tags"):
        if not get(tag): errors.append(f"{name}: metadata '{tag}' missing")
    if get("license") != "MIT": errors.append(f"{name}: license expression is '{get('license')}'")
    repo = meta.find("n:repository", ns)
    if repo is None or not repo.get("url"): errors.append(f"{name}: repository url missing")
    if name == "Runwick.Dashboard" and "THIRD-PARTY-NOTICES.md" not in names: errors.append(f"{name}: THIRD-PARTY-NOTICES.md not in package")
    if "README.md" not in names: errors.append(f"{name}: README.md not in package")
    # dependencies: exactly the allowed edges/packages, in the net10.0 group only
    want = set(spec["allowedProjectRefs"]) | {p for p in spec["allowedPackages"] if "(framework)" not in p}
    want_fw = {p.split(" ")[0] for p in spec["allowedPackages"] if "(framework)" in p}
    is_tool = any(n.startswith("tools/") for n in names)
    deps = meta.find("n:dependencies", ns)
    groups = deps.findall("n:group", ns) if deps is not None else []
    tfms = {g.get("targetFramework") for g in groups}
    if not is_tool and tfms - {"net10.0", ".NETCoreApp,Version=v10.0"}: errors.append(f"{name}: unexpected target frameworks {tfms}")
    have = {d.get("id") for g in groups for d in g.findall("n:dependency", ns)}
    # Central transitive pinning lists pinned transitive packages as dependencies too; those must come from the allowed closure or be pinned.
    if not is_tool and (want - have or have - want - pinned):
        errors.append(f"{name}: dependencies {sorted(have)} != allowed {sorted(want)} (+ pinned transitive)")
    if is_tool and have: errors.append(f"{name}: tool package lists dependencies {sorted(have)}")
    fw = {f.get("assemblyName") for f in meta.findall("n:frameworkReferences/n:group/n:frameworkReference", ns)}
    fw |= {f.get("name") for f in meta.findall("n:frameworkReferences/n:group/n:frameworkReference", ns)}
    fw.discard(None)
    want_fw_all = {p.split(" ")[0] for p in closure(name) if "(framework)" in p}
    if not want_fw <= fw: errors.append(f"{name}: framework references {sorted(fw)} lack {sorted(want_fw)}")
    if fw - want_fw_all: errors.append(f"{name}: unexpected framework references {sorted(fw - want_fw_all)}")
    # files: only lib/net10.0 (or tool payload), README, shipped content, and NuGet metadata
    ok = re.compile(r"(lib/net10\.0/[^/]+\.(dll|xml)|README\.md|THIRD-PARTY-NOTICES\.md|content/(grafana|prometheus)/+[^/]+|tools/net10\.0/any/.+"
                    r"|\[Content_Types\]\.xml|_rels/.rels|package/services/metadata/core-properties/.+\.psmdcp|[^/]+\.nuspec)")
    for n in names:
        if n.endswith("/"): continue
        if not ok.fullmatch(n): errors.append(f"{name}: unexpected file {n}")
        if re.match(r"(lib|ref|tools)/(?!net10\.0)", n): errors.append(f"{name}: non-net10.0 payload {n}")
    if not is_tool and not any(n == f"lib/net10.0/{name}.dll" for n in names): errors.append(f"{name}: lib/net10.0/{name}.dll missing")
    print(f"ok  {pkg.name}" if not any(e.startswith(name + ":") for e in errors) else f"BAD {pkg.name}")
extra = [p.name for p in pathlib.Path("artifacts/packages").glob("*.nupkg")
         if not any(re.fullmatch(re.escape(n) + r"\.\d.*\.nupkg", p.name) for n in graph)]
errors += [f"{e}: package not in eng/package-graph.json" for e in extra]
# Every file the dashboard manifest names must be committed: the package embeds dist/** from the checkout,
# and 0.1.1 shipped a manifest whose app script and stylesheet were never added to git.
import subprocess
dist = pathlib.Path("src/Runwick.Dashboard/dist")
tracked = set(subprocess.run(["git", "ls-files", str(dist)], capture_output=True, text=True, check=True).stdout.split())
named = {e["file"] for e in json.load(open(dist / "manifest.json")).values()}
named |= {f for e in json.load(open(dist / "manifest.json")).values() for f in e.get("css", []) + e.get("assets", [])}
for f in sorted(named):
    if f"{dist}/{f}" not in tracked: errors.append(f"Runwick.Dashboard: dist/{f} is named in the manifest but not committed")
if errors:
    print("release check FAILED"); [print("  " + e) for e in errors]; sys.exit(1)
print("release check passed")
PY
