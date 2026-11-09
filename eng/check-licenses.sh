#!/usr/bin/env bash
# Lists every NuGet package in the restore closure of the shipped src projects (packages.lock.json) with its licence
# (read from the nuspec in the NuGet cache) and every npm production dependency of the dashboard client
# (package-lock.json), and fails on anything outside eng/license-allowlist.txt. Run `dotnet restore` first.
set -euo pipefail
cd "$(dirname "$0")/.."
python3 - <<'PY'
import json, glob, os, sys, pathlib
import xml.etree.ElementTree as ET
cache = pathlib.Path(os.environ.get("NUGET_PACKAGES", os.path.expanduser("~/.nuget/packages")))
allow = {l.split("#")[0].strip() for l in open("eng/license-allowlist.txt") if l.split("#")[0].strip()}
pkgs = {}
for lock in glob.glob("src/*/packages.lock.json"):
    for tfm, deps in json.load(open(lock))["dependencies"].items():
        for name, d in deps.items():
            if d.get("type") != "Project":
                pkgs[(name, d["resolved"])] = 1
rows, bad = [], []
for (name, ver) in sorted(pkgs):
    nus = list((cache / name.lower() / ver).glob("*.nuspec"))
    lic = "MISSING (package not in the NuGet cache)"
    if nus:
        root = ET.parse(nus[0]).getroot()
        ns = {"n": root.tag.split("}")[0].strip("{")}
        e = root.find("n:metadata/n:license", ns)
        if e is not None and e.get("type") == "expression":
            lic = e.text.strip()
        elif e is not None:
            lic = "file:" + e.text.strip()
        else:
            u = root.find("n:metadata/n:licenseUrl", ns)
            lic = "url:" + (u.text.strip() if u is not None and u.text else "none")
    ok = lic in allow or f"{name} {ver}" in allow
    rows.append((name, ver, lic, ok))
    if not ok:
        bad.append(f"{name} {ver}: {lic}")
w = max(len(r[0]) for r in rows)
for n, v, l, ok in rows:
    print(f"{'ok ' if ok else 'BAD'} {n:<{w}} {v:<14} {l}")
print(f"{len(rows)} NuGet packages")
cdir = "src/Runwick.Dashboard/ClientApp"
lockf = json.load(open(f"{cdir}/package-lock.json"))
n_npm = 0
for k, v in sorted(lockf["packages"].items()):
    if not k or v.get("dev") or v.get("link"):  # a link entry points at a local package, checked under its own path
        continue
    lic = v.get("license", "MISSING")
    name = k.split("node_modules/")[-1]
    ok = f"npm:{lic}" in allow or f"npm:{name} {lic}" in allow  # the second form allows one named package (fonts under OFL-1.1)
    n_npm += 1
    print(f"{'ok ' if ok else 'BAD'} npm {k.split('node_modules/')[-1]} {v['version']} {lic}")
    if not ok:
        bad.append(f"npm {k} {v['version']}: {lic}")
print(f"{n_npm} npm production packages (bundled into the dashboard assets)")
if bad:
    print("licence check FAILED; each needs a user decision, do not just add it to the allow-list:")
    for b in bad:
        print("  " + b)
    sys.exit(1)
print("licence check passed")
PY
