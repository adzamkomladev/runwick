#!/usr/bin/env python3
"""Options: --within TEXT keeps only samples whose stack contains a frame with TEXT (for example JobDispatcher.DispatchAsync).
Top frames of a dotnet-trace Speedscope file (EventPipe sample profiler): inclusive and exclusive share of samples by frame, plus a grouping by
namespace prefix. Usage: trace-top.py file.speedscope.json [--top 25] [--contains Runwick]
Samples include threads that are blocked in a wait, so read shares among the frames you care about, not of the whole total."""
import json, sys, collections
f = sys.argv[1]; top = int(sys.argv[sys.argv.index("--top") + 1]) if "--top" in sys.argv else 25
d = json.load(open(f)); names = [x["name"] for x in d["shared"]["frames"]]
kinds = collections.Counter(); incl = collections.Counter(); excl = collections.Counter(); total = 0; groups = collections.Counter()
GROUPS = ["Runwick.Dispatching", "Runwick.Serialization", "Runwick.Redis", "Runwick.Postgres", "Runwick.Sqlite", "Runwick.Worker", "Runwick.Protocol", "Runwick.Hosting", "Runwick",
          "StackExchange.Redis", "Npgsql", "Microsoft.Data.Sqlite", "SQLitePCL", "System.Text.Json", "System.Threading", "System.Runtime", "System.Net.Sockets", "System.Security.Cryptography"]
WITHIN = sys.argv[sys.argv.index("--within") + 1] if "--within" in sys.argv else None
def account(stack, w):
    global total
    if not stack: return
    if WITHIN and not any(WITHIN in names[i] for i in stack): return
    if names[stack[-1]] in ("CPU_TIME", "UNMANAGED_CODE_TIME"):
        # The sampler's own marker: CPU_TIME is a thread running managed code, UNMANAGED_CODE_TIME one inside native code or parked. Only the first is CPU work.
        kinds[names[stack[-1]]] += w
        if names[stack[-1]] == "UNMANAGED_CODE_TIME": return
        stack = stack[:-1]
        if not stack: return
    leaf = names[stack[-1]]
    if any(k in leaf for k in ("WaitHandle", "Monitor.Wait", "Thread.Sleep", "LowLevelLifoSemaphore", "WorkerThread.", "Interop+Sys.Poll", "epoll", "Process64", "Threads", "Non-Activities", "Thread (")): return
    total += w
    seen = set()
    for i in stack:
        n = names[i]
        if n not in seen: incl[n] += w; seen.add(n)
    excl[leaf] += w
    for x in {next((x for x in GROUPS if x in names[i]), None) for i in stack} - {None}: groups[x] += w
for p in d["profiles"]:
    if p["type"] == "sampled":
        for stack, w in zip(p["samples"], p["weights"]): account(stack, w)
    else:  # evented: frames open and close at timestamps; the time between events belongs to the stack as it stood
        stack = []; last = None
        for e in p["events"]:
            if last is not None and e["at"] > last: account(stack, e["at"] - last)
            last = e["at"]
            if e["type"] == "O": stack.append(e["frame"])
            elif stack: stack.pop()
print(f"{f}: {total} samples ms of managed CPU samples")
print("thread time by kind (ms):", {k: round(v) for k, v in kinds.items()})
print("\n-- inclusive by namespace (a sample counts for every namespace on its stack) --")
for g, w in groups.most_common(): print(f"{100*w/total:5.1f}%  {g}")
print("\n-- top inclusive frames --")
for n, w in incl.most_common(top): print(f"{100*w/total:5.1f}%  {n[:150]}")
print("\n-- top exclusive frames --")
for n, w in excl.most_common(top): print(f"{100*w/total:5.1f}%  {n[:150]}")
