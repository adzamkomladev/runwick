Redis grouped-dispatch measurements (perf round two, work area F). See docs/reports/fragments/redis-f-grouped.md.
Run `npm ci` here once, build tests/Runwick.Workloads in Release, then: `pairs.sh <enq16|enq64|steady500|delayed100|drain> <rounds> [off on on4 on8 on16 bullmq]`. Each round holds /tmp/runwick-bench.lock and alternates arm order; results land in results/<workload>[$RESULT_TAG]/.
profile.py times the phases inside the real dispatch scripts against a disposable AOF-always Redis; rawthroughput.py compares single and grouped scripts without the .NET client.
