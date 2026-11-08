# Driver conformance (SP-028, gate G-4)

This folder holds the evidence that each storage driver (PostgreSQL, SQLite, Redis) meets the shared test cases, with nothing hidden.

| File | What it is |
|---|---|
| `cases.csv` | The case registry. One row per case, written by hand (first drafted from the spec tables, then corrected). |
| `exemptions.csv` | The only tests allowed to skip or return early for a driver, each with a reason. A test not listed here that skips is a failure of the tool. |
| `postgres.md`, `sqlite.md`, `redis.md` | Generated reports. Do not edit by hand. |

Run it with `eng/conformance.sh` (starts nothing; needs the PostgreSQL test server and an AOF-always Redis, see the script header).
`eng/conformance.py` rebuilds the reports from the test result files in `artifacts/conformance/`; `--check` writes nothing.

## Registry columns

- `case`: the case ID. `OBS-01/telemetry` is the telemetry case of spec 18 that shares an ID with the driver-suite `OBS-01`.
- `family`, `layer`: `driver` cases must pass on the driver's own run; `engine` cases are driver-independent and may be answered by the shared engine tests; `host` cases are product surfaces (CLI, dashboard, outbox) run on the PostgreSQL host.
- `source`: where the case is defined.
- `mandatory`: `yes` means G-4 needs a passing result when the case applies to the driver.
- `postgres`, `sqlite`, `redis`: the applicability cell, one of
  - `applicable`;
  - `n/a: <reason>`: the specs say the case does not apply (reason cites the spec);
  - `unsupported: <what>`: the driver declares it cannot do this. The case needs a test that carries the `Unsupported` trait and asserts the explicit error;
  - `partial: <what is missing>`: applicable, but only part of the case is built or run. Counts as missing for G-4;
  - the gap form is on the `tests` column.
- `tests`: selectors joined with `;`. `Case=ID` means every test carrying `[Trait("Case","ID")]`; `Case=ID@Project|Project` limits it to those projects; `Method=Class.Method` names one test; `GAP:<reason>` declares that no test exists, which is reported as not-run and never as pass.

A registry row whose selectors match no test in the source (and is not a declared `GAP`) is an error. Hidden skips are also errors: any skipped test, and any theory row that returns early for a store kind (found by reading the source), must be listed in `exemptions.csv`, and an exempt early return is still not counted as a pass.

## What counts as evidence

A test result counts for a driver only when the test ran against that driver: tests in the driver's own project, theory rows whose store kind is that driver, and for `engine` and `host` cases the shared projects. A test in `Runwick.Core.Tests` never counts for a `driver` case, because it runs on fakes.

Scoping choices (the specs are the authority; these are where they were silent or out of date):

- Cases of spec 07 and 15 that spec 20 §12 and spec 22 §12 do not list as applicable (worker timing, PostgreSQL protocol, rate cases 05-10, autoscaler, readiness, controls, schedules above SCH) are `n/a` for SQLite and Redis with that reason. Where SQLite or Redis tests for them exist and pass (the shared contract theories run many of them), the report does not list that evidence for an `n/a` row; it is simply not required by the specs.
- Spec 20 §12 lists RET-01 as not applicable to SQLite pending SP-021. SP-021 shipped cleanup for both, so the registry treats it as applicable.
- CLI, dashboard and outbox cases (`host` layer) are verified once, on the PostgreSQL host. SQLite and Redis rows are `n/a`, except `CLI-08` (the SQLite half) and the Redis refusal of `--provider redis`.
- `IC-*` (implementation conditions), `PF-*` and `PERF-*` (performance, SP-029) and the `HARNESS-*`/`SMOKE` self-checks are not driver cases and are not in the registry.
- A pass for a theory row that skips only part of its body for one driver is listed under exemptions as `partial`.
