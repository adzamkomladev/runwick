# 19 — Command-line tool (SP-032)

**Sources:** [v1-delivery-plan.md](../v1-delivery-plan.md) §6 (SP-032 row), [implementation-backlog.md](../implementation-backlog.md) §6b (SP-032), [commercial-strategy-and-seams.md](../commercial-strategy-and-seams.md) §4 (command list, `--agent` mode), [review-2026-10-30.md](../review-2026-10-30.md) item 9, [16-management.md](16-management.md) (the HTTP API, §4–§5), [08-controls-and-completion.md](08-controls-and-completion.md) §7 (events), [04-postgres.md](04-postgres.md) P-18 (migrations), [20-sqlite.md](20-sqlite.md) §9 (SQLite migrations), [00-overview.md](00-overview.md) §6, §9.

## In short

SP-032 ships `runwick`, a command-line tool for operators, CI scripts and AI agents. It talks to a running Runwick management host over the HTTP API of spec 16. It does not open the job database for everyday work. The only exceptions are `migrate` and `doctor`, which need the database itself.

The same commands produce three kinds of output:

1. **Human** (default): tables and short sentences.
2. **`--output json`**: the API's own JSON result, for scripts.
3. **`--agent`**: one fixed, versioned JSON "envelope" per command, made for AI agents. It says whether the command worked, gives a stable error code with a hint, the revision numbers needed for the next change, and suggested next commands. `runwick --agent describe` prints a machine-readable list of every command and option, so an agent can learn the tool without reading documentation.

All three come from one internal description of each command, so they cannot offer different features, only different presentation.

The CLI can also **create a job** (`runwick dispatch <job-name> --payload @file.json`). The management host checks the payload against the job type it has registered, and writes the job and an audit row together, so every job created by a person or a script is on record.

Every command that changes something takes `--command-id` and `--expected-revision`. Sending the same command ID again is safe: the server returns the original result instead of doing the work twice. This is what makes the tool safe in retrying CI jobs.

The CLI never stores passwords or tokens on disk. A saved "context" holds the server URL, the login method, a user name and the **name** of an environment variable that holds the secret.

Not in SP-032 [choice]: `schedules edit` (schedules are edited by changing the registered definition, 13 §6.4), an OIDC/device login flow, storing secrets in an OS keychain, and Native AOT or trimmed builds.

## Changes

- (2026-11-08) New document.
- (2026-11-08) Decisions taken where the sources were silent, each marked [choice] in the text: HTTP-only client except `migrate`/`doctor` (§1); secrets never written to disk (§3); `schedules edit` left out (§2); new read endpoint `GET {prefix}/v1/events` added to spec 16 (§6, and 16 Changes); own argument parser over the command model, no System.CommandLine (§4); agent mode requires explicit expected revisions (§5.3); automatic resend on unknown outcome (§5.4).

- (2026-11-08, user decision, adzamkomla) **`dispatch` is in SP-032.** Specified as an audited dispatch by job name with a JSON payload: CLI command (§2), API route `POST {prefix}/v1/jobs:dispatch` and permission `Jobs.Dispatch` (§6.2, 16), store operation `ManagementDispatch/1` writing the job and the audit row in one transaction at PostgreSQL schema revision 17 (§6.2, §10), cases CLI-10 and MGMT-13. Replaces the earlier deferral.
- (2026-11-08, user decision, adzamkomla) **CLI secrets are never written to disk** (§3): only an environment-variable name is saved; the secret comes from that variable, stdin, or a prompt in human mode. Decided, no longer a [choice].
- (2026-11-08, implementation of SP-032) What the build settled:
  - **No `management_dispatch_v1` function.** `ManagementDispatch/1` runs on the PostgreSQL statement path (dispatch core statement, then the `queue_events.actor_id` update and the audit insert, in one transaction), like the other management controls. Migration 0017 therefore only replaces the `management_audit` operation check (now `management_audit_operation_v7_ck`, dropping v5/v6 `IF EXISTS` so it applies after revision 16 retention) and adds `Dispatch`. The manifest adds `ManagementDispatch/1` at 17.
  - The job ID of a management dispatch is derived from the command ID (SHA-256, UUID version 8), and no trace context is stored, so a resend fingerprints identically.
  - `GET events`: `limit` above 1,000 is clamped, not refused; an empty page returns the input cursor (or `"0.0"`) as `next`.
  - CLI: no colour; `--url` with no context means a bearer login; `context add --url` is that command's own option, not the global override; `describe` works in every output mode; `doctor` failures are error `check_failed` (exit 1) with the checks still in `result`; a reveal's `commandId` is local only (reveals carry no command ID on the wire); `events tail --limit` is the page size per request.
  - `migrate`/`doctor --provider sqlite` is a `usage_error` in this build: SQLite is SP-019's and plugs in after its merge (CLI-08's SQLite half not run).
  - (2026-11-08, SQLite half) `migrate`/`doctor --provider sqlite` now work: `migrate` calls `RunwickSqliteMigrations.MigrateAsync`; `doctor` opens the file without creating it and uses the store's own startup and Describe checks (engine version, `journal_mode`, topology, revision, `migration_mode`, schema objects). SQLite `migration_lock_timeout` is retryable (exit 4) like PostgreSQL's `migration_lock_unavailable`. The single-file binary is named `Runwick.Cli` (the assembly name); only the tool package installs it as `runwick`.
  - The agent envelope and describe schemas are committed as `schemas/runwick-cli-agent-envelope-v1.json` and `schemas/runwick-cli-describe-v1.json`; CLI-03 fails when they differ from the generated ones (regenerate with `RUNWICK_WRITE_SCHEMAS=1`).

## 1. Package and process

| Item | Rule |
|---|---|
| Package | `Runwick.Cli`, packed two ways: a `dotnet tool` (`PackAsTool`, `ToolCommandName = runwick`) and self-contained single-file binaries (`PublishSingleFile`, `IncludeNativeLibrariesForSelfExtract`) for `linux-x64` and `win-x64` only, the certified hosts (00 §7). Not trimmed, no AOT claim (00 §9 rule 13). |
| Runwick references | `Runwick` (to reuse the public management records of `Runwick.Management`: `ManagementPage<T>`, `JobDetail`, `JobControlResult`, …), `Runwick.Postgres` and `Runwick.Sqlite` (for `migrate`/`doctor` only). Never `Runwick.AspNetCore`. |
| External dependencies | None beyond the framework and the providers' own [choice]. Argument parsing, help and the describe manifest are generated from the command model (§4). JSON schemas come from `System.Text.Json.Schema.JsonSchemaExporter` (in the framework). |
| Store access | Every command except `migrate` and `doctor --connection-*` uses HTTP only [choice]. `migrate`/`doctor` reach the provider through its migration API and `InternalsVisibleTo("Runwick.Cli")` for the store's Describe and profile checks; no new public provider API. |
| Version coupling | `migrate` installs the migrations compiled into this CLI's provider assemblies. The CLI prints its version in `version`, in `doctor`, and in every agent envelope (`cli.version`). Use the CLI of the same Runwick release as the application. |

The CLI library is testable in-process: `RunwickCli.RunAsync(string[] args, CliEnvironment env)` where `CliEnvironment` carries stdout, stderr, stdin, environment variables, whether stdout is a terminal, the config path and an optional `HttpMessageHandler`. `Program.Main` only builds the real environment and returns the exit code.

## 2. Commands

`<id>` is a UUID in `D` format. Times are ISO 8601 UTC. All mutations take `--command-id <uuid>` (optional; generated as UUIDv7 when absent) and `--reason <text>` (optional, ≤ 512 UTF-8 bytes, 16 §4).

| Command | HTTP call (16 §5, under `{url}/v1`) | Permission | Notes |
|---|---|---|---|
| `context add <name> --url <url> --auth basic\|bearer [--user <u>] [--secret-env <VAR>]` | — | — | §3 |
| `context use <name>`, `context list`, `context show`, `context remove <name>` | — | — | `show` never prints a secret value |
| `login` | `GET antiforgery` | authenticated | Checks the credentials of the current context; stores nothing |
| `jobs list [--queue] [--state] [--job-name] [--from] [--to] [--limit] [--cursor]` | `GET jobs` | `Jobs.Read` | §5.2 paging |
| `jobs get <id>` | `GET jobs/{id}` | `Jobs.Read` | |
| `jobs tail <id> [--interval 2s]` | `GET jobs/{id}` repeated | `Jobs.Read` | Streaming; one item per state or revision change; ends when the job is terminal |
| `jobs cancel\|retry\|rerun <id> --expected-generation <n> --expected-revision <n>` | `POST jobs/{id}/cancel\|retry\|rerun` | `Jobs.Cancel\|Retry\|Rerun` | |
| `jobs cancel-many --file <path\|->` | `POST jobs:cancel` | `Jobs.Cancel` | File is JSON Lines `{jobId, expectedGeneration, expectedRevision}`, 1–100 lines, else usage error. Item command IDs: §5.4 |
| `jobs reveal-payload <id>` | `POST jobs/{id}/payload:reveal` | `Jobs.ReadPayload` | Writes an audit row (16 §3); the only way the CLI ever prints a payload |
| `jobs reveal-error <attempt-id>` | `POST attempts/{id}/error:reveal` | `Jobs.ReadError` | Same |
| `dispatch <job-name> --payload <@file\|-> [--payload-version <n>] [--queue <q>] [--priority low\|medium\|high] [--not-before <instant>]` | `POST jobs:dispatch` (§6.2) | `Jobs.Dispatch` on the resulting queue | The payload is a JSON value read from a file or stdin, never inline on the command line [choice: argv is visible to other users]. At most 1 MiB. Result: job ID, disposition (`created`/`reused`), generation, revision, queue, `replayed`. The payload is never echoed |
| `queues list` | `GET queues` | `Queues.Read` | |
| `queues pause\|resume <name> --expected-revision <n>` | `POST queues/{name}/pause\|resume` | `Queues.Control` | |
| `workers list [--include-expired]` | `GET workers` | `Capacity.Read` | |
| `capacity list [--kind queue\|job-type\|resource]` | `GET scopes` | `Capacity.Read` | Plan's `capacity get` [choice: named `list`, like the other reads] |
| `capacity set <kind> <key> <capacity> --expected-revision <n>` | `POST scopes/{kind}/{key}/capacity` | `Capacity.Edit` | `--expected-revision` is the policy revision |
| `schedules list` | `GET schedules` | `Schedules.Read` | |
| `schedules pause\|resume <key> --expected-revision <n> [--policy skipPaused\|runOnce\|catchUp]` | `POST schedules/{key}/pause\|resume` | `Schedules.Control` | Control revision; `--policy` on resume only |
| `schedules trigger\|revalidate <key> --expected-revision <n>` | `POST schedules/{key}/trigger\|revalidate` | `Schedules.Trigger` / `Schedules.Edit` | Definition revision; for trigger the command ID is the trigger ID (16 §4) |
| `events tail [--after <cursor>] [--follow] [--interval 2s] [--limit]` | `GET events` (new, §6) | `Jobs.Read`, application-wide | Streaming |
| `audit list [--target-kind] [--target-id] [--from] [--to] [--limit] [--cursor]` | `GET audit` | `Audit.Read` | |
| `settings` | `GET settings` | `Settings.Read` | |
| `migrate --provider postgres\|sqlite (--connection-env <VAR> \| --connection-stdin)` | — (store) | migration role | `RunwickPostgresMigrations.MigrateAsync` / `RunwickSqliteMigrations.MigrateAsync`; prints store ID, revision, applied phases; provider error codes become CLI error codes unchanged |
| `doctor [--provider … --connection-env …]` | `GET antiforgery`, `GET settings` (+ store Describe) | authenticated | §7 |
| `version` | — | — | |
| `--agent describe` | — | — | §5.5 |

Global options: `--context <name>`, `--url <url>` (overrides the context URL), `--output human|json`, `--agent`, `--timeout <seconds>` (per HTTP request, default 30, 1–300), `--verbose` (HTTP trace to stderr with `Authorization`, `Cookie`, `Set-Cookie` and the antiforgery header replaced by `[redacted]`; never in agent mode). `--agent` and `--output` are exclusive.

A connection string is never accepted as an argument value [choice]: command lines are visible to other users of the host. Only `--connection-env` (name of an environment variable) or `--connection-stdin` (first line of stdin).

## 3. Contexts and credentials

Config file: `$RUNWICK_CONFIG`, else `Environment.SpecialFolder.ApplicationData/runwick/config.json` (`~/.config/runwick/config.json` on Linux, `%APPDATA%\runwick\config.json` on Windows). Created with mode `0600` on Unix.

```json
{ "version": 1, "current": "prod",
  "contexts": { "prod": { "url": "https://ops.example.com/runwick/api", "auth": "basic", "user": "ops", "secretEnv": "RUNWICK_PROD_SECRET" } } }
```

- `url` is the prefix passed to `MapRunwickManagement` (the CLI appends `/v1/…`). It must be `https://`, or `http://` to a loopback host (`localhost`, `127.0.0.0/8`, `::1`); anything else is `insecure_url` [choice]. Server certificates use the system trust store; there is no option to skip verification.
- **Secrets are never written to the file** (user decision 2026-11-08). Secret lookup order: `--secret-stdin` (first line of stdin), the context's `secretEnv` variable, `RUNWICK_SECRET`, then an interactive hidden prompt **only** in human mode with a terminal on stdin. Agent and JSON modes never prompt: a missing secret is `credential_missing`.
- `auth: basic` sends `Authorization: Basic` (user + secret). Mutations need an antiforgery token (16 §5): before the first mutation of a process the CLI calls `GET antiforgery`, keeps the returned cookie and token in memory for that process only, and sends the token in the header the response names (`X-Runwick-Antiforgery`). A 400 `antiforgery_invalid` triggers one fresh fetch and one resend with the same command ID.
- `auth: bearer` sends `Authorization: Bearer <secret>`. The token comes from the host's own scheme (JWT, API key handler…); Runwick ships none. The host must map that group with `.ForMachineClients()`; the CLI then sends no antiforgery token. If a mutation answers 400 `antiforgery_invalid` under bearer, the error hint says the group needs `ForMachineClients()`.
- `login` calls `GET antiforgery` (it needs authentication but no permission) and reports `ok`, the URL and the auth kind. It writes nothing.

## 4. Command model

Each command is one declaration; parser, help text, `--agent describe`, the HTTP call and all three renderers read it.

```csharp
internal sealed record CliCommand(
    string Name,                         // "jobs cancel"
    string Summary,
    IReadOnlyList<CliArgument> Arguments,
    IReadOnlyList<CliOption> Options,
    bool Mutation, bool Streaming,
    string? Permission,                  // 16 §1 name, for describe and hints
    Type ResultType,                     // e.g. JobControlResult, ManagementPage<JobSummary>
    IReadOnlyList<HumanColumn> Columns,  // human table layout
    Func<CliInvocation, CancellationToken, Task<CommandOutcome>> Run);

internal sealed record CliArgument(string Name, CliValueType Type, string Description);
internal sealed record CliOption(string Name, CliValueType Type, bool Required, bool RequiredInAgentMode,
    IReadOnlyList<string>? Allowed, string? Default, string Description);
internal enum CliValueType { String, Uuid, Integer, Instant, Duration, Path, Flag }

internal sealed record CommandOutcome(
    object? Result,                                   // the API body (or CLI-built record)
    CliError? Error,
    Guid? CommandId,                                  // mutations
    IReadOnlyDictionary<string, JsonNode> Revisions,  // §5.3
    IReadOnlyList<NextAction> NextActions);
internal sealed record CliError(string Code, string Message, bool Retryable, string Hint, long? ActualRevision, int ExitCode);
internal sealed record NextAction(string Command, IReadOnlyList<string> Argv, string Reason);
```

Parsing: POSIX-style long options (`--name value`, `--name=value`), arguments in declared order, unknown or repeated options and bad values are `usage_error` (exit 2). No abbreviations, no positional reordering. `Run` produces the outcome once; the renderer is chosen afterwards. A command never branches on the output mode except for the two rules in §5.3 (implicit revision read) and §3 (prompting).

## 5. Output contracts

### 5.1 Human and JSON

- **Human:** one table (columns from `Columns`, UTC ISO times, full IDs) or a few `key: value` lines; colour only when stdout is a terminal and `NO_COLOR` is unset. Mutations print the outcome, the command ID and, when replayed, `replayed (original result returned)`. Errors go to stderr as `error <code>: <message>` plus `hint: <hint>`.
- **`--output json`:** stdout gets the API response body exactly as the server sent it (camelCase, 16 §5), indented. Lists: one `{items, observedAt, next}` object after §5.2 paging. Mutations: the API body plus `commandId` if the API body lacks it. Errors: `{code, message, commandId?, actualRevision?}` on **stdout**, non-zero exit code [choice: one stream to parse]. Streaming commands write JSON Lines of items.

### 5.2 Paging

`--limit N` (default 50 in human mode, 200 in JSON and agent modes; 1–10,000) is the number of **items** wanted. The CLI fetches API pages of up to 200 until it has N items or `next` is null, and returns the API `next` cursor of the last page (or null). Nothing is cut: if more items exist, `next` says so. `--cursor` starts from a cursor; cursors expire after 15 minutes (16 §2.1), which gives `invalid_cursor`.

### 5.3 Agent envelope (schema version 1)

Exactly one JSON object on stdout, one line, UTF-8, no colour, no prompt, no pager, nothing on stderr:

```json
{
  "schemaVersion": 1,
  "command": "jobs cancel",
  "ok": false,
  "result": null,
  "error": { "code": "stale_revision", "message": "…", "retryable": false,
             "hint": "Read the job again and retry with its current revision.", "actualRevision": 9 },
  "commandId": "0193…",
  "revisions": { "job:0193…": { "generation": 2, "revision": 9 } },
  "nextActions": [ { "command": "jobs get", "argv": ["runwick", "--agent", "jobs", "get", "0193…"],
                     "reason": "Read the current generation and revision." } ],
  "cli": { "version": "1.0.0", "context": "prod", "url": "https://ops.example.com/runwick/api" }
}
```

- `result` is the same object `--output json` prints. `error` is null when `ok`.
- `commandId` is present (non-null) for every mutation, also on failure, so an agent can resend it.
- `revisions` lists every revisioned target seen in the result, keyed `job:<id>` → `{generation, revision}`, `queue:<name>` → `revision`, `scope:<kind>/<key>` → `policyRevision`, `schedule:<key>` → `{definition, control}`. Full identifiers only.
- **Expected revisions in agent mode are required** [choice]. In human and JSON modes, a missing `--expected-revision` (and `--expected-generation`) makes the CLI read the target first (`GET jobs/{id}`, or page through `queues`/`scopes`/`schedules`) and use what it read; the output says which revision was used. Agent mode gets `usage_error` with a `nextActions` entry that reads the target.
- `nextActions` rules (only these; the CLI does not know the caller's grants, so they are suggestions):

| Situation | Next action |
|---|---|
| `outcome_unknown`, `store_unavailable` or a transport failure after the request was sent, on a mutation | The same command with the same `--command-id` |
| `stale_revision`, `stale_generation` | `jobs get` / `queues list` / `capacity list` / `schedules list` for the target |
| `jobs get` of a `failed` or `cancelled` job | `jobs retry` with its generation and revision |
| `jobs get` of a `succeeded` job | `jobs rerun` with its generation and revision |
| List result with `next` | The same list with `--cursor <next>` |
| `events tail` ended | `events tail --after <last cursor>` |
| `credential_missing`, `unauthenticated` | `login` |

**Streaming** (`jobs tail`, `events tail`): JSON Lines. Every line is a full envelope with an extra field `stream`: `"item"` lines carry one item in `result`; the last line is `"stream": "end"` with `ok`, `error`, `nextActions` and `result: {cursor}` (events) or the final job (jobs). SIGINT/Ctrl-C ends the stream cleanly with an `end` line, `ok: true`.

### 5.4 Command IDs and resending

- Generated IDs are UUIDv7. The CLI passes the ID to the API unchanged (16 §4); the server's receipt resolves a resend.
- **Automatic resend** [choice]: when a mutation's reply is lost (timeout or connection reset after the request was written), or the API answers 503 `outcome_unknown`, the CLI resends the identical request with the same command ID, at most 3 times within 30 s (1 s, 2 s, 4 s apart). If none answers, the result is `outcome_unknown` (exit 3) with the command ID. A connection failure before anything was written is `connection_failed` (exit 4), not unknown.
- `jobs cancel-many`: item `i` gets command ID = the first 16 bytes of SHA-256(`"runwick.cli.bulk.v1"` ‖ 0x00 ‖ command ID bytes (big-endian) ‖ job ID bytes), with the version nibble set to 8 and the RFC 4122 variant. A resent bulk with the same `--command-id` therefore replays every item.

### 5.5 `--agent describe`

Prints one envelope with `command: "describe"` and `result`:

```json
{ "name": "runwick", "version": "1.0.0", "apiVersion": "v1",
  "envelopeSchema": { /* JSON Schema 2020-12 of §5.3 */ },
  "exitCodes": { "0": "ok", "1": "rejected", "2": "usage_error", "3": "outcome_unknown", "4": "unavailable", "5": "unauthenticated_or_forbidden" },
  "errorCodes": [ { "code": "stale_revision", "retryable": false, "hint": "…" }, … ],
  "commands": [ { "name": "jobs cancel", "summary": "…", "mutation": true, "streaming": false, "permission": "Jobs.Cancel",
                  "arguments": [ … ], "options": [ { "name": "--expected-revision", "type": "integer", "required": false, "requiredInAgentMode": true, … } ],
                  "resultSchema": { /* JsonSchemaExporter output for ResultType */ } } ] }
```

The same schema files are committed at `schemas/runwick-cli-agent-envelope-v1.json` and `schemas/runwick-cli-describe-v1.json` and a test keeps them equal to the generated ones. A field is never removed or retyped within `schemaVersion` 1; new optional fields may be added.

### 5.6 Errors and exit codes

| Code | Source | Retryable | Exit | Hint (short form) |
|---|---|---|---|---|
| `usage_error` | CLI | no | 2 | Run `runwick <command> --help` or `--agent describe` |
| `context_not_found`, `insecure_url` | CLI | no | 2 | Add a context / use https |
| `credential_missing` | CLI | no | 5 | Set the context's secret variable or pass `--secret-stdin` |
| `connection_failed`, `tls_error` | CLI | yes | 4 | Server unreachable; nothing was sent |
| `response_invalid` | CLI (unparseable or unexpected body) | no | 4 | CLI and server versions may differ |
| `endpoint_not_supported` | 404/405 on a route this CLI expects | no | 4 | Server is older than the CLI |
| `unauthenticated` (401), `forbidden` (403) | API | no | 5 | Check credentials / ask for permission `<P>` |
| `not_found` (404), `detail_expired` (410) | API | no | 1 | — |
| `invalid_request`, `invalid_cursor`, `antiforgery_invalid`, `payload_too_large`, `unsupported_media_type` (400/413/415) | API | no | 1 | — |
| `stale_revision`, `stale_generation`, `invalid_state`, `unique_conflict`, `command_conflict`, `cancel_already_requested`, `no_change` (409) | API | no | 1 | Read again; `command_conflict`: this command ID was used with another body |
| `job_not_registered` (404), `payload_invalid`, `payload_version_unsupported` (400), `payload_too_large` (413) | API, dispatch | no | 1 | Register the job type in the management host / fix the payload / send the current payload version |
| `admission_closed` (503) | API, dispatch while the application drains or is in maintenance | yes | 4 | Retry after the application reopens |
| `rate_limited` (429) | API | yes | 4 | Wait `Retry-After` |
| `busy`, `query_budget_exceeded`, `store_unavailable` (503) | API | yes | 4 | — |
| `outcome_unknown` (503 or lost reply after resends) | API/CLI | yes | 3 | Resend with the same `--command-id` |
| migration codes (04 P-18, 20 §9) | provider | as the provider says | 1 or 4 | — |

`cancel_requested` (202) is success (`ok: true`, exit 0) with `result.effect = "cancel_requested"`. A bulk result is `ok: true` even when items are rejected; per-item status is in `result` (16 §4). Server messages are shown as received (they never contain exception text, 16 §5); the CLI adds no request echo.

## 6. API additions

### 6.1 Events read

`events tail` needs a read the SP-020 API lacks. Added to `IManagementSession` and `Runwick.AspNetCore` (recorded in 16 Changes):

- `ListEventsAsync(ManagementCaller caller, string? after, int? limit, CancellationToken ct)` → `EventsPage(IReadOnlyList<ManagementEvent> Items, DateTimeOffset ObservedAt, string Next, bool Partial)`; route `GET {prefix}/v1/events?after&limit`.
- Backed by the existing `EventsSince/1` (08 §7.2): `limit` 1–1,000, default 100. No schema change, no new store operation.
- Permission: `Jobs.Read` with `Queues = null` (application-wide) [choice: events carry no queue column, so per-queue filtering would need a join on every page]. A queue-scoped caller gets 403.
- `ManagementEvent(long EventId, string Kind, Guid? JobId, Guid? AttemptId, Guid? CommandId, DateTimeOffset OccurredAt, string? ActorId, JsonObject Details)`. `ActorId` is null unless the caller also has `Audit.Read` [choice: actor subjects are audit data].
- The cursor is the **plain** position `"<txId>.<eventId>"` (decimal), not a signed 15-minute cursor [choice]: it encodes no filter or scope, grants nothing, and a tail must survive hours. A malformed value is 400 `invalid_cursor`. `Next` is the input cursor when the page is empty.
- `events tail` without `--follow` reads until a page is empty and not `Partial`, then ends. With `--follow` it polls every `--interval` (default 2 s, 250 ms–60 s).

### 6.2 Audited dispatch

`dispatch` needs a mutation the SP-020 API lacks. Added to `IManagementSession`, `Runwick.AspNetCore` and the stores (recorded in 16 Changes):

**Permission.** `ManagementPermissions.JobsDispatch = "Jobs.Dispatch"`. It may be queue-scoped (it covers the queue the job lands in). It is in **no preset**; it is granted explicitly, like `Jobs.ReadPayload`.

**Route.** `POST {prefix}/v1/jobs:dispatch`, antiforgery as every mutation (16 §5). Body (JSON, camelCase):

| Field | Rule |
|---|---|
| `commandId` | UUID, required. Becomes the dispatch operation ID; the proposed job ID is the first 16 bytes of SHA-256(`"runwick.mgmt.dispatch.v1"` ‖ 0x00 ‖ command ID bytes) with version nibble 8 and the RFC 4122 variant, so a resend has the same body |
| `jobName` | Required; job-name grammar (02 §1) |
| `payloadVersion` | Optional; if present it must equal the registered **current** payload version, else 400 `payload_version_unsupported` [choice: upgraders are for stored payloads, not for new ones] |
| `payload` | Required JSON value |
| `queue`, `priority`, `notBefore` | Optional overrides, validated as `DispatchOptions` (01 §10); defaults from the job's registration |
| `reason` | Optional, ≤ 512 UTF-8 bytes |

The request body limit for this route is **1 MiB + 16 KiB** (other routes keep 64 KiB, 16 §5); a larger body is 413 `payload_too_large` before parsing.

**Session steps** (`DispatchAsync(ManagementCaller caller, ManagementDispatchRequest request, CancellationToken ct)` → `ManagementDispatchResult(Guid JobId, string Disposition, int Generation, long Revision, string Queue, bool Replayed, Guid CommandId)`):

1. Look the job name up in the host's `JobRegistry`. Unknown → 404 `job_not_registered`. The management host must register the job types it dispatches, as a producer-only host does (01 §6: `AddJob<T>()` or `AddJobsFromAssemblyContaining<T>()`, no `AddWorker`).
2. Resolve the target queue (override or registration) and check `Jobs.Dispatch` on it; missing → 403. Job names are not hidden resources, so an unregistered name is 404 for every caller.
3. Deserialize `payload` with the job's registered serializer profile into the job type (`runwick.json` v1, 02). Failure → 400 `payload_invalid`; the message names the JSON path and never echoes values.
4. Prepare as any dispatch (01 §13): re-serialize to the canonical payload bytes, apply the 1 MiB limit and the 02 §4.2 scanner (`payload_too_large`, `payload_invalid`), freeze policies, scopes and uniqueness from the registration. Local admission (`local_capacity_exceeded`) → 503 `busy`.
5. Send `ManagementDispatch/1` (below) with the caller's actor. Unknown outcome: resent with the same command until NotAfter, as the other mutations (16 §4).

**Store operation `ManagementDispatch/1`** (internal `IJobControlStore.ManagementDispatchAsync`), one transaction:

- Body = the `Dispatch/1` body (03 §3.2, single item) + `actor` {`subject`, `display`?, `scheme`, `reason`?}. Fingerprint: 02 §6 canonical form over that body, operation version 1. Receipt class `durable`, lifetime 5 min (03 §8.4). Receipt result: the `Dispatch/1` result (03 §8.5).
- Steps: gates (creation path, 03 §10) → receipt → the shared dispatch core, unchanged (PostgreSQL: `rw_dispatch_core_v2`) → one `management_audit` row: `operation = 'Dispatch'`, `target_kind = 'job'`, `target_id` = the resulting job ID, actor, reason, `resulting_revision` = the job revision, `details` = {`jobName`, `queue`, `disposition`} → the `dispatched` queue event carries `actor_id = actor.subject` and `command_id` → receipt decided → commit.
- A `reused` result (uniqueness, 01 §11) also writes its audit row: the command applied, it just created nothing. `UniquenessConflict` writes nothing (409 `unique_conflict`).
- Audit failure rolls back the dispatch (MGMT-09).

**HTTP mapping.** 200 with the result (`replayed: true` on a replay); 409 `unique_conflict`, `command_conflict`; `AdmissionClosed` → 503 `admission_closed`; others as 16 §4.

**PostgreSQL (schema revision 17).** Migration `0017_management_dispatch.sql`: `management_audit` operation check adds `Dispatch`; new function `management_dispatch_v1` (frame + dispatch core + audit insert, as the other server functions, 04 Changes PF-05); the manifest adds `ManagementDispatch/1`. No new table or index.

**SQLite.** Part of the SQLite manifest and of its `0001_initial.sql` from the start (20 §9.3).

## 7. `doctor`

Prints one check list; `ok` is false when any check is `fail`.

| Check | How | Result |
|---|---|---|
| `api.reachable` | TCP/TLS to the URL | pass / fail (`connection_failed`, `tls_error`) |
| `api.auth` | `GET antiforgery` | pass / fail (`unauthenticated`) |
| `api.version` | `GET settings` answers on `/v1` | pass / fail (`endpoint_not_supported`) / `unverified` when 403 |
| `api.clock` | server `Date` header vs local clock | warn above 5 s difference |
| `store.kind`, `store.schemaRevision`, `store.profile` | `GET settings` (16 §2) | reported; `unverified` without `Settings.Read` |
| `store.migration` (with `--connection-*`) | provider Describe | pass when the store revision equals the CLI's; warn "run migrate" when lower; fail "CLI older than store" when higher; fail when `migration_mode ≠ normal` |
| `store.settings` (with `--connection-*`) | PostgreSQL: server version, encoding, `synchronous_commit`; SQLite: 20 §3 pragmas and topology | pass / fail / `unverified` (never reported as passed when it could not be read, 00 §9 rule 10) |

## 8. What the CLI never prints

- Secrets: no password, token, cookie, antiforgery token or connection string appears in stdout, stderr, the config file, `--verbose` traces or exception output.
- Payloads and error details: only `jobs reveal-payload` / `jobs reveal-error`, which the server audits. Lists and `jobs get` carry no payload (16 §2).
- Unhandled exceptions: caught in `Program.Main`, printed as `internal_error` with the exception **type** only (exit 4); the stack trace only with `--verbose`.

## 9. Tests

Project `tests/Runwick.Cli.Tests` (trait `Item=SP-032`). The CLI runs in-process through `RunwickCli.RunAsync` with a `TestServer` handler over a standalone management host (16 MGMT-12 setup) on the real PostgreSQL store; one smoke case runs the published single-file binary as a separate process against Kestrel on loopback HTTPS. Envelopes are validated with a test-only JSON Schema validator (JsonSchema.Net, pinned at implementation [choice]).

| Case | What it runs |
|---|---|
| CLI-01 | Parity: every 16 §4 mutation and every 16 §2/§3 read, through the CLI, with Basic+antiforgery and with a bearer `ForMachineClients()` group; store state equals the same calls made directly to the session |
| CLI-02 | Replay: a mutation run twice with one `--command-id` → second `replayed`, one audit row; same ID with another body → `command_conflict` exit 1; reply dropped (`AfterCommitBeforeReply` + `DropReply`) → automatic resend resolves it, one transition; server stopped after the request was written → `outcome_unknown` exit 3, then a manual resend replays; `cancel-many` resent with one ID replays every item |
| CLI-03 | Agent schema: every envelope produced anywhere in this suite validates against the committed schema; the committed schema equals the generated one; describe lists every command of the model and every described command parses with its required options |
| CLI-04 | One model: for each command, `--output json` `result` equals the agent `result`; human output contains every full ID of the result |
| CLI-05 | No leakage: secret values placed in env/stdin never appear in any output, trace or the config file; no option accepts a password or connection string; a payload containing a marker string appears only in `reveal-payload` output |
| CLI-06 | Errors: each §5.6 code (forced through the server or a fault handler) has its exit code, retryable flag and hint; agent mode with no secret and closed stdin returns `credential_missing` at once without prompting; agent mutation without `--expected-revision` is `usage_error` with a read next action |
| CLI-07 | Streaming: `events tail` from a cursor while jobs run returns every event exactly once across resumes; `jobs tail` ends at the terminal state; Ctrl-C emits an `end` line; queue-scoped caller gets 403 on events; `actorId` null without `Audit.Read` |
| CLI-08 | `migrate` on an empty PostgreSQL database and again (no-op); `doctor` with store checks reports the revisions; a store one revision ahead of the CLI is `fail`. The SQLite half runs once SP-019 is merged |
| CLI-09 | Paging: `--limit 450` over 3 API pages returns 450 items and a `next`; an expired cursor is `invalid_cursor` |
| CLI-10 | Dispatch: `dispatch` from a file and from stdin creates one job and one audit row; resent with the same `--command-id` → `replayed`, still one job and one audit row; reply dropped → automatic resend, one job; unknown job name → `job_not_registered`; a payload that does not bind to the job type → `payload_invalid` with no payload text in any output; wrong `--payload-version` → `payload_version_unsupported`; a 1 MiB + 1 byte payload → `payload_too_large`; a caller without `Jobs.Dispatch` (every preset) → 403; a uniqueness-bound job dispatched twice with different command IDs → `reused` with an audit row each |

## 10. Schema revision

PostgreSQL **schema revision 17**: `ManagementDispatch/1` and the audit `Dispatch` operation (§6.2). The events endpoint needs none; it reads `queue_events` through `EventsSince/1`.

## 11. Left for later

- `schedules edit`: schedule definitions come from code registration (`UpdateSchedule/1`, 13 §6.4); an API edit would be overwritten by the next deploy.
