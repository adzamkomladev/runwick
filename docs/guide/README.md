# Runwick user guide

Runwick runs background jobs in .NET 10. Your app writes a job to a database (PostgreSQL, SQLite or Redis); a worker, inside your app or in another process, picks it up and runs your handler.

Read these in order the first time:

1. [Getting started](getting-started.md): install, set up each store, dispatch and run a first job.
2. [What Runwick promises (and does not)](delivery-semantics.md): at-least-once delivery, idempotency keys, the one exactly-once case.
3. [Cancelling jobs](cancellation.md): what "cancel" can and cannot do to code that is already running.
4. [Which store, which topology](topology.md): what each store supports, and what it does not.
5. [Configuration reference](configuration.md): the options and their defaults.
6. [Management, dashboard and CLI](management.md): operating jobs from a web page, an HTTP API or a terminal.
7. [Observability](observability.md): traces, metrics, logs, health checks, shipped alert rules.

The numbers in these pages come from `docs/spec/06-defaults.md` and the store specs; a test (`DefaultsMatchCodeTests`) fails if a documented default differs from the code. The spec files in `docs/spec` are the exact rules; this guide is the short version. Release checks and licences: [docs/release](../release/licenses.md).

Status: version 0.1.2. The conformance, performance and soak qualification reports (SP-028, SP-029) are not finished, so no store is yet labelled "certified".
