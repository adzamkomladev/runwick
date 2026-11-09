# G-3 evidence summary (for the owner's sign-off)

Date: 2026-11-09. Status: **not passed, waiting on the owner's decision.** This page collects the evidence so you can decide in one read. It does not record a decision. If you accept, add a line at the bottom with your name, the date and "G-3 passed"; if not, write what is missing.

G-3, as defined in `docs/v1-delivery-plan.md` §10: "Basic/host/OIDC embedded and standalone; every protected path enforces authz/antiforgery; audit atomic with changes; adapter isolation tests; dashboard areas within budgets; CLI parity".

Where the results come from: the SP-020 (management API), SP-024 (dashboard) and SP-032 (CLI) reports, and the SP-028 conformance run on PostgreSQL (`docs/conformance/postgres.md`, commit 8f64ffb, 2026-11-08), where every MGMT, DASH, DSH and CLI case passes except DASH-07, which is partial.

## Each part of G-3

| G-3 part | What shows it | Cases (all pass on PostgreSQL unless noted) | Open |
|---|---|---|---|
| Basic, host and OIDC sign-in, embedded | API: only the listed schemes, 401 never a redirect, Basic only over HTTPS with a failure limiter (`AuthenticationTests`). Dashboard: Basic, host cookie and an OIDC-style external challenge (`SecurityTests.Host_cookie_and_OIDC_style_challenges_become_full_visits_for_Inertia_requests`). | MGMT-01, MGMT-08, DASH-03 | OIDC is tested with a stand-in identity provider handler that behaves like the usual cookie + OpenIdConnect pairing, not with a real OIDC server. |
| The same, standalone | A management-only host (no worker, no scheduler) serves every read and control and writes no presence row (`StandaloneHostTests`, and the contract test run on all three stores). Standalone dashboard (spec 21 §2.3). | MGMT-12 (PostgreSQL, SQLite, Redis), DASH-07 | DASH-07 is partial: the browser smoke passed in Chromium and Firefox; the WebKit smoke is owed on a Mac (see `sp-030.md`). The server-side DASH-07 tests pass. |
| Every protected path enforces authorization and antiforgery | Browser POSTs need the antiforgery token, `X-Inertia` does not skip it, GET cannot change anything; queue-scoped callers get 404 outside their queues; missing, forged, other-user or other-mount tokens are 403 with no store change. | MGMT-02, MGMT-03, DASH-03, DSH-03 | None found. |
| Audit is atomic with changes | A failing audit insert applies nothing and a reveal returns nothing; audited dispatch rolls back whole when the audit insert fails; every CLI mutation leaves exactly one audit row; Redis writes the audit row in the same intent (REDIS-12). | MGMT-09, MGMT-13 (all three stores), MGMT-04, MGMT-05, CLI-01 | None found. |
| Adapter isolation | Two dashboard mounts and two apps in one process keep their own schemes, grants, cookies and props; a host's own Inertia pages are byte-identical with and without the dashboard; two management mounts in one process stay separate. | DASH-01, DASH-02, MGMT-07 | None found. |
| Dashboard areas within budgets | Polls no faster than every 5 s, paused while the tab is hidden, one in flight (Chromium flow, 17 of 17 steps); request limits (120 reads, 30 mutations, 10 reveals per minute, 16 active) give 429 or 503; each read has a 1 s query budget; page cap 200, 31-day range. | MGMT-10, DASH-04, Chromium flow in SP-024 | This is my reading of "budgets": the spec's poll and request limits. There is no page-load time budget in spec 21, so none was measured. Timing tests ran on a loaded shared host and passed. |
| CLI parity | Every read and mutation of the API works through `runwick`, with the same results in JSON and agent output, command-ID replay, exit codes, streams, paging and secrets kept out of output. | CLI-01 to CLI-10 | None found. Redis store support (`migrate`, `doctor`, `store ...`) was added on 2026-11-09 (spec 19 Changes, CLI-08). |

## What is open, in one list

1. DASH-07 WebKit smoke, owed on a Mac or a Linux machine with the WebKit libraries (steps in `sp-030.md`).
2. OIDC proven only with a stand-in identity provider.
3. "Budgets" read as the spec's poll and request limits; no page-load timing.
4. (Closed 2026-11-09) The CLI's direct-to-store commands now cover Redis too.

## Decision

_Not recorded. The owner writes it here._
