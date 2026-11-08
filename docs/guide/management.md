# Management, dashboard and CLI

Three ways to look at and control jobs. All three go through the same management API, so they follow the same permissions and write the same audit trail.

## HTTP API (`Runwick.AspNetCore`)

```csharp
services.AddRunwickManagement(m => m
    .AuthenticationSchemes("Cookies")                          // required
    .MapGrants(principal => ManagementRoles.Operator));        // required
app.MapRunwickManagement("/runwick/api");                      // endpoints under /runwick/api/v1/...
```

- You bring the sign-in (cookie, OIDC, JWT...). Runwick ships an optional Basic scheme (`AddRunwickBasic`) that needs your own password check and works over HTTPS only.
- There is no anonymous or "localhost only" mode. Missing configuration stops the app at startup.
- Every change needs an anti-forgery token (browsers) unless the route group is mapped for machine clients with a non-cookie scheme.
- Permissions are separate for reading jobs, reading payloads, reading errors, cancel, retry, rerun, dispatch, queues, capacity, schedules, retention and settings. Reading a payload or error is audited.
- You can run the API in a host with no workers (a standalone management host).

## Dashboard (`Runwick.Dashboard`)

A web UI served from inside your ASP.NET Core app: `AddRunwickDashboard("default", ...)` and `app.MapRunwickDashboard("/runwick")`, mounted before your own `UseInertia()` if your app also uses Inertia. It uses the management API's permissions; hiding a button is not security, the server checks every request.

What it is not: it is not live (pages poll and say when they were read), it shows no queue-depth totals of its own, "cancel requested" is not "stopped", payloads you reveal cannot be taken back from a browser, and permissions are per application and queue, not per tenant. Rate limits on the API are per process. Dashboard support on SQLite and Redis follows those stores' management reads and their conformance reports.

## Command line (`runwick`)

`dotnet tool install Runwick.Cli`. It talks to a management host over HTTP:

```
runwick context add prod --url https://ops.example.com/runwick/api --auth basic --user ops --secret-env RUNWICK_PROD_SECRET
runwick jobs list --state failed
runwick jobs cancel <id> --expected-generation 1 --expected-revision 7
runwick queues pause default --expected-revision 3
runwick schedules list
runwick events tail --follow
runwick migrate --provider postgres --connection-env MY_CONN      # direct to the database
runwick doctor
```

Secrets are never stored in the config file and connection strings are never accepted as arguments. `--output json` and `--agent` give stable machine-readable output (`runwick --agent describe` lists the commands). Use the CLI version that matches your Runwick release. Single-file binaries exist for Linux x64 and Windows x64.

Changes use `--expected-revision`, so two people acting on the same stale view cannot overwrite each other; you get a conflict answer instead.
