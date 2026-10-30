# Dashboard integration — planning closure 3 of 6

Status: D-10 implementation baseline, 2026-10-29. Source inspected; no Runwick integration/browser tests run. Preserves embedded/standalone Inertia.NET and the [management authorization contract](management-and-dashboard.md).

## Selected pairing

Select **Inertia.Net 1.5.3**, package repository `adzamkomladev/Inertia.Net`, with **@inertiajs/react/core 3.9.1, React/react-dom 19.3.1 and Vite 8.3.4** as the initial dashboard build pairing. These client versions are resolved in the inspected repository's React sample lockfile; this is a concrete compatibility baseline, not a claim that every component is today's latest release. Keep exact versions and integrity hashes in Runwick's frontend lockfile at SP-023.

Evidence: local checkout `/root/work/OpenSource/Inertia.Net` was clean at commit `5d0fa8ac95fc0922de1a7219e513ded5662401db`; `src/Directory.Build.props` declares 1.5.3 and the NuGet index also lists 1.5.3. [Server source at inspected commit](https://github.com/adzamkomladev/Inertia.Net/tree/5d0fa8ac95fc0922de1a7219e513ded5662401db), [React sample lock](https://github.com/adzamkomladev/Inertia.Net/blob/5d0fa8ac95fc0922de1a7219e513ded5662401db/samples/MinimalApi.React/package-lock.json), [NuGet version index](https://api.nuget.org/v3-flatcontainer/inertia.net/index.json).

This supersedes the earlier InertiaCore candidate, whose package/repository identity must not be inferred from similarly named packages. No InertiaCore dependency is selected. Source revision and package version are evidence identifiers; verify the actual restored package provenance/API when integrating.

Confirmed by the user 2026-10-30 (R5): Inertia.Net is the selected transport, **SPA mode only — no SSR**, for both the self-hosted dashboard and the hosted-service dashboard. The review's SPA-plus-JSON alternative is closed; the adapter-extension/transport fallback below remains the only fallback. Use the core Minimal API result path, no mandatory MVC/FastEndpoints integration. All frontend assets are built during package production, embedded in Runwick.Dashboard and served under its mount. Disable SSR, development Vite discovery, runtime TypeScript generation and external asset fetching. Consumers require neither Node nor a second UI server. Contracts/core/providers remain independent of dashboard dependencies.

## Source findings and host isolation

The inspected adapter uses request-local InertiaFeature state and HttpContext service resolution for results. It avoids the earlier candidate's static response-factory binding. However, AddInertia uses unnamed InertiaOptions and shared singleton registrations within a service provider; it does **not** establish isolation between two independently configured mounts in the same host. [Registration](https://github.com/adzamkomladev/Inertia.Net/blob/5d0fa8ac95fc0922de1a7219e513ded5662401db/src/Inertia.Net/InertiaServiceCollectionExtensions.cs), [request state](https://github.com/adzamkomladev/Inertia.Net/blob/5d0fa8ac95fc0922de1a7219e513ded5662401db/src/Inertia.Net/InertiaFeature.cs).

Runwick owns `IRunwickDashboardTransport` internally. Its context is bound to a validated mount ID, root view, immutable asset manifest, authorized application binding and named authentication configuration. Register no global MVC filter, global shared props or host-default authentication changes. Endpoint metadata selects this context; arbitrary request headers cannot select a different mount.

The existing adapter can be the transport for a dedicated host or a host without conflicting Inertia registration. For embedded hosts already using Inertia, SP-023 must add or consume explicit instance/named-context hooks for options, renderer, state store and asset/version provider. Do not solve this with a second general-purpose application service provider or by swapping RequestServices for user handlers. Preserve the selected protocol behind the internal transport interface.

If those hooks are unavailable, the bounded fallback is a Runwick-owned, mount-scoped implementation of the **used** v3 protocol subset inside Runwick.Dashboard, with the same React client and management services. It must pass the same fixtures; it is not a silent fallback at runtime. Prefer a reviewed adapter extension and pin its resulting package before release. Until either path passes embedded isolation, the required embedded feature is incomplete; standalone success alone cannot close the gate.

## Transport contract

Default mount `/runwick`, configurable and validated against route collisions. Management services authorize safe DTOs before transport. Authentication/authorization run before version negotiation or partial prop selection. Use initial encoded HTML plus Inertia page JSON, partial reloads, local redirects and asset-version refresh; ordinary management JSON endpoints retain their typed command responses. The official protocol defines page data and negotiation; Runwick narrows its used surface without claiming the server supports unused optional features. [Inertia v3 protocol](https://inertiajs.com/docs/v3/core-concepts/the-protocol)

Browser mutations use explicit command IDs and antiforgery. Return a safe state refresh after confirmed success; unknown results preserve their original operation ID for resolution rather than redirecting as if completed. A revision conflict is a management conflict; distinguish it from an Inertia asset-version 409 using the transport headers/body. No mutation replay on asset refresh.

Use mount-specific state and antiforgery cookie names, secure flags and path boundaries. Shared Data Protection keys/purposes must be intentionally configured across replicas; names do not replace authorization. Basic, host cookie and host OIDC use the same capability checks. No payload/detail in shared props, history, prefetch, flash or remembered state. Sensitive reveal remains a separate no-store request. History encryption/clearing is supplementary and must not replace DTO filtering.

Each mount serves immutable hashed assets and an asset version tied to the server/client manifest. HTML and protected JSON are private/no-store. Restrictive CSP, initial HTML attribute encoding, safe redirects, path base and trusted proxies must be verified against the selected build. The adapter's source has antiforgery support, but Runwick configures and tests its own route boundary rather than assuming AddInertia authenticates or protects requests.

Baseline UI: jobs/attempts, queues, workers/capacity, schedules, scaling events, sanitized settings, retention status and permitted audit views. Polling is the only v1 live-update transport; SSE/WebSocket hints remain optional future work, not a release dependency. No arbitrary SQL, payload editing, workflow designer or external CDN.

## Fixed acceptance/fallback gate

DASH-01: two hosts and two mounts with different principals/options/props; DASH-02: host already using Inertia, unchanged unrelated routes; DASH-03: Basic/cookie/OIDC, CSRF, denied partial/reveal requests; DASH-04: hostile strings, history, CSP and shared caches; DASH-05: PathBase/proxy and local redirects; DASH-06: old/new assets and ambiguous mutations; DASH-07: standalone without workers and embedded without global side effects. Run Chromium browser coverage plus protocol/service tests, then Firefox/WebKit smoke coverage.

SP-023 implements this bounded compatibility experiment; SP-024 builds the UI after it passes. Failure selects the stated adapter-extension/transport fallback, not a new frontend framework. No integration test result is claimed here.
