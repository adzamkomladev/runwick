> History; see [docs/spec](spec/00-overview.md) for the normative specification.

# Runwick public API proposal

The current reconciled proposal is [Public API and configuration contract](architecture/public-api-contract.md), dated 2026-10-29. It preserves `IQueueJob`, `IJobDispatcher.DispatchAsync`, `HandleAsync(job, CancellationToken)`, and optional injected `IJobContext`, with attributes and fluent configuration both supported.

Read it together with the [dispatch preparation and fingerprint contract](architecture/dispatch-wire-contract.md). These are reviewable planning proposals, not an implemented package or approval to begin runtime implementation.

The superseded [initial API draft](history/public-api-initial-draft.md) is retained for provenance. Its alternate dispatcher naming, required handler context argument, marker removal, and reservation-based retry accounting are not the current baseline.
