using Runwick;

namespace Runwick.Samples.Contracts;

[JobName("email.welcome")]
[PayloadVersion(2)]
[Queue("emails")]
[Priority("medium")]
[MaxExecutions(3)]
public sealed record SendWelcomeEmail(Guid UserId, string Locale) : IQueueJob;

/// <summary>Historical v1 payload; a decoding type only, not an executable job.</summary>
public sealed record SendWelcomeEmailV1(Guid UserId);

/// <summary>A fluent-only contract: no attributes, configured by the host.</summary>
public sealed record BuildReport(string ReportId, int Rows) : IQueueJob;

/// <summary>A job used by the harness and workloads: sleeps, then succeeds or fails on request.</summary>
[JobName("harness.work")]
[Queue("default")]
public sealed record HarnessWork(int SleepMilliseconds, string Behaviour, string? Tag = null) : IQueueJob;
