using System.Globalization;
using System.Security.Claims;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Antiforgery;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Runwick;
using Runwick.AspNetCore;
using Runwick.Management;

namespace Microsoft.AspNetCore.Builder;

/// <summary>The mapped management endpoints; <see cref="ForMachineClients"/> drops antiforgery for non-cookie, non-Basic schemes.</summary>
public sealed class RunwickManagementEndpoints : IEndpointConventionBuilder
{
    readonly RouteGroupBuilder _group;
    readonly ManagementEndpointState _state;

    internal RunwickManagementEndpoints(RouteGroupBuilder group, ManagementEndpointState state) => (_group, _state) = (group, state);

    /// <summary>
    /// Mutations skip antiforgery. Only schemes that are neither cookie-based nor Basic are accepted; mapping a machine
    /// group with such a scheme fails here, at startup.
    /// </summary>
    public RunwickManagementEndpoints ForMachineClients()
    {
        if (_state.BrowserSchemes.Count > 0)
        {
            throw new InvalidOperationException(
                $"ForMachineClients cannot accept cookie or Basic schemes ({string.Join(", ", _state.BrowserSchemes)}).");
        }

        _state.MachineClients = true;
        return this;
    }

    public void Add(Action<EndpointBuilder> convention) => ((IEndpointConventionBuilder)_group).Add(convention);

    public void Finally(Action<EndpointBuilder> finallyConvention) => ((IEndpointConventionBuilder)_group).Finally(finallyConvention);
}

internal sealed class ManagementEndpointState
{
    public required IReadOnlyList<string> Schemes { get; init; }

    public required IReadOnlyList<string> BrowserSchemes { get; init; }

    public required IReadOnlyList<string> BasicSchemes { get; init; }

    public bool MachineClients { get; set; }
}

public static class RunwickManagementEndpointRouteBuilderExtensions
{
    public const string AntiforgeryHeader = "X-Runwick-Antiforgery";
    const int MaxBody = 64 * 1024;
    const int DispatchMaxBody = 1024 * 1024 + 16 * 1024; // jobs:dispatch only (19 §6.2)
    const string CallerKey = "runwick.management.caller";

    static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    /// <summary>
    /// Maps the management API under <c>{prefix}/v1</c>. Fails at startup when schemes or the grant mapping are missing,
    /// or a scheme is not registered; there is no anonymous mode.
    /// </summary>
    public static RunwickManagementEndpoints MapRunwickManagement(this IEndpointRouteBuilder endpoints, string prefix)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        var sp = endpoints.ServiceProvider;
        var settings = sp.GetService<RunwickManagementBuilder>()
            ?? throw new InvalidOperationException("Call services.AddRunwickManagement(...) before MapRunwickManagement.");
        if (settings.Schemes.Count == 0 || settings.Grants is null)
        {
            throw new InvalidOperationException("AddRunwickManagement requires AuthenticationSchemes(...) and MapGrants(...).");
        }

        var auth = sp.GetRequiredService<IOptions<AuthenticationOptions>>().Value;
        var browser = new List<string>();
        var basic = new List<string>();
        foreach (var name in settings.Schemes)
        {
            var scheme = auth.SchemeMap.TryGetValue(name, out var s) ? s
                : throw new InvalidOperationException($"Authentication scheme '{name}' is not registered.");
            if (typeof(RunwickBasicHandler).IsAssignableFrom(scheme.HandlerType))
            {
                sp.GetRequiredService<IOptionsMonitor<RunwickBasicOptions>>().Get(name).Validate();
                basic.Add(name);
                browser.Add(name);
            }
            else if (typeof(CookieAuthenticationHandler).IsAssignableFrom(scheme.HandlerType))
            {
                browser.Add(name);
            }
        }

        var state = new ManagementEndpointState { Schemes = [.. settings.Schemes], BrowserSchemes = browser, BasicSchemes = basic };
        var group = endpoints.MapGroup(prefix.TrimEnd('/') + "/v1");
        group.AddEndpointFilter((ctx, next) => GuardAsync(ctx, next, state, settings));
        MapRoutes(group);
        return new RunwickManagementEndpoints(group, state);
    }

    // ------------------------------------------------------------------ the guard: limits, authentication, antiforgery, errors

    static async ValueTask<object?> GuardAsync(EndpointFilterInvocationContext invocation, EndpointFilterDelegate next, ManagementEndpointState state,
        RunwickManagementBuilder settings)
    {
        var ctx = invocation.HttpContext;
        ctx.Response.Headers.CacheControl = "no-store, private";
        ctx.Response.Headers.XContentTypeOptions = "nosniff";
        var limiter = ctx.RequestServices.GetRequiredService<ManagementLimiter>();
        if (!await limiter.EnterAsync(ctx.RequestAborted).ConfigureAwait(false))
        {
            return Error(503, "busy", "The management API is busy.");
        }

        try
        {
            // Only the listed schemes; never a challenge that redirects (cookie schemes are not challenged).
            ClaimsPrincipal? principal = null;
            string? used = null;
            foreach (var scheme in state.Schemes)
            {
                var result = await ctx.AuthenticateAsync(scheme).ConfigureAwait(false);
                if (result.Succeeded && result.Principal.Identity?.IsAuthenticated == true)
                {
                    (principal, used) = (result.Principal, scheme);
                    break;
                }
            }

            if (principal is null)
            {
                foreach (var basic in state.BasicSchemes)
                {
                    await ctx.ChallengeAsync(basic).ConfigureAwait(false);
                }

                // Event 1401, limited to 10 a minute (18 §5); the source is a coarse hash bucket, never the address or user.
                var source = ctx.Connection.RemoteIpAddress?.ToString() ?? "";
                var bucket = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(source)))[..4];
                Runwick.Telemetry.RunwickLog.ManagementAuthenticationFailed(
                    ctx.RequestServices.GetRequiredService<ILoggerFactory>().CreateLogger("Runwick.Management"), string.Join(',', state.Schemes), bucket);
                return Error(401, "unauthenticated", "Authentication is required.");
            }

            ctx.User = principal;
            var actor = settings.ActorMap?.Invoke(principal) ?? DefaultActor(principal, used!);
            var caller = new ManagementCaller(actor, settings.Grants!(principal));
            ctx.Items[CallerKey] = caller;

            var post = HttpMethods.IsPost(ctx.Request.Method);
            var kind = !post ? ManagementLimiter.Kind.Read
                : ctx.Request.Path.Value!.EndsWith(":reveal", StringComparison.Ordinal) ? ManagementLimiter.Kind.Reveal
                : ManagementLimiter.Kind.Mutation;
            if (limiter.TryTake(actor.Scheme + "\n" + actor.Subject, kind) is { } retryAfter)
            {
                ctx.Response.Headers.RetryAfter = retryAfter.ToString(CultureInfo.InvariantCulture);
                Runwick.Telemetry.RunwickLog.ManagementRateLimited(
                    ctx.RequestServices.GetRequiredService<ILoggerFactory>().CreateLogger("Runwick.Management"), kind.ToString());
                return Error(429, "rate_limited", "Too many requests.");
            }

            if (post)
            {
                if (ctx.Request.ContentLength > BodyLimit(ctx))
                {
                    return Error(413, "payload_too_large", "The request body is too large.");
                }

                if ((ctx.Request.ContentLength > 0 || ctx.Request.ContentType is not null) && !ctx.Request.HasJsonContentType())
                {
                    return Error(415, "unsupported_media_type", "Requests must be application/json.");
                }

                if (!state.MachineClients && !await AntiforgeryValidAsync(ctx).ConfigureAwait(false))
                {
                    return Error(400, "antiforgery_invalid", "The antiforgery token is missing or invalid.");
                }
            }

            return await next(invocation).ConfigureAwait(false);
        }
        catch (Exception ex) when (Map(ex) is { } mapped)
        {
            return mapped;
        }
        finally
        {
            limiter.Exit();
        }
    }

    static ManagementActor DefaultActor(ClaimsPrincipal p, string scheme)
    {
        var subject = p.FindFirstValue(ClaimTypes.NameIdentifier) ?? p.Identity?.Name
            ?? throw new InvalidOperationException("The principal has no NameIdentifier or Name; configure Actor(...).");
        return new ManagementActor(subject, p.Identity?.Name, scheme);
    }

    /// <summary>
    /// Validates the request token sent in <c>X-Runwick-Antiforgery</c> with the host's antiforgery services. The token is
    /// moved to the host's configured header so the host's options are not changed; no other header skips the check.
    /// </summary>
    static async Task<bool> AntiforgeryValidAsync(HttpContext ctx)
    {
        var header = ctx.RequestServices.GetRequiredService<IOptions<AntiforgeryOptions>>().Value.HeaderName ?? AntiforgeryHeader;
        var token = ctx.Request.Headers[AntiforgeryHeader].ToString();
        ctx.Request.Headers.Remove(header);
        if (token.Length == 0)
        {
            return false;
        }

        ctx.Request.Headers[header] = token;
        try
        {
            await ctx.RequestServices.GetRequiredService<IAntiforgery>().ValidateRequestAsync(ctx).ConfigureAwait(false);
            return true;
        }
        catch (AntiforgeryValidationException)
        {
            return false;
        }
    }

    static IResult? Map(Exception ex) => ex switch
    {
        ManagementDeniedException => Error(403, "forbidden", "The caller lacks the permission for this operation."),
        ManagementNotFoundException n => Error(404, n.Code, n.Message),
        ManagementValidationException { Code: "payload_too_large" } v => Error(413, v.Code, v.Message),
        ManagementValidationException v => Error(400, v.Code, v.Message),
        ManagementConflictException c => Error(409, c.Code, c.Message),
        ManagementUnavailableException u => Error(503, u.Code, u.Message),
        ManagementDetailExpiredException d => Results.Json(new { code = d.Code, message = d.Message, jobId = d.JobId }, Json, statusCode: 410),
        ControlOutcomeUnknownException u => Results.Json(new ErrorBody(u.Code, "The outcome is unknown; resend the same command.", u.CommandId, null), Json,
            statusCode: 503),
        RunwickConfigurationException or ArgumentException => null,
        RunwickException r => Error(503, r.Code, "The store is unavailable."),
        BadHttpRequestException { StatusCode: 413 } => Error(413, "payload_too_large", "The request body is too large."),
        JsonException or BadHttpRequestException => Error(400, "invalid_request", "The request body is not valid."),
        _ => null,
    };

    sealed record ErrorBody(string Code, string Message, Guid? CommandId, long? ActualRevision);

    static IResult Error(int status, string code, string message, Guid? commandId = null, long? actualRevision = null) =>
        Results.Json(new ErrorBody(code, message, commandId, actualRevision), Json, statusCode: status);

    // ------------------------------------------------------------------ routes (16 §5)

    static ManagementCaller Caller(HttpContext ctx) => (ManagementCaller)ctx.Items[CallerKey]!;

    static IManagementSession Session(HttpContext ctx) => ctx.RequestServices.GetRequiredService<IManagementSession>();

    static IResult Ok(object value) => Results.Json(value, Json);

    static int BodyLimit(HttpContext ctx) => ctx.Request.Path.Value!.EndsWith("/jobs:dispatch", StringComparison.Ordinal) ? DispatchMaxBody : MaxBody;

    static async Task<T> Body<T>(HttpContext ctx) where T : class, new()
    {
        var max = BodyLimit(ctx);
        if (ctx.Request.ContentLength is 0 || (ctx.Request.ContentLength is null && !ctx.Request.HasJsonContentType()))
        {
            return new T();
        }

        using var buffer = new MemoryStream();
        var chunk = new byte[8192];
        int n;
        while ((n = await ctx.Request.Body.ReadAsync(chunk, ctx.RequestAborted).ConfigureAwait(false)) > 0)
        {
            if (buffer.Length + n > max)
            {
                throw new BadHttpRequestException("The request body is too large.", 413);
            }

            await buffer.WriteAsync(chunk.AsMemory(0, n), ctx.RequestAborted).ConfigureAwait(false);
        }

        return buffer.Length == 0 ? new T() : JsonSerializer.Deserialize<T>(buffer.GetBuffer().AsSpan(0, (int)buffer.Length), Json) ?? new T();
    }

    static string? Q(HttpContext ctx, string name) => ctx.Request.Query[name] is { Count: > 0 } v ? v.ToString() : null;

    static int? QInt(HttpContext ctx, string name) => Q(ctx, name) is { } s
        ? int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : throw Invalid(name)
        : null;

    static DateTimeOffset? QTime(HttpContext ctx, string name) => Q(ctx, name) is { } s
        ? DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var v) ? v : throw Invalid(name)
        : null;

    static ManagementValidationException Invalid(string name) => new("invalid_request", $"Query parameter '{name}' is not valid.");

    // Every async handler takes a second parameter so it binds as a Delegate, not as a RequestDelegate whose result is dropped.
    static void MapRoutes(RouteGroupBuilder g)
    {
        g.MapGet("antiforgery", (HttpContext ctx) =>
        {
            var tokens = ctx.RequestServices.GetRequiredService<IAntiforgery>().GetAndStoreTokens(ctx);
            return Ok(new { token = tokens.RequestToken, headerName = AntiforgeryHeader });
        });

        g.MapGet("jobs", async (HttpContext ctx, CancellationToken _) => Ok(await Session(ctx).ListJobsAsync(Caller(ctx),
            new JobListQuery(Q(ctx, "queue"), Q(ctx, "state"), Q(ctx, "jobName"), QTime(ctx, "from"), QTime(ctx, "to"), QInt(ctx, "limit"), Q(ctx, "cursor")),
            ctx.RequestAborted)));
        g.MapGet("jobs/{id:guid}", async (HttpContext ctx, Guid id) => Ok(await Session(ctx).GetJobAsync(Caller(ctx), id, ctx.RequestAborted)));
        g.MapPost("jobs/{id:guid}/payload:reveal", async (HttpContext ctx, Guid id) =>
            Ok(await Session(ctx).RevealPayloadAsync(Caller(ctx), id, (await Body<ReasonBody>(ctx)).Reason, ctx.RequestAborted)));
        g.MapPost("attempts/{id:guid}/error:reveal", async (HttpContext ctx, Guid id) =>
            Ok(await Session(ctx).RevealErrorAsync(Caller(ctx), id, (await Body<ReasonBody>(ctx)).Reason, ctx.RequestAborted)));

        g.MapPost("jobs/{id:guid}/cancel", (HttpContext ctx, Guid id) => JobAsync(ctx, id, (s, c, b, t) => s.CancelJobAsync(c, id, b, t)));
        g.MapPost("jobs/{id:guid}/retry", (HttpContext ctx, Guid id) => JobAsync(ctx, id, (s, c, b, t) => s.RetryJobAsync(c, id, b, t)));
        g.MapPost("jobs/{id:guid}/rerun", (HttpContext ctx, Guid id) => JobAsync(ctx, id, (s, c, b, t) => s.RerunJobAsync(c, id, b, t)));
        g.MapPost("jobs:cancel", async (HttpContext ctx, CancellationToken _) =>
        {
            var body = await Body<BulkBody>(ctx);
            var items = await Session(ctx).CancelJobsAsync(Caller(ctx), body.Items ?? [], body.Reason, ctx.RequestAborted);
            return Ok(new { items });
        });

        g.MapGet("queues", async (HttpContext ctx, CancellationToken _) =>
            Ok(await Session(ctx).ListQueuesAsync(Caller(ctx), QInt(ctx, "limit"), Q(ctx, "cursor"), ctx.RequestAborted)));
        g.MapPost("queues/{name}/pause", (HttpContext ctx, string name) => QueueAsync(ctx, name, true));
        g.MapPost("queues/{name}/resume", (HttpContext ctx, string name) => QueueAsync(ctx, name, false));

        g.MapGet("workers", async (HttpContext ctx, CancellationToken _) => Ok(await Session(ctx).ListWorkersAsync(Caller(ctx),
            Q(ctx, "includeExpired") is { } e && (bool.TryParse(e, out var b) ? b : throw Invalid("includeExpired")), QInt(ctx, "limit"), Q(ctx, "cursor"),
            ctx.RequestAborted)));
        g.MapGet("scopes", async (HttpContext ctx, CancellationToken _) =>
            Ok(await Session(ctx).ListScopesAsync(Caller(ctx), Q(ctx, "kind"), QInt(ctx, "limit"), Q(ctx, "cursor"), ctx.RequestAborted)));
        g.MapPost("scopes/{kind}/{key}/capacity", async (HttpContext ctx, string kind, string key) =>
        {
            var b = await Body<CapacityBody>(ctx);
            var r = await Session(ctx).SetScopeCapacityAsync(Caller(ctx), kind, key, b.CommandId, b.Capacity, b.ExpectedPolicyRevision, b.Reason, ctx.RequestAborted);
            return r.Succeeded
                ? Ok(new { scopeKind = r.ScopeKind, scopeKey = r.ScopeKey, capacity = r.Capacity, policyRevision = r.PolicyRevision, livePermits = r.LivePermits,
                    replayed = r.Replayed, commandId = b.CommandId })
                : Error(409, r.Code!, "The control was rejected.", b.CommandId, r.PolicyRevision);
        });

        g.MapGet("schedules", async (HttpContext ctx, CancellationToken _) =>
            Ok(await Session(ctx).ListSchedulesAsync(Caller(ctx), QInt(ctx, "limit"), Q(ctx, "cursor"), ctx.RequestAborted)));
        g.MapPost("schedules/{key}/pause", (HttpContext ctx, string key) =>
            ScheduleAsync(ctx, (s, c, b, t) => s.PauseScheduleAsync(c, key, b.CommandId, b.ExpectedRevision, b.Reason, t)));
        g.MapPost("schedules/{key}/resume", (HttpContext ctx, string key) =>
            ScheduleAsync(ctx, (s, c, b, t) => s.ResumeScheduleAsync(c, key, b.CommandId, b.ExpectedRevision, Policy(b.Policy), b.Reason, t)));
        g.MapPost("schedules/{key}/trigger", (HttpContext ctx, string key) =>
            ScheduleAsync(ctx, (s, c, b, t) => s.TriggerScheduleAsync(c, key, b.CommandId, b.ExpectedRevision, b.Reason, t)));
        g.MapPost("schedules/{key}/revalidate", (HttpContext ctx, string key) =>
            ScheduleAsync(ctx, (s, c, b, t) => s.RevalidateScheduleAsync(c, key, b.CommandId, b.ExpectedRevision, b.Reason, t)));

        g.MapGet("audit", async (HttpContext ctx, CancellationToken _) => Ok(await Session(ctx).ListAuditAsync(Caller(ctx),
            new AuditQuery(Q(ctx, "targetKind"), Q(ctx, "targetId"), QTime(ctx, "from"), QTime(ctx, "to"), QInt(ctx, "limit"), Q(ctx, "cursor")),
            ctx.RequestAborted)));
        g.MapGet("settings", async (HttpContext ctx, CancellationToken _) => Ok(await Session(ctx).GetSettingsAsync(Caller(ctx), ctx.RequestAborted)));

        g.MapGet("retention", async (HttpContext ctx, CancellationToken _) =>
        {
            var v = await Session(ctx).GetRetentionAsync(Caller(ctx), ctx.RequestAborted);
            return Ok(new { policy = RetentionBody.Hours(v.Policy), revision = v.Revision, classes = v.Classes, observedAt = v.ObservedAt });
        });
        g.MapPost("retention/policy", async (HttpContext ctx, CancellationToken _) =>
        {
            var b = await Body<RetentionBody>(ctx);
            var r = await Session(ctx).EditRetentionPolicyAsync(Caller(ctx), b.CommandId, b.ExpectedRevision, b.Policy(), b.Reason, ctx.RequestAborted);
            return r.Succeeded
                ? Ok(new { revision = r.Revision, policy = RetentionBody.Hours(r.Policy!), replayed = r.Replayed, commandId = b.CommandId })
                : Error(409, r.Code!, "The edit was rejected.", b.CommandId, r.Revision);
        });
        g.MapGet("events", async (HttpContext ctx, CancellationToken _) =>
            Ok(await Session(ctx).ListEventsAsync(Caller(ctx), Q(ctx, "after"), QInt(ctx, "limit"), ctx.RequestAborted)));
        g.MapPost("jobs:dispatch", async (HttpContext ctx, CancellationToken _) =>
        {
            var b = await Body<DispatchBody>(ctx);
            return Ok(await Session(ctx).DispatchAsync(Caller(ctx), new ManagementDispatchRequest(b.CommandId, b.JobName!, b.Payload, b.PayloadVersion,
                b.Queue, b.Priority, b.NotBefore, b.Reason), ctx.RequestAborted));
        });
    }

    static ResumePolicy Policy(string? policy) => policy switch
    {
        null or "skipPaused" => ResumePolicy.SkipPaused,
        "runOnce" => ResumePolicy.RunOnce,
        "catchUp" => ResumePolicy.CatchUp,
        _ => throw new ManagementValidationException("invalid_request", "policy must be skipPaused, runOnce or catchUp."),
    };

    static async Task<IResult> JobAsync(HttpContext ctx, Guid id,
        Func<IManagementSession, ManagementCaller, JobControlRequest, CancellationToken, Task<JobControlResult>> send)
    {
        var b = await Body<JobBody>(ctx);
        var r = await send(Session(ctx), Caller(ctx), new JobControlRequest(b.CommandId, b.ExpectedGeneration, b.ExpectedRevision, b.Reason), ctx.RequestAborted);
        return r.Succeeded
            ? Results.Json(new { jobId = id, effect = r.Effect, generation = r.Generation, revision = r.Revision, state = r.State, replayed = r.Replayed,
                commandId = b.CommandId }, Json, statusCode: r.Effect == "cancel_requested" ? 202 : 200)
            : Error(409, r.Code!, "The control was rejected.", b.CommandId, r.Revision);
    }

    static async Task<IResult> QueueAsync(HttpContext ctx, string name, bool pause)
    {
        var b = await Body<QueueBody>(ctx);
        var s = Session(ctx);
        var r = pause
            ? await s.PauseQueueAsync(Caller(ctx), name, b.CommandId, b.ExpectedRevision, b.Reason, ctx.RequestAborted)
            : await s.ResumeQueueAsync(Caller(ctx), name, b.CommandId, b.ExpectedRevision, b.Reason, ctx.RequestAborted);
        return r.Succeeded
            ? Ok(new { queue = r.Queue, paused = r.Paused, revision = r.Revision, replayed = r.Replayed, commandId = b.CommandId })
            : Error(409, r.Code!, "The control was rejected.", b.CommandId, r.Revision);
    }

    static async Task<IResult> ScheduleAsync(HttpContext ctx,
        Func<IManagementSession, ManagementCaller, ScheduleBody, CancellationToken, Task<ScheduleControlResult>> send)
    {
        var b = await Body<ScheduleBody>(ctx);
        var r = await send(Session(ctx), Caller(ctx), b, ctx.RequestAborted);
        return r.Succeeded
            ? Ok(new { key = r.Key, paused = r.Paused, needsReview = r.NeedsReview, definitionRevision = r.DefinitionRevision,
                controlRevision = r.ControlRevision, jobId = r.JobId?.Value, replayed = r.Replayed, commandId = b.CommandId })
            : Error(409, r.Code!, "The control was rejected.", b.CommandId);
    }

    // Request bodies: only these fields are read; an actor or grant in a body is ignored.
    sealed class ReasonBody
    {
        public string? Reason { get; set; }
    }

    sealed class JobBody
    {
        public Guid CommandId { get; set; }

        public int ExpectedGeneration { get; set; }

        public long ExpectedRevision { get; set; }

        public string? Reason { get; set; }
    }

    sealed class BulkBody
    {
        public List<BulkCancelItem>? Items { get; set; }

        public string? Reason { get; set; }
    }

    sealed class QueueBody
    {
        public Guid CommandId { get; set; }

        public long ExpectedRevision { get; set; }

        public string? Reason { get; set; }
    }

    sealed class CapacityBody
    {
        public Guid CommandId { get; set; }

        public int Capacity { get; set; }

        public long ExpectedPolicyRevision { get; set; }

        public string? Reason { get; set; }
    }

    /// <summary>Policy values in whole hours; null keeps forever (17 §7). Worker presence is always finite.</summary>
    sealed class RetentionBody
    {
        public Guid CommandId { get; set; }

        public long ExpectedRevision { get; set; }

        public int? CompletedDetailHours { get; set; }

        public int? FailedDetailHours { get; set; }

        public int? JobHistoryHours { get; set; }

        public int? AuditHours { get; set; }

        public int? ScalingEventsHours { get; set; }

        public int? ScheduleOccurrencesHours { get; set; }

        public int? WorkerPresenceHours { get; set; }

        public string? Reason { get; set; }

        public RetentionPolicy Policy() => new(H(CompletedDetailHours), H(FailedDetailHours), H(JobHistoryHours), H(AuditHours), H(ScalingEventsHours),
            H(ScheduleOccurrencesHours), H(WorkerPresenceHours) ?? throw new ManagementValidationException("invalid_retention_policy",
                "workerPresenceHours is required."));

        static TimeSpan? H(int? hours) => hours is { } h ? TimeSpan.FromHours(h) : null;

        static int? I(TimeSpan? t) => t is { } v ? (int)v.TotalHours : null;

        /// <summary>A JSON object, so a null (forever) value is written rather than dropped.</summary>
        public static JsonObject Hours(RetentionPolicy p) => new()
        {
            ["completedDetailHours"] = I(p.CompletedDetail), ["failedDetailHours"] = I(p.FailedDetail), ["jobHistoryHours"] = I(p.JobHistory),
            ["auditHours"] = I(p.Audit), ["scalingEventsHours"] = I(p.ScalingEvents), ["scheduleOccurrencesHours"] = I(p.ScheduleOccurrences),
            ["workerPresenceHours"] = I(p.WorkerPresence),
        };
    }

    sealed class DispatchBody
    {
        public Guid CommandId { get; set; }

        public string? JobName { get; set; }

        public System.Text.Json.Nodes.JsonNode? Payload { get; set; }

        public int? PayloadVersion { get; set; }

        public string? Queue { get; set; }

        public string? Priority { get; set; }

        public DateTimeOffset? NotBefore { get; set; }

        public string? Reason { get; set; }
    }

    sealed class ScheduleBody
    {
        public Guid CommandId { get; set; }

        public long ExpectedRevision { get; set; }

        public string? Policy { get; set; }

        public string? Reason { get; set; }
    }
}
