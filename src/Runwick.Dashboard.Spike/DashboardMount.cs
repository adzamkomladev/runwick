using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using Inertia.Net;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Authorization.Policy;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace Runwick.Dashboard.Spike;

/// <summary>
/// One dashboard mount on an Inertia.Net 1.7.0 named mount. The adapter owns the protocol: page JSON, initial HTML body,
/// asset-version 409, partial reloads, Vary, 303 and other-origin redirects, per-mount state cookies. This class keeps only
/// what the adapter does not do per mount: authorization before the version check, per-mount antiforgery, embedded assets,
/// response headers, redirects that leave the mount on the same origin, denied partials, and props serialized with Runwick's
/// own JSON settings rather than the host's. It is also the mount's keyed root view.
/// </summary>
internal sealed class DashboardMount : IInertiaRootView
{
    internal const string Csp = "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; font-src 'self'; "
        + "connect-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'";

    internal const string CsrfHeader = "X-Runwick-Csrf";
    internal const string CommandHeader = "X-Runwick-Command-Id";
    private const int MaxBodyBytes = 16 * 1024;
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private readonly DashboardMountOptions _options;
    private readonly IDashboardSpikeData _data;
    private readonly AuthorizationPolicy _view;
    private readonly AuthorizationPolicy _operate;
    private readonly IDataProtector _csrf;
    private readonly string _cookie;

    public DashboardMount(string mountId, DashboardMountOptions options, IDataProtectionProvider dataProtection)
    {
        MountId = mountId;
        _options = options;
        _data = options.Data!;
        _view = options.ViewPolicy!;
        _operate = options.OperatePolicy!;
        _csrf = dataProtection.CreateProtector("Runwick.Dashboard.Csrf.v1", mountId);
        _cookie = $"rwk.{mountId}.csrf";
    }

    public string MountId { get; }

    /// <summary>The Inertia.Net mount name: its state cookies are <c>.Inertia.State.runwick-{id}</c>.</summary>
    public static string InertiaName(string mountId) => "runwick-" + mountId;

    /// <summary>The mount's branch: our gate, then the adapter's middleware for this mount only, then the pages.</summary>
    public void Configure(IApplicationBuilder branch)
    {
        branch.Use((context, next) => GateAsync(context, next));
        branch.UseInertia(InertiaName(MountId));
        branch.Run(RouteAsync);
    }

    private async Task GateAsync(HttpContext context, RequestDelegate next)
    {
        var response = context.Response;
        response.Headers.XContentTypeOptions = "nosniff";
        response.Headers["Referrer-Policy"] = "same-origin";
        if (Segments(context) is ["assets", var file])
        {
            await ServeAssetAsync(context, file);
            return;
        }

        response.Headers.CacheControl = "no-store, private";
        if (IsInertia(context.Request))
        {
            response.OnStarting(static state => LeaveMountInFull((HttpContext)state), context);
        }

        // Authentication and authorization run before the adapter's version check and prop selection.
        if (await AuthorizeAsync(context, _view))
        {
            await next(context);
        }
    }

    private async Task RouteAsync(HttpContext context)
    {
        switch (Segments(context))
        {
            case []:
                await GetAsync(context, () => RenderAsync(context, "Overview", new InertiaProps
                {
                    ["base"] = Base(context),
                    ["mount"] = _options.Title,
                    ["user"] = UserName(context.User),
                    ["jobs"] = Element(_data.Jobs()),
                    ["stats"] = Inertia.Net.Inertia.Optional(() => Element(new { total = _data.Jobs().Count })),
                }));
                break;
            case ["jobs", var id]:
                await GetAsync(context, async () =>
                {
                    if (_data.Find(id) is { } job)
                    {
                        await RenderJobAsync(context, job, outcome: null);
                    }
                    else
                    {
                        context.Response.StatusCode = StatusCodes.Status404NotFound;
                    }
                });
                break;
            case ["jobs", var id, "payload"]:
                await GetAsync(context, async () =>
                {
                    if (!await AuthorizeAsync(context, _operate))
                    {
                        return;
                    }

                    if (_data.Payload(id) is { } payload)
                    {
                        await context.Response.WriteAsJsonAsync(new { id, payload }, Json);
                    }
                    else
                    {
                        context.Response.StatusCode = StatusCodes.Status404NotFound;
                    }
                });
                break;
            case ["jobs", var id, "retry"]:
                await RetryAsync(context, id);
                break;
            default:
                context.Response.StatusCode = StatusCodes.Status404NotFound;
                break;
        }
    }

    private async Task RenderJobAsync(HttpContext context, SpikeJob job, object? outcome)
    {
        // A partial reload of an operator-only prop is refused, not silently emptied.
        if (RequestsPartial(context.Request, "Job", "audit") && !await AllowedAsync(context, _operate))
        {
            context.Response.StatusCode = StatusCodes.Status403Forbidden;
            return;
        }

        await RenderAsync(context, "Job", new InertiaProps
        {
            ["base"] = Base(context),
            ["job"] = Element(job),
            ["canOperate"] = await AllowedAsync(context, _operate),
            ["outcome"] = outcome is null ? null : Element(outcome),
            ["audit"] = Inertia.Net.Inertia.Optional(() => new[] { $"retried {job.Retries} time(s)" }),
        });
    }

    private static Task RenderAsync(HttpContext context, string component, InertiaProps props) =>
        Inertia.Net.Inertia.Render(component, props).ExecuteAsync(context);

    // The adapter serializes props with the host's HttpJsonOptions (naming policy, converters). A JsonElement is written as is,
    // so the client's prop names do not depend on how the host configured JSON.
    private static JsonElement Element(object value) => JsonSerializer.SerializeToElement(value, Json);

    /// <summary>The mount's root view: our head (token, title, assets) around the adapter's page script and root element.</summary>
    public ValueTask RenderAsync(InertiaRootViewContext context)
    {
        var http = context.HttpContext;
        http.Response.Headers.ContentSecurityPolicy = Csp;
        var html = HtmlEncoder.Default;
        var assets = Base(http) + "/";
        var builder = new StringBuilder(context.PageJson.Length + 1024);
        builder.Append("<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\">")
            .Append("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">")
            .Append("<meta name=\"runwick-csrf\" content=\"").Append(html.Encode(IssueCsrf(http))).Append("\">")
            .Append("<title>").Append(html.Encode(_options.Title)).Append("</title>");
        foreach (var css in _options.Assets.Css)
        {
            builder.Append("<link rel=\"stylesheet\" href=\"").Append(html.Encode(assets + css)).Append("\">");
        }

        builder.Append("<script type=\"module\" src=\"").Append(html.Encode(assets + _options.Assets.Entry)).Append("\"></script>")
            .Append("</head><body>").Append(context.GetBodyHtml()).Append("</body></html>");
        return new ValueTask(http.Response.WriteAsync(builder.ToString(), Encoding.UTF8));
    }

    private async Task RetryAsync(HttpContext context, string id)
    {
        var request = context.Request;
        if (!HttpMethods.IsPost(request.Method))
        {
            context.Response.StatusCode = StatusCodes.Status405MethodNotAllowed;
            context.Response.Headers.Allow = "POST";
            return;
        }

        if (!await AuthorizeAsync(context, _operate))
        {
            return;
        }

        // An Inertia or Authorization header is not proof of intent; only this mount's token for this principal is.
        if (!ValidCsrf(context))
        {
            context.Response.StatusCode = StatusCodes.Status403Forbidden;
            return;
        }

        var commandId = request.Headers[CommandHeader].ToString();
        if (commandId.Length is 0 or > 64 || await ReadRevisionAsync(request) is not { } revision)
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            return;
        }

        switch (_data.Retry(id, revision, commandId))
        {
            case RetryOutcome.Done:
                // A confirmed success refreshes state with a GET. 303 so the browser never re-sends the POST.
                context.Response.StatusCode = StatusCodes.Status303SeeOther;
                context.Response.Headers.Location = Base(context) + "/jobs/" + Uri.EscapeDataString(id);
                break;
            case RetryOutcome.NotFound:
                context.Response.StatusCode = StatusCodes.Status404NotFound;
                break;
            case var outcome:
                // Conflict or unknown: answer with the page (an Inertia 200, not a 409), keeping the command id so an unknown
                // result can be resolved later instead of looking completed.
                await RenderJobAsync(context, _data.Find(id)!, new { status = outcome.ToString().ToLowerInvariant(), commandId });
                break;
        }
    }

    private static async Task GetAsync(HttpContext context, Func<Task> handler)
    {
        if (HttpMethods.IsGet(context.Request.Method) || HttpMethods.IsHead(context.Request.Method))
        {
            await handler();
            return;
        }

        context.Response.StatusCode = StatusCodes.Status405MethodNotAllowed;
        context.Response.Headers.Allow = "GET, HEAD";
    }

    private static async Task<bool> AuthorizeAsync(HttpContext context, AuthorizationPolicy policy)
    {
        var evaluator = context.RequestServices.GetRequiredService<IPolicyEvaluator>();
        var authentication = await evaluator.AuthenticateAsync(policy, context);
        var authorization = await evaluator.AuthorizeAsync(policy, authentication, context, resource: null);
        if (authorization.Succeeded)
        {
            return true;
        }

        if (policy.AuthenticationSchemes.Count == 0)
        {
            await (authorization.Challenged ? context.ChallengeAsync() : context.ForbidAsync());
        }

        foreach (var scheme in policy.AuthenticationSchemes)
        {
            await (authorization.Challenged ? context.ChallengeAsync(scheme) : context.ForbidAsync(scheme));
        }

        return false;
    }

    private static async Task<bool> AllowedAsync(HttpContext context, AuthorizationPolicy policy) =>
        (await context.RequestServices.GetRequiredService<IAuthorizationService>().AuthorizeAsync(context.User, policy)).Succeeded;

    private static async Task<long?> ReadRevisionAsync(HttpRequest request)
    {
        if (request.ContentLength is > MaxBodyBytes || !request.HasJsonContentType())
        {
            return null;
        }

        try
        {
            using var body = await JsonDocument.ParseAsync(request.Body, default, request.HttpContext.RequestAborted);
            return body.RootElement.TryGetProperty("revision", out var revision) && revision.TryGetInt64(out var value) ? value : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private async Task ServeAssetAsync(HttpContext context, string file)
    {
        var response = context.Response;
        if (!_options.Assets.TryGetAsset("assets/" + file, out var bytes))
        {
            response.StatusCode = StatusCodes.Status404NotFound;
            return;
        }

        response.Headers.CacheControl = "public, max-age=31536000, immutable"; // hashed names: a new build has new names
        response.ContentType = Path.GetExtension(file) switch
        {
            ".js" => "text/javascript; charset=utf-8",
            ".css" => "text/css; charset=utf-8",
            ".svg" => "image/svg+xml",
            ".woff2" => "font/woff2",
            _ => "application/octet-stream",
        };
        response.ContentLength = bytes.Length;
        if (!HttpMethods.IsHead(context.Request.Method))
        {
            await response.Body.WriteAsync(bytes, context.RequestAborted);
        }
    }

    private string IssueCsrf(HttpContext context)
    {
        var secret = context.Request.Cookies[_cookie];
        if (secret is not { Length: 43 })
        {
            secret = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32)).TrimEnd('=').Replace('+', '-').Replace('/', '_');
            context.Response.Cookies.Append(_cookie, secret, new CookieOptions
            {
                HttpOnly = true,
                Secure = context.Request.IsHttps,
                SameSite = SameSiteMode.Strict,
                Path = Base(context),
                IsEssential = true,
            });
        }

        return _csrf.Protect(secret + "\n" + UserName(context.User));
    }

    private bool ValidCsrf(HttpContext context)
    {
        var secret = context.Request.Cookies[_cookie];
        var token = context.Request.Headers[CsrfHeader].ToString();
        if (secret is not { Length: 43 } || token.Length == 0)
        {
            return false;
        }

        try
        {
            var expected = Encoding.UTF8.GetBytes(secret + "\n" + UserName(context.User));
            return CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(_csrf.Unprotect(token)), expected);
        }
        catch (CryptographicException)
        {
            return false;
        }
    }

    // The adapter turns other-origin redirects into a full visit; a same-origin login or access-denied page outside the mount
    // also cannot be followed by XHR, so the client visits it in full too.
    private static Task LeaveMountInFull(HttpContext context)
    {
        var response = context.Response;
        if (response.StatusCode is 301 or 302 or 303 or 307 or 308 && response.Headers.Location.ToString() is { Length: > 0 } location
            && !location.StartsWith(Base(context) + "/", StringComparison.Ordinal))
        {
            response.StatusCode = StatusCodes.Status409Conflict;
            response.Headers.Location = default;
            response.Headers["X-Inertia-Location"] = location;
        }

        return Task.CompletedTask;
    }

    private static bool RequestsPartial(HttpRequest request, string component, string prop) =>
        request.Headers["X-Inertia-Partial-Component"] == component
        && request.Headers["X-Inertia-Partial-Data"].ToString().Split(',', StringSplitOptions.TrimEntries).Contains(prop);

    private static string[] Segments(HttpContext context) =>
        (context.Request.Path.Value ?? "").Split('/', StringSplitOptions.RemoveEmptyEntries);

    private static bool IsInertia(HttpRequest request) => !string.IsNullOrEmpty(request.Headers["X-Inertia"]);

    // Inside the mount's branch, PathBase is the host's PathBase (including a trusted proxy prefix) plus the mount path.
    private static string Base(HttpContext context) => context.Request.PathBase.ToUriComponent();

    private static string UserName(ClaimsPrincipal user) =>
        user.Identity?.Name ?? user.FindFirst(ClaimTypes.NameIdentifier)?.Value ?? "";
}
