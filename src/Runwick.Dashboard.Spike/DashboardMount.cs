using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Authorization.Policy;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace Runwick.Dashboard.Spike;

/// <summary>
/// One mount's transport: the Runwick-owned subset of the Inertia v3 protocol the dashboard uses (initial HTML, page JSON,
/// asset-version 409, partial reloads, 303 and external-location redirects), with its own authorization, antiforgery and
/// assets. It reads no host Inertia, antiforgery or routing configuration, so two mounts, or a host using Inertia itself,
/// cannot leak into each other.
/// </summary>
internal sealed class DashboardMount
{
    internal const string Csp = "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; font-src 'self'; "
        + "connect-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'";

    internal const string CsrfHeader = "X-Runwick-Csrf";
    internal const string CommandHeader = "X-Runwick-Command-Id";
    private const int MaxBodyBytes = 16 * 1024;
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web); // default encoder escapes < > & ' + and U+2028
    private readonly DashboardMountOptions _options;
    private readonly IDashboardSpikeData _data;
    private readonly AuthorizationPolicy _view;
    private readonly AuthorizationPolicy _operate;
    private readonly IDataProtector _csrf;
    private readonly string _cookie;

    public DashboardMount(DashboardMountOptions options, IDataProtectionProvider dataProtection)
    {
        _options = options;
        _data = options.Data!;
        _view = options.ViewPolicy!;
        _operate = options.OperatePolicy!;
        _csrf = dataProtection.CreateProtector("Runwick.Dashboard.Csrf.v1", options.MountId);
        _cookie = $"rwk.{options.MountId}.csrf";
    }

    public async Task InvokeAsync(HttpContext context)
    {
        var request = context.Request;
        var response = context.Response;
        var inertia = IsInertia(request);
        response.Headers.XContentTypeOptions = "nosniff";
        response.Headers["Referrer-Policy"] = "same-origin";
        if (inertia)
        {
            response.OnStarting(static state => FinishInertiaRedirect((HttpContext)state), context);
        }

        var segments = (request.Path.Value ?? "").Trim('/').Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments is ["assets", var file])
        {
            await ServeAssetAsync(context, file);
            return;
        }

        response.Headers.CacheControl = "no-store, private";
        response.Headers.Vary = "X-Inertia";
        switch (segments)
        {
            case []:
                await GetAsync(context, _view, () => RenderAsync(context, "Overview", OverviewProps(context)));
                break;
            case ["jobs", var id]:
                await GetAsync(context, _view, async () =>
                {
                    if (_data.Find(id) is { } job)
                    {
                        await RenderAsync(context, "Job", await JobPropsAsync(context, job, outcome: null));
                    }
                    else
                    {
                        await NotFoundAsync(context);
                    }
                });
                break;
            case ["jobs", var id, "payload"]:
                await GetAsync(context, _operate, () => _data.Payload(id) is { } payload
                    ? response.WriteAsJsonAsync(new { id, payload }, Json)
                    : NotFoundAsync(context));
                break;
            case ["jobs", var id, "retry"]:
                await RetryAsync(context, id);
                break;
            default:
                await NotFoundAsync(context);
                break;
        }
    }

    // Authentication and authorization run before anything else: version negotiation, prop selection, the handler.
    private static async Task<bool> AuthorizeAsync(HttpContext context, AuthorizationPolicy policy)
    {
        var services = context.RequestServices;
        var evaluator = services.GetRequiredService<IPolicyEvaluator>();
        var authentication = await evaluator.AuthenticateAsync(policy, context);
        var authorization = await evaluator.AuthorizeAsync(policy, authentication, context, resource: null);
        if (authorization.Succeeded)
        {
            return true;
        }

        if (authorization.Challenged)
        {
            await ForEachScheme(policy, scheme => context.ChallengeAsync(scheme));
        }
        else
        {
            await ForEachScheme(policy, scheme => context.ForbidAsync(scheme));
        }

        return false;
    }

    private static async Task ForEachScheme(AuthorizationPolicy policy, Func<string?, Task> action)
    {
        if (policy.AuthenticationSchemes.Count == 0)
        {
            await action(null);
            return;
        }

        foreach (var scheme in policy.AuthenticationSchemes)
        {
            await action(scheme);
        }
    }

    private static async Task GetAsync(HttpContext context, AuthorizationPolicy policy, Func<Task> handler)
    {
        if (!HttpMethods.IsGet(context.Request.Method) && !HttpMethods.IsHead(context.Request.Method))
        {
            context.Response.StatusCode = StatusCodes.Status405MethodNotAllowed;
            context.Response.Headers.Allow = "GET, HEAD";
            return;
        }

        if (await AuthorizeAsync(context, policy))
        {
            await handler();
        }
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
                context.Response.Headers.Location = JobUrl(context, id);
                break;
            case RetryOutcome.NotFound:
                await NotFoundAsync(context);
                break;
            case var outcome:
                // Conflict or unknown: answer with the page (an Inertia 200, not a 409), keeping the command id so an unknown
                // result can be resolved later instead of looking completed.
                var job = _data.Find(id)!;
                await RenderAsync(context, "Job", await JobPropsAsync(context, job, new { status = outcome.ToString().ToLowerInvariant(), commandId }));
                break;
        }
    }

    private static async Task<long?> ReadRevisionAsync(HttpRequest request)
    {
        if (request.ContentLength is > MaxBodyBytes || request.HasJsonContentType() is false)
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

    private Dictionary<string, PageProp> OverviewProps(HttpContext context) => new(StringComparer.Ordinal)
    {
        ["base"] = PageProp.Value(Base(context)),
        ["mount"] = PageProp.Value(_options.Title),
        ["user"] = PageProp.Value(UserName(context.User)),
        ["jobs"] = PageProp.Lazy(() => _data.Jobs()),
        ["stats"] = PageProp.Optional(() => new { total = _data.Jobs().Count }),
    };

    private async Task<Dictionary<string, PageProp>> JobPropsAsync(HttpContext context, SpikeJob job, object? outcome) => new(StringComparer.Ordinal)
    {
        ["base"] = PageProp.Value(Base(context)),
        ["job"] = PageProp.Value(job),
        ["canOperate"] = PageProp.Value(await AllowedAsync(context, _operate)),
        ["outcome"] = PageProp.Value(outcome),
        ["audit"] = PageProp.Optional(() => new[] { $"retried {job.Retries} time(s)" }, _operate),
    };

    private async Task RenderAsync(HttpContext context, string component, Dictionary<string, PageProp> props)
    {
        var request = context.Request;
        var response = context.Response;
        var version = _options.Assets.Version;
        var inertia = IsInertia(request);
        if (inertia && HttpMethods.IsGet(request.Method) && request.Headers["X-Inertia-Version"] != version)
        {
            // Stale client build: reload in full. Only GETs are checked, so a mutation is never replayed by this.
            response.StatusCode = StatusCodes.Status409Conflict;
            response.Headers["X-Inertia-Location"] = Url(context);
            return;
        }

        var partial = inertia && request.Headers["X-Inertia-Partial-Component"] == component;
        var only = partial ? Split(request.Headers["X-Inertia-Partial-Data"]) : [];
        var except = partial ? Split(request.Headers["X-Inertia-Partial-Except"]) : [];
        var values = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var (key, prop) in props)
        {
            var wanted = partial
                ? (only.Count == 0 || only.Contains(key)) && !except.Contains(key)
                : !prop.IsOptional;
            if (!wanted)
            {
                continue;
            }

            if (prop.Policy is { } policy && !await AllowedAsync(context, policy))
            {
                response.StatusCode = StatusCodes.Status403Forbidden; // a denied partial is refused, not silently emptied
                return;
            }

            values[key] = prop.Resolve();
        }

        var page = new Dictionary<string, object?>
        {
            ["component"] = component,
            ["props"] = values,
            ["url"] = Url(context),
            ["version"] = version,
            ["encryptHistory"] = true, // supplementary: props are already filtered and carry no payload
            ["clearHistory"] = false,
        };

        if (inertia)
        {
            response.Headers["X-Inertia"] = "true";
            await response.WriteAsJsonAsync(page, Json, contentType: "application/json; charset=utf-8");
            return;
        }

        response.ContentType = "text/html; charset=utf-8";
        response.Headers.ContentSecurityPolicy = Csp;
        await response.WriteAsync(Html(context, JsonSerializer.Serialize(page, Json)), Encoding.UTF8);
    }

    private static async Task<bool> AllowedAsync(HttpContext context, AuthorizationPolicy policy) =>
        (await context.RequestServices.GetRequiredService<IAuthorizationService>().AuthorizeAsync(context.User, policy)).Succeeded;

    private string Html(HttpContext context, string pageJson)
    {
        var html = HtmlEncoder.Default;
        var assets = Base(context) + "/";
        var builder = new StringBuilder(pageJson.Length + 1024);
        builder.Append("<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\">")
            .Append("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">")
            .Append("<meta name=\"runwick-csrf\" content=\"").Append(html.Encode(IssueCsrf(context))).Append("\">")
            .Append("<title>").Append(html.Encode(_options.Title)).Append("</title>");
        foreach (var css in _options.Assets.Css)
        {
            builder.Append("<link rel=\"stylesheet\" href=\"").Append(html.Encode(assets + css)).Append("\">");
        }

        builder.Append("<script type=\"module\" src=\"").Append(html.Encode(assets + _options.Assets.Entry)).Append("\"></script>")
            .Append("</head><body><script data-page=\"app\" type=\"application/json\">").Append(pageJson)
            .Append("</script><div id=\"app\"></div></body></html>");
        return builder.ToString();
    }

    private async Task ServeAssetAsync(HttpContext context, string file)
    {
        if (!_options.Assets.TryGetAsset("assets/" + file, out var bytes))
        {
            await NotFoundAsync(context);
            return;
        }

        var response = context.Response;
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
            secret = Base64Url(RandomNumberGenerator.GetBytes(32));
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

    // A redirect out of the mount (a login page, an identity provider) cannot be followed by XHR: the client visits it in full.
    private static Task FinishInertiaRedirect(HttpContext context)
    {
        var response = context.Response;
        if (response.StatusCode is 301 or 302 or 303 or 307 or 308 && response.Headers.Location.ToString() is { Length: > 0 } location
            && !location.StartsWith(Base(context) + "/", StringComparison.Ordinal))
        {
            response.StatusCode = StatusCodes.Status409Conflict;
            response.Headers.Location = default;
            response.Headers["X-Inertia-Location"] = location;
        }
        else if (response.StatusCode == StatusCodes.Status302Found && !HttpMethods.IsGet(context.Request.Method))
        {
            response.StatusCode = StatusCodes.Status303SeeOther;
        }

        return Task.CompletedTask;
    }

    private static Task NotFoundAsync(HttpContext context)
    {
        context.Response.StatusCode = StatusCodes.Status404NotFound;
        return Task.CompletedTask;
    }

    private static bool IsInertia(HttpRequest request) => !string.IsNullOrEmpty(request.Headers["X-Inertia"]);

    // Inside the mount's branch, PathBase is the host's PathBase (including a trusted proxy prefix) plus the mount path.
    private static string Base(HttpContext context) => context.Request.PathBase.ToUriComponent();

    private static string Url(HttpContext context) =>
        context.Request.PathBase.Add(context.Request.Path).ToUriComponent() + context.Request.QueryString.ToUriComponent();

    private static string JobUrl(HttpContext context, string id) => Base(context) + "/jobs/" + Uri.EscapeDataString(id);

    private static string UserName(ClaimsPrincipal user) =>
        user.Identity?.Name ?? user.FindFirst(ClaimTypes.NameIdentifier)?.Value ?? "";

    private static HashSet<string> Split(string? header) =>
        [.. (header ?? "").Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)];

    private static string Base64Url(byte[] bytes) => Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    private sealed record PageProp(Func<object?> Resolve, bool IsOptional, AuthorizationPolicy? Policy)
    {
        public static PageProp Value(object? value) => new(() => value, false, null);

        public static PageProp Lazy(Func<object?> loader) => new(loader, false, null);

        public static PageProp Optional(Func<object?> loader, AuthorizationPolicy? policy = null) => new(loader, true, policy);
    }
}
