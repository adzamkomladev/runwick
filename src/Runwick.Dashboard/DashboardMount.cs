using System.Globalization;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using Inertia.Net;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Runwick.AspNetCore;
using Runwick.Management;

namespace Runwick.Dashboard;

/// <summary>
/// One dashboard mount on an Inertia.Net 1.7.0 named mount (21 §3). The adapter owns the protocol (page JSON, asset-version 409,
/// partial reloads, Vary, state cookies). This class keeps what the adapter does not do per mount (sp-023 gaps): authentication
/// and grants before the version check, request and rate limits, per-mount antiforgery, embedded assets, response headers,
/// redirects that leave the mount, denied partial reloads, and props written with Runwick's own JSON options. It is also the
/// mount's keyed root view.
/// </summary>
internal sealed class DashboardMount : IInertiaRootView
{
    internal const string Csp = "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; font-src 'self'; "
        + "connect-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'";

    internal const string CsrfHeader = "X-Runwick-Csrf";
    internal const string CommandHeader = "X-Runwick-Command-Id";
    const string CallerKey = "runwick.dashboard.caller";
    internal static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    readonly DashboardOptions _options;
    readonly IServiceProvider _services;
    readonly IDataProtector _csrf;
    readonly string _cookie;

    public DashboardMount(string mountId, DashboardOptions options, IServiceProvider services)
    {
        MountId = mountId;
        _options = options;
        _services = services;
        // The Runwick application is part of the purpose: two apps sharing a key ring and a mount id still refuse each other's tokens.
        _csrf = services.GetRequiredService<IDataProtectionProvider>().CreateProtector("Runwick.Dashboard.Csrf.v1",
            services.GetService<global::Runwick.IRunwickJobRegistry>()?.Application ?? "", mountId);
        _cookie = $"rwk.{mountId}.csrf";
    }

    public string MountId { get; }

    public string Title => _options.Title;

    public DashboardAssets Assets => _options.Assets;

    RunwickManagementBuilder? Management => _services.GetService<RunwickManagementBuilder>();

    IReadOnlyList<string> Schemes => _options.Schemes.Count > 0 ? _options.Schemes : Management?.Schemes ?? [];

    Func<ClaimsPrincipal, IReadOnlyList<ManagementGrant>>? Grants => _options.Grants ?? Management?.Grants;

    Func<ClaimsPrincipal, ManagementActor>? ActorMap => _options.ActorMap ?? Management?.ActorMap;

    /// <summary>The Inertia.Net mount name: its state cookies are <c>.Inertia.State.runwick-{id}</c>.</summary>
    public static string InertiaName(string mountId) => "runwick-" + mountId;

    /// <summary>Startup refusals of 21 §2.2 that need the management registration.</summary>
    public void Validate()
    {
        if (Management is null)
        {
            throw new InvalidOperationException("Call services.AddRunwickManagement(...) before MapRunwickDashboard.");
        }

        if (Schemes.Count == 0 || Grants is null)
        {
            throw new InvalidOperationException(
                $"Dashboard mount '{MountId}' has no authentication schemes or grant map (set them on the mount or in AddRunwickManagement); there is no anonymous mode.");
        }
    }

    /// <summary>The mount's branch: the gate, then the adapter's middleware for this mount only, then the router.</summary>
    public void Configure(IApplicationBuilder branch, Func<bool> ready)
    {
        branch.Use((context, next) => GateAsync(context, next, ready));
        branch.UseInertia(InertiaName(MountId));
        branch.Run(context => DashboardRouter.RouteAsync(new DashboardRequest(this, context, (ManagementCaller)context.Items[CallerKey]!)));
    }

    // ------------------------------------------------------------------ gate (21 §3.1–3.4)

    async Task GateAsync(HttpContext context, RequestDelegate next, Func<bool> ready)
    {
        var response = context.Response;
        response.Headers.XContentTypeOptions = "nosniff";
        response.Headers["Referrer-Policy"] = "same-origin";
        if (DashboardRequest.Segments(context) is ["assets", var file])
        {
            await ServeAssetAsync(context, file);
            return;
        }

        response.Headers.CacheControl = "no-store, private";
        if (!ready())
        {
            await WriteErrorAsync(context, 503, "starting", "The dashboard is starting.");
            return;
        }

        if (!string.IsNullOrEmpty(context.Request.Headers["X-Inertia"]))
        {
            response.OnStarting(static state => LeaveMountInFull((HttpContext)state), context);
        }

        var limiter = context.RequestServices.GetRequiredService<ManagementLimiter>();
        if (!await limiter.EnterAsync(context.RequestAborted))
        {
            await WriteErrorAsync(context, 503, "busy", "The dashboard is busy.");
            return;
        }

        try
        {
            ClaimsPrincipal? principal = null;
            string? used = null;
            foreach (var scheme in Schemes)
            {
                var result = await context.AuthenticateAsync(scheme);
                if (result.Succeeded && result.Principal.Identity?.IsAuthenticated == true)
                {
                    (principal, used) = (result.Principal, scheme);
                    break;
                }
            }

            if (principal is null)
            {
                foreach (var scheme in Schemes)
                {
                    await context.ChallengeAsync(scheme);
                }

                return;
            }

            // Actor and grants come from the principal only; nothing in the request supplies them.
            context.User = principal;
            var actor = ActorMap?.Invoke(principal) ?? RunwickManagementEndpointRouteBuilderExtensions.DefaultActor(principal, used!);
            var caller = new ManagementCaller(actor, Grants!(principal));
            if (caller.Grants.Count == 0)
            {
                foreach (var scheme in Schemes)
                {
                    await context.ForbidAsync(scheme);
                }

                return;
            }

            var post = HttpMethods.IsPost(context.Request.Method);
            var kind = !post ? ManagementLimiter.Kind.Read
                : context.Request.Path.Value!.EndsWith(":reveal", StringComparison.Ordinal) ? ManagementLimiter.Kind.Reveal
                : ManagementLimiter.Kind.Mutation;
            if (limiter.TryTake(actor.Scheme + "\n" + actor.Subject, kind) is { } retryAfter)
            {
                response.Headers.RetryAfter = retryAfter.ToString(CultureInfo.InvariantCulture);
                await WriteErrorAsync(context, 429, "rate_limited", "Too many requests.");
                return;
            }

            context.Items[CallerKey] = caller;
            await next(context);
        }
        finally
        {
            limiter.Exit();
        }
    }

    internal static Task WriteErrorAsync(HttpContext context, int status, string code, string message, object? extra = null)
    {
        context.Response.StatusCode = status;
        return context.Response.WriteAsJsonAsync(extra ?? new { code, message }, Json);
    }

    async Task ServeAssetAsync(HttpContext context, string file)
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

    // The adapter turns other-origin redirects into a full visit; a same-origin login or access-denied page outside the mount
    // also cannot be followed by XHR, so the client visits it in full too.
    static Task LeaveMountInFull(HttpContext context)
    {
        var response = context.Response;
        if (response.StatusCode is 301 or 302 or 303 or 307 or 308 && response.Headers.Location.ToString() is { Length: > 0 } location
            && !location.StartsWith(DashboardRequest.BaseOf(context) + "/", StringComparison.Ordinal) && location != DashboardRequest.BaseOf(context))
        {
            var r = context.Request;
            response.StatusCode = StatusCodes.Status409Conflict;
            response.Headers.Location = default;
            response.Headers["X-Inertia-Location"] = location.StartsWith('/') && !location.StartsWith("//", StringComparison.Ordinal)
                ? $"{r.Scheme}://{r.Host}{location}"
                : location;
        }

        return Task.CompletedTask;
    }

    // ------------------------------------------------------------------ root view and antiforgery (21 §3.1, §3.3)

    /// <summary>The mount's root view: head (token, title, assets, CSP) around the adapter's page script and root element.</summary>
    public ValueTask RenderAsync(InertiaRootViewContext context)
    {
        var http = context.HttpContext;
        http.Response.Headers.ContentSecurityPolicy = Csp;
        var html = HtmlEncoder.Default;
        var assets = DashboardRequest.BaseOf(http) + "/";
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

    string IssueCsrf(HttpContext context)
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
                Path = DashboardRequest.BaseOf(context),
                IsEssential = true,
            });
        }

        return _csrf.Protect(secret + "\n" + Binding(context));
    }

    /// <summary>This mount's token, for this cookie and this actor (scheme, issuer, subject). No other header replaces it.</summary>
    internal bool ValidCsrf(HttpContext context)
    {
        var secret = context.Request.Cookies[_cookie];
        var token = context.Request.Headers[CsrfHeader].ToString();
        if (secret is not { Length: 43 } || token.Length == 0)
        {
            return false;
        }

        try
        {
            var expected = Encoding.UTF8.GetBytes(secret + "\n" + Binding(context));
            return CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(_csrf.Unprotect(token)), expected);
        }
        catch (CryptographicException)
        {
            return false;
        }
    }

    static string Binding(HttpContext context)
    {
        var caller = (ManagementCaller)context.Items[CallerKey]!;
        var issuer = context.User.FindFirst(ClaimTypes.NameIdentifier)?.Issuer ?? context.User.FindFirst(ClaimTypes.Name)?.Issuer ?? "";
        return caller.Actor.Scheme + "\n" + issuer + "\n" + caller.Actor.Subject;
    }
}
